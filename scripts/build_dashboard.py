#!/usr/bin/env python3
"""
Museokauppa Dashboard — cloud build script.

Fetches bookings from the Bokún REST API (Museokortti channel, scoped by the
server-side API key), aggregates KPIs, persists a per-day history file, and
renders two HTML pages:

    docs/latest.html  — today's snapshot (yesterday's sales; Mon = Fri+Sat+Sun)
    docs/trends.html  — rolling 30d daily chart + 12m monthly table + top products
    docs/YYYY-MM-DD.html — dated archive copy of the day's dashboard
    docs/index.html   — recent-days index
    docs/history.json — per-day aggregates {"2026-04-21": {orders, gross_eur, ...}}

Runs inside GitHub Actions. Stdlib only — no pip install needed.

Environment variables (set as GitHub repo secrets):
    BOKUN_ACCESS_KEY   — Bokún REST access key (Museokortti channel)
    BOKUN_SECRET_KEY   — Bokún REST secret key
    BOKUN_BASE_URL     — optional, defaults to https://api.bokun.io

CLI:
    python scripts/build_dashboard.py                 # daily run (normal cron)
    python scripts/build_dashboard.py --backfill FROM # iterate day-by-day from
                                                      # FROM (YYYY-MM-DD) to yesterday
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import html
import json
import os
import sys
import time
import urllib.error
import urllib.request
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parent.parent
DOCS = REPO_ROOT / "docs"
DOCS.mkdir(parents=True, exist_ok=True)
HISTORY_FILE = DOCS / "history.json"
# PII-free per-day aggregates produced locally by scripts/trip_aggregate.py
# from the Trip admin-UI "Dashboard-packages" CSV export.
TRIP_FILE = REPO_ROOT / "data" / "trip-aggregates.json"


def helsinki_offset(on: date) -> timedelta:
    """
    Europe/Helsinki offset without depending on tzdata. DST: last Sunday of
    March to last Sunday of October, offset = UTC+3 (EEST); otherwise UTC+2.
    """
    y = on.year
    d = date(y, 3, 31)
    while d.weekday() != 6:
        d -= timedelta(days=1)
    dst_start = d
    d = date(y, 10, 31)
    while d.weekday() != 6:
        d -= timedelta(days=1)
    dst_end = d
    if dst_start <= on < dst_end:
        return timedelta(hours=3)
    return timedelta(hours=2)


def today_helsinki() -> date:
    off = helsinki_offset(datetime.now(timezone.utc).date())
    return (datetime.now(timezone.utc) + off).date()


# ---------------------------------------------------------------------------
# Bokún HMAC-SHA1 client
# ---------------------------------------------------------------------------

BASE_URL = os.environ.get("BOKUN_BASE_URL", "https://api.bokun.io").rstrip("/")
ACCESS_KEY = os.environ.get("BOKUN_ACCESS_KEY", "")
SECRET_KEY = os.environ.get("BOKUN_SECRET_KEY", "").encode("utf-8")


class BokunError(Exception):
    pass


def _sign_headers(method: str, path: str) -> dict[str, str]:
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    raw = (date_str + ACCESS_KEY + method + path).encode("utf-8")
    signature = base64.b64encode(
        hmac.new(SECRET_KEY, raw, hashlib.sha1).digest()
    ).decode("ascii")
    return {
        "X-Bokun-AccessKey": ACCESS_KEY,
        "X-Bokun-Date": date_str,
        "X-Bokun-Signature": signature,
        "Content-Type": "application/json;charset=UTF-8",
        "Accept": "application/json",
    }


def bokun_call(method: str, path: str, body: dict | None = None) -> Any:
    if not ACCESS_KEY or not SECRET_KEY:
        raise BokunError("BOKUN_ACCESS_KEY / BOKUN_SECRET_KEY not set in env")
    url = BASE_URL + path
    data = json.dumps(body).encode("utf-8") if body is not None else None
    last_err: Exception | None = None
    for attempt in range(3):
        req = urllib.request.Request(
            url, data=data, method=method, headers=_sign_headers(method, path)
        )
        try:
            with urllib.request.urlopen(req, timeout=45) as resp:
                raw = resp.read().decode("utf-8")
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as e:
            body_txt = e.read().decode("utf-8", errors="replace")
            if 500 <= e.code < 600 and attempt < 2:
                last_err = BokunError(f"HTTP {e.code} transient: {body_txt[:300]}")
                time.sleep(2 * (attempt + 1))
                continue
            raise BokunError(f"HTTP {e.code}: {body_txt[:500]}") from e
        except urllib.error.URLError as e:
            last_err = BokunError(f"URLError: {e}")
            if attempt < 2:
                time.sleep(2 * (attempt + 1))
                continue
            raise last_err from e
    assert last_err is not None
    raise last_err


_FIRST_DUMP_DONE = False


def search_product_bookings(day_from: date, day_to: date) -> list[dict]:
    """
    Pull all product-bookings whose *creation* date falls in the given range.
    Creation date = when the order was placed (== "sales day" in retail terms).
    Dumps the first record's structure to stdout once per process, to help
    diagnose field-name drift in the Bokún response.
    """
    global _FIRST_DUMP_DONE
    path = "/booking.json/product-booking-search"
    out: list[dict] = []
    page = 1
    while True:
        body = {
            "creationDateRange": {
                "from": f"{day_from.isoformat()}T00:00:00",
                "to": f"{day_to.isoformat()}T23:59:59",
            },
            "bookingRole": "SELLER",
            "pageSize": 200,
            "page": page,
        }
        resp = bokun_call("POST", path, body)
        if isinstance(resp, list):
            batch = resp
            total = None
        elif isinstance(resp, dict):
            batch = resp.get("results") or resp.get("items") or resp.get("hits") or []
            total = resp.get("totalHits") or resp.get("total")
        else:
            batch = []
            total = None
        out.extend(batch)
        if not batch or len(batch) < 200:
            break
        if total is not None and len(out) >= total:
            break
        page += 1
        if page > 50:
            break

    if out and not _FIRST_DUMP_DONE:
        _FIRST_DUMP_DONE = True
        sample = out[0]
        print("[museokauppa] sample booking top-level keys: "
              + ", ".join(sorted(sample.keys()))[:500])
        try:
            dumped = json.dumps(sample, indent=2, default=str)
            if len(dumped) > 2000:
                dumped = dumped[:2000] + "\n... (truncated)"
            print("[museokauppa] sample booking (truncated):\n" + dumped)
        except Exception:
            pass
    return out


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------


def _deep_get(obj: dict, *paths: str) -> Any:
    """
    Try a series of dotted paths against a dict, return the first non-empty
    value. Example: _deep_get(line, "product.title", "activity.title", "title")
    """
    for path in paths:
        cur: Any = obj
        ok = True
        for part in path.split("."):
            if isinstance(cur, dict) and part in cur:
                cur = cur[part]
            else:
                ok = False
                break
        if ok and cur:
            return cur
    return None


def _product_title(line: dict) -> str:
    """
    Pull a human-readable product title out of a Bokún booking record. Tries
    a generous set of field paths because Bokún's schema varies by booking
    type (activity / accommodation / car / package / custom).
    """
    title = _deep_get(
        line,
        "product.title",
        "product.name",
        "productTitle",
        "productName",
        "title",
        "activity.title",
        "activity.name",
        "accommodation.title",
        "accommodation.name",
        "experience.title",
        "experience.name",
        "offering.title",
        "offering.name",
        "productBooking.product.title",
        "productBooking.productTitle",
    )
    if title:
        return str(title).strip()
    # Last resort: any id-ish field, so the row isn't just "Unknown product".
    pid = _deep_get(line, "productId", "product.id", "productCode",
                    "productExternalId", "productConfirmationCode")
    if pid:
        return f"[product #{pid}]"
    return "Unknown product"


@dataclass
class Totals:
    orders: int = 0
    gross_eur: float = 0.0
    cancellations: int = 0
    by_product: dict[str, dict[str, float]] = field(
        default_factory=lambda: defaultdict(lambda: {"orders": 0, "eur": 0.0})
    )
    by_country: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    record_count: int = 0

    def add_line(self, line: dict) -> None:
        self.record_count += 1
        status = (line.get("status") or "").upper()
        if "CANCEL" in status:
            self.cancellations += 1
            return
        price = _extract_eur(line)
        self.gross_eur += price
        title = _product_title(line)
        self.by_product[title]["orders"] += 1
        self.by_product[title]["eur"] += price
        country = _deep_get(line, "customerCountry", "country",
                            "customer.country", "customer.address.country",
                            "contact.country")
        if country:
            self.by_country[str(country).upper()[:2]] += 1


def _extract_eur(line: dict) -> float:
    for key in ("totalPrice", "totalAmount", "amount", "price", "total"):
        v = line.get(key)
        if v is None:
            continue
        if isinstance(v, (int, float)):
            return float(v)
        if isinstance(v, dict):
            amt = v.get("amount") if isinstance(v.get("amount"), (int, float)) else None
            if amt is not None:
                return float(amt)
    # Some Bokún responses nest totals under "pricing" / "totals".
    nested = _deep_get(line, "pricing.total", "pricing.amount",
                       "totals.total", "totals.gross")
    if isinstance(nested, (int, float)):
        return float(nested)
    if isinstance(nested, dict) and isinstance(nested.get("amount"), (int, float)):
        return float(nested["amount"])
    return 0.0


def uniq_orders(lines: Iterable[dict]) -> int:
    seen = set()
    for ln in lines:
        code = (
            ln.get("parentBookingConfirmationCode")
            or (ln.get("parentBooking") or {}).get("confirmationCode")
            or ln.get("confirmationCode")
        )
        if code and (ln.get("status") or "").upper().find("CANCEL") < 0:
            seen.add(code)
    return len(seen)


def aggregate(lines: list[dict]) -> Totals:
    t = Totals()
    for ln in lines:
        t.add_line(ln)
    t.orders = uniq_orders(lines)
    return t


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------


def target_range(today: date) -> tuple[date, date, str]:
    """Return (from, to, label) for the sales window to display on latest.html."""
    wd = today.weekday()  # Mon=0 ... Sun=6
    if wd == 0:
        fri = today - timedelta(days=3)
        sun = today - timedelta(days=1)
        return fri, sun, f"Weekend {fri.isoformat()} → {sun.isoformat()}"
    y = today - timedelta(days=1)
    return y, y, y.strftime("%a %Y-%m-%d")


def iter_days(d_from: date, d_to: date) -> Iterable[date]:
    d = d_from
    while d <= d_to:
        yield d
        d += timedelta(days=1)


# ---------------------------------------------------------------------------
# History persistence
# ---------------------------------------------------------------------------


def load_history() -> dict[str, Any]:
    if HISTORY_FILE.exists():
        try:
            with HISTORY_FILE.open(encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict) and "days" in data:
                return data
        except Exception as e:
            print(f"[museokauppa] WARN: history.json unreadable, rebuilding: {e}",
                  file=sys.stderr)
    return {"updated": None, "days": {}}


def save_history(hist: dict[str, Any]) -> None:
    hist["updated"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    HISTORY_FILE.write_text(
        json.dumps(hist, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )


def load_trip_aggregates() -> dict[str, Any]:
    """Load the PII-free Trip aggregates JSON if present; empty dict if not."""
    if TRIP_FILE.exists():
        try:
            with TRIP_FILE.open(encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict) and "days" in data:
                return data
        except Exception as e:
            print(f"[museokauppa] WARN: trip-aggregates.json unreadable: {e}",
                  file=sys.stderr)
    return {"updated": None, "days": {}}


def trip_range_totals(trip: dict[str, Any],
                      d_from: date, d_to: date) -> dict[str, Any]:
    """Sum per-day Trip aggregates across the inclusive date range."""
    days = trip.get("days") or {}
    out = {
        "orders_confirmed": 0,
        "orders_pending": 0,
        "orders_cancelled": 0,
        "gross_eur_confirmed": 0.0,
        "gross_eur_pending": 0.0,
        "by_package": defaultdict(lambda: {"orders": 0, "eur": 0.0}),
        "by_country": defaultdict(int),
        "days_with_data": 0,
    }
    d = d_from
    while d <= d_to:
        rec = days.get(d.isoformat())
        if rec:
            out["days_with_data"] += 1
            out["orders_confirmed"]    += int(rec.get("orders_confirmed") or 0)
            out["orders_pending"]      += int(rec.get("orders_pending") or 0)
            out["orders_cancelled"]    += int(rec.get("orders_cancelled") or 0)
            out["gross_eur_confirmed"] += float(rec.get("gross_eur_confirmed") or 0)
            out["gross_eur_pending"]   += float(rec.get("gross_eur_pending") or 0)
            for name, p in (rec.get("by_package") or {}).items():
                out["by_package"][name]["orders"] += int(p.get("orders") or 0)
                out["by_package"][name]["eur"]    += float(p.get("eur") or 0)
            for c, n in (rec.get("by_country") or {}).items():
                out["by_country"][c] += int(n)
        d += timedelta(days=1)
    out["by_package"] = dict(out["by_package"])
    out["by_country"] = dict(out["by_country"])
    return out


def totals_to_day_record(t: Totals) -> dict[str, Any]:
    return {
        "orders": t.orders,
        "gross_eur": round(t.gross_eur, 2),
        "cancellations": t.cancellations,
        "by_product": {
            name: {"orders": int(d["orders"]), "eur": round(d["eur"], 2)}
            for name, d in t.by_product.items()
        },
        "by_country": dict(t.by_country),
        "record_count": t.record_count,
    }


# ---------------------------------------------------------------------------
# HTML / CSS
# ---------------------------------------------------------------------------


CSS = """
:root {
  --bg: #0f172a; --panel: #1e293b; --ink: #f1f5f9; --muted: #94a3b8;
  --border: #334155; --ok: #22c55e; --warn: #f59e0b; --bad: #ef4444;
  --accent: #38bdf8; --bar: #38bdf8; --bar-dim: #334155;
}
* { box-sizing: border-box; }
body { margin:0; padding:24px; background:var(--bg); color:var(--ink);
  font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;
  line-height:1.5; }
.wrap { max-width:1100px; margin:0 auto; }
header { display:flex; justify-content:space-between; align-items:flex-end;
  border-bottom:1px solid var(--border); padding-bottom:16px; margin-bottom:24px;
  flex-wrap:wrap; gap:12px; }
h1 { margin:0 0 4px 0; font-size:22px; font-weight:600; }
.subtitle { color:var(--muted); font-size:13px; }
.meta { text-align:right; color:var(--muted); font-size:12px; }
.meta strong { color:var(--ink); font-weight:500; }
.nav { display:flex; gap:16px; margin-bottom:20px; font-size:13px; }
.nav a { color:var(--accent); text-decoration:none; }
.nav a:hover { text-decoration:underline; }
.nav a.active { color:var(--ink); font-weight:600; }
.grid-kpi { display:grid; grid-template-columns:repeat(4,1fr); gap:12px; margin-bottom:24px; }
.kpi { background:var(--panel); border:1px solid var(--border); border-radius:10px; padding:16px; }
.kpi .label { font-size:11px; text-transform:uppercase; letter-spacing:.8px; color:var(--muted); margin-bottom:8px; }
.kpi .value { font-size:26px; font-weight:600; line-height:1.1; }
.kpi .delta { font-size:12px; color:var(--muted); margin-top:6px; }
.kpi .delta.up { color:var(--ok); } .kpi .delta.down { color:var(--bad); }
section.card { background:var(--panel); border:1px solid var(--border); border-radius:10px;
  padding:18px; margin-bottom:20px; }
section.card h2 { margin:0 0 12px 0; font-size:14px; font-weight:600;
  text-transform:uppercase; letter-spacing:.6px; }
table { width:100%; border-collapse:collapse; font-size:13px; }
th,td { text-align:left; padding:8px 10px; border-bottom:1px solid var(--border); }
th { color:var(--muted); font-weight:500; font-size:11px; text-transform:uppercase; letter-spacing:.6px; }
td.num { text-align:right; font-variant-numeric:tabular-nums; }
.pill { display:inline-block; padding:3px 10px; border-radius:999px; font-size:11px;
  font-weight:600; letter-spacing:.4px; text-transform:uppercase; }
.pill.ok { background:rgba(34,197,94,.15); color:var(--ok); border:1px solid rgba(34,197,94,.35); }
.pill.warn { background:rgba(245,158,11,.15); color:var(--warn); border:1px solid rgba(245,158,11,.35); }
.pill.bad { background:rgba(239,68,68,.15); color:var(--bad); border:1px solid rgba(239,68,68,.35); }
.status-row { display:grid; grid-template-columns:110px 1fr 110px; gap:12px;
  align-items:center; padding:10px 0; border-bottom:1px solid var(--border); font-size:13px; }
.status-row:last-child { border-bottom:none; }
.src-detail { color:var(--muted); font-size:12px; }
footer { margin-top:24px; font-size:11px; color:var(--muted); text-align:center;
  border-top:1px solid var(--border); padding-top:12px; }
.chart { width:100%; height:auto; display:block; }
.chart-caption { color:var(--muted); font-size:11px; margin-top:4px; text-align:right; }
.tag { font-size:11px; color:var(--muted); }
.pos { color:var(--ok); } .neg { color:var(--bad); } .flat { color:var(--muted); }
@media (max-width:780px) { .grid-kpi { grid-template-columns:repeat(2,1fr); }
  .status-row { grid-template-columns:90px 1fr; } }
"""


def euro(x: float) -> str:
    return f"{x:,.0f}".replace(",", " ") + " €"


def _common_head(title: str) -> str:
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="robots" content="noindex, nofollow, noarchive, nosnippet">
<meta name="referrer" content="no-referrer">
<title>{html.escape(title)}</title>
<style>{CSS}</style>
</head>"""


def _nav(active: str) -> str:
    items = [("Today", "latest.html"), ("Trends", "trends.html"), ("Archive", "index.html")]
    return '<div class="nav">' + " · ".join(
        f'<a class="{"active" if name.lower()==active else ""}" href="{href}">{name}</a>'
        for name, href in items
    ) + "</div>"


# ---------------------------------------------------------------------------
# Daily view
# ---------------------------------------------------------------------------


def render_html(label: str, t: Totals, wow: Totals | None,
                generated_at: str, sales_label: str,
                trip: dict[str, Any] | None = None,
                trip_wow: dict[str, Any] | None = None,
                trip_meta: dict[str, Any] | None = None) -> str:
    aov = (t.gross_eur / t.orders) if t.orders else 0.0
    wow_html = '<div class="delta">no comparison</div>'
    wow_value_html = "—"
    if wow and wow.gross_eur > 0:
        pct = (t.gross_eur - wow.gross_eur) / wow.gross_eur * 100
        cls = "up" if pct >= 0 else "down"
        arrow = "▲" if pct >= 0 else "▼"
        wow_html = f'<div class="delta {cls}">{arrow} {pct:+.1f}% vs same day last week ({euro(wow.gross_eur)})</div>'
        wow_value_html = euro(t.gross_eur - wow.gross_eur)

    # --- Trip section (B&E packages) ------------------------------------
    trip_kpis_html = ""
    trip_top_html = ""
    trip_pipe_html = ""
    if trip is not None:
        trip_conf_orders = int(trip.get("orders_confirmed") or 0)
        trip_conf_eur = float(trip.get("gross_eur_confirmed") or 0)
        trip_pend_orders = int(trip.get("orders_pending") or 0)
        trip_pend_eur = float(trip.get("gross_eur_pending") or 0)
        trip_aov = trip_conf_eur / trip_conf_orders if trip_conf_orders else 0.0
        trip_wow_html = '<div class="delta">no comparison</div>'
        trip_wow_value = "—"
        if trip_wow and (trip_wow.get("gross_eur_confirmed") or 0) > 0:
            base = float(trip_wow["gross_eur_confirmed"])
            pct = (trip_conf_eur - base) / base * 100
            cls = "up" if pct >= 0 else "down"
            arrow = "▲" if pct >= 0 else "▼"
            trip_wow_html = f'<div class="delta {cls}">{arrow} {pct:+.1f}% vs same day last week ({euro(base)})</div>'
            trip_wow_value = euro(trip_conf_eur - base)

        trip_kpis_html = f"""
    <section class="card">
      <h2>Trip · B&amp;E packages · {html.escape(label)}</h2>
      <div class="grid-kpi" style="margin-bottom:0;">
        <div class="kpi"><div class="label">Confirmed orders</div>
          <div class="value">{trip_conf_orders}</div>
          <div class="delta">Status=1</div></div>
        <div class="kpi"><div class="label">Confirmed gross (BM)</div>
          <div class="value">{euro(trip_conf_eur)}</div>
          <div class="delta">bruttomyynti, not FAS LV</div></div>
        <div class="kpi"><div class="label">AOV</div>
          <div class="value">{euro(trip_aov)}</div>
          <div class="delta">gross ÷ confirmed</div></div>
        <div class="kpi"><div class="label">WoW Δ</div>
          <div class="value">{trip_wow_value}</div>
          {trip_wow_html}</div>
      </div>
    </section>"""

        # Top 10 packages for this window
        tp = sorted((trip.get("by_package") or {}).items(),
                    key=lambda kv: -kv[1]["eur"])[:10]
        if tp:
            tp_rows = "\n".join(
                f"<tr><td>{i+1}</td><td>{html.escape(name)}</td>"
                f"<td class='num'>{int(d['orders'])}</td>"
                f"<td class='num'>{euro(d['eur'])}</td></tr>"
                for i, (name, d) in enumerate(tp)
            )
        else:
            tp_rows = "<tr><td colspan='4' style='color:var(--muted);padding:14px 10px;'>No Trip packages confirmed for this window.</td></tr>"
        trip_top_html = f"""
    <section class="card">
      <h2>Trip · top packages · {html.escape(label)}</h2>
      <table>
        <thead><tr><th>#</th><th>Package</th><th class="num">Orders</th><th class="num">Gross (EUR)</th></tr></thead>
        <tbody>{tp_rows}</tbody>
      </table>
    </section>"""

        if trip_pend_orders > 0:
            trip_pipe_html = f"""
    <section class="card">
      <h2>Trip · pending pipeline · {html.escape(label)}</h2>
      <div style="font-size:13px;color:var(--muted);">
        <strong style="color:var(--warn);font-size:18px;">{trip_pend_orders}</strong> pending orders totalling
        <strong style="color:var(--ink);">{euro(trip_pend_eur)}</strong> — Status=0 (not yet confirmed).
      </div>
    </section>"""

    top = sorted(t.by_product.items(), key=lambda kv: -kv[1]["eur"])[:10]
    if top:
        top_rows = "\n".join(
            f"<tr><td>{i+1}</td><td>{html.escape(name)}</td>"
            f"<td class='num'>{int(d['orders'])}</td>"
            f"<td class='num'>{euro(d['eur'])}</td></tr>"
            for i, (name, d) in enumerate(top)
        )
    else:
        top_rows = "<tr><td colspan='4' style='color:var(--muted);padding:14px 10px;'>No product data for this day.</td></tr>"

    if t.by_country:
        total_c = sum(t.by_country.values())
        country_rows = "\n".join(
            f"<tr><td>{html.escape(c)}</td><td class='num'>{n}</td>"
            f"<td class='num'>{n/total_c*100:.0f}%</td></tr>"
            for c, n in sorted(t.by_country.items(), key=lambda kv: -kv[1])
        )
        geo = f"<table><thead><tr><th>Country</th><th class='num'>Orders</th><th class='num'>Share</th></tr></thead><tbody>{country_rows}</tbody></table>"
    else:
        geo = "<div style='color:var(--muted);font-size:13px;'>Traveler-country breakdown unavailable for this day.</div>"

    # Trip source status
    if trip is not None:
        meta = trip_meta or {}
        trip_updated = meta.get("updated") or "unknown"
        trip_src_rows = meta.get("source_row_count") or "—"
        trip_days_data = int(trip.get("days_with_data") or 0)
        trip_status = f"""
      <div class="status-row">
        <div><span class="pill ok">OK</span></div>
        <div>
          <div style="font-weight:600;">Trip (elamys-trip2) aggregates</div>
          <div class="src-detail">
            Source: PII-free aggregates at <code>data/trip-aggregates.json</code><br>
            Produced locally by <code>scripts/trip_aggregate.py</code> from the
            admin-UI "Dashboard-packages" CSV export<br>
            Last refreshed: <code>{html.escape(str(trip_updated))}</code> ·
            days in window with data: {trip_days_data}
          </div>
        </div>
        <div class="num"><code>{trip_src_rows} src rows</code></div>
      </div>"""
    else:
        trip_status = """
      <div class="status-row">
        <div><span class="pill warn">Missing</span></div>
        <div>
          <div style="font-weight:600;">Trip (elamys-trip2) aggregates</div>
          <div class="src-detail">
            No <code>data/trip-aggregates.json</code> in the repo. Run
            <code>python3 scripts/trip_aggregate.py &lt;CSV&gt;</code> locally and
            commit the output to see Trip alongside Bokún.
          </div>
        </div>
        <div class="num"><code>—</code></div>
      </div>"""

    status = f"""
      <div class="status-row">
        <div><span class="pill ok">OK</span></div>
        <div>
          <div style="font-weight:600;">Bokún REST API</div>
          <div class="src-detail">
            Endpoint: <code>POST /booking.json/product-booking-search</code><br>
            Scope: server-side key (channel "ELAMYSSUOMI activity shop" id 212670)<br>
            Window: creationDateRange = {sales_label}
          </div>
        </div>
        <div class="num"><code>{t.record_count} lines</code></div>
      </div>{trip_status}
    """

    return f"""{_common_head(f"Museokauppa Dashboard — {label}")}
<body>
  <div class="wrap">
    <header>
      <div>
        <h1>Museokauppa Dashboard</h1>
        <div class="subtitle">Museokortti channel · sales day <strong>{html.escape(label)}</strong></div>
      </div>
      <div class="meta">
        Generated <strong>{html.escape(generated_at)}</strong><br>
        Schedule: daily 07:00 Europe/Helsinki<br>
        Owner: Lassi Nummi (CEO, B&amp;E)
      </div>
    </header>

    {_nav("today")}

    <section class="card">
      <h2>Bokún · activity shop · {html.escape(label)}</h2>
      <div class="grid-kpi" style="margin-bottom:0;">
        <div class="kpi"><div class="label">Orders</div>
          <div class="value">{t.orders}</div>
          <div class="delta">unique parent bookings</div></div>
        <div class="kpi"><div class="label">Gross sales (BM), EUR</div>
          <div class="value">{euro(t.gross_eur)}</div>
          <div class="delta">bruttomyynti, not FAS LV</div></div>
        <div class="kpi"><div class="label">AOV, EUR</div>
          <div class="value">{euro(aov)}</div>
          <div class="delta">gross ÷ orders</div></div>
        <div class="kpi"><div class="label">WoW Δ</div>
          <div class="value">{wow_value_html}</div>
          {wow_html}</div>
      </div>
    </section>

    {trip_kpis_html}

    <section class="card">
      <h2>Bokún · top 10 products</h2>
      <table>
        <thead><tr><th>#</th><th>Product</th><th class="num">Orders</th><th class="num">Gross (EUR)</th></tr></thead>
        <tbody>{top_rows}</tbody>
      </table>
    </section>

    {trip_top_html}
    {trip_pipe_html}

    <section class="card">
      <h2>Bokún · geography split</h2>
      {geo}
    </section>

    <section class="card">
      <h2>Data sources</h2>
      {status}
    </section>

    <footer>
      Museokauppa Dashboard · Elämys Group B&amp;E · automated run ·
      "Gross sales (BM)" = bruttomyynti (not FAS liikevaihto) ·
      cancellations excluded from totals ({t.cancellations} cancelled line{'s' if t.cancellations!=1 else ''} observed).
    </footer>
  </div>
</body>
</html>
"""


def render_status_page(label: str, generated_at: str, error: str) -> str:
    return f"""{_common_head(f"Museokauppa Dashboard — {label}")}
<body>
  <div class="wrap">
    <header>
      <div>
        <h1>Museokauppa Dashboard</h1>
        <div class="subtitle">Museokortti channel · sales day <strong>{html.escape(label)}</strong></div>
      </div>
      <div class="meta">Generated <strong>{html.escape(generated_at)}</strong></div>
    </header>
    {_nav("today")}
    <section class="card">
      <h2>Run status</h2>
      <div style="color:#fde68a;"><span class="pill bad">Blocked</span> Bokún fetch failed this run.</div>
      <pre style="background:#0b1120;padding:12px;border-radius:6px;color:#fca5a5;font-size:12px;overflow:auto;">{html.escape(error)}</pre>
    </section>
    <footer>Museokauppa Dashboard · Elämys Group B&amp;E · automated run</footer>
  </div>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Trends view (30d daily, 12m monthly, rolling top products)
# ---------------------------------------------------------------------------


def render_trends(hist: dict[str, Any], generated_at: str,
                  trip: dict[str, Any] | None = None) -> str:
    days = hist.get("days") or {}
    trip_days = (trip or {}).get("days") or {}
    trip_meta = {k: v for k, v in (trip or {}).items() if k != "days"}
    if not days and not trip_days:
        body = ("<section class='card'><h2>Trends</h2>"
                "<div style='color:var(--muted);font-size:13px;'>"
                "No history yet. Run the <em>Backfill history</em> workflow "
                "(Actions → Backfill → Run workflow) to populate past days, "
                "or wait for the daily cron to accumulate data.</div></section>")
        return f"""{_common_head("Museokauppa Dashboard — Trends")}
<body><div class="wrap">
  <header><div><h1>Museokauppa Dashboard</h1>
  <div class="subtitle">Trends &amp; history</div></div>
  <div class="meta">Generated <strong>{html.escape(generated_at)}</strong></div>
  </header>
  {_nav("trends")}
  {body}
</div></body></html>"""

    # Sort dates ascending
    sorted_days = sorted(days.keys())
    today = today_helsinki()
    thirty_ago = today - timedelta(days=30)

    # 30-day window (include days that exist in history within window)
    daily = []
    for i in range(30, 0, -1):
        d = today - timedelta(days=i)
        entry = days.get(d.isoformat())
        if entry:
            daily.append((d, float(entry.get("gross_eur") or 0.0),
                          int(entry.get("orders") or 0)))
        else:
            daily.append((d, 0.0, 0))

    max_gross = max((g for _, g, _ in daily), default=0.0) or 1.0
    bar_w = 28
    gap = 6
    chart_h = 160
    left_pad = 48
    right_pad = 10
    total_w = left_pad + len(daily) * (bar_w + gap) + right_pad
    # Y-axis ticks at 0, 25, 50, 75, 100%
    y_ticks_svg = "".join(
        f'<line x1="{left_pad-4}" y1="{chart_h - chart_h*p/100}" '
        f'x2="{total_w-right_pad}" y2="{chart_h - chart_h*p/100}" '
        f'stroke="rgba(148,163,184,0.15)" stroke-width="1"/>'
        f'<text x="{left_pad-8}" y="{chart_h - chart_h*p/100 + 3}" '
        f'fill="#94a3b8" font-size="10" text-anchor="end">'
        f'{int(max_gross*p/100)}</text>'
        for p in (0, 25, 50, 75, 100)
    )
    bars_svg = ""
    for i, (d, g, o) in enumerate(daily):
        x = left_pad + i * (bar_w + gap)
        h = int((g / max_gross) * chart_h) if max_gross > 0 else 0
        y = chart_h - h
        color = "var(--bar)" if g > 0 else "var(--bar-dim)"
        bars_svg += (
            f'<rect x="{x}" y="{y}" width="{bar_w}" height="{max(h,1)}" '
            f'fill="{color}" rx="2">'
            f'<title>{d.isoformat()} ({d.strftime("%a")}) — '
            f'{int(g)} € · {o} orders</title></rect>'
        )
        # X label every 5 days + last
        if i % 5 == 0 or i == len(daily) - 1:
            bars_svg += (
                f'<text x="{x + bar_w/2}" y="{chart_h + 14}" fill="#94a3b8" '
                f'font-size="10" text-anchor="middle">{d.strftime("%d.%m")}</text>'
            )

    chart_svg = (
        f'<svg class="chart" viewBox="0 0 {total_w} {chart_h + 24}" '
        f'preserveAspectRatio="xMidYMid meet" xmlns="http://www.w3.org/2000/svg">'
        f'{y_ticks_svg}{bars_svg}</svg>'
        f'<div class="chart-caption">EUR per day · last 30 days</div>'
    )

    # Totals across last 30 days (only days in history)
    d30_total_eur = sum(g for _, g, _ in daily)
    d30_total_orders = sum(o for _, _, o in daily)
    d30_days_with_data = sum(1 for _, g, _ in daily if g > 0)
    d30_aov = d30_total_eur / d30_total_orders if d30_total_orders else 0.0

    # Monthly rolling last 12 months
    month_totals: dict[str, dict[str, float]] = defaultdict(
        lambda: {"orders": 0, "eur": 0.0, "days": 0}
    )
    for ds, entry in days.items():
        try:
            d = date.fromisoformat(ds)
        except ValueError:
            continue
        key = d.strftime("%Y-%m")
        month_totals[key]["orders"] += int(entry.get("orders") or 0)
        month_totals[key]["eur"] += float(entry.get("gross_eur") or 0.0)
        month_totals[key]["days"] += 1

    # Build ordered month list last 12 calendar months
    def month_keys_back(n: int) -> list[str]:
        out: list[str] = []
        y, m = today.year, today.month
        for _ in range(n):
            out.append(f"{y:04d}-{m:02d}")
            m -= 1
            if m == 0:
                m = 12; y -= 1
        return list(reversed(out))

    month_order = month_keys_back(12)
    month_rows = ""
    prev_eur: float | None = None
    for mk in month_order:
        m = month_totals.get(mk, {"orders": 0, "eur": 0.0, "days": 0})
        orders = int(m["orders"])
        eur = float(m["eur"])
        aov = eur / orders if orders else 0.0
        if prev_eur is None or prev_eur == 0:
            delta_html = '<span class="flat">—</span>'
        else:
            pct = (eur - prev_eur) / prev_eur * 100
            cls = "pos" if pct > 0 else ("neg" if pct < 0 else "flat")
            arrow = "▲" if pct > 0 else ("▼" if pct < 0 else "·")
            delta_html = f'<span class="{cls}">{arrow} {pct:+.1f}%</span>'
        prev_eur = eur
        month_rows += (
            f"<tr><td>{mk}</td>"
            f"<td class='num'>{orders}</td>"
            f"<td class='num'>{euro(eur)}</td>"
            f"<td class='num'>{euro(aov)}</td>"
            f"<td class='num'>{delta_html}</td>"
            f"<td class='num'><span class='tag'>{int(m['days'])} d</span></td></tr>"
        )

    # Rolling top products across the most recent 30 calendar days
    prod_tot: dict[str, dict[str, float]] = defaultdict(
        lambda: {"orders": 0, "eur": 0.0}
    )
    for i in range(30):
        d = today - timedelta(days=i + 1)
        entry = days.get(d.isoformat())
        if not entry:
            continue
        for name, p in (entry.get("by_product") or {}).items():
            prod_tot[name]["orders"] += int(p.get("orders") or 0)
            prod_tot[name]["eur"] += float(p.get("eur") or 0.0)
    top_prods = sorted(prod_tot.items(), key=lambda kv: -kv[1]["eur"])[:15]
    if top_prods:
        top_rows = "\n".join(
            f"<tr><td>{i+1}</td><td>{html.escape(name)}</td>"
            f"<td class='num'>{int(d['orders'])}</td>"
            f"<td class='num'>{euro(d['eur'])}</td></tr>"
            for i, (name, d) in enumerate(top_prods)
        )
    else:
        top_rows = "<tr><td colspan='4' style='color:var(--muted);padding:14px 10px;'>No product data in last 30 days.</td></tr>"

    first_date = sorted_days[0] if sorted_days else "—"
    last_date = sorted_days[-1] if sorted_days else "—"

    # =====================================================================
    # Trip · B&E packages — daily 30d chart, monthly 12m, top packages 30d
    # =====================================================================
    trip_sections_html = ""
    if trip_days:
        # 30-day daily
        t_daily = []
        for i in range(30, 0, -1):
            d = today - timedelta(days=i)
            rec = trip_days.get(d.isoformat())
            conf = float((rec or {}).get("gross_eur_confirmed") or 0)
            confn = int((rec or {}).get("orders_confirmed") or 0)
            t_daily.append((d, conf, confn))
        t_max = max((g for _, g, _ in t_daily), default=0.0) or 1.0
        t_bar_w = 28; t_gap = 6; t_ch_h = 160
        t_lpad = 60; t_rpad = 10
        t_tw = t_lpad + len(t_daily) * (t_bar_w + t_gap) + t_rpad
        t_yticks = "".join(
            f'<line x1="{t_lpad-4}" y1="{t_ch_h - t_ch_h*p/100}" '
            f'x2="{t_tw-t_rpad}" y2="{t_ch_h - t_ch_h*p/100}" '
            f'stroke="rgba(148,163,184,0.15)" stroke-width="1"/>'
            f'<text x="{t_lpad-8}" y="{t_ch_h - t_ch_h*p/100 + 3}" '
            f'fill="#94a3b8" font-size="10" text-anchor="end">'
            f'{int(t_max*p/100):,}</text>'.replace(",", " ")
            for p in (0, 25, 50, 75, 100)
        )
        t_bars = ""
        for i, (d, g, o) in enumerate(t_daily):
            x = t_lpad + i * (t_bar_w + t_gap)
            h = int((g / t_max) * t_ch_h) if t_max > 0 else 0
            y = t_ch_h - h
            color = "#a78bfa" if g > 0 else "var(--bar-dim)"  # purple accent for Trip
            t_bars += (
                f'<rect x="{x}" y="{y}" width="{t_bar_w}" height="{max(h,1)}" '
                f'fill="{color}" rx="2">'
                f'<title>{d.isoformat()} ({d.strftime("%a")}) — '
                f'{int(g):,} € · {o} orders</title></rect>'.replace(",", " ")
            )
            if i % 5 == 0 or i == len(t_daily) - 1:
                t_bars += (
                    f'<text x="{x + t_bar_w/2}" y="{t_ch_h + 14}" fill="#94a3b8" '
                    f'font-size="10" text-anchor="middle">{d.strftime("%d.%m")}</text>'
                )
        t_chart = (
            f'<svg class="chart" viewBox="0 0 {t_tw} {t_ch_h + 24}" '
            f'preserveAspectRatio="xMidYMid meet" xmlns="http://www.w3.org/2000/svg">'
            f'{t_yticks}{t_bars}</svg>'
            f'<div class="chart-caption">Confirmed EUR per day · last 30 days · Trip</div>'
        )

        # 12-month table for Trip
        t_month_totals: dict[str, dict[str, float]] = defaultdict(
            lambda: {"conf_n": 0, "conf_eur": 0.0, "pend_n": 0,
                     "pend_eur": 0.0, "days": 0}
        )
        for ds, rec in trip_days.items():
            try:
                d = date.fromisoformat(ds)
            except ValueError:
                continue
            k = d.strftime("%Y-%m")
            t_month_totals[k]["conf_n"]   += int(rec.get("orders_confirmed") or 0)
            t_month_totals[k]["conf_eur"] += float(rec.get("gross_eur_confirmed") or 0)
            t_month_totals[k]["pend_n"]   += int(rec.get("orders_pending") or 0)
            t_month_totals[k]["pend_eur"] += float(rec.get("gross_eur_pending") or 0)
            t_month_totals[k]["days"] += 1

        t_month_keys = []
        yr, mo = today.year, today.month
        for _ in range(12):
            t_month_keys.append(f"{yr:04d}-{mo:02d}")
            mo -= 1
            if mo == 0:
                mo = 12; yr -= 1
        t_month_keys = list(reversed(t_month_keys))
        t_month_rows = ""
        prev_eur = None
        for mk in t_month_keys:
            m = t_month_totals.get(mk, {"conf_n": 0, "conf_eur": 0.0,
                                        "pend_n": 0, "pend_eur": 0.0, "days": 0})
            conf_n = int(m["conf_n"])
            conf_eur = float(m["conf_eur"])
            aov = conf_eur / conf_n if conf_n else 0.0
            if prev_eur is None or prev_eur == 0:
                delta_html = '<span class="flat">—</span>'
            else:
                pct = (conf_eur - prev_eur) / prev_eur * 100
                cls = "pos" if pct > 0 else ("neg" if pct < 0 else "flat")
                arrow = "▲" if pct > 0 else ("▼" if pct < 0 else "·")
                delta_html = f'<span class="{cls}">{arrow} {pct:+.1f}%</span>'
            prev_eur = conf_eur
            t_month_rows += (
                f"<tr><td>{mk}</td>"
                f"<td class='num'>{conf_n}</td>"
                f"<td class='num'>{euro(conf_eur)}</td>"
                f"<td class='num'>{euro(aov)}</td>"
                f"<td class='num'>{delta_html}</td>"
                f"<td class='num'>{int(m['pend_n'])}</td>"
                f"<td class='num'>{euro(m['pend_eur'])}</td></tr>"
            )

        # Rolling top packages last 30 days
        t_pkg_tot: dict[str, dict[str, float]] = defaultdict(
            lambda: {"orders": 0, "eur": 0.0}
        )
        for i in range(30):
            d = today - timedelta(days=i + 1)
            rec = trip_days.get(d.isoformat())
            if not rec:
                continue
            for name, p in (rec.get("by_package") or {}).items():
                t_pkg_tot[name]["orders"] += int(p.get("orders") or 0)
                t_pkg_tot[name]["eur"]    += float(p.get("eur") or 0)
        t_top = sorted(t_pkg_tot.items(), key=lambda kv: -kv[1]["eur"])[:15]
        if t_top:
            t_top_rows = "\n".join(
                f"<tr><td>{i+1}</td><td>{html.escape(name)}</td>"
                f"<td class='num'>{int(d['orders'])}</td>"
                f"<td class='num'>{euro(d['eur'])}</td></tr>"
                for i, (name, d) in enumerate(t_top)
            )
        else:
            t_top_rows = "<tr><td colspan='4' style='color:var(--muted);padding:14px 10px;'>No Trip packages in last 30 days.</td></tr>"

        # 30d summary KPIs
        t30_eur = sum(g for _, g, _ in t_daily)
        t30_orders = sum(o for _, _, o in t_daily)
        t30_aov = t30_eur / t30_orders if t30_orders else 0.0
        t30_days = sum(1 for _, g, _ in t_daily if g > 0)

        trip_updated = trip_meta.get("updated") or "—"

        trip_sections_html = f"""
    <section class="card" style="border-left:3px solid #a78bfa;">
      <h2>Trip · B&amp;E packages · last 30 days</h2>
      <div class="grid-kpi" style="margin-bottom:16px;">
        <div class="kpi"><div class="label">Confirmed orders (30d)</div>
          <div class="value">{t30_orders}</div>
          <div class="delta">{t30_days} days with data</div></div>
        <div class="kpi"><div class="label">Confirmed gross (30d)</div>
          <div class="value">{euro(t30_eur)}</div>
          <div class="delta">bruttomyynti, not FAS LV</div></div>
        <div class="kpi"><div class="label">Trip AOV (30d)</div>
          <div class="value">{euro(t30_aov)}</div>
          <div class="delta">gross ÷ orders</div></div>
        <div class="kpi"><div class="label">Data refreshed</div>
          <div class="value" style="font-size:14px;">{html.escape(str(trip_updated)[:10])}</div>
          <div class="delta">PII-free aggregates</div></div>
      </div>
      {t_chart}
    </section>

    <section class="card">
      <h2>Trip · monthly · last 12 months</h2>
      <table>
        <thead><tr>
          <th>Month</th>
          <th class="num">Confirmed</th>
          <th class="num">Gross (EUR)</th>
          <th class="num">AOV</th>
          <th class="num">MoM Δ</th>
          <th class="num">Pending</th>
          <th class="num">Pending (EUR)</th>
        </tr></thead>
        <tbody>{t_month_rows}</tbody>
      </table>
    </section>

    <section class="card">
      <h2>Trip · top 15 packages · rolling 30 days</h2>
      <table>
        <thead><tr><th>#</th><th>Package</th><th class="num">Orders</th><th class="num">Gross (EUR)</th></tr></thead>
        <tbody>{t_top_rows}</tbody>
      </table>
    </section>"""

    return f"""{_common_head("Museokauppa Dashboard — Trends")}
<body>
  <div class="wrap">
    <header>
      <div>
        <h1>Museokauppa Dashboard</h1>
        <div class="subtitle">Trends &amp; history · Museokortti channel</div>
      </div>
      <div class="meta">
        Generated <strong>{html.escape(generated_at)}</strong><br>
        History: <strong>{first_date}</strong> → <strong>{last_date}</strong> ({len(days)} days)
      </div>
    </header>

    {_nav("trends")}

    <div class="grid-kpi">
      <div class="kpi"><div class="label">Last 30 days · orders</div>
        <div class="value">{d30_total_orders}</div>
        <div class="delta">{d30_days_with_data} days with data</div></div>
      <div class="kpi"><div class="label">Last 30 days · gross</div>
        <div class="value">{euro(d30_total_eur)}</div>
        <div class="delta">bruttomyynti, not FAS LV</div></div>
      <div class="kpi"><div class="label">Last 30 days · AOV</div>
        <div class="value">{euro(d30_aov)}</div>
        <div class="delta">gross ÷ orders</div></div>
      <div class="kpi"><div class="label">Days tracked</div>
        <div class="value">{len(days)}</div>
        <div class="delta">since backfill start</div></div>
    </div>

    <section class="card">
      <h2>Daily · last 30 days</h2>
      {chart_svg}
    </section>

    <section class="card">
      <h2>Monthly · last 12 months</h2>
      <table>
        <thead><tr>
          <th>Month</th>
          <th class="num">Orders</th>
          <th class="num">Gross (EUR)</th>
          <th class="num">AOV</th>
          <th class="num">MoM Δ</th>
          <th class="num">Days</th>
        </tr></thead>
        <tbody>{month_rows}</tbody>
      </table>
    </section>

    <section class="card">
      <h2>Bokún · top 15 products · rolling 30 days</h2>
      <table>
        <thead><tr><th>#</th><th>Product</th><th class="num">Orders</th><th class="num">Gross (EUR)</th></tr></thead>
        <tbody>{top_rows}</tbody>
      </table>
    </section>

    {trip_sections_html}

    <footer>
      Museokauppa Dashboard · Trends view · sources: Bokún REST (activity shop) + Trip (B&amp;E packages) ·
      "Gross sales (BM)" = bruttomyynti (not FAS liikevaihto)
    </footer>
  </div>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Archive index
# ---------------------------------------------------------------------------


def write_index() -> None:
    files = sorted(
        [p for p in DOCS.glob("20??-??-??*.html")],
        reverse=True,
    )
    rows = "\n".join(
        f"<li><a href='{p.name}'>{p.stem}</a></li>" for p in files[:120]
    )
    (DOCS / "index.html").write_text(
        f"""{_common_head("Museokauppa Dashboards")}
<body><div class="wrap">
  <header><div><h1>Museokauppa Dashboards</h1>
  <div class="subtitle">Recent daily runs</div>
  </div></header>
  {_nav("archive")}
  <section class="card"><h2>Recent days</h2>
  <ul style="line-height:1.8;font-size:14px;">{rows or '<li style="color:var(--muted);">No dashboards yet.</li>'}</ul>
  </section>
</div></body></html>""",
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# Run modes
# ---------------------------------------------------------------------------


def run_daily() -> int:
    today = today_helsinki()
    d_from, d_to, label = target_range(today)
    gen_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    out_name = d_to.isoformat()
    if d_from != d_to:
        out_name = f"{d_to.isoformat()}-weekend"

    print(f"[museokauppa] daily target range {d_from} → {d_to} ({label})")

    hist = load_history()
    trip = load_trip_aggregates()
    trip_meta = {k: v for k, v in trip.items() if k != "days"} if trip else None
    trip_totals = trip_range_totals(trip, d_from, d_to) if trip.get("days") else None
    wow_from_base = d_from - timedelta(days=7)
    wow_to_base = d_to - timedelta(days=7)
    trip_wow_totals = (trip_range_totals(trip, wow_from_base, wow_to_base)
                       if trip.get("days") else None)

    try:
        # For Mondays this is 3 days; for other days 1 day. We persist per-day
        # entries in history, but render the dashboard for the aggregated window.
        aggregated = Totals()
        for d in iter_days(d_from, d_to):
            lines = search_product_bookings(d, d)
            per_day = aggregate(lines)
            print(f"[museokauppa] {d.isoformat()} → {per_day.record_count} lines, "
                  f"{per_day.orders} orders, {per_day.gross_eur:.0f} EUR")
            hist["days"][d.isoformat()] = totals_to_day_record(per_day)
            for ln in lines:
                aggregated.add_line(ln)
            aggregated.orders += per_day.orders  # sum of uniq-per-day; fine for Mon rollup

        # WoW comparison: same weekday(s) one week earlier.
        wow_from = d_from - timedelta(days=7)
        wow_to = d_to - timedelta(days=7)
        try:
            wow = Totals()
            for d in iter_days(wow_from, wow_to):
                wl = search_product_bookings(d, d)
                wow_day = aggregate(wl)
                # persist too — fills earlier week automatically
                hist["days"][d.isoformat()] = totals_to_day_record(wow_day)
                for ln in wl:
                    wow.add_line(ln)
                wow.orders += wow_day.orders
            print(f"[museokauppa] WoW baseline ({wow_from}→{wow_to}): "
                  f"{wow.orders} orders, {wow.gross_eur:.0f} EUR")
        except BokunError as e:
            print(f"[museokauppa] WoW fetch failed (non-fatal): {e}")
            wow = None

        html_out = render_html(label, aggregated, wow, gen_at, label,
                                trip=trip_totals, trip_wow=trip_wow_totals,
                                trip_meta=trip_meta)
        trip_summary = ""
        if trip_totals is not None:
            trip_summary = (f" | trip_conf={trip_totals['orders_confirmed']}/"
                            f"{trip_totals['gross_eur_confirmed']:.0f}EUR "
                            f"pend={trip_totals['orders_pending']}/"
                            f"{trip_totals['gross_eur_pending']:.0f}EUR")
        summary = (f"bokun orders={aggregated.orders} "
                   f"gross={aggregated.gross_eur:.0f}EUR "
                   f"cancellations={aggregated.cancellations}{trip_summary}")
    except BokunError as e:
        print(f"[museokauppa] FETCH FAILED: {e}", file=sys.stderr)
        html_out = render_status_page(label, gen_at, str(e))
        summary = f"BLOCKED: {e}"

    dated = DOCS / f"{out_name}.html"
    latest = DOCS / "latest.html"
    dated.write_text(html_out, encoding="utf-8")
    latest.write_text(html_out, encoding="utf-8")

    save_history(hist)
    (DOCS / "trends.html").write_text(
        render_trends(hist, gen_at, trip=trip), encoding="utf-8"
    )
    write_index()

    (DOCS / ".nojekyll").write_text("", encoding="utf-8")
    (DOCS / "robots.txt").write_text(
        "User-agent: *\nDisallow: /\n", encoding="utf-8"
    )

    print(f"[museokauppa] wrote {dated.relative_to(REPO_ROOT)} and latest.html")
    print(f"[museokauppa] summary: {summary}")
    _step_summary(f"## Museokauppa {label}\n\n- {summary}\n- File: `docs/{dated.name}`\n")
    return 0


def run_backfill(from_date: date) -> int:
    today = today_helsinki()
    to_date = today - timedelta(days=1)
    gen_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"[museokauppa] backfill range {from_date} → {to_date}")

    hist = load_history()
    n_ok = 0
    n_fail = 0
    total_orders = 0
    total_eur = 0.0
    for d in iter_days(from_date, to_date):
        try:
            lines = search_product_bookings(d, d)
            per_day = aggregate(lines)
            hist["days"][d.isoformat()] = totals_to_day_record(per_day)
            n_ok += 1
            total_orders += per_day.orders
            total_eur += per_day.gross_eur
            print(f"[museokauppa] {d.isoformat()} → {per_day.orders} orders, "
                  f"{per_day.gross_eur:.0f} EUR ({per_day.record_count} lines)")
        except BokunError as e:
            n_fail += 1
            print(f"[museokauppa] {d.isoformat()} → FAILED: {e}", file=sys.stderr)

    save_history(hist)
    trip = load_trip_aggregates()
    (DOCS / "trends.html").write_text(
        render_trends(hist, gen_at, trip=trip), encoding="utf-8"
    )
    (DOCS / ".nojekyll").write_text("", encoding="utf-8")
    (DOCS / "robots.txt").write_text(
        "User-agent: *\nDisallow: /\n", encoding="utf-8"
    )

    summary = (f"backfill {from_date}→{to_date}: "
               f"{n_ok} days OK, {n_fail} failed; "
               f"total {total_orders} orders / {total_eur:.0f} EUR")
    print(f"[museokauppa] {summary}")
    _step_summary(f"## Museokauppa backfill\n\n- {summary}\n")
    return 0 if n_fail == 0 else 1


def _step_summary(text: str) -> None:
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a", encoding="utf-8") as f:
            f.write(text)


def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser(description="Museokauppa Dashboard build script")
    p.add_argument("--backfill", metavar="FROM_DATE",
                   help="Backfill mode: iterate day-by-day from FROM_DATE "
                        "(YYYY-MM-DD) to yesterday, writing history.json")
    args = p.parse_args(argv[1:])
    if args.backfill:
        try:
            start = date.fromisoformat(args.backfill)
        except ValueError:
            print(f"[museokauppa] invalid --backfill date: {args.backfill!r}",
                  file=sys.stderr)
            return 2
        return run_backfill(start)
    return run_daily()


if __name__ == "__main__":
    sys.exit(main(sys.argv))
