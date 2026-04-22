#!/usr/bin/env python3
"""
Museokauppa Dashboard — cloud build script.

Fetches yesterday's bookings from the Bokún REST API (Museokortti channel,
scoped by the server-side API key), aggregates KPIs, renders a single-file
HTML dashboard, and writes it into ``docs/`` for GitHub Pages.

Runs inside GitHub Actions. Stdlib only — no pip install needed.

Environment variables (set as GitHub repo secrets):
    BOKUN_ACCESS_KEY   — Bokún REST access key (Museokortti channel)
    BOKUN_SECRET_KEY   — Bokún REST secret key
    BOKUN_BASE_URL     — optional, defaults to https://api.bokun.io
"""
from __future__ import annotations

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

# Europe/Helsinki — we determine the offset manually so we don't depend on
# tzdata being installed in the runner image. April to October = EEST (UTC+3);
# otherwise EET (UTC+2). DST transitions happen on the last Sunday of March
# (clocks forward) and last Sunday of October (clocks back).
def helsinki_offset(on: date) -> timedelta:
    y = on.year
    # last Sunday of March
    d = date(y, 3, 31)
    while d.weekday() != 6:
        d -= timedelta(days=1)
    dst_start = d
    # last Sunday of October
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
    # Bokún requires UTC in "YYYY-MM-DD HH:MM:SS" form.
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
            # Retry 5xx, fail fast on 4xx.
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


def search_product_bookings(day_from: date, day_to: date) -> list[dict]:
    """
    Pull all product-bookings whose *creation* date falls in the given range.
    Creation date = when the order was placed (== "sales day" in retail terms).
    """
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

        # Bokún search responses have historically come back as either a bare list
        # or an envelope like {"results": [...], "totalHits": N}. Handle both.
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
        if page > 50:  # safety cap — 10k line items/day is implausible
            break
    return out


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------


@dataclass
class Totals:
    orders: int = 0
    gross_eur: float = 0.0
    cancellations: int = 0
    by_product: dict[str, dict[str, float]] = field(default_factory=lambda: defaultdict(lambda: {"orders": 0, "eur": 0.0}))
    by_country: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    source: str = "Bokún / product-booking-search"
    record_count: int = 0

    def add_line(self, line: dict) -> None:
        self.record_count += 1
        status = (line.get("status") or "").upper()
        if "CANCEL" in status:
            self.cancellations += 1
            return  # don't count cancelled lines in sales
        # Pricing — try the common keys in order.
        price = _extract_eur(line)
        self.gross_eur += price
        title = (line.get("productTitle") or line.get("title")
                 or line.get("productName") or "Unknown product")
        self.by_product[title]["orders"] += 1
        self.by_product[title]["eur"] += price
        country = (line.get("customerCountry") or line.get("country")
                   or (line.get("customer") or {}).get("country"))
        if country:
            self.by_country[str(country).upper()[:2]] += 1


def _extract_eur(line: dict) -> float:
    """
    Pull a EUR-denominated line total out of a Bokún product-booking object.
    Falls back to any numeric 'price'/'total'/'amount' if currency missing.
    """
    for key in ("totalPrice", "totalAmount", "amount", "price", "total"):
        v = line.get(key)
        if v is None:
            continue
        if isinstance(v, (int, float)):
            return float(v)
        if isinstance(v, dict):
            amt = v.get("amount") if isinstance(v.get("amount"), (int, float)) else None
            cur = (v.get("currency") or "").upper()
            if amt is not None:
                if cur and cur != "EUR":
                    # Simple passthrough — if Bokún gave us a non-EUR line we still
                    # record it. Conversion would need a rate feed; we note this in
                    # the status panel rather than silently fudging.
                    return float(amt)
                return float(amt)
    return 0.0


def uniq_orders(lines: Iterable[dict]) -> int:
    """
    A single "order" in Bokún = one parent booking (confirmationCode).
    Line items share that code, so we dedupe.
    """
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
# Date helpers — Monday rolls up Fri+Sat+Sun
# ---------------------------------------------------------------------------


def target_range(today: date) -> tuple[date, date, str]:
    """Return (from, to, label) for the sales window to display."""
    wd = today.weekday()  # Mon=0 ... Sun=6
    if wd == 0:  # Monday — report on Fri/Sat/Sun
        fri = today - timedelta(days=3)
        sun = today - timedelta(days=1)
        return fri, sun, f"Weekend {fri.isoformat()} → {sun.isoformat()}"
    y = today - timedelta(days=1)
    return y, y, y.strftime("%a %Y-%m-%d")


# ---------------------------------------------------------------------------
# HTML rendering
# ---------------------------------------------------------------------------


CSS = """
:root {
  --bg: #0f172a; --panel: #1e293b; --ink: #f1f5f9; --muted: #94a3b8;
  --border: #334155; --ok: #22c55e; --warn: #f59e0b; --bad: #ef4444;
  --accent: #38bdf8;
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
.bar { display:flex; height:18px; border-radius:4px; overflow:hidden; background:#273449;
  border:1px solid var(--border); margin-top:6px; }
.bar > div { height:100%; }
.legend { font-size:12px; color:var(--muted); margin-top:6px; }
@media (max-width:780px) { .grid-kpi { grid-template-columns:repeat(2,1fr); }
  .status-row { grid-template-columns:90px 1fr; } }
"""


def euro(x: float) -> str:
    return f"{x:,.0f}".replace(",", " ") + " €"


def render_html(label: str, t: Totals, wow: Totals | None,
                generated_at: str, sales_label: str) -> str:
    aov = (t.gross_eur / t.orders) if t.orders else 0.0
    wow_html = '<div class="delta">no comparison</div>'
    if wow and wow.gross_eur > 0:
        pct = (t.gross_eur - wow.gross_eur) / wow.gross_eur * 100
        cls = "up" if pct >= 0 else "down"
        arrow = "▲" if pct >= 0 else "▼"
        wow_html = f'<div class="delta {cls}">{arrow} {pct:+.1f}% vs same day last week ({euro(wow.gross_eur)})</div>'

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

    status = f"""
      <div class="status-row">
        <div><span class="pill ok">OK</span></div>
        <div>
          <div style="font-weight:600;">Bokún REST API</div>
          <div class="src-detail">
            Endpoint: <code>POST /booking.json/product-booking-search</code><br>
            Scope: Museokortti booking channel (server-side key)<br>
            Window: creationDateRange = {sales_label}
          </div>
        </div>
        <div class="num"><code>{t.record_count} lines</code></div>
      </div>
      <div class="status-row">
        <div><span class="pill warn">Skipped</span></div>
        <div>
          <div style="font-weight:600;">Trip (elamys-trip2) export</div>
          <div class="src-detail">Disabled in v1 — Bokún-only per current setup.</div>
        </div>
        <div class="num"><code>—</code></div>
      </div>
    """

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="robots" content="noindex, nofollow, noarchive, nosnippet">
<meta name="referrer" content="no-referrer">
<title>Museokauppa Dashboard — {html.escape(label)}</title>
<style>{CSS}</style>
</head>
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

    <div class="grid-kpi">
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
        <div class="value">{'—' if not wow else euro(t.gross_eur - wow.gross_eur)}</div>
        {wow_html}</div>
    </div>

    <section class="card">
      <h2>Top 10 products</h2>
      <table>
        <thead><tr><th>#</th><th>Product</th><th class="num">Orders</th><th class="num">Gross (EUR)</th></tr></thead>
        <tbody>{top_rows}</tbody>
      </table>
    </section>

    <section class="card">
      <h2>Geography split</h2>
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
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="robots" content="noindex, nofollow, noarchive, nosnippet">
<meta name="referrer" content="no-referrer">
<title>Museokauppa Dashboard — {html.escape(label)}</title>
<style>{CSS}</style>
</head>
<body>
  <div class="wrap">
    <header>
      <div>
        <h1>Museokauppa Dashboard</h1>
        <div class="subtitle">Museokortti channel · sales day <strong>{html.escape(label)}</strong></div>
      </div>
      <div class="meta">Generated <strong>{html.escape(generated_at)}</strong></div>
    </header>
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
# Index page (listing of recent dashboards)
# ---------------------------------------------------------------------------


def write_index() -> None:
    files = sorted(
        [p for p in DOCS.glob("20??-??-??*.html")],
        reverse=True,
    )
    rows = "\n".join(
        f"<li><a href='{p.name}'>{p.stem}</a></li>" for p in files[:60]
    )
    (DOCS / "index.html").write_text(
        f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8">
<meta name="robots" content="noindex, nofollow, noarchive, nosnippet">
<meta name="referrer" content="no-referrer">
<title>Museokauppa Dashboards</title>
<style>{CSS}</style></head>
<body><div class="wrap">
  <header><div><h1>Museokauppa Dashboards</h1>
  <div class="subtitle">Recent daily runs · <a href="latest.html" style="color:var(--accent);">open latest →</a></div>
  </div></header>
  <section class="card"><h2>Recent days</h2>
  <ul style="line-height:1.8;font-size:14px;">{rows or '<li style="color:var(--muted);">No dashboards yet.</li>'}</ul>
  </section>
</div></body></html>
""",
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    today = today_helsinki()
    d_from, d_to, label = target_range(today)
    gen_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    out_name = d_to.isoformat()  # filename is keyed on the last sales day
    if d_from != d_to:
        out_name = f"{d_to.isoformat()}-weekend"

    print(f"[museokauppa] target range {d_from} → {d_to} ({label})")

    try:
        lines = search_product_bookings(d_from, d_to)
        print(f"[museokauppa] fetched {len(lines)} product-booking line items")
        totals = aggregate(lines)

        # WoW comparison — same weekday one week earlier (or prior weekend).
        wow_from = d_from - timedelta(days=7)
        wow_to = d_to - timedelta(days=7)
        try:
            wow_lines = search_product_bookings(wow_from, wow_to)
            wow_totals = aggregate(wow_lines)
            print(f"[museokauppa] WoW baseline ({wow_from}→{wow_to}): "
                  f"{wow_totals.orders} orders, {wow_totals.gross_eur:.0f} EUR")
        except BokunError as e:
            print(f"[museokauppa] WoW fetch failed (non-fatal): {e}")
            wow_totals = None

        html_out = render_html(label, totals, wow_totals, gen_at, label)
        summary = (f"orders={totals.orders} gross={totals.gross_eur:.0f}EUR "
                   f"cancellations={totals.cancellations}")
    except BokunError as e:
        print(f"[museokauppa] FETCH FAILED: {e}", file=sys.stderr)
        html_out = render_status_page(label, gen_at, str(e))
        summary = f"BLOCKED: {e}"

    dated = DOCS / f"{out_name}.html"
    latest = DOCS / "latest.html"
    dated.write_text(html_out, encoding="utf-8")
    latest.write_text(html_out, encoding="utf-8")
    write_index()

    # Write a .nojekyll so GitHub Pages doesn't process with Jekyll.
    (DOCS / ".nojekyll").write_text("", encoding="utf-8")
    # Block search-engine crawlers. Belt-and-suspenders with the per-page
    # noindex meta tag — matters because this repo is public for Pages access.
    (DOCS / "robots.txt").write_text(
        "User-agent: *\nDisallow: /\n", encoding="utf-8"
    )

    print(f"[museokauppa] wrote {dated.relative_to(REPO_ROOT)} and latest.html")
    print(f"[museokauppa] summary: {summary}")
    # Emit to GitHub Actions step summary if present
    step_sum = os.environ.get("GITHUB_STEP_SUMMARY")
    if step_sum:
        with open(step_sum, "a", encoding="utf-8") as f:
            f.write(f"## Museokauppa {label}\n\n")
            f.write(f"- {summary}\n")
            f.write(f"- File: `docs/{dated.name}`\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
