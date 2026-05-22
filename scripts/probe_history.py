#!/usr/bin/env python3
"""
Museokauppa history probe — fetches 24 months of Bokún bookings for the
Museokortti channel and writes a slim+aggregated JSON for the summary dashboard.

Run on your Mac (Cowork sandbox cannot reach api.bokun.io):

    cd "$HOME/Documents/Claude/Projects/Elämys Group/museokauppa-cloud"
    python3 scripts/probe_history.py

Output:
    data/bokun-museokauppa.json     — bookings_slim + aggregates + summary

Stdlib only — no pip install needed.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import sys
import time
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from statistics import median
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
OUT = DATA_DIR / "bokun-museokauppa.json"

# ---- Credentials ---------------------------------------------------------

CREDS_PATH = REPO_ROOT.parent / ".bokun-credentials"  # Elämys Group/.bokun-credentials


def load_credentials() -> tuple[str, str, str]:
    if not CREDS_PATH.exists():
        sys.exit(f"Credentials file not found at {CREDS_PATH}")
    out: dict[str, str] = {}
    for line in CREDS_PATH.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        out[k.strip()] = v.strip().strip('"').strip("'")
    access = out.get("BOKUN_ACCESS_KEY", "") or os.environ.get("BOKUN_ACCESS_KEY", "")
    secret = out.get("BOKUN_SECRET_KEY", "") or os.environ.get("BOKUN_SECRET_KEY", "")
    base = (out.get("BOKUN_BASE_URL") or os.environ.get("BOKUN_BASE_URL")
            or "https://api.bokun.io").rstrip("/")
    if not access or not secret:
        sys.exit("Missing BOKUN_ACCESS_KEY or BOKUN_SECRET_KEY in .bokun-credentials")
    return access, secret, base


ACCESS, SECRET, BASE = load_credentials()


# ---- HMAC-SHA1 signing ---------------------------------------------------


def sign_headers(method: str, path: str) -> dict[str, str]:
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    raw = (date_str + ACCESS + method + path).encode("utf-8")
    signature = base64.b64encode(
        hmac.new(SECRET.encode("utf-8"), raw, hashlib.sha1).digest()
    ).decode("ascii")
    return {
        "X-Bokun-AccessKey": ACCESS,
        "X-Bokun-Date": date_str,
        "X-Bokun-Signature": signature,
        "Content-Type": "application/json;charset=UTF-8",
        "Accept": "application/json",
    }


def bokun_post(path: str, body: dict, timeout: int = 45) -> Any:
    data = json.dumps(body).encode("utf-8")
    last_err: Exception | None = None
    for attempt in range(3):
        req = urllib.request.Request(
            BASE + path, data=data, method="POST", headers=sign_headers("POST", path)
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8")
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as e:
            txt = e.read().decode("utf-8", errors="replace")[:400]
            if 500 <= e.code < 600 and attempt < 2:
                last_err = RuntimeError(f"HTTP {e.code} transient: {txt}")
                time.sleep(2 * (attempt + 1))
                continue
            raise RuntimeError(f"HTTP {e.code}: {txt}") from e
        except urllib.error.URLError as e:
            last_err = RuntimeError(f"URLError: {e}")
            if attempt < 2:
                time.sleep(2 * (attempt + 1))
                continue
            raise last_err from e
    assert last_err is not None
    raise last_err


# ---- Search bookings -----------------------------------------------------


def search_chunk(day_from: date, day_to: date) -> list[dict]:
    """Pull all product-bookings created in [day_from .. day_to]."""
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
        resp = bokun_post(path, body)
        if isinstance(resp, list):
            batch = resp
        elif isinstance(resp, dict):
            batch = resp.get("results") or resp.get("items") or resp.get("hits") or []
        else:
            batch = []
        out.extend(batch)
        if not batch or len(batch) < 200:
            break
        page += 1
        if page > 50:
            break
    return out


# ---- Slim mapping --------------------------------------------------------


def deep(obj: dict, *paths: str) -> Any:
    for p in paths:
        cur: Any = obj
        ok = True
        for part in p.split("."):
            if isinstance(cur, dict) and part in cur:
                cur = cur[part]
            else:
                ok = False
                break
        if ok and cur not in (None, ""):
            return cur
    return None


def ms_to_date(ms: Any) -> str | None:
    if ms is None:
        return None
    try:
        if isinstance(ms, (int, float)):
            return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).date().isoformat()
        s = str(ms)
        if "T" in s:
            return s[:10]
        if s.isdigit():
            return datetime.fromtimestamp(int(s) / 1000, tz=timezone.utc).date().isoformat()
        return s[:10]
    except Exception:
        return None


def to_slim(b: dict) -> dict:
    return {
        "id": b.get("id"),
        "status": b.get("status"),
        "channel_title": deep(b, "channel.title", "channelTitle"),
        "channel_id": deep(b, "channel.id", "channelId"),
        "channelId_marketplace": b.get("channelId"),
        "product_id": deep(b, "product.id", "productId"),
        "product_title": deep(b, "product.title", "product.name", "productTitle", "title") or "Unknown",
        "vendor_id": deep(b, "vendor.id", "vendorId"),
        "vendor_title": deep(b, "vendor.title", "vendor.companyName", "vendorTitle") or "",
        "seller_title": deep(b, "seller.title", "seller.companyName", "sellerTitle") or "",
        "product_category": b.get("productCategory") or b.get("productType"),
        "totalPriceAmount": deep(b, "totalPrice", "totalPriceAmount", "customerInvoice.totalAmount") or 0,
        "currency": deep(b, "currency", "totalPriceCurrency", "customerInvoice.currency") or "EUR",
        "discountAmount": deep(b, "discountAmount", "customerInvoice.discountAmount") or 0,
        "sellerCommission": deep(b, "sellerCommission", "resellerInvoice.totalAmount", "affiliateInvoice.totalAmount") or 0,
        "creationDate": ms_to_date(b.get("creationDate") or b.get("bookingCreationDate")),
        "startDate": ms_to_date(deep(b, "startDate", "startDateTime", "productBooking.startDate", "firstStartDate")),
        "totalParticipants": deep(b, "totalParticipants", "passengers", "productBooking.totalParticipants") or 0,
        "resold": bool(b.get("resold")),
        "country": deep(b, "customer.country", "customer.address.country", "customer.nationality") or "UNKNOWN",
        "rateTitle": deep(b, "rate.title", "rateTitle", "productBooking.rateTitle") or "",
    }


# ---- Aggregations --------------------------------------------------------


def aggregate(slim: list[dict]) -> dict:
    counted = [b for b in slim if b.get("status") in ("CONFIRMED", "ARRIVED")]
    by_day = defaultdict(lambda: {"orders_confirmed": 0, "gross_confirmed": 0.0,
                                  "orders_cancelled": 0, "gross_cancelled": 0.0,
                                  "pax_confirmed": 0})
    by_month = defaultdict(lambda: dict(by_day.default_factory()))
    by_product = defaultdict(lambda: {"orders_confirmed": 0, "gross_confirmed": 0.0,
                                      "orders_cancelled": 0, "gross_cancelled": 0.0,
                                      "pax_confirmed": 0, "title": ""})
    by_vendor = defaultdict(lambda: {"orders_confirmed": 0, "gross_confirmed": 0.0,
                                     "orders_cancelled": 0, "gross_cancelled": 0.0,
                                     "pax_confirmed": 0, "title": ""})
    by_country = defaultdict(lambda: {"orders_confirmed": 0, "gross_confirmed": 0.0,
                                      "orders_cancelled": 0, "gross_cancelled": 0.0,
                                      "pax_confirmed": 0})
    by_category = defaultdict(lambda: dict(by_country.default_factory()))

    lead_days: list[int] = []

    for b in slim:
        status = b.get("status")
        bucket = "confirmed" if status in ("CONFIRMED", "ARRIVED") else "cancelled"
        cd = b.get("creationDate") or ""
        ym = cd[:7] if cd else ""
        gross = float(b.get("totalPriceAmount") or 0)
        pax = int(b.get("totalParticipants") or 0)

        if cd:
            by_day[cd][f"orders_{bucket}"] += 1
            by_day[cd][f"gross_{bucket}"] += gross
            if bucket == "confirmed":
                by_day[cd]["pax_confirmed"] += pax
        if ym:
            by_month[ym][f"orders_{bucket}"] += 1
            by_month[ym][f"gross_{bucket}"] += gross
            if bucket == "confirmed":
                by_month[ym]["pax_confirmed"] += pax

        pid = str(b.get("product_id") or "?")
        by_product[pid][f"orders_{bucket}"] += 1
        by_product[pid][f"gross_{bucket}"] += gross
        if bucket == "confirmed":
            by_product[pid]["pax_confirmed"] += pax
        if b.get("product_title"):
            by_product[pid]["title"] = b["product_title"]

        vid = str(b.get("vendor_id") or "?")
        by_vendor[vid][f"orders_{bucket}"] += 1
        by_vendor[vid][f"gross_{bucket}"] += gross
        if bucket == "confirmed":
            by_vendor[vid]["pax_confirmed"] += pax
        if b.get("vendor_title"):
            by_vendor[vid]["title"] = b["vendor_title"]

        c = b.get("country") or "UNKNOWN"
        by_country[c][f"orders_{bucket}"] += 1
        by_country[c][f"gross_{bucket}"] += gross
        if bucket == "confirmed":
            by_country[c]["pax_confirmed"] += pax

        cat = b.get("product_category") or "UNKNOWN"
        by_category[cat][f"orders_{bucket}"] += 1
        by_category[cat][f"gross_{bucket}"] += gross
        if bucket == "confirmed":
            by_category[cat]["pax_confirmed"] += pax

        # lead time
        if bucket == "confirmed" and b.get("creationDate") and b.get("startDate"):
            try:
                d1 = date.fromisoformat(b["creationDate"])
                d2 = date.fromisoformat(b["startDate"])
                ld = (d2 - d1).days
                if ld >= 0:
                    lead_days.append(ld)
            except Exception:
                pass

    def pctile(xs: list[int], p: float) -> int:
        if not xs:
            return 0
        xs = sorted(xs)
        k = int((len(xs) - 1) * p)
        return xs[k]

    lead_stats = {
        "count": len(lead_days),
        "median_days": int(median(lead_days)) if lead_days else 0,
        "p25_days": pctile(lead_days, 0.25),
        "p75_days": pctile(lead_days, 0.75),
        "mean_days": round(sum(lead_days) / len(lead_days), 1) if lead_days else 0,
    }

    return {
        "by_day": dict(by_day),
        "by_month": dict(by_month),
        "by_product": dict(by_product),
        "by_vendor": dict(by_vendor),
        "by_country": dict(by_country),
        "by_category": dict(by_category),
        "lead_time_stats": lead_stats,
    }


# ---- Main ----------------------------------------------------------------


def main() -> None:
    # 24 months ending yesterday
    today = datetime.now(timezone.utc).date()
    end = today
    start = date(today.year - 2, today.month, 1)  # ~24 months back
    print(f"Probe: Museokortti channel, creationDate {start} → {end}")
    print(f"Base URL: {BASE}")
    print(f"Access key length: {len(ACCESS)}")
    print()

    # Loop month-by-month to respect Bokun's per-query result limits
    chunk_start = start
    all_raw: list[dict] = []
    seen_ids: set[Any] = set()
    chunks_done = 0
    while chunk_start <= end:
        next_month = (chunk_start.replace(day=28) + timedelta(days=4)).replace(day=1)
        chunk_end = min(next_month - timedelta(days=1), end)
        print(f"  {chunk_start} → {chunk_end} ...", end=" ", flush=True)
        try:
            batch = search_chunk(chunk_start, chunk_end)
        except Exception as e:
            print(f"ERROR {e}")
            break
        new = 0
        for b in batch:
            bid = b.get("id")
            if bid is None or bid in seen_ids:
                continue
            seen_ids.add(bid)
            all_raw.append(b)
            new += 1
        print(f"{len(batch)} returned, {new} new (running total: {len(all_raw)})")
        chunks_done += 1
        chunk_start = next_month
        time.sleep(0.4)

    print(f"\nTotal chunks: {chunks_done}, unique bookings: {len(all_raw)}")
    if not all_raw:
        sys.exit("No bookings returned — check credentials/scope.")

    # Sample
    first = all_raw[0]
    print("\nSample booking top-level keys:")
    print("  " + ", ".join(sorted(first.keys()))[:400])

    # Slim + aggregate
    slim = [to_slim(b) for b in all_raw]
    agg = aggregate(slim)
    status_dist = Counter(b.get("status") for b in slim)

    out = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "history_window": {"start": start.isoformat(), "end": end.isoformat()},
        "channel_label": "Museokortti",
        "museokauppa_count_unique": len(slim),
        "status_distribution": dict(status_dist),
        "aggregates": agg,
        "bookings_slim": slim,
    }

    OUT.write_text(json.dumps(out, ensure_ascii=False, indent=2))
    print(f"\n✓ Wrote {OUT.relative_to(REPO_ROOT)}")
    print(f"  Unique bookings: {len(slim)}")
    print(f"  Status: {dict(status_dist)}")
    confirmed_gross = sum(float(b.get('totalPriceAmount') or 0) for b in slim if b.get('status') in ('CONFIRMED', 'ARRIVED'))
    print(f"  Confirmed/arrived gross: {confirmed_gross:.0f} EUR")
    print(f"  Months covered: {len(agg['by_month'])}")
    print(f"  Unique products: {len(agg['by_product'])}")
    print(f"  Unique vendors: {len(agg['by_vendor'])}")


if __name__ == "__main__":
    main()
