#!/usr/bin/env python3
"""
Trip CSV → aggregated history JSON.

Takes the "Dashboard-packages" CSV that the Trip admin UI produces, strips
all PII, and writes a per-day aggregate JSON file that is safe to commit to
a public GitHub repo.

Usage:
    python3 scripts/trip_aggregate.py PATH/TO/Dashboard-packages.csv
    # writes → data/trip-aggregates.json

    python3 scripts/trip_aggregate.py PATH/TO/Dashboard-packages.csv \\
        --out some/other/path.json

What goes in:
    The raw Trip export containing 53 columns including customer names, emails,
    phone numbers, addresses. This file MUST NOT be committed to the public repo.

What goes out:
    A JSON with structure:
    {
      "updated": "2026-04-22T...Z",
      "source": "elamys-trip2 Dashboard-packages export",
      "assumptions": {...},
      "days": {
        "2026-04-21": {
          "orders_confirmed": N,       # Status=1
          "orders_pending":   N,       # Status=0
          "orders_cancelled": N,       # Status=2
          "gross_eur_confirmed": X,    # sum of Amount where Status=1
          "gross_eur_pending":   X,
          "by_package": {              # confirmed only
            "Raviristeily 2026": {"orders": N, "eur": X}, ...
          },
          "by_country": {"FI": N, ...} # confirmed only, ISO codes
        }, ...
      }
    }

Conventions:
    - "Confirmed" = Status=1. Only confirmed orders contribute to main KPIs.
    - "Pending" = Status=0. Tracked separately for pipeline view.
    - "Cancelled" = Status=2. Counted but excluded from EUR totals.
    - Sales day = Date column (order creation timestamp), truncated to date.
    - Test orders (package name containing "testi" case-insensitive) are
      excluded from the output unless --include-test is passed.
    - Non-EUR rows are included with their native amount recorded under
      gross_eur — v1 has no FX conversion. Note appears in assumptions.

No PII in the output: no names, emails, phones, addresses, customer IDs.
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


def parse_date(s: str):
    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").date()
    except Exception:
        return None


def f_amount(row: dict) -> float:
    try:
        return float(row.get("Amount") or 0)
    except Exception:
        return 0.0


def aggregate(rows: list[dict], include_test: bool = False) -> dict:
    """Group rows by sales date, collapse to PII-free per-day totals."""
    days: dict[str, dict] = defaultdict(lambda: {
        "orders_confirmed": 0,
        "orders_pending": 0,
        "orders_cancelled": 0,
        "gross_eur_confirmed": 0.0,
        "gross_eur_pending": 0.0,
        "by_package": defaultdict(lambda: {"orders": 0, "eur": 0.0}),
        "by_country": defaultdict(int),
        "currencies": defaultdict(int),
    })
    skipped_test = 0
    for r in rows:
        d = parse_date(r.get("Date", ""))
        if not d:
            continue
        name = (r.get("package.PackageName") or "").strip() or "(no package name)"
        if (not include_test) and "testi" in name.lower():
            skipped_test += 1
            continue
        status = (r.get("Status") or "").strip()
        amount = f_amount(r)
        currency = (r.get("Currency") or "").strip().upper() or "EUR"
        iso = (r.get("Owner.CountryISO") or "").strip().upper()
        key = d.isoformat()
        slot = days[key]
        slot["currencies"][currency] += 1
        if status == "1":
            slot["orders_confirmed"] += 1
            slot["gross_eur_confirmed"] += amount
            slot["by_package"][name]["orders"] += 1
            slot["by_package"][name]["eur"] += amount
            if iso:
                slot["by_country"][iso] += 1
        elif status == "0":
            slot["orders_pending"] += 1
            slot["gross_eur_pending"] += amount
        elif status == "2":
            slot["orders_cancelled"] += 1
        else:
            # Unknown status — count as pending for pipeline visibility
            slot["orders_pending"] += 1
            slot["gross_eur_pending"] += amount

    # Convert defaultdicts to plain dicts; round EUR to 2 decimals
    out = {}
    for k, v in days.items():
        out[k] = {
            "orders_confirmed": int(v["orders_confirmed"]),
            "orders_pending": int(v["orders_pending"]),
            "orders_cancelled": int(v["orders_cancelled"]),
            "gross_eur_confirmed": round(v["gross_eur_confirmed"], 2),
            "gross_eur_pending": round(v["gross_eur_pending"], 2),
            "by_package": {
                name: {"orders": int(d["orders"]), "eur": round(d["eur"], 2)}
                for name, d in v["by_package"].items()
            },
            "by_country": dict(v["by_country"]),
            "currencies": dict(v["currencies"]),
        }
    return {"days": out, "_skipped_test_rows": skipped_test}


def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("csv_path",
                   help="Path to the Trip Dashboard-packages CSV export")
    p.add_argument("--out", default=None,
                   help="Output JSON path (default: data/trip-aggregates.json "
                        "relative to repo root)")
    p.add_argument("--include-test", action="store_true",
                   help="Include rows whose package name contains 'testi' "
                        "(default: excluded)")
    args = p.parse_args(argv[1:])

    src = Path(args.csv_path).expanduser()
    if not src.exists():
        print(f"[trip_aggregate] ERROR: CSV not found at {src}", file=sys.stderr)
        return 2

    repo_root = Path(__file__).resolve().parent.parent
    out = Path(args.out) if args.out else (repo_root / "data" / "trip-aggregates.json")
    out.parent.mkdir(parents=True, exist_ok=True)

    with src.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    print(f"[trip_aggregate] read {len(rows)} rows from {src}")

    agg = aggregate(rows, include_test=args.include_test)
    payload = {
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": "elamys-trip2 Dashboard-packages export",
        "source_row_count": len(rows),
        "skipped_test_rows": agg["_skipped_test_rows"],
        "assumptions": {
            "sales_day_field": "Date (order creation timestamp, truncated to date)",
            "confirmed_status":  "Status=1",
            "pending_status":    "Status=0 or empty/unknown",
            "cancelled_status":  "Status=2",
            "currency_handling": "Non-EUR amounts recorded as-is — no FX conversion in v1",
            "test_rows":         "Excluded if package name contains 'testi' (case-insensitive)",
        },
        "days": agg["days"],
    }
    out.write_text(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False),
                   encoding="utf-8")
    print(f"[trip_aggregate] wrote {out} ({out.stat().st_size:,} bytes, "
          f"{len(agg['days'])} days, {agg['_skipped_test_rows']} test rows skipped)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
