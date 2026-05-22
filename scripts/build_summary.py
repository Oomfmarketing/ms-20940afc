#!/usr/bin/env python3
"""
Build the Museomatkat summary dashboard from the probe JSON.

Takes:  data/bokun-museokauppa.json  (created by scripts/probe_history.py)
Writes: docs/summary.html             (with embedded data, ready to commit)

Reads the existing docs/summary.html as a template and replaces the
__DATA_JSON__ placeholder (or any existing const DATA = {...}; literal)
with a freshly-computed compact JSON.

Stdlib only.
"""
from __future__ import annotations

import json
import re
import sys
from collections import Counter, defaultdict, OrderedDict
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PROBE_FILE = REPO_ROOT / "data" / "bokun-museokauppa.json"
HTML_FILE = REPO_ROOT / "docs" / "summary.html"

# Channel scope: only "ELAMYSSUOMI activity shop" — same as the daily dashboard.
# The probe pulls everything visible to the API key (Sweden Arena, Book Turku,
# DCS Plus, …); the daily Museokauppa scope is just channel 212670.
MUSEOKAUPPA_CHANNEL_ID = 212670


def filter_bookings(bookings: list[dict]) -> list[dict]:
    return [b for b in bookings if b.get("channel_id") == MUSEOKAUPPA_CHANNEL_ID]


def reaggregate(bookings: list[dict]) -> dict:
    """Recompute the same aggregate shape as probe_history.aggregate, but from
    a filtered subset of bookings_slim."""
    from collections import defaultdict
    from statistics import median
    from datetime import date as _date

    counted = [b for b in bookings if b.get("status") in ("CONFIRMED", "ARRIVED")]
    by_day = defaultdict(lambda: {"orders_confirmed": 0, "gross_confirmed": 0.0,
                                  "orders_cancelled": 0, "gross_cancelled": 0.0,
                                  "pax_confirmed": 0})
    by_month = defaultdict(lambda: {"orders_confirmed": 0, "gross_confirmed": 0.0,
                                    "orders_cancelled": 0, "gross_cancelled": 0.0,
                                    "pax_confirmed": 0})
    by_product = defaultdict(lambda: {"orders_confirmed": 0, "gross_confirmed": 0.0,
                                      "orders_cancelled": 0, "gross_cancelled": 0.0,
                                      "pax_confirmed": 0, "title": ""})
    by_vendor = defaultdict(lambda: {"orders_confirmed": 0, "gross_confirmed": 0.0,
                                     "orders_cancelled": 0, "gross_cancelled": 0.0,
                                     "pax_confirmed": 0, "title": ""})
    by_country = defaultdict(lambda: {"orders_confirmed": 0, "gross_confirmed": 0.0,
                                      "orders_cancelled": 0, "gross_cancelled": 0.0,
                                      "pax_confirmed": 0})
    lead_days: list[int] = []

    for b in bookings:
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

        if bucket == "confirmed" and b.get("creationDate") and b.get("startDate"):
            try:
                d1 = _date.fromisoformat(b["creationDate"])
                d2 = _date.fromisoformat(b["startDate"])
                ld = (d2 - d1).days
                if ld >= 0:
                    lead_days.append(ld)
            except Exception:
                pass

    def pctile(xs, p):
        if not xs:
            return 0
        xs = sorted(xs)
        return xs[int((len(xs) - 1) * p)]

    return {
        "by_day": dict(by_day),
        "by_month": dict(by_month),
        "by_product": dict(by_product),
        "by_vendor": dict(by_vendor),
        "by_country": dict(by_country),
        "lead_time_stats": {
            "count": len(lead_days),
            "median_days": int(median(lead_days)) if lead_days else 0,
            "p25_days": pctile(lead_days, 0.25),
            "p75_days": pctile(lead_days, 0.75),
            "mean_days": round(sum(lead_days) / len(lead_days), 1) if lead_days else 0,
        },
    }


def build_dashboard_data(probe: dict) -> dict:
    agg = probe["aggregates"]
    bookings = probe["bookings_slim"]
    counted = [b for b in bookings if b.get("status") in ("CONFIRMED", "ARRIVED")]

    by_month = OrderedDict(sorted(agg["by_month"].items()))
    months_list = [
        {"ym": k, "orders": v["orders_confirmed"], "gross": round(v["gross_confirmed"]),
         "pax": v["pax_confirmed"]}
        for k, v in by_month.items()
    ]

    vendors = sorted(
        [(vid, v) for vid, v in agg["by_vendor"].items()],
        key=lambda kv: kv[1]["orders_confirmed"], reverse=True,
    )
    vendors_list = [
        {
            "title": v.get("title", "?").strip(),
            "orders": v["orders_confirmed"],
            "gross": round(v["gross_confirmed"]),
            "pax": v["pax_confirmed"],
            "white_label": ("elämys" in v.get("title", "").lower()),
        }
        for _vid, v in vendors
    ]

    countries = sorted(
        [(c, v) for c, v in agg["by_country"].items()],
        key=lambda kv: kv[1]["orders_confirmed"], reverse=True,
    )
    country_list = [
        {"code": c, "orders": v["orders_confirmed"],
         "gross": round(v["gross_confirmed"]), "pax": v["pax_confirmed"]}
        for c, v in countries
    ]

    all_products = sorted(
        [
            {"title": v["title"], "orders": v["orders_confirmed"],
             "gross": round(v["gross_confirmed"]), "pax": v["pax_confirmed"]}
            for _pid, v in agg["by_product"].items()
        ],
        key=lambda x: (-x["orders"], -x["gross"]),
    )

    # Monthly top-3 from raw bookings
    mp: dict[str, dict[str, dict]] = defaultdict(
        lambda: defaultdict(lambda: {"orders": 0, "gross": 0.0, "pax": 0, "title": ""})
    )
    for b in counted:
        month = (b.get("creationDate") or "")[:7]
        pid = str(b.get("product_id"))
        if not month or pid == "None":
            continue
        cell = mp[month][pid]
        cell["orders"] += 1
        cell["gross"] += float(b.get("totalPriceAmount") or 0)
        cell["pax"] += int(b.get("totalParticipants") or 0)
        cell["title"] = b.get("product_title") or cell["title"]

    monthly_top = []
    for month in sorted(mp.keys()):
        prods = sorted(mp[month].items(), key=lambda kv: (-kv[1]["orders"], -kv[1]["gross"]))
        total_orders = sum(v["orders"] for _, v in prods)
        monthly_top.append({
            "ym": month,
            "month_total_orders": total_orders,
            "top3": [
                {"title": v["title"], "orders": v["orders"], "gross": round(v["gross"]),
                 "pax": v["pax"]}
                for _pid, v in prods[:3]
            ],
        })

    # YoY Jan-May 2025 vs 2026
    yoy_month: dict[str, dict[str, float]] = defaultdict(lambda: {"orders": 0, "gross": 0.0})
    for b in counted:
        cd = b.get("creationDate") or ""
        if cd:
            yoy_month[cd[:7]]["orders"] += 1
            yoy_month[cd[:7]]["gross"] += float(b.get("totalPriceAmount") or 0)
    yoy_compare = []
    for mo in ["01", "02", "03", "04", "05"]:
        y25 = yoy_month.get(f"2025-{mo}", {"orders": 0, "gross": 0.0})
        y26 = yoy_month.get(f"2026-{mo}", {"orders": 0, "gross": 0.0})
        yoy_compare.append({
            "month": mo,
            "y25_orders": int(y25["orders"]), "y25_gross": round(y25["gross"]),
            "y26_orders": int(y26["orders"]), "y26_gross": round(y26["gross"]),
        })

    # Summer compare (Jun-Aug startDate)
    season: dict[str, dict[str, float]] = defaultdict(lambda: {"orders": 0, "gross": 0.0, "pax": 0})
    for b in counted:
        sd = b.get("startDate") or ""
        if len(sd) >= 7 and sd[5:7] in ("06", "07", "08"):
            y = sd[:4]
            season[y]["orders"] += 1
            season[y]["gross"] += float(b.get("totalPriceAmount") or 0)
            season[y]["pax"] += int(b.get("totalParticipants") or 0)
    summer_compare = [
        {"year": y, "orders": int(v["orders"]), "gross": round(v["gross"]), "pax": int(v["pax"])}
        for y, v in sorted(season.items())
    ]

    # Vendor mix shift 2024 → 2025
    vm = {"2024": defaultdict(int), "2025": defaultdict(int)}
    for b in counted:
        y = (b.get("creationDate") or "")[:4]
        if y in vm:
            t = (b.get("vendor_title") or "?").strip()
            if t.lower().startswith("elämys"):
                t = "Elämys Group (white-label)"
            vm[y][t] += 1
    union = sorted(
        {v for y in vm for v in vm[y]},
        key=lambda x: -(vm["2024"].get(x, 0) + vm["2025"].get(x, 0)),
    )[:8]
    tot24 = sum(vm["2024"].values()) or 1
    tot25 = sum(vm["2025"].values()) or 1
    vendor_mix_shift = []
    for t in union:
        n24 = vm["2024"].get(t, 0)
        n25 = vm["2025"].get(t, 0)
        vendor_mix_shift.append({
            "title": t,
            "n24": n24, "p24": round(n24 / tot24 * 100, 1),
            "n25": n25, "p25": round(n25 / tot25 * 100, 1),
        })

    # Resold + commission
    resold_yes = sum(1 for b in counted if b.get("resold"))
    total_gross = sum(float(b.get("totalPriceAmount") or 0) for b in counted) or 1
    total_comm = sum(float(b.get("sellerCommission") or 0) for b in counted)
    arch_facts = {
        "resold_count": resold_yes,
        "total_count": len(counted),
        "resold_pct": round(resold_yes / max(1, len(counted)) * 100),
        "commission_ratio_pct": round(total_comm / total_gross * 100, 1),
        "total_commission": round(total_comm),
    }

    totals_orders = sum(v["orders_confirmed"] for v in by_month.values())
    return {
        "window": probe["history_window"],
        "fetched_at": probe["fetched_at"],
        "totals": {
            "orders": totals_orders,
            "gross": round(sum(v["gross_confirmed"] for v in by_month.values())),
            "pax": sum(v["pax_confirmed"] for v in by_month.values()),
            "vendors": len(vendors),
            "products": len(agg["by_product"]),
        },
        "status_distribution": probe["status_distribution"],
        "lead_time": agg["lead_time_stats"],
        "months": months_list,
        "products_all": all_products,
        "monthly_top": monthly_top,
        "vendors": vendors_list,
        "countries": country_list,
        "yoy_jan_may": yoy_compare,
        "summer_compare": summer_compare,
        "vendor_mix_shift": vendor_mix_shift,
        "arch_facts": arch_facts,
    }


def inline_into_html(template_html: str, data: dict) -> str:
    data_str = json.dumps(data, ensure_ascii=False)
    new_line = f"const DATA = {data_str};\n"

    # Try placeholder first, then existing DATA literal
    if "__DATA_JSON__" in template_html:
        return template_html.replace("__DATA_JSON__", data_str, 1)
    pattern = re.compile(r"const DATA\s*=\s*\{.*?\};\s*\n", re.DOTALL)
    new_html, n = pattern.subn(new_line, template_html, count=1)
    if n != 1:
        sys.exit("Could not find __DATA_JSON__ placeholder or existing DATA literal in template.")
    return new_html


def main() -> None:
    if not PROBE_FILE.exists():
        sys.exit(f"Probe data missing: {PROBE_FILE} — run scripts/probe_history.py first.")
    if not HTML_FILE.exists():
        sys.exit(f"Template missing: {HTML_FILE}")

    probe = json.loads(PROBE_FILE.read_text())
    template = HTML_FILE.read_text()

    # Filter probe to Museokauppa channel only
    full_bookings = probe["bookings_slim"]
    museo_bookings = filter_bookings(full_bookings)
    print(f"Filtered: {len(museo_bookings)}/{len(full_bookings)} bookings in channel "
          f"{MUSEOKAUPPA_CHANNEL_ID} (ELAMYSSUOMI activity shop)")

    # Re-aggregate from the filtered subset and rebuild a probe-shaped dict
    from collections import Counter
    filtered_probe = {
        "fetched_at": probe["fetched_at"],
        "history_window": probe["history_window"],
        "channel_label": "Museokauppa (ELAMYSSUOMI activity shop)",
        "status_distribution": dict(Counter(b.get("status") for b in museo_bookings)),
        "aggregates": reaggregate(museo_bookings),
        "bookings_slim": museo_bookings,
    }

    data = build_dashboard_data(filtered_probe)

    out = inline_into_html(template, data)
    HTML_FILE.write_text(out)
    print(f"✓ Wrote {HTML_FILE.relative_to(REPO_ROOT)}")
    print(f"  Size: {len(out)/1024:.1f} KB")
    print(f"  KPIs: {data['totals']}")
    print(f"  Months: {len(data['months'])}  Products: {len(data['products_all'])}  Vendors: {len(data['vendors'])}")


if __name__ == "__main__":
    main()
