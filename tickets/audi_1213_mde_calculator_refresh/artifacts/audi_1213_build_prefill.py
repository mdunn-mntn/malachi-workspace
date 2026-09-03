"""Build the refreshed MDE-calculator prefill payload from the AUDI-1213 metrics CSV."""
import csv
import json
import statistics
import sys
from pathlib import Path

TICKET = Path(__file__).resolve().parents[1]
CSV_PATH = TICKET / "outputs" / "audi_1213_prefill_metrics.csv"
JSON_PATH = TICKET / "outputs" / "audi_1213_prefill_compact.json"


def load_rows(path):
    with open(path, newline="") as fh:
        return list(csv.DictReader(fh))


def to_record(row):
    return {
        "id": int(row["advertiser_id"]),
        "name": row["advertiser_name"],
        "spend30": round(float(row["spend_30d"]), 2),
        "imps30": int(row["impressions_30d"]),
        "ips30": int(row["distinct_ips_30d"]),
        "cpm": round(float(row["cpm"]), 4),
        "impsIp": round(float(row["imps_per_ip"]), 4),
        "pVisit": round(float(row["p_visit"]), 6),
        "pCvr": round(float(row["p_cvr"]), 6),
        "typical": round(float(row["typical_active_month_spend"]), 2),
        "maxMo": round(float(row["max_month_spend"]), 2),
        "months": int(row["active_months_count"]),
    }


def cohort_defaults(records):
    positive = lambda key: [r[key] for r in records if r[key] > 0]
    return {
        "cpm": round(statistics.median(positive("cpm")), 2),
        "impsIp": round(statistics.median(positive("impsIp")), 2),
        "ivr": round(statistics.median(positive("pVisit")) * 100, 3),
        "cvr": round(statistics.median(positive("pCvr")) * 100, 3),
    }


def main():
    records = [to_record(r) for r in load_rows(CSV_PATH)]
    records.sort(key=lambda r: r["spend30"], reverse=True)
    cohort = cohort_defaults(records)
    payload = {"cohort": cohort, "advertisers": records}
    JSON_PATH.write_text(json.dumps(payload, separators=(",", ":")))
    print(f"advertisers {len(records)}")
    print(f"cohort {cohort}")
    print(f"json {JSON_PATH.stat().st_size / 1024:.0f} KB")
    top = records[:5]
    for r in top:
        print(f"  {r['id']:>6} {r['name'][:34]:<34} ${r['spend30']:>12,.0f} cpm {r['cpm']:>7.2f} ivr {r['pVisit']*100:.2f}%")


if __name__ == "__main__":
    sys.exit(main())
