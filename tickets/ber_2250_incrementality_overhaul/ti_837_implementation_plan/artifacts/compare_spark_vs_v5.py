#!/usr/bin/env python3
"""
TI-837 Phase 0 benchmark — compare Spark 7-day output to v5 BQ ground truth.

For each (segment, advertiser, group_name, intent_tier) cell present in BOTH
files, prints:
  - n_ips and visit-rate deltas
  - per-cell ATT deltas (treated_served minus holdout_biddable for both
    clickpass and guid)

Expected drift: ~0.1pp on win-rate-subsampled cells (Spark uses MD5 for
wr_bucket, v5 BQ used FARM_FINGERPRINT). Treated-side cells should match
exactly (same MD5 holdout hash, same source filters).

Usage:
  ~/.databricks-py312/bin/python tickets/.../artifacts/compare_spark_vs_v5.py
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
TICKET_ROOT = REPO_ROOT / "tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan"

V5_PATH = TICKET_ROOT / "outputs/ti_837_lift_30adv_7day_v5_2026_04_20_to_26.json"
SPARK_PATH = TICKET_ROOT / "outputs/ti_837_benchmark_phase0_spark.json"


def normalize_cell(row: dict) -> dict:
    """Coerce string-valued fields from v5 BQ JSON to numeric."""
    return {
        "segment": row["segment"],
        "advertiser_id": int(row["advertiser_id"]),
        "group_name": row["group_name"],
        "intent_tier": row["intent_tier"],
        "n_ips": int(row["n_ips"]),
        "clickpass_visitors": int(row["clickpass_visitors"]),
        "guid_visitors": int(row["guid_visitors"]),
        "clickpass_visit_rate": float(row["clickpass_visit_rate"] or 0),
        "guid_visit_rate": float(row["guid_visit_rate"] or 0),
    }


def key(c: dict):
    return (c["segment"], c["advertiser_id"], c["group_name"], c["intent_tier"])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cohort", type=int, nargs="+",
                        default=[31276, 31455, 38422])
    args = parser.parse_args()
    cohort = set(args.cohort)

    print(f"[compare] cohort: {sorted(cohort)}")
    print(f"[compare] v5:     {V5_PATH}")
    print(f"[compare] spark:  {SPARK_PATH}")

    v5_raw = json.loads(V5_PATH.read_text())
    spark_obj = json.loads(SPARK_PATH.read_text())

    v5_cells = {
        key(normalize_cell(r)): normalize_cell(r)
        for r in v5_raw
        if int(r["advertiser_id"]) in cohort
    }
    spark_cells = {
        key(normalize_cell(r)): normalize_cell(r)
        for r in spark_obj["rows"]
    }

    print(f"\n[compare] v5 cells (filtered to cohort):  {len(v5_cells)}")
    print(f"[compare] spark cells:                    {len(spark_cells)}")

    only_v5 = sorted(set(v5_cells) - set(spark_cells))
    only_sk = sorted(set(spark_cells) - set(v5_cells))
    both    = sorted(set(v5_cells) & set(spark_cells))

    if only_v5:
        print(f"\n[compare] cells in v5 but not spark ({len(only_v5)}):")
        for k in only_v5[:10]:
            print(f"  {k}  v5_n_ips={v5_cells[k]['n_ips']}")
    if only_sk:
        print(f"\n[compare] cells in spark but not v5 ({len(only_sk)}):")
        for k in only_sk[:10]:
            print(f"  {k}  spark_n_ips={spark_cells[k]['n_ips']}")

    print(f"\n[compare] PER-CELL DELTA TABLE  (n={len(both)} matching cells)")
    print(f"  segment,adv,grp,tier  n_ips_v5  n_ips_sk  Δn  cp_rate_v5  cp_rate_sk  Δcp_pp  guid_rate_v5  guid_rate_sk  Δguid_pp")
    n_match_n_ips = 0
    n_match_rates = 0
    rate_tol = 0.005  # 0.5pp
    for k in both:
        v = v5_cells[k]
        s = spark_cells[k]
        dn = s["n_ips"] - v["n_ips"]
        dcp = (s["clickpass_visit_rate"] - v["clickpass_visit_rate"]) * 100  # pp
        dgu = (s["guid_visit_rate"] - v["guid_visit_rate"]) * 100
        if abs(dn) < max(50, 0.01 * v["n_ips"]):
            n_match_n_ips += 1
        if abs(dcp) < rate_tol * 100 and abs(dgu) < rate_tol * 100:
            n_match_rates += 1
        # Compact one-line per cell
        print(
            f"  {k[0]:>6},{k[1]},{k[2]:>16},{k[3]:>10}  "
            f"{v['n_ips']:>9,}  {s['n_ips']:>9,}  {dn:+8,d}  "
            f"{v['clickpass_visit_rate']*100:>7.4f}%  {s['clickpass_visit_rate']*100:>7.4f}%  {dcp:+6.3f}pp  "
            f"{v['guid_visit_rate']*100:>7.4f}%  {s['guid_visit_rate']*100:>7.4f}%  {dgu:+6.3f}pp"
        )

    print()
    print(f"[compare] cells with |Δn| < max(50, 1%): {n_match_n_ips}/{len(both)}")
    print(f"[compare] cells with rate Δ < 0.5pp:     {n_match_rates}/{len(both)}")

    # Compute per-cell ATTs and compare
    # ATT = visit_rate(treated_served) - visit_rate(holdout_biddable)
    print(f"\n[compare] PER-(segment,adv,tier) ATT comparison")
    cell_keys = set()
    for k in both:
        seg, adv, _, tier = k
        cell_keys.add((seg, adv, tier))

    print(f"  segment,adv,tier  v5_cp_ATT  sk_cp_ATT  Δcp_ATT  v5_guid_ATT  sk_guid_ATT  Δguid_ATT")
    n_match_att = 0
    for ck in sorted(cell_keys):
        seg, adv, tier = ck
        kt_v5 = (seg, adv, "treated_served", tier)
        kh_v5 = (seg, adv, "holdout_biddable", tier)
        kt_sk = (seg, adv, "treated_served", tier)
        kh_sk = (seg, adv, "holdout_biddable", tier)
        if kt_v5 not in v5_cells or kh_v5 not in v5_cells: continue
        if kt_sk not in spark_cells or kh_sk not in spark_cells: continue
        v5_cp_att   = (v5_cells[kt_v5]["clickpass_visit_rate"] - v5_cells[kh_v5]["clickpass_visit_rate"]) * 100
        sk_cp_att   = (spark_cells[kt_sk]["clickpass_visit_rate"] - spark_cells[kh_sk]["clickpass_visit_rate"]) * 100
        v5_guid_att = (v5_cells[kt_v5]["guid_visit_rate"]      - v5_cells[kh_v5]["guid_visit_rate"]) * 100
        sk_guid_att = (spark_cells[kt_sk]["guid_visit_rate"]   - spark_cells[kh_sk]["guid_visit_rate"]) * 100
        d_cp = sk_cp_att - v5_cp_att
        d_gu = sk_guid_att - v5_guid_att
        if abs(d_cp) < 0.5 and abs(d_gu) < 0.5:
            n_match_att += 1
        print(f"  {seg:>6},{adv},{tier:>10}  {v5_cp_att:+7.3f}pp  {sk_cp_att:+7.3f}pp  {d_cp:+7.3f}pp  {v5_guid_att:+7.3f}pp  {sk_guid_att:+7.3f}pp  {d_gu:+7.3f}pp")

    print(f"\n[compare] ATT cells with |ΔATT| < 0.5pp on BOTH outcomes: {n_match_att}/{len(cell_keys)}")
    print(f"\n[compare] timings (Spark): {spark_obj.get('timings', {})}")


if __name__ == "__main__":
    main()
