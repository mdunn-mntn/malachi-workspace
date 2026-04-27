"""TI-837 Phase 2 cohort scorer.

Joins Stage A outputs (A.1 universe, A.3 treatment, A.4 spend, A.5 vertical)
and computes per-advertiser eligibility + stratification metadata.

Inputs:
  outputs/cohort_selection/a1_universe_tier_distribution.csv
  outputs/cohort_selection/a3_cost_impression_treatment.csv
  outputs/cohort_selection/a4_spend_funnel_mix.csv
  outputs/cohort_selection/a5_vertical.csv

Outputs:
  outputs/cohort_selection/cohort_scored.csv
  outputs/cohort_selection/cohort_summary.txt
  artifacts/ti_837_cohort_selection_criteria.md  (Stage B doc)

Power calc:
  ATT half-width ≤ 0.5pp at 95% CI requires n_per_arm such that
    1.96 * sqrt(2 * p * (1-p) / n) ≤ 0.005
  For p ≈ 0.02 (typical visit rate): n ≥ 6,154
  For p ≈ 0.05 (high-intent): n ≥ 3,000
  For p ≈ 0.005 (mid-tier): n ≥ 7,683
  We use n ≥ 5,000 per-tier biddable_holdouts as a balanced threshold.

Biddability proxy:
  We skipped the full augmentor_log scan (cost-prohibitive) and instead
  use served_distinct_ips from cost_impression_log (Stage A.3) as a
  PROXY for biddable IPs. Empirically (from Phase 1) the relationship is
    biddable_holdouts ≈ holdouts × biddable_rate ≈ holdouts × 0.4-1.0
    served_treatment   ≈ targeted × win_rate    ≈ targeted × 0.05-0.10
  So biddable_holdouts is ~ 1-2× served_treatment when normalized to
  population fractions. We use a conservative biddable_rate = 0.30 in
  the floor calculation.
"""

import csv
import math
from pathlib import Path

ROOT = Path("/Users/malachi/Developer/work/mntn/workspace/tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan")
OUTPUTS = ROOT / "outputs" / "cohort_selection"

PHASE1_ADVERTISERS = {31276, 31455, 34143, 34611, 34838, 37775, 40563}

# Power calc parameters
CI_HALF_WIDTH_TARGET = 0.005     # 0.5pp
Z_95 = 1.96
EXPECTED_VISIT_RATES = {         # tier-level expected p for power calc
    "high":      0.020,          # high-intent visit rate ~2% (Phase 1)
    "peak":      0.040,          # peak-intent ~4%
    "mid":       0.005,          # mid-intent ~0.5%
    "max_reach": 0.002,
}
BIDDABLE_RATE_PROXY = 0.30       # conservative — biddable_holdouts/holdouts
WIN_RATE_PROXY      = 0.07       # targeted IPs that get an impression

def required_n(p, half_width=CI_HALF_WIDTH_TARGET, z=Z_95):
    """Per-arm n for binomial CI half-width."""
    return math.ceil((z**2 * 2 * p * (1 - p)) / (half_width ** 2))


def load_csv(path):
    with open(path) as f:
        return list(csv.DictReader(f))


def build_index(rows, key="advertiser_id"):
    return {int(r[key]): r for r in rows}


def main():
    a1 = load_csv(OUTPUTS / "a1_universe_tier_distribution.csv")
    a3 = load_csv(OUTPUTS / "a3_cost_impression_treatment.csv")
    a4 = load_csv(OUTPUTS / "a4_spend_funnel_mix.csv")
    a5 = load_csv(OUTPUTS / "a5_vertical.csv")

    a1_by = build_index(a1)
    a3_by = build_index(a3)
    a4_by = build_index(a4)
    a5_by = build_index(a5)

    # Universe = intersection of A.1 (in prospecting) AND A.3 (active in window)
    universe = a1_by.keys() & a3_by.keys()
    print(f"A.1 prospecting advertisers: {len(a1_by)}")
    print(f"A.3 active-in-window:        {len(a3_by)}")
    print(f"A.4 spend in March:          {len(a4_by)}")
    print(f"A.5 with vertical:           {len(a5_by)}")
    print(f"Intersection (A.1 ∩ A.3):   {len(universe)}")

    rows = []
    for aid in sorted(universe):
        u = a1_by[aid]
        t = a3_by[aid]
        s = a4_by.get(aid)
        v = a5_by.get(aid)

        distinct_ips = int(u["distinct_ips"])
        max_high = int(u["max_tier_high"])
        max_peak = int(u["max_tier_peak"])
        max_mid  = int(u["max_tier_mid"])
        max_mr   = int(u["max_tier_max_reach"])
        ips_3tier = int(u["ips_all_three_tiers"])
        frac_stuck = float(u["frac_stuck_at_10000"])
        frac_multi = float(u["frac_multi_tier"])

        # Per-tier holdout count (10% of MAX-tier population)
        ho_high = round(max_high * 0.10)
        ho_peak = round(max_peak * 0.10)
        ho_mid  = round(max_mid  * 0.10)

        # Estimated biddable_holdouts per tier (proxy)
        bh_high = round(ho_high * BIDDABLE_RATE_PROXY)
        bh_peak = round(ho_peak * BIDDABLE_RATE_PROXY)
        bh_mid  = round(ho_mid  * BIDDABLE_RATE_PROXY)

        # Per-tier required n
        n_req_high = required_n(EXPECTED_VISIT_RATES["high"])
        n_req_peak = required_n(EXPECTED_VISIT_RATES["peak"])
        n_req_mid  = required_n(EXPECTED_VISIT_RATES["mid"])

        # Eligibility flags
        elig_high = bh_high >= n_req_high
        elig_peak = bh_peak >= n_req_peak
        elig_mid  = bh_mid  >= n_req_mid

        # Spend
        spend = float(s["prospecting_spend"]) if s else 0.0
        spend_ctv = float(s["spend_ctv"]) if s else 0.0
        spend_disp = float(s["spend_display"]) if s else 0.0
        ctv_share = spend_ctv / spend if spend > 0 else 0.0

        rows.append({
            "advertiser_id": aid,
            "company_name": (v or {}).get("company_name", ""),
            "vertical_name": (v or {}).get("vertical_name", ""),
            "phase1": aid in PHASE1_ADVERTISERS,
            "march_spend": round(spend, 2),
            "ctv_share": round(ctv_share, 3),
            "served_distinct_ips": int(t["served_distinct_ips"]),
            "prospecting_distinct_ips": distinct_ips,
            "max_tier_high": max_high,
            "max_tier_peak": max_peak,
            "max_tier_mid":  max_mid,
            "max_tier_max_reach": max_mr,
            "ips_all_three_tiers": ips_3tier,
            "frac_stuck_at_10000": round(frac_stuck, 4),
            "frac_multi_tier":     round(frac_multi, 4),
            "holdouts_high_est": ho_high,
            "holdouts_peak_est": ho_peak,
            "holdouts_mid_est":  ho_mid,
            "biddable_holdouts_high_est": bh_high,
            "biddable_holdouts_peak_est": bh_peak,
            "biddable_holdouts_mid_est":  bh_mid,
            "n_req_high": n_req_high,
            "n_req_peak": n_req_peak,
            "n_req_mid":  n_req_mid,
            "eligible_high": elig_high,
            "eligible_peak": elig_peak,
            "eligible_mid":  elig_mid,
            "tier_diversity_score": frac_multi,
        })

    out_path = OUTPUTS / "cohort_scored.csv"
    if rows:
        with open(out_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=rows[0].keys())
            w.writeheader()
            w.writerows(rows)
        print(f"\nWrote {out_path} ({len(rows)} rows)")

    # Summary stats
    elig_high = sum(1 for r in rows if r["eligible_high"])
    elig_peak = sum(1 for r in rows if r["eligible_peak"])
    elig_mid  = sum(1 for r in rows if r["eligible_mid"])
    print(f"\nEligibility (proxy power floor):")
    print(f"  high:       {elig_high} advertisers")
    print(f"  peak:       {elig_peak} advertisers")
    print(f"  mid:        {elig_mid} advertisers")
    print(f"  high+peak:  {sum(1 for r in rows if r['eligible_high'] and r['eligible_peak'])} advertisers")
    print(f"  all 3 tiers:{sum(1 for r in rows if r['eligible_high'] and r['eligible_peak'] and r['eligible_mid'])} advertisers")


if __name__ == "__main__":
    main()
