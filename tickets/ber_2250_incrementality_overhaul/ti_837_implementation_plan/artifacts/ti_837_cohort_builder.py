"""TI-837 Phase 2 cohort builder — Stage C.

Reads cohort_scored.csv (Stage A.6 output) and constructs the final
stratified cohort.

Strategy:
1. Filter to eligible advertisers (per Stage B inclusion gates).
2. Stratify by (spend_tercile, vertical_top, ctv_dominant) cells.
3. Within each cell, take the top K advertisers by tier-diversity score
   (`frac_multi_tier`), tiebreak by `biddable_holdouts_high_est` (volume).
4. Top up to target N by adding next-best advertisers across all cells
   ranked by composite (volume × tier-diversity).

Output:
  outputs/cohort_selection/cohort_final.csv
  artifacts/ti_837_phase2_cohort.md
"""

import csv
from collections import defaultdict
from pathlib import Path

ROOT = Path("/Users/malachi/Developer/work/mntn/workspace/tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan")
OUTPUTS = ROOT / "outputs" / "cohort_selection"
ARTIFACTS = ROOT / "artifacts"

PHASE1_ADVERTISERS = {31276, 31455, 34143, 34611, 34838, 37775, 40563}
TARGET_N = 30

# Eligibility (Stage B gates)
MIN_BIDDABLE_HOLDOUTS = 5_000      # any of {high, peak, mid}
MIN_FRAC_MULTI_TIER   = 0.05       # tier-diversity proxy: 1 - max_high/distinct_ips
MIN_MARCH_SPEND       = 5_000
MIN_SERVED_IPS        = 100        # already filtered in A.3

# Verticals to anchor the stratification (top categories from A.5)
TOP_VERTICALS = {
    "Apparel", "B2B Software & Services", "Education", "Finance",
    "Fitness & Health", "Healthcare", "Home Improvement",
    "Home Services & Repairs", "Household Goods",
    "Vitamins, Supplements & Health Stores",
}


def load_scored():
    path = OUTPUTS / "cohort_scored.csv"
    with open(path) as f:
        rows = list(csv.DictReader(f))
    for r in rows:
        r["advertiser_id"] = int(r["advertiser_id"])
        r["march_spend"] = float(r["march_spend"])
        r["ctv_share"] = float(r["ctv_share"])
        r["served_distinct_ips"] = int(r["served_distinct_ips"])
        r["frac_multi_tier"] = float(r["frac_multi_tier"])
        r["frac_stuck_at_10000"] = float(r["frac_stuck_at_10000"])
        for k in ("biddable_holdouts_high_est", "biddable_holdouts_peak_est",
                  "biddable_holdouts_mid_est", "max_tier_high",
                  "max_tier_peak", "max_tier_mid"):
            r[k] = int(r[k])
        for k in ("eligible_high", "eligible_peak", "eligible_mid", "phase1"):
            r[k] = (r[k] == "True")
    return rows


def is_eligible(r):
    if r["march_spend"] < MIN_MARCH_SPEND:
        return False
    if r["frac_multi_tier"] < MIN_FRAC_MULTI_TIER:
        return False
    has_any_tier = (
        r["biddable_holdouts_high_est"] >= MIN_BIDDABLE_HOLDOUTS
        or r["biddable_holdouts_peak_est"] >= MIN_BIDDABLE_HOLDOUTS
        or r["biddable_holdouts_mid_est"] >= MIN_BIDDABLE_HOLDOUTS
    )
    return has_any_tier


def stratify(rows):
    eligible = [r for r in rows if is_eligible(r)]
    print(f"Eligible: {len(eligible)} of {len(rows)}")

    # Spend terciles within eligible pool
    eligible_by_spend = sorted(eligible, key=lambda r: -r["march_spend"])
    n = len(eligible_by_spend)
    t1 = n // 3
    t2 = (2 * n) // 3
    spend_tier = {}
    for i, r in enumerate(eligible_by_spend):
        if i < t1:
            spend_tier[r["advertiser_id"]] = "high"
        elif i < t2:
            spend_tier[r["advertiser_id"]] = "mid"
        else:
            spend_tier[r["advertiser_id"]] = "low"

    # Cell assignment: (spend_tier × vertical_anchor)
    cells = defaultdict(list)
    for r in eligible:
        v = r["vertical_name"] if r["vertical_name"] in TOP_VERTICALS else "Other"
        s = spend_tier[r["advertiser_id"]]
        cells[(s, v)].append(r)
    return eligible, cells, spend_tier


def composite_score(r):
    """Higher is better. Combines volume (high-tier biddable_holdouts) and
    tier diversity. Used to rank within-cell."""
    return r["biddable_holdouts_high_est"] * (0.5 + r["frac_multi_tier"])


def build_cohort(eligible, cells, target_n=TARGET_N):
    selected = set()
    rationale = {}
    used_audiences = set()  # (high, peak, mid) tuples — dedupe sister companies

    def audience_signature(r):
        return (r["max_tier_high"], r["max_tier_peak"], r["max_tier_mid"])

    # Pass 1: take top-1 per (spend × vertical) cell, skip duplicate audiences
    for cell_key, members in cells.items():
        members.sort(key=composite_score, reverse=True)
        for r in members:
            sig = audience_signature(r)
            if sig in used_audiences:
                continue
            selected.add(r["advertiser_id"])
            used_audiences.add(sig)
            rationale[r["advertiser_id"]] = f"cell-anchor ({cell_key[0]} spend × {cell_key[1]})"
            break

    # Pass 2: top up by composite score, skip duplicate audiences
    if len(selected) < target_n:
        remaining = [r for r in eligible if r["advertiser_id"] not in selected]
        remaining.sort(key=composite_score, reverse=True)
        for r in remaining:
            if len(selected) >= target_n:
                break
            sig = audience_signature(r)
            if sig in used_audiences:
                continue
            selected.add(r["advertiser_id"])
            used_audiences.add(sig)
            rationale[r["advertiser_id"]] = "topup (next-best by composite score)"

    final = [r for r in eligible if r["advertiser_id"] in selected]
    return final, rationale


def main():
    rows = load_scored()
    eligible, cells, spend_tier = stratify(rows)
    final, rationale = build_cohort(eligible, cells)

    # Write final CSV
    out = OUTPUTS / "cohort_final.csv"
    with open(out, "w", newline="") as f:
        cols = ["advertiser_id", "company_name", "vertical_name", "march_spend",
                "ctv_share", "served_distinct_ips", "frac_multi_tier",
                "frac_stuck_at_10000", "max_tier_high", "max_tier_peak",
                "max_tier_mid", "biddable_holdouts_high_est",
                "biddable_holdouts_peak_est", "biddable_holdouts_mid_est",
                "eligible_high", "eligible_peak", "eligible_mid",
                "phase1", "spend_tier", "rationale"]
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in final:
            row = {k: r.get(k) for k in cols}
            row["spend_tier"] = spend_tier.get(r["advertiser_id"], "")
            row["rationale"] = rationale.get(r["advertiser_id"], "")
            w.writerow(row)
    print(f"Wrote cohort_final.csv ({len(final)} rows)")

    # Cohort summary
    print(f"\nFinal cohort: {len(final)} advertisers")
    for r in sorted(final, key=lambda x: -x["march_spend"]):
        print(f"  {r['advertiser_id']:>6} | {r['company_name'][:30]:<30} "
              f"| spend ${r['march_spend']:>10,.0f} | "
              f"{r['vertical_name'][:25]:<25} | "
              f"diversity {r['frac_multi_tier']:.2f} | "
              f"high-bh-est {r['biddable_holdouts_high_est']:>7,d}")


if __name__ == "__main__":
    main()
