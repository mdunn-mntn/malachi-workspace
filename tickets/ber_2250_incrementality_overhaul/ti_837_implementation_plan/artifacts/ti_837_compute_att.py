"""TI-837: Compute ATT and propensity-matched ATT from BQ lift query output.

Input: JSON output of ti_837_lift_analysis.sql (one row per group_name × intent_tier).
Output: per-tier ATT + weighted-overall ATT, both for clickpass and guid outcomes,
        with two-proportion z-test and 95% Wald CIs.

Usage:
    python ti_837_compute_att.py outputs/ti_837_lift_zazzle_1day_2026_04_24.json
"""
import json
import math
import sys
from pathlib import Path


def two_prop_z(p1, n1, p0, n0):
    """Two-proportion z-test on visit rates. Returns (diff, se, z, p_value, ci_low, ci_high)."""
    if n1 == 0 or n0 == 0:
        return 0.0, 0.0, 0.0, 1.0, 0.0, 0.0
    diff = p1 - p0
    p_pool = (p1 * n1 + p0 * n0) / (n1 + n0)
    se = math.sqrt(p_pool * (1 - p_pool) * (1 / n1 + 1 / n0)) if (0 < p_pool < 1) else 0.0
    z = diff / se if se > 0 else 0.0
    # two-sided p
    from math import erf, sqrt
    p_value = 2 * (1 - 0.5 * (1 + erf(abs(z) / sqrt(2))))
    se_diff = math.sqrt(
        (p1 * (1 - p1) / n1 if n1 > 0 else 0) + (p0 * (1 - p0) / n0 if n0 > 0 else 0)
    )
    return diff, se, z, p_value, diff - 1.96 * se_diff, diff + 1.96 * se_diff


def main(path):
    rows = json.loads(Path(path).read_text())
    by_group = {}
    for r in rows:
        g = r["group_name"]
        t = r["intent_tier"]
        by_group.setdefault(t, {})[g] = {
            "n_ips": int(r["n_ips"]),
            "clickpass_visitors": int(r["clickpass_visitors"]),
            "guid_visitors": int(r["guid_visitors"]),
            "clickpass_visit_rate": float(r["clickpass_visit_rate"] or 0),
            "guid_visit_rate": float(r["guid_visit_rate"] or 0),
        }

    print("=" * 88)
    print(f"{'tier':<10} {'outcome':<10} {'tx_rate':>9} {'ctrl_rate':>9} {'lift_pp':>9} {'p':>8} {'ci_low':>9} {'ci_high':>9}")
    print("-" * 88)

    weighted = {"clickpass": [], "guid": []}
    for tier, groups in by_group.items():
        if "treated_served" not in groups or "holdout_biddable" not in groups:
            continue
        tx, ctrl = groups["treated_served"], groups["holdout_biddable"]
        for outcome in ("clickpass", "guid"):
            p1, n1 = tx[f"{outcome}_visit_rate"], tx["n_ips"]
            p0, n0 = ctrl[f"{outcome}_visit_rate"], ctrl["n_ips"]
            diff, se, z, pval, lo, hi = two_prop_z(p1, n1, p0, n0)
            print(f"{tier:<10} {outcome:<10} {p1:>9.4%} {p0:>9.4%} {diff*100:>8.3f}pp {pval:>8.4f} {lo*100:>8.3f}pp {hi*100:>8.3f}pp")
            weighted[outcome].append((diff, n1))  # weight by treatment size (ATT weighting)

    print("-" * 88)
    for outcome, vals in weighted.items():
        if not vals:
            continue
        total_w = sum(w for _, w in vals)
        if total_w == 0:
            continue
        weighted_att = sum(d * w for d, w in vals) / total_w
        print(f"weighted ATT (ATT-style stratification) — {outcome:<10}: {weighted_att*100:>7.3f}pp  (n_treated={total_w:,})")
    print("=" * 88)


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "outputs/ti_837_lift_zazzle_1day_2026_04_24.json")
