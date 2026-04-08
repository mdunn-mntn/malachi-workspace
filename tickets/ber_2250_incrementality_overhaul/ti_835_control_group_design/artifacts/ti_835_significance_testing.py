"""
TI-835: Statistical Significance Testing for Observational Incrementality Analysis

Tests whether the observed holdout vs targeted visit proportions differ from
the null hypothesis (10% holdout / 90% targeted = no ad effect).

Methods:
- Binomial test: P(targeted_visits | n_total, p=0.9) under null
- Bootstrap CIs for lift estimates
- Benjamini-Hochberg FDR correction across advertisers
"""

import csv
import json
import os
import sys
from pathlib import Path

import numpy as np
from scipy import stats

TICKET_DIR = Path(__file__).parent.parent
OUTPUTS_DIR = TICKET_DIR / "outputs"
ARTIFACTS_DIR = TICKET_DIR / "artifacts"

NULL_TARGETED_PROPORTION = 0.9  # under null, 90% of visitors are in targeted group


def load_data(filepath):
    """Load BQ results CSV and pivot to per-advertiser holdout/targeted pairs."""
    advertisers = {}
    with open(filepath) as f:
        reader = csv.DictReader(f)
        for row in reader:
            aid = int(row["advertiser_id"])
            name = row["company_name"]
            group = row["group_name"]
            visitors = int(row["unique_visitors"])
            visits = int(row["total_visits"])
            if aid not in advertisers:
                advertisers[aid] = {"name": name}
            advertisers[aid][f"{group}_visitors"] = visitors
            advertisers[aid][f"{group}_visits"] = visits
    return advertisers


def binomial_test(targeted, total, null_p=NULL_TARGETED_PROPORTION):
    """Two-sided binomial test: is the targeted proportion different from null_p?"""
    result = stats.binomtest(targeted, total, null_p, alternative="two-sided")
    return result.pvalue


def bootstrap_lift_ci(holdout, targeted, n_boot=10000, ci=0.95, seed=42):
    """Bootstrap CI for lift = (observed_ratio / expected_ratio) - 1.

    Expected ratio under null = 9.0 (90% targeted / 10% holdout).
    Observed ratio = targeted / holdout.
    Lift = (observed_ratio / 9.0) - 1.
    """
    rng = np.random.default_rng(seed)
    total = holdout + targeted
    observed_p = targeted / total  # proportion that are targeted

    # parametric bootstrap: sample from binomial(total, observed_p)
    boot_targeted = rng.binomial(total, observed_p, size=n_boot)
    boot_holdout = total - boot_targeted

    # avoid division by zero
    valid = boot_holdout > 0
    boot_ratio = np.full(n_boot, np.nan)
    boot_ratio[valid] = boot_targeted[valid] / boot_holdout[valid]
    boot_lift = boot_ratio / 9.0 - 1.0

    alpha = 1 - ci
    lo = np.nanpercentile(boot_lift, 100 * alpha / 2)
    hi = np.nanpercentile(boot_lift, 100 * (1 - alpha / 2))
    point = (targeted / holdout) / 9.0 - 1.0 if holdout > 0 else np.nan

    return point, lo, hi


def benjamini_hochberg(pvalues, alpha=0.05):
    """Apply BH FDR correction. Returns adjusted p-values and significance flags."""
    n = len(pvalues)
    sorted_idx = np.argsort(pvalues)
    sorted_p = np.array(pvalues)[sorted_idx]

    adjusted = np.zeros(n)
    for i in range(n - 1, -1, -1):
        rank = i + 1
        adjusted[i] = sorted_p[i] * n / rank

    # enforce monotonicity
    for i in range(n - 2, -1, -1):
        adjusted[i] = min(adjusted[i], adjusted[i + 1])

    adjusted = np.minimum(adjusted, 1.0)

    # map back to original order
    result = np.zeros(n)
    for i, idx in enumerate(sorted_idx):
        result[idx] = adjusted[i]

    return result.tolist(), [p < alpha for p in result.tolist()]


def analyze_source(filepath, source_name):
    """Run full analysis for one visit source (guid_log or clickpass_log)."""
    advertisers = load_data(filepath)
    results = []

    for aid in sorted(advertisers.keys()):
        data = advertisers[aid]
        h_vis = data.get("holdout_visitors", 0)
        t_vis = data.get("targeted_visitors", 0)
        total_vis = h_vis + t_vis

        # skip tiny samples
        if total_vis < 100:
            continue

        holdout_pct = h_vis / total_vis * 100 if total_vis > 0 else 0

        # binomial test on unique visitors
        p_val = binomial_test(t_vis, total_vis)

        # bootstrap lift CI
        lift, ci_lo, ci_hi = bootstrap_lift_ci(h_vis, t_vis)

        results.append({
            "advertiser_id": aid,
            "company_name": data["name"],
            "source": source_name,
            "holdout_visitors": h_vis,
            "targeted_visitors": t_vis,
            "total_visitors": total_vis,
            "holdout_pct": round(holdout_pct, 2),
            "lift": round(lift, 4) if not np.isnan(lift) else None,
            "ci_lower": round(ci_lo, 4) if not np.isnan(ci_lo) else None,
            "ci_upper": round(ci_hi, 4) if not np.isnan(ci_hi) else None,
            "p_value": p_val,
        })

    # FDR correction
    if results:
        pvals = [r["p_value"] for r in results]
        adj_pvals, significant = benjamini_hochberg(pvals)
        for i, r in enumerate(results):
            r["p_value_adj"] = round(adj_pvals[i], 6)
            r["significant_fdr05"] = significant[i]
            r["p_value"] = round(r["p_value"], 6)

    return results


def main():
    guid_file = OUTPUTS_DIR / "ti_835_guid_log_results.csv"
    click_file = OUTPUTS_DIR / "ti_835_clickpass_log_results.csv"

    guid_results = analyze_source(guid_file, "guid_log")
    click_results = analyze_source(click_file, "clickpass_log")

    all_results = guid_results + click_results

    # save CSV
    out_csv = OUTPUTS_DIR / "ti_835_significance_results.csv"
    fieldnames = [
        "advertiser_id", "company_name", "source",
        "holdout_visitors", "targeted_visitors", "total_visitors",
        "holdout_pct", "lift", "ci_lower", "ci_upper",
        "p_value", "p_value_adj", "significant_fdr05",
    ]
    with open(out_csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_results)

    # print summary
    print(f"\n{'='*80}")
    print(f"TI-835 STATISTICAL SIGNIFICANCE RESULTS")
    print(f"{'='*80}\n")

    for source_name, results in [("guid_log", guid_results), ("clickpass_log", click_results)]:
        print(f"\n--- {source_name} (unique visitors) ---")
        print(f"{'Advertiser':<20} {'Holdout%':>9} {'Lift':>8} {'95% CI':>20} {'p-val':>10} {'p-adj':>10} {'Sig?':>5}")
        print("-" * 85)
        for r in results:
            ci_str = f"[{r['ci_lower']:.3f}, {r['ci_upper']:.3f}]" if r['ci_lower'] is not None else "N/A"
            lift_str = f"{r['lift']:.3f}" if r['lift'] is not None else "N/A"
            sig_str = "***" if r['significant_fdr05'] else ""
            print(f"{r['company_name']:<20} {r['holdout_pct']:>8.2f}% {lift_str:>8} {ci_str:>20} {r['p_value']:>10.6f} {r['p_value_adj']:>10.6f} {sig_str:>5}")

    print(f"\n\nResults saved to: {out_csv}")
    return all_results


if __name__ == "__main__":
    main()
