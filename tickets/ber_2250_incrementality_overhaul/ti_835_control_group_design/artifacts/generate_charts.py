"""
TI-835: Visualization — Observational Incrementality Analysis

Charts follow Tufte principles:
- Maximize data-ink ratio (no gridlines, borders, backgrounds)
- Color encodes meaning (red = key insight, navy = supporting, gray = context)
- Direct labels on data points
- Titles state the finding, not the metric

Generates:
1. Dual story chart: guid_log vs clickpass_log holdout share
2. Lift by advertiser: horizontal bars with 95% CI error bars
3. Holdout share scatter: observed vs expected 10%
"""

import csv
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import numpy as np

TICKET_DIR = Path(__file__).parent.parent
OUTPUTS_DIR = TICKET_DIR / "outputs"
ARTIFACTS_DIR = TICKET_DIR / "artifacts"

# Tufte palette
RED = "#C0392B"
NAVY = "#2C3E50"
GRAY = "#95A5A6"
LIGHT_GRAY = "#BDC3C7"
OFF_WHITE = "#FAFAFA"
DARK_TEXT = "#2C3E50"

plt.rcParams.update({
    "font.family": "Helvetica Neue",
    "font.size": 11,
    "axes.facecolor": OFF_WHITE,
    "figure.facecolor": OFF_WHITE,
    "axes.edgecolor": LIGHT_GRAY,
    "axes.linewidth": 0.5,
    "xtick.color": DARK_TEXT,
    "ytick.color": DARK_TEXT,
    "text.color": DARK_TEXT,
})


def load_significance_results():
    """Load the combined significance results CSV."""
    results = {"guid_log": [], "clickpass_log": []}
    with open(OUTPUTS_DIR / "ti_835_significance_results.csv") as f:
        reader = csv.DictReader(f)
        for row in reader:
            source = row["source"]
            results[source].append({
                "name": row["company_name"],
                "holdout_pct": float(row["holdout_pct"]),
                "lift": float(row["lift"]) if row["lift"] else None,
                "ci_lo": float(row["ci_lower"]) if row["ci_lower"] else None,
                "ci_hi": float(row["ci_upper"]) if row["ci_upper"] else None,
                "significant": row["significant_fdr05"] == "True",
                "holdout_visitors": int(row["holdout_visitors"]),
                "targeted_visitors": int(row["targeted_visitors"]),
            })
    return results


def chart_1_dual_story(results):
    """Side-by-side: guid_log holdout% ~10% vs clickpass_log holdout% <<10%."""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6), sharey=True)

    guid = results["guid_log"]
    click = results["clickpass_log"]

    # sort both by clickpass holdout pct for visual coherence
    click_sorted = sorted(click, key=lambda x: x["holdout_pct"])
    name_order = [c["name"] for c in click_sorted]

    # reorder guid to match
    guid_by_name = {g["name"]: g for g in guid}
    guid_sorted = [guid_by_name[n] for n in name_order if n in guid_by_name]

    y = np.arange(len(name_order))

    # guid_log panel
    guid_pcts = [guid_by_name[n]["holdout_pct"] for n in name_order if n in guid_by_name]
    bars1 = ax1.barh(y, guid_pcts, color=GRAY, height=0.6, edgecolor="none")
    ax1.axvline(x=10, color=NAVY, linestyle="--", linewidth=1, alpha=0.7)
    ax1.set_xlim(0, 14)
    for i, (bar, pct) in enumerate(zip(bars1, guid_pcts)):
        ax1.text(pct + 0.3, i, f"{pct:.1f}%", va="center", fontsize=10, color=DARK_TEXT)
    ax1.set_yticks(y)
    ax1.set_yticklabels(name_order, fontsize=10)
    ax1.set_title("All Pixel Visits (guid_log)", fontsize=13, fontweight="bold", pad=10)
    ax1.text(10.3, -0.8, "10% null", fontsize=9, color=NAVY, alpha=0.7)
    ax1.set_xlabel("Holdout Share (%)", fontsize=10)
    ax1.tick_params(left=False)
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)

    # clickpass_log panel
    click_pcts = [c["holdout_pct"] for c in click_sorted]
    colors = [RED if pct < 8 else GRAY for pct in click_pcts]
    bars2 = ax2.barh(y, click_pcts, color=colors, height=0.6, edgecolor="none")
    ax2.axvline(x=10, color=NAVY, linestyle="--", linewidth=1, alpha=0.7)
    ax2.set_xlim(0, 14)
    for i, (bar, pct) in enumerate(zip(bars2, click_pcts)):
        ax2.text(pct + 0.3, i, f"{pct:.1f}%", va="center", fontsize=10, color=DARK_TEXT)
    ax2.set_title("Attributed Visits (clickpass_log)", fontsize=13, fontweight="bold", pad=10)
    ax2.text(10.3, -0.8, "10% null", fontsize=9, color=NAVY, alpha=0.7)
    ax2.set_xlabel("Holdout Share (%)", fontsize=10)
    ax2.tick_params(left=False)
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)

    fig.suptitle(
        "CTV Ads Don't Increase Total Site Traffic — But Drive 2-8x More Attributed Visits",
        fontsize=14, fontweight="bold", y=0.98
    )
    fig.text(
        0.5, 0.01,
        "Holdout share of unique visitors (30-day window, 9 advertisers). Dashed line = 10% expected under null (no ad effect).",
        ha="center", fontsize=9, color=GRAY
    )
    plt.tight_layout(rect=[0, 0.04, 1, 0.94])
    out = ARTIFACTS_DIR / "ti_835_chart_dual_story.png"
    fig.savefig(out, dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {out}")


def chart_2_lift_by_advertiser(results):
    """Horizontal bar: clickpass_log lift with 95% CI error bars."""
    click = results["clickpass_log"]
    click_sorted = sorted(click, key=lambda x: x["lift"] if x["lift"] else 0)

    names = [c["name"] for c in click_sorted]
    lifts = [c["lift"] for c in click_sorted]
    ci_lo = [c["ci_lo"] for c in click_sorted]
    ci_hi = [c["ci_hi"] for c in click_sorted]

    y = np.arange(len(names))
    errors = [[l - lo for l, lo in zip(lifts, ci_lo)],
              [hi - l for l, hi in zip(lifts, ci_hi)]]

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.barh(y, lifts, color=RED, height=0.6, edgecolor="none", alpha=0.85)
    ax.errorbar(lifts, y, xerr=errors, fmt="none", ecolor=DARK_TEXT, elinewidth=1, capsize=3)

    for i, (lift, name) in enumerate(zip(lifts, names)):
        ax.text(lift + 0.15, i, f"{lift:.1f}x", va="center", fontsize=10, fontweight="bold", color=DARK_TEXT)

    ax.axvline(x=0, color=NAVY, linestyle="-", linewidth=0.8)
    ax.set_yticks(y)
    ax.set_yticklabels(names, fontsize=10)
    ax.set_xlabel("Incremental Lift (x)", fontsize=10)
    ax.set_title(
        "Attributed Visit Lift Ranges from 1.1x to 7.4x Across All Advertisers",
        fontsize=13, fontweight="bold", pad=10
    )
    fig.text(
        0.5, 0.01,
        "Lift = (observed targeted:holdout ratio / expected 9:1 ratio) - 1. Error bars = 95% bootstrap CI. All p < 0.001.",
        ha="center", fontsize=9, color=GRAY
    )
    ax.tick_params(left=False)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    plt.tight_layout(rect=[0, 0.04, 1, 0.96])
    out = ARTIFACTS_DIR / "ti_835_chart_lift_by_advertiser.png"
    fig.savefig(out, dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {out}")


def chart_3_holdout_scatter(results):
    """Scatter: observed holdout% vs expected 10%, both sources overlaid."""
    fig, ax = plt.subplots(figsize=(10, 7))

    guid = results["guid_log"]
    click = results["clickpass_log"]

    # guid_log points
    guid_names = [g["name"] for g in guid]
    guid_pcts = [g["holdout_pct"] for g in guid]
    ax.scatter(guid_pcts, guid_names, color=GRAY, s=80, zorder=3, label="guid_log (all visits)")

    # clickpass_log points
    click_by_name = {c["name"]: c["holdout_pct"] for c in click}
    click_names = [n for n in guid_names if n in click_by_name]
    click_pcts = [click_by_name[n] for n in click_names]
    ax.scatter(click_pcts, click_names, color=RED, s=80, zorder=3, label="clickpass_log (attributed)")

    # draw connecting lines
    for name in click_names:
        g_pct = next(g["holdout_pct"] for g in guid if g["name"] == name)
        c_pct = click_by_name[name]
        ax.plot([g_pct, c_pct], [name, name], color=LIGHT_GRAY, linewidth=1, zorder=1)

    # null line
    ax.axvline(x=10, color=NAVY, linestyle="--", linewidth=1, alpha=0.7)
    ax.text(10.2, -0.5, "Expected 10%\n(no ad effect)", fontsize=9, color=NAVY, alpha=0.7, va="top")

    ax.set_xlabel("Holdout Share of Unique Visitors (%)", fontsize=10)
    ax.set_title(
        "The Two Stories: Total Visits Match Null, Attributed Visits Show Massive Shift",
        fontsize=13, fontweight="bold", pad=10
    )
    ax.legend(loc="lower right", frameon=False, fontsize=10)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.tick_params(left=False)
    ax.set_xlim(0, 14)
    fig.text(
        0.5, 0.01,
        "Each advertiser shown twice: gray = all site visits, red = MNTN-attributed visits. Gap = ad-driven attribution signal.",
        ha="center", fontsize=9, color=GRAY
    )
    plt.tight_layout(rect=[0, 0.04, 1, 0.96])
    out = ARTIFACTS_DIR / "ti_835_chart_holdout_scatter.png"
    fig.savefig(out, dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {out}")


def main():
    results = load_significance_results()
    chart_1_dual_story(results)
    chart_2_lift_by_advertiser(results)
    chart_3_holdout_scatter(results)
    print("\nAll charts generated.")


if __name__ == "__main__":
    main()
