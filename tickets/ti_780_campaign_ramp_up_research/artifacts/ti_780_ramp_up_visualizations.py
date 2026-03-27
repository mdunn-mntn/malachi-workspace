"""
TI-780: Campaign Ramp-Up Visualizations (v2)
=============================================

Produces presentation-ready charts showing how new prospecting campaigns
stabilize to steady-state IVR, segmented by spend tier and channel.

v2 fixes (from Kirsa meeting 2 feedback):
  - Corrected narrative: campaigns OVERSHOOT then settle (not monotonic ramp-up)
  - Chart 2: replaced confusing "% of steady state" bars with deviation-from-steady-state
    (shows volatility decreasing, not misleading overshoot)
  - Chart 4: fixed false subtitle claim about WoW change "staying below 5%"
  - Chart 5: exec summary now tells the correct overshoot-and-settle story
  - All aggregations use n_campaigns-weighted means (fixed mean-of-medians bug)

Output: PNG files in outputs/
"""

import warnings
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.ticker as mtick
import numpy as np
import pandas as pd
from google.cloud import bigquery

warnings.filterwarnings("ignore")

BQ_PROJECT = "dw-main-silver"
OUTPUT_DIR = Path(__file__).parent.parent / "outputs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# MNTN brand-ish colors
MNTN_BLUE = "#1a73e8"
MNTN_GREEN = "#34a853"
MNTN_RED = "#ea4335"
MNTN_YELLOW = "#fbbc04"
MNTN_GRAY = "#9aa0a6"
MNTN_DARK = "#202124"
BG_COLOR = "#fafafa"


def _weighted_agg(df, value_cols=None):
    """Aggregate across groups weighted by n_campaigns."""
    if value_cols is None:
        value_cols = ["median_ivr", "p25_ivr", "p75_ivr", "p10_ivr", "p90_ivr"]

    def _agg_fn(g):
        w = g["n_campaigns"]
        result = {"n_campaigns": w.sum()}
        for col in value_cols:
            if col in g.columns:
                result[col] = (g[col] * w).sum() / w.sum()
        return pd.Series(result)

    return df.groupby("weeks_since_launch").apply(_agg_fn).reset_index()


def load_ramp_up_data():
    """Load weekly IVR from launch for all prospecting campaigns."""
    client = bigquery.Client(project=BQ_PROJECT)

    overall = client.query("""
    WITH campaign_launch AS (
        SELECT c.campaign_group_id, c.advertiser_id, c.channel_id,
            MIN(s.day) AS first_delivery_date,
            SUM(s.media_spend + s.data_spend + s.platform_spend) AS total_spend
        FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` s
        JOIN `dw-main-bronze.integrationprod.campaigns` c ON c.campaign_id = s.campaign_id
        WHERE c.funnel_level = 1 AND c.deleted = FALSE AND c.is_test = FALSE
          AND s.day >= "2024-06-01" AND s.impressions > 0
        GROUP BY 1, 2, 3
        HAVING SUM(s.media_spend + s.data_spend + s.platform_spend) >= 10000
    ),
    weekly AS (
        SELECT cl.campaign_group_id, cl.channel_id, cl.total_spend,
            DATE_DIFF(DATE_TRUNC(s.day, WEEK(MONDAY)), DATE_TRUNC(cl.first_delivery_date, WEEK(MONDAY)), WEEK) AS weeks_since_launch,
            SUM(s.impressions) AS impressions,
            SUM(s.clicks + s.views + COALESCE(s.competing_views, 0)) AS vv
        FROM campaign_launch cl
        JOIN `dw-main-bronze.integrationprod.campaigns` c
            ON c.campaign_group_id = cl.campaign_group_id AND c.deleted = FALSE AND c.is_test = FALSE
        JOIN `dw-main-silver.summarydata.sum_by_campaign_by_day` s
            ON s.campaign_id = c.campaign_id AND s.day >= "2024-06-01"
            AND s.day >= cl.first_delivery_date AND s.day < DATE_ADD(cl.first_delivery_date, INTERVAL 20 WEEK)
            AND s.impressions > 0
        GROUP BY 1, 2, 3, 4
    )
    SELECT
        weeks_since_launch,
        CASE WHEN channel_id = 8 THEN 'CTV' WHEN channel_id = 1 THEN 'Display' ELSE 'Other' END AS channel,
        CASE WHEN total_spend >= 100000 THEN 'High ($100K+)'
             WHEN total_spend >= 30000 THEN 'Mid ($30-100K)'
             ELSE 'Low ($10-30K)' END AS spend_tier,
        COUNT(DISTINCT campaign_group_id) AS n_campaigns,
        SAFE_DIVIDE(SUM(vv), SUM(impressions)) AS ivr,
        APPROX_QUANTILES(SAFE_DIVIDE(vv, impressions), 100)[OFFSET(10)] AS p10_ivr,
        APPROX_QUANTILES(SAFE_DIVIDE(vv, impressions), 100)[OFFSET(25)] AS p25_ivr,
        APPROX_QUANTILES(SAFE_DIVIDE(vv, impressions), 100)[OFFSET(50)] AS median_ivr,
        APPROX_QUANTILES(SAFE_DIVIDE(vv, impressions), 100)[OFFSET(75)] AS p75_ivr,
        APPROX_QUANTILES(SAFE_DIVIDE(vv, impressions), 100)[OFFSET(90)] AS p90_ivr
    FROM weekly
    WHERE impressions >= 1000 AND weeks_since_launch BETWEEN 0 AND 19
    GROUP BY 1, 2, 3
    ORDER BY 1, 2, 3
    """).to_dataframe()

    for c in ["ivr", "p10_ivr", "p25_ivr", "median_ivr", "p75_ivr", "p90_ivr", "n_campaigns"]:
        overall[c] = pd.to_numeric(overall[c], errors="coerce").astype(float)
    overall["weeks_since_launch"] = overall["weeks_since_launch"].astype(int)

    return overall


def plot_main_ramp_up_curve(df):
    """Plot 1: Median IVR curve with IQR band — zoomed to readable range."""
    agg = _weighted_agg(df)
    steady = agg[agg["weeks_since_launch"] >= 8]["median_ivr"].mean()

    fig, ax = plt.subplots(figsize=(14, 8))
    fig.patch.set_facecolor(BG_COLOR)
    ax.set_facecolor(BG_COLOR)

    weeks = agg["weeks_since_launch"]

    # IQR band only (25th-75th) — much more readable than 10th-90th
    ax.fill_between(weeks, agg["p25_ivr"] * 100, agg["p75_ivr"] * 100,
                     alpha=0.25, color=MNTN_BLUE, label="25th-75th percentile (IQR)")

    # median line
    ax.plot(weeks, agg["median_ivr"] * 100, color=MNTN_BLUE, linewidth=3,
            marker="o", markersize=8, markerfacecolor="white", markeredgewidth=2,
            markeredgecolor=MNTN_BLUE, label="Median IVR", zorder=5)

    # steady state line
    ax.axhline(y=steady * 100, color=MNTN_GREEN, linestyle="--", linewidth=2, alpha=0.7)
    ax.text(19.5, steady * 100, f"Steady State\n({steady*100:.2f}%)",
            fontsize=10, color=MNTN_GREEN, va="center", fontweight="bold")

    # volatile zone (0-3 weeks)
    ax.axvspan(-0.5, 3.5, alpha=0.08, color=MNTN_RED)
    ax.text(1.5, ax.get_ylim()[1] * 0.95, "VOLATILE\nZONE",
            fontsize=14, color=MNTN_RED, ha="center", va="top", fontweight="bold", alpha=0.6)

    # week 0 annotation
    wk0_ivr = agg[agg["weeks_since_launch"] == 0]["median_ivr"].values[0]
    ax.annotate(f"Week 0: {wk0_ivr/steady*100:.0f}% of steady state\n(bidder exploring)",
                xy=(0, wk0_ivr * 100), xytext=(3, wk0_ivr * 100 * 0.75),
                fontsize=10, color=MNTN_RED,
                arrowprops=dict(arrowstyle="->", color=MNTN_RED, lw=1.5),
                bbox=dict(boxstyle="round,pad=0.3", facecolor="white", edgecolor=MNTN_RED, alpha=0.9))

    # week 4 annotation
    wk4_ivr = agg[agg["weeks_since_launch"] == 4]["median_ivr"].values[0]
    ax.annotate(f"Week 4: volatility settles\n(within {abs(wk4_ivr/steady*100 - 100):.0f}% of steady state)",
                xy=(4, wk4_ivr * 100), xytext=(7, wk4_ivr * 100 * 1.15),
                fontsize=11, fontweight="bold", color=MNTN_DARK,
                arrowprops=dict(arrowstyle="->", color=MNTN_DARK, lw=1.5),
                bbox=dict(boxstyle="round,pad=0.4", facecolor="white", edgecolor=MNTN_DARK, alpha=0.9))

    # zoom y-axis to the median range for readability
    y_min = agg["p25_ivr"].min() * 100 * 0.7
    y_max = agg["p75_ivr"].max() * 100 * 1.3
    ax.set_ylim(max(0, y_min), y_max)

    ax.set_xlabel("Weeks Since Campaign Launch", fontsize=13, fontweight="bold")
    ax.set_ylabel("Median IVR (%)", fontsize=13, fontweight="bold")
    ax.set_title("New Prospecting Campaign IVR: Stabilization Curve",
                 fontsize=16, fontweight="bold", pad=15)
    ax.set_xlim(-0.5, 20)
    ax.legend(loc="lower right", fontsize=10, framealpha=0.9)
    ax.grid(True, alpha=0.3, linestyle="--")

    fig.text(0.5, 0.92,
             f"N = {agg['n_campaigns'].iloc[0]:,.0f} prospecting campaigns | $10K+ spend | June 2024 \u2013 March 2026",
             ha="center", fontsize=11, color=MNTN_GRAY)

    plt.tight_layout(rect=[0, 0, 0.95, 0.91])
    fig.savefig(OUTPUT_DIR / "ramp_up_main_curve.png", dpi=200, bbox_inches="tight", facecolor=BG_COLOR)
    plt.close(fig)
    print(f"Saved: {OUTPUT_DIR / 'ramp_up_main_curve.png'}")


def plot_volatility_by_week(df):
    """Plot 2: Absolute deviation from steady state — shows volatility decreasing over time.

    Replaces the old "% of steady state" bar chart, which was confusing because
    campaigns overshoot above 100% in weeks 1-3 before settling.
    This chart shows |median - steady_state| / steady_state, which correctly captures
    that weeks 0-3 are volatile (whether above OR below steady state) and week 4+
    is stable.
    """
    agg = _weighted_agg(df, value_cols=["median_ivr"])
    steady = agg[agg["weeks_since_launch"] >= 8]["median_ivr"].mean()
    agg["deviation_pct"] = ((agg["median_ivr"] - steady) / steady * 100).abs()
    agg["above_or_below"] = np.where(agg["median_ivr"] >= steady, "above", "below")

    fig, ax = plt.subplots(figsize=(14, 6))
    fig.patch.set_facecolor(BG_COLOR)
    ax.set_facecolor(BG_COLOR)

    colors = [MNTN_RED if d > 10 else MNTN_YELLOW if d > 5 else MNTN_GREEN for d in agg["deviation_pct"]]

    bars = ax.bar(agg["weeks_since_launch"], agg["deviation_pct"], color=colors,
                  edgecolor="white", linewidth=0.5)

    # threshold lines
    ax.axhline(y=10, color=MNTN_RED, linestyle="--", linewidth=1, alpha=0.5)
    ax.text(19.5, 10.5, "10% deviation", fontsize=9, color=MNTN_RED, va="bottom")
    ax.axhline(y=5, color=MNTN_YELLOW, linestyle="--", linewidth=1, alpha=0.5)
    ax.text(19.5, 5.5, "5% deviation", fontsize=9, color=MNTN_YELLOW, va="bottom")

    # annotate direction (above/below steady state) on each bar
    for bar, dev, direction in zip(bars, agg["deviation_pct"], agg["above_or_below"]):
        label = f"{dev:.0f}%\n({'↑' if direction == 'above' else '↓'})"
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                label, ha="center", va="bottom", fontsize=7.5, fontweight="bold")

    ax.set_xlabel("Weeks Since Campaign Launch", fontsize=13, fontweight="bold")
    ax.set_ylabel("Absolute Deviation from Steady State (%)", fontsize=13, fontweight="bold")
    ax.set_title("IVR Volatility by Week: How Far from Steady State?",
                 fontsize=16, fontweight="bold", pad=15)
    ax.set_ylim(0, max(agg["deviation_pct"]) * 1.3)
    ax.set_xlim(-0.7, 20.5)
    ax.grid(True, alpha=0.2, axis="y")

    legend_elements = [
        mpatches.Patch(facecolor=MNTN_RED, label="> 10% deviation (volatile)"),
        mpatches.Patch(facecolor=MNTN_YELLOW, label="5-10% deviation (settling)"),
        mpatches.Patch(facecolor=MNTN_GREEN, label="< 5% deviation (stable)"),
    ]
    ax.legend(handles=legend_elements, loc="upper right", fontsize=10)

    fig.text(0.5, 0.93,
             "Arrows show direction: \u2191 = above steady state (overshoot), \u2193 = below (undershoot). "
             "Volatility drops below 5% by week 4.",
             ha="center", fontsize=10, color=MNTN_DARK, fontweight="bold")

    plt.tight_layout(rect=[0, 0, 1, 0.92])
    fig.savefig(OUTPUT_DIR / "ramp_up_volatility.png", dpi=200, bbox_inches="tight", facecolor=BG_COLOR)
    plt.close(fig)
    print(f"Saved: {OUTPUT_DIR / 'ramp_up_volatility.png'}")


def plot_spend_tier_comparison(df):
    """Plot 3: Ramp-up by spend tier — do bigger spenders stabilize faster?"""
    ctv = df[df["channel"] == "CTV"]

    fig, ax = plt.subplots(figsize=(14, 7))
    fig.patch.set_facecolor(BG_COLOR)
    ax.set_facecolor(BG_COLOR)

    tier_colors = {"High ($100K+)": MNTN_BLUE, "Mid ($30-100K)": MNTN_GREEN, "Low ($10-30K)": MNTN_YELLOW}

    for tier, color in tier_colors.items():
        tier_data = ctv[ctv["spend_tier"] == tier].sort_values("weeks_since_launch")
        if tier_data.empty:
            continue

        steady = tier_data[tier_data["weeks_since_launch"] >= 8]["median_ivr"].mean()
        pct = tier_data["median_ivr"] / steady * 100

        ax.plot(tier_data["weeks_since_launch"], pct, color=color, linewidth=2.5,
                marker="o", markersize=7, markerfacecolor="white", markeredgewidth=2,
                markeredgecolor=color, label=f"{tier} (n={tier_data['n_campaigns'].iloc[0]:,.0f})")

    ax.axhline(y=100, color=MNTN_DARK, linestyle="--", linewidth=1, alpha=0.3)
    ax.axvspan(-0.5, 3.5, alpha=0.06, color=MNTN_RED)

    ax.set_xlabel("Weeks Since Campaign Launch", fontsize=13, fontweight="bold")
    ax.set_ylabel("% of Steady-State IVR", fontsize=13, fontweight="bold")
    ax.set_title("Stabilization Speed by Campaign Spend Tier (CTV Prospecting)",
                 fontsize=16, fontweight="bold", pad=15)
    ax.set_ylim(30, 120)
    ax.legend(fontsize=11, loc="lower right", framealpha=0.9)
    ax.grid(True, alpha=0.2)

    fig.text(0.5, 0.93,
             "All spend tiers show the same pattern: low start \u2192 overshoot \u2192 settle by week 4-5",
             ha="center", fontsize=11, color=MNTN_DARK, fontweight="bold")

    plt.tight_layout(rect=[0, 0, 1, 0.92])
    fig.savefig(OUTPUT_DIR / "ramp_up_by_spend_tier.png", dpi=200, bbox_inches="tight", facecolor=BG_COLOR)
    plt.close(fig)
    print(f"Saved: {OUTPUT_DIR / 'ramp_up_by_spend_tier.png'}")


def plot_week_over_week_change(df):
    """Plot 4: Week-over-week IVR change with rolling average to show trend."""
    agg = _weighted_agg(df, value_cols=["median_ivr"])
    agg["wow_change"] = agg["median_ivr"].pct_change() * 100
    agg["abs_wow"] = agg["wow_change"].abs()
    # 3-week rolling average of absolute change (smooths single-week noise)
    agg["rolling_abs_wow"] = agg["abs_wow"].rolling(3, min_periods=1, center=True).mean()

    fig, ax = plt.subplots(figsize=(14, 6))
    fig.patch.set_facecolor(BG_COLOR)
    ax.set_facecolor(BG_COLOR)

    colors = [MNTN_RED if abs(w) >= 5 else MNTN_GREEN for w in agg["wow_change"].fillna(0)]

    # bars for raw WoW change
    ax.bar(agg["weeks_since_launch"][1:], agg["wow_change"][1:],
           color=colors[1:], edgecolor="white", linewidth=0.5, alpha=0.7)

    # overlay rolling average of absolute change
    ax.plot(agg["weeks_since_launch"][1:], agg["rolling_abs_wow"][1:],
            color=MNTN_DARK, linewidth=2.5, linestyle="-", marker="s", markersize=5,
            label="3-week rolling avg |WoW change|", zorder=5)

    ax.axhline(y=5, color=MNTN_RED, linestyle="--", linewidth=1.5, alpha=0.7)
    ax.axhline(y=-5, color=MNTN_RED, linestyle="--", linewidth=1.5, alpha=0.7)
    ax.axhline(y=0, color=MNTN_DARK, linewidth=0.8)
    ax.text(19.5, 5.5, "\u00b15% threshold", fontsize=9, color=MNTN_RED)

    ax.set_xlabel("Weeks Since Campaign Launch", fontsize=13, fontweight="bold")
    ax.set_ylabel("Week-over-Week IVR Change (%)", fontsize=13, fontweight="bold")
    ax.set_title("When Does Campaign Performance Stabilize?",
                 fontsize=16, fontweight="bold", pad=15)
    ax.grid(True, alpha=0.2, axis="y")

    legend_elements = [
        mpatches.Patch(facecolor=MNTN_RED, alpha=0.7, label="Volatile (\u22655% WoW change)"),
        mpatches.Patch(facecolor=MNTN_GREEN, alpha=0.7, label="Stable (<5% WoW change)"),
        plt.Line2D([0], [0], color=MNTN_DARK, linewidth=2.5, marker="s", markersize=5,
                    label="Rolling avg |change| (trend)"),
    ]
    ax.legend(handles=legend_elements, loc="upper right", fontsize=10)

    fig.text(0.5, 0.93,
             "Largest WoW swings in weeks 1-3; rolling average shows volatility decreasing from week 4 onward",
             ha="center", fontsize=11, color=MNTN_DARK, fontweight="bold")

    plt.tight_layout(rect=[0, 0, 1, 0.92])
    fig.savefig(OUTPUT_DIR / "ramp_up_wow_change.png", dpi=200, bbox_inches="tight", facecolor=BG_COLOR)
    plt.close(fig)
    print(f"Saved: {OUTPUT_DIR / 'ramp_up_wow_change.png'}")


def plot_key_takeaway(df):
    """Plot 5: Executive summary — corrected to show overshoot-and-settle pattern."""
    agg = _weighted_agg(df, value_cols=["median_ivr"])
    steady = agg[agg["weeks_since_launch"] >= 8]["median_ivr"].mean()

    fig, axes = plt.subplots(1, 3, figsize=(18, 6))
    fig.patch.set_facecolor(BG_COLOR)
    fig.suptitle("Campaign Stabilization: Key Findings", fontsize=20, fontweight="bold", y=1.02)

    # Panel 1: The number
    ax = axes[0]
    ax.set_facecolor(BG_COLOR)
    ax.text(0.5, 0.65, "4", fontsize=120, fontweight="bold", color=MNTN_BLUE,
            ha="center", va="center", transform=ax.transAxes)
    ax.text(0.5, 0.3, "weeks until\nIVR stabilizes", fontsize=18, color=MNTN_DARK,
            ha="center", va="center", transform=ax.transAxes)
    ax.text(0.5, 0.08, f"Based on {agg['n_campaigns'].iloc[0]:,.0f} campaigns",
            fontsize=11, color=MNTN_GRAY, ha="center", va="center", transform=ax.transAxes)
    ax.axis("off")

    # Panel 2: The curve (mini) — show deviation from steady state
    ax = axes[1]
    ax.set_facecolor(BG_COLOR)
    deviation = ((agg["median_ivr"] - steady) / steady * 100).abs()
    ax.fill_between(agg["weeks_since_launch"], 0, deviation, alpha=0.3, color=MNTN_BLUE)
    ax.plot(agg["weeks_since_launch"], deviation, color=MNTN_BLUE, linewidth=3)
    ax.axhline(y=5, color=MNTN_GREEN, linestyle="--", linewidth=1.5, alpha=0.7)
    ax.axvline(x=4, color=MNTN_RED, linestyle="--", linewidth=2)
    ax.text(4.3, ax.get_ylim()[1] * 0.9, "Week 4", fontsize=10, color=MNTN_RED, fontweight="bold")
    ax.set_xlabel("Weeks", fontsize=12)
    ax.set_ylabel("Deviation from Steady State (%)", fontsize=12)
    ax.set_title("IVR Volatility Over Time", fontsize=14, fontweight="bold")
    ax.set_ylim(0, max(deviation) * 1.2)
    ax.grid(True, alpha=0.2)

    # Panel 3: Key milestones — corrected narrative
    ax = axes[2]
    ax.set_facecolor(BG_COLOR)

    wk0_pct = agg[agg["weeks_since_launch"] == 0]["median_ivr"].values[0] / steady * 100
    wk2_pct = agg[agg["weeks_since_launch"] == 2]["median_ivr"].values[0] / steady * 100
    wk4_dev = abs(agg[agg["weeks_since_launch"] == 4]["median_ivr"].values[0] / steady * 100 - 100)

    stats = [
        ("Week 0", f"{wk0_pct:.0f}%", "of steady state (low start)", MNTN_RED),
        ("Weeks 1-3", "volatile", f"overshoot to {wk2_pct:.0f}% then correct", MNTN_RED),
        ("Week 4", f"\u00b1{wk4_dev:.0f}%", "deviation \u2014 stabilized", MNTN_GREEN),
        ("Week 8+", "100%", "fully at steady state", MNTN_GREEN),
    ]
    for i, (label, value, note, color) in enumerate(stats):
        y = 0.85 - i * 0.22
        ax.text(0.05, y, label, fontsize=13, fontweight="bold", color=MNTN_DARK,
                transform=ax.transAxes, va="center")
        ax.text(0.45, y, value, fontsize=20, fontweight="bold", color=color,
                transform=ax.transAxes, va="center", ha="center")
        ax.text(0.95, y, note, fontsize=9, color=MNTN_GRAY,
                transform=ax.transAxes, va="center", ha="right")
    ax.axis("off")
    ax.set_title("Stabilization Milestones", fontsize=14, fontweight="bold")

    plt.tight_layout(rect=[0, 0, 1, 0.96])
    fig.savefig(OUTPUT_DIR / "ramp_up_executive_summary.png", dpi=200, bbox_inches="tight", facecolor=BG_COLOR)
    plt.close(fig)
    print(f"Saved: {OUTPUT_DIR / 'ramp_up_executive_summary.png'}")


def main():
    print("Loading ramp-up data from BQ...")
    df = load_ramp_up_data()
    print(f"Loaded {len(df)} rows")

    print("\nGenerating visualizations (v2 — corrected narrative)...")
    plot_main_ramp_up_curve(df)
    plot_volatility_by_week(df)
    plot_spend_tier_comparison(df)
    plot_week_over_week_change(df)
    plot_key_takeaway(df)

    print(f"\nAll charts saved to {OUTPUT_DIR}")
    print("Done.")


if __name__ == "__main__":
    main()
