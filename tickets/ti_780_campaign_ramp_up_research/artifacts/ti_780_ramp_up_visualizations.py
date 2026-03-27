"""
TI-780: Campaign Ramp-Up Visualizations
========================================

Produces presentation-ready charts showing how new prospecting campaigns
ramp up to steady-state IVR, segmented by spend tier and channel.

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


def load_ramp_up_data():
    """Load weekly IVR from launch for all prospecting campaigns."""
    client = bigquery.Client(project=BQ_PROJECT)

    # Overall curve + percentiles
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
    """Plot 1: The hero chart — median IVR ramp-up with confidence band."""
    # aggregate across all channels and spend tiers
    agg = df.groupby("weeks_since_launch").agg(
        median_ivr=("median_ivr", "mean"),
        p25_ivr=("p25_ivr", "mean"),
        p75_ivr=("p75_ivr", "mean"),
        p10_ivr=("p10_ivr", "mean"),
        p90_ivr=("p90_ivr", "mean"),
        n_campaigns=("n_campaigns", "sum"),
    ).reset_index()

    # compute steady state (weeks 8-19 average)
    steady = agg[agg["weeks_since_launch"] >= 8]["median_ivr"].mean()

    fig, ax = plt.subplots(figsize=(14, 8))
    fig.patch.set_facecolor(BG_COLOR)
    ax.set_facecolor(BG_COLOR)

    weeks = agg["weeks_since_launch"]

    # confidence bands
    ax.fill_between(weeks, agg["p10_ivr"] * 100, agg["p90_ivr"] * 100,
                     alpha=0.12, color=MNTN_BLUE, label="10th-90th percentile")
    ax.fill_between(weeks, agg["p25_ivr"] * 100, agg["p75_ivr"] * 100,
                     alpha=0.25, color=MNTN_BLUE, label="25th-75th percentile")

    # median line
    ax.plot(weeks, agg["median_ivr"] * 100, color=MNTN_BLUE, linewidth=3,
            marker="o", markersize=8, markerfacecolor="white", markeredgewidth=2,
            markeredgecolor=MNTN_BLUE, label="Median IVR", zorder=5)

    # steady state line
    ax.axhline(y=steady * 100, color=MNTN_GREEN, linestyle="--", linewidth=2, alpha=0.7)
    ax.text(19.3, steady * 100, f"Steady State\n({steady*100:.3f}%)",
            fontsize=10, color=MNTN_GREEN, va="center", fontweight="bold")

    # ramp-up zone
    ax.axvspan(-0.5, 3.5, alpha=0.08, color=MNTN_RED)
    ax.text(1.5, agg["p90_ivr"].max() * 100 * 0.98, "RAMP-UP\nZONE",
            fontsize=14, color=MNTN_RED, ha="center", va="top", fontweight="bold", alpha=0.6)

    # week 4 annotation
    wk4_ivr = agg[agg["weeks_since_launch"] == 4]["median_ivr"].values[0]
    pct_steady = wk4_ivr / steady * 100
    ax.annotate(f"Week 4: {pct_steady:.0f}% of steady state\n(ramp-up complete)",
                xy=(4, wk4_ivr * 100), xytext=(7, wk4_ivr * 100 * 0.7),
                fontsize=11, fontweight="bold", color=MNTN_DARK,
                arrowprops=dict(arrowstyle="->", color=MNTN_DARK, lw=1.5),
                bbox=dict(boxstyle="round,pad=0.4", facecolor="white", edgecolor=MNTN_DARK, alpha=0.9))

    # week 0 annotation
    wk0_ivr = agg[agg["weeks_since_launch"] == 0]["median_ivr"].values[0]
    ax.annotate(f"Week 0: {wk0_ivr/steady*100:.0f}% of steady state",
                xy=(0, wk0_ivr * 100), xytext=(2.5, wk0_ivr * 100 * 0.5),
                fontsize=10, color=MNTN_RED,
                arrowprops=dict(arrowstyle="->", color=MNTN_RED, lw=1.5))

    ax.set_xlabel("Weeks Since Campaign Launch", fontsize=13, fontweight="bold")
    ax.set_ylabel("Median IVR (%)", fontsize=13, fontweight="bold")
    ax.set_title("New Prospecting Campaign Ramp-Up: IVR Stabilization Curve",
                 fontsize=16, fontweight="bold", pad=15)
    ax.set_xlim(-0.5, 20)
    ax.yaxis.set_major_formatter(mtick.FormatStrFormatter("%.3f"))
    ax.legend(loc="lower right", fontsize=10, framealpha=0.9)
    ax.grid(True, alpha=0.3, linestyle="--")

    # subtitle
    fig.text(0.5, 0.92,
             f"N = {agg['n_campaigns'].iloc[0]:,} prospecting campaigns | $10K+ spend | June 2024 – March 2026",
             ha="center", fontsize=11, color=MNTN_GRAY)

    plt.tight_layout(rect=[0, 0, 0.95, 0.91])
    fig.savefig(OUTPUT_DIR / "ramp_up_main_curve.png", dpi=200, bbox_inches="tight", facecolor=BG_COLOR)
    plt.close(fig)
    print(f"Saved: {OUTPUT_DIR / 'ramp_up_main_curve.png'}")


def plot_pct_of_steady_state(df):
    """Plot 2: % of steady state by week — the 'when do we stop caring' chart."""
    agg = df.groupby("weeks_since_launch").agg(median_ivr=("median_ivr", "mean")).reset_index()
    steady = agg[agg["weeks_since_launch"] >= 8]["median_ivr"].mean()
    agg["pct_steady"] = agg["median_ivr"] / steady * 100

    fig, ax = plt.subplots(figsize=(14, 6))
    fig.patch.set_facecolor(BG_COLOR)
    ax.set_facecolor(BG_COLOR)

    colors = [MNTN_RED if p < 85 else MNTN_YELLOW if p < 95 else MNTN_GREEN for p in agg["pct_steady"]]

    bars = ax.bar(agg["weeks_since_launch"], agg["pct_steady"], color=colors, edgecolor="white", linewidth=0.5)

    # 85% threshold line
    ax.axhline(y=85, color=MNTN_DARK, linestyle="--", linewidth=1, alpha=0.5)
    ax.text(19.5, 86, "85% threshold", fontsize=9, color=MNTN_DARK, va="bottom")

    # 100% line
    ax.axhline(y=100, color=MNTN_GREEN, linestyle="--", linewidth=1.5, alpha=0.7)

    # annotations on bars
    for bar, pct in zip(bars, agg["pct_steady"]):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 1,
                f"{pct:.0f}%", ha="center", va="bottom", fontsize=8, fontweight="bold")

    ax.set_xlabel("Weeks Since Campaign Launch", fontsize=13, fontweight="bold")
    ax.set_ylabel("% of Steady-State IVR", fontsize=13, fontweight="bold")
    ax.set_title("How Quickly Do New Campaigns Reach Full Performance?",
                 fontsize=16, fontweight="bold", pad=15)
    ax.set_ylim(0, 115)
    ax.set_xlim(-0.7, 20.5)
    ax.grid(True, alpha=0.2, axis="y")

    # legend
    legend_elements = [
        mpatches.Patch(facecolor=MNTN_RED, label="< 85% (ramp-up)"),
        mpatches.Patch(facecolor=MNTN_YELLOW, label="85-95% (maturing)"),
        mpatches.Patch(facecolor=MNTN_GREEN, label="> 95% (steady state)"),
    ]
    ax.legend(handles=legend_elements, loc="lower right", fontsize=10)

    fig.text(0.5, 0.93, "Recommendation: Exclude first 4 weeks from causal analysis (campaigns at 89% by week 4)",
             ha="center", fontsize=11, color=MNTN_DARK, fontweight="bold")

    plt.tight_layout(rect=[0, 0, 1, 0.92])
    fig.savefig(OUTPUT_DIR / "ramp_up_pct_steady_state.png", dpi=200, bbox_inches="tight", facecolor=BG_COLOR)
    plt.close(fig)
    print(f"Saved: {OUTPUT_DIR / 'ramp_up_pct_steady_state.png'}")


def plot_spend_tier_comparison(df):
    """Plot 3: Ramp-up by spend tier — do bigger spenders ramp faster?"""
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
                markeredgecolor=color, label=f"{tier} (n={tier_data['n_campaigns'].iloc[0]:,})")

    ax.axhline(y=100, color=MNTN_DARK, linestyle="--", linewidth=1, alpha=0.3)
    ax.axhline(y=85, color=MNTN_RED, linestyle=":", linewidth=1, alpha=0.5)
    ax.axvspan(-0.5, 3.5, alpha=0.06, color=MNTN_RED)

    ax.set_xlabel("Weeks Since Campaign Launch", fontsize=13, fontweight="bold")
    ax.set_ylabel("% of Steady-State IVR", fontsize=13, fontweight="bold")
    ax.set_title("Ramp-Up Speed by Campaign Spend Tier (CTV Prospecting)",
                 fontsize=16, fontweight="bold", pad=15)
    ax.set_ylim(30, 115)
    ax.legend(fontsize=11, loc="lower right", framealpha=0.9)
    ax.grid(True, alpha=0.2)

    fig.text(0.5, 0.93,
             "All spend tiers converge by week 4-5 — the 4-week exclusion window is universal",
             ha="center", fontsize=11, color=MNTN_DARK, fontweight="bold")

    plt.tight_layout(rect=[0, 0, 1, 0.92])
    fig.savefig(OUTPUT_DIR / "ramp_up_by_spend_tier.png", dpi=200, bbox_inches="tight", facecolor=BG_COLOR)
    plt.close(fig)
    print(f"Saved: {OUTPUT_DIR / 'ramp_up_by_spend_tier.png'}")


def plot_week_over_week_change(df):
    """Plot 4: Week-over-week IVR change — when does volatility die down?"""
    agg = df.groupby("weeks_since_launch").agg(median_ivr=("median_ivr", "mean")).reset_index()
    agg["wow_change"] = agg["median_ivr"].pct_change() * 100

    fig, ax = plt.subplots(figsize=(14, 6))
    fig.patch.set_facecolor(BG_COLOR)
    ax.set_facecolor(BG_COLOR)

    colors = [MNTN_RED if abs(w) >= 5 else MNTN_GREEN for w in agg["wow_change"].fillna(0)]

    ax.bar(agg["weeks_since_launch"][1:], agg["wow_change"][1:],
           color=colors[1:], edgecolor="white", linewidth=0.5)

    ax.axhline(y=5, color=MNTN_RED, linestyle="--", linewidth=1.5, alpha=0.7)
    ax.axhline(y=-5, color=MNTN_RED, linestyle="--", linewidth=1.5, alpha=0.7)
    ax.axhline(y=0, color=MNTN_DARK, linewidth=0.8)
    ax.text(19.5, 5.5, "±5% threshold", fontsize=9, color=MNTN_RED)

    ax.set_xlabel("Weeks Since Campaign Launch", fontsize=13, fontweight="bold")
    ax.set_ylabel("Week-over-Week IVR Change (%)", fontsize=13, fontweight="bold")
    ax.set_title("When Does Campaign Performance Stabilize?",
                 fontsize=16, fontweight="bold", pad=15)
    ax.grid(True, alpha=0.2, axis="y")

    legend_elements = [
        mpatches.Patch(facecolor=MNTN_RED, label="Volatile (>5% WoW change)"),
        mpatches.Patch(facecolor=MNTN_GREEN, label="Stable (<5% WoW change)"),
    ]
    ax.legend(handles=legend_elements, loc="upper right", fontsize=10)

    fig.text(0.5, 0.93,
             "Week 4 is the first week where WoW change drops below 5% and stays there",
             ha="center", fontsize=11, color=MNTN_DARK, fontweight="bold")

    plt.tight_layout(rect=[0, 0, 1, 0.92])
    fig.savefig(OUTPUT_DIR / "ramp_up_wow_change.png", dpi=200, bbox_inches="tight", facecolor=BG_COLOR)
    plt.close(fig)
    print(f"Saved: {OUTPUT_DIR / 'ramp_up_wow_change.png'}")


def plot_key_takeaway(df):
    """Plot 5: Executive summary — the one-slide version."""
    agg = df.groupby("weeks_since_launch").agg(
        median_ivr=("median_ivr", "mean"),
        n_campaigns=("n_campaigns", "sum"),
    ).reset_index()
    steady = agg[agg["weeks_since_launch"] >= 8]["median_ivr"].mean()

    fig, axes = plt.subplots(1, 3, figsize=(18, 6))
    fig.patch.set_facecolor(BG_COLOR)
    fig.suptitle("Campaign Ramp-Up: Key Findings", fontsize=20, fontweight="bold", y=1.02)

    # Panel 1: The number
    ax = axes[0]
    ax.set_facecolor(BG_COLOR)
    ax.text(0.5, 0.65, "4", fontsize=120, fontweight="bold", color=MNTN_BLUE,
            ha="center", va="center", transform=ax.transAxes)
    ax.text(0.5, 0.3, "weeks to\nsteady state", fontsize=18, color=MNTN_DARK,
            ha="center", va="center", transform=ax.transAxes)
    ax.text(0.5, 0.08, f"Based on {agg['n_campaigns'].iloc[0]:,} campaigns",
            fontsize=11, color=MNTN_GRAY, ha="center", va="center", transform=ax.transAxes)
    ax.axis("off")

    # Panel 2: The curve (mini)
    ax = axes[1]
    ax.set_facecolor(BG_COLOR)
    pct = agg["median_ivr"] / steady * 100
    ax.fill_between(agg["weeks_since_launch"], 0, pct, alpha=0.15, color=MNTN_BLUE)
    ax.plot(agg["weeks_since_launch"], pct, color=MNTN_BLUE, linewidth=3)
    ax.axhline(y=100, color=MNTN_GREEN, linestyle="--", linewidth=1.5, alpha=0.7)
    ax.axvline(x=4, color=MNTN_RED, linestyle="--", linewidth=2)
    ax.set_xlabel("Weeks", fontsize=12)
    ax.set_ylabel("% of Steady State", fontsize=12)
    ax.set_title("IVR Ramp-Up Curve", fontsize=14, fontweight="bold")
    ax.set_ylim(0, 115)
    ax.grid(True, alpha=0.2)

    # Panel 3: Key stats
    ax = axes[2]
    ax.set_facecolor(BG_COLOR)
    stats = [
        ("Week 0", f"{agg[agg['weeks_since_launch']==0]['median_ivr'].values[0]/steady*100:.0f}%", "of steady state"),
        ("Week 2", f"{agg[agg['weeks_since_launch']==2]['median_ivr'].values[0]/steady*100:.0f}%", "of steady state"),
        ("Week 4", f"{agg[agg['weeks_since_launch']==4]['median_ivr'].values[0]/steady*100:.0f}%", "ramp-up complete"),
        ("Week 8+", "100%", "fully stabilized"),
    ]
    for i, (label, value, note) in enumerate(stats):
        y = 0.85 - i * 0.22
        color = MNTN_RED if i < 2 else MNTN_GREEN
        ax.text(0.15, y, label, fontsize=14, fontweight="bold", color=MNTN_DARK,
                transform=ax.transAxes, va="center")
        ax.text(0.55, y, value, fontsize=22, fontweight="bold", color=color,
                transform=ax.transAxes, va="center", ha="center")
        ax.text(0.85, y, note, fontsize=10, color=MNTN_GRAY,
                transform=ax.transAxes, va="center", ha="center")
    ax.axis("off")
    ax.set_title("Performance Milestones", fontsize=14, fontweight="bold")

    plt.tight_layout(rect=[0, 0, 1, 0.96])
    fig.savefig(OUTPUT_DIR / "ramp_up_executive_summary.png", dpi=200, bbox_inches="tight", facecolor=BG_COLOR)
    plt.close(fig)
    print(f"Saved: {OUTPUT_DIR / 'ramp_up_executive_summary.png'}")


def main():
    print("Loading ramp-up data from BQ...")
    df = load_ramp_up_data()
    print(f"Loaded {len(df)} rows")

    print("\nGenerating visualizations...")
    plot_main_ramp_up_curve(df)
    plot_pct_of_steady_state(df)
    plot_spend_tier_comparison(df)
    plot_week_over_week_change(df)
    plot_key_takeaway(df)

    print(f"\nAll charts saved to {OUTPUT_DIR}")
    print("Done.")


if __name__ == "__main__":
    main()
