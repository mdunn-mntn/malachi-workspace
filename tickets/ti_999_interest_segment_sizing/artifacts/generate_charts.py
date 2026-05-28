"""Generate exec-quality PNG charts for TI-999. Tufte-principle: high data-ink ratio,
direct labels, accent color only on the key insight, no decorative chartjunk.

Reads: outputs/*.csv
Writes: artifacts/ti_999_chart_*.png at 200 DPI.

Style spec (per workspace Visualization Standards):
- Font: Helvetica Neue
- Background: #FAFAFA off-white
- Accent: #C0392B brick red for "the one insight"; #2C3E50 navy for supporting; #95A5A6 gray for context
"""

from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd

HERE = Path(__file__).parent
OUTPUTS = HERE.parent / "outputs"

plt.rcParams.update({
    "font.family": "Helvetica Neue",
    "font.size": 11,
    "axes.facecolor": "#FAFAFA",
    "figure.facecolor": "#FAFAFA",
    "axes.edgecolor": "#2C3E50",
    "axes.linewidth": 0.6,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.titlesize": 14,
    "axes.titleweight": "bold",
    "axes.titlepad": 22,
    "axes.labelcolor": "#2C3E50",
    "xtick.color": "#2C3E50",
    "ytick.color": "#2C3E50",
    "xtick.major.size": 0,
    "ytick.major.size": 0,
    # Disable matplotlib mathtext so "$" renders as a literal dollar sign.
    "text.parse_math": False,
    "mathtext.default": "regular",
})

ACCENT_RED = "#C0392B"
NAVY = "#2C3E50"
GRAY = "#95A5A6"


def chart_bucket_spend_share():
    df = pd.read_csv(OUTPUTS / "ti_999_campaign_buckets_30d_2026_05_28.csv")
    total_spend = df["total_spend_30d"].sum()
    total_camps = df["n_campaigns"].sum()

    df["spend_pct"] = df["total_spend_30d"] / total_spend * 100
    df["camp_pct"] = df["n_campaigns"] / total_camps * 100

    # Display order: interest_only, interest_mixed, no_interest
    order = ["interest_only", "interest_mixed", "no_interest"]
    df = df.set_index("bucket").loc[order].reset_index()
    labels = ["Interest-only", "Interest + internal", "No interest"]

    fig, ax = plt.subplots(figsize=(10, 6), dpi=200)
    fig.suptitle("35.7% of MNTN spend touches third-party interest segments",
                 fontsize=14, fontweight="bold", color=NAVY, x=0.05, ha="left", y=0.97)
    fig.text(0.05, 0.92, "30-day window ending 2026-05-28. Share of $40.3M total spend.",
             color=GRAY, fontsize=10)

    x = range(len(df))
    bar_colors = [ACCENT_RED, ACCENT_RED, GRAY]
    bars = ax.bar(x, df["spend_pct"], color=bar_colors, width=0.55)

    for i, b in enumerate(bars):
        ax.text(b.get_x() + b.get_width() / 2, b.get_height() + 1.2,
                f"${df['total_spend_30d'].iloc[i] / 1e6:.2f}M\n{df['spend_pct'].iloc[i]:.1f}%",
                ha="center", va="bottom", color=NAVY, fontsize=11, fontweight="bold")
        ax.text(b.get_x() + b.get_width() / 2, -4,
                f"{df['n_campaigns'].iloc[i]:,} campaigns\n({df['camp_pct'].iloc[i]:.1f}% of active)",
                ha="center", va="top", color=GRAY, fontsize=9)

    ax.set_xticks(x)
    ax.set_xticklabels(labels, color=NAVY, fontsize=12)
    ax.set_ylim(0, 80)
    ax.set_yticks([])
    ax.set_ylabel("")
    fig.tight_layout(rect=[0, 0, 1, 0.88])
    out = HERE / "ti_999_chart_bucket_spend_share.png"
    fig.savefig(out, dpi=200, bbox_inches="tight", facecolor="#FAFAFA")
    print(f"wrote {out}")


def chart_staleness_by_ds():
    df = pd.read_csv(OUTPUTS / "ti_999_staleness_histogram_2026_05_28.csv")
    df = df[df["bucket"] != "deprecated"].copy()

    bucket_order = ["0_30d", "31_90d", "91_180d", "181_365d", "366_730d", "over_730d"]
    bucket_labels = ["0-30d", "31-90d", "91-180d", "181-365d", "366-730d", ">730d"]
    ds_meta = [(35, "LiveRamp IP"), (17, "ShareThis"), (18, "Dstillery")]

    fig, axes = plt.subplots(1, 3, figsize=(14, 5), dpi=200, sharey=False)
    fig.suptitle("Two of three active 3P interest providers haven't refreshed metadata in 2+ years",
                 fontsize=14, fontweight="bold", color=NAVY, x=0.02, ha="left", y=1.02)
    fig.text(0.02, 0.95, "Active (non-deprecated) categories by days since last update.",
             color=GRAY, fontsize=10)

    for ax, (ds_id, name) in zip(axes, ds_meta):
        sub = df[df["data_source_id"] == ds_id].set_index("bucket").reindex(bucket_order).fillna(0)
        # Highlight ">730d" stale bar in red; everything else gray for ShareThis/Dstillery,
        # everything else navy for LiveRamp (the fresh one)
        colors = []
        for b in bucket_order:
            if b == "over_730d":
                colors.append(ACCENT_RED)
            elif name == "LiveRamp IP" and b == "0_30d":
                colors.append(NAVY)
            else:
                colors.append(GRAY)
        bars = ax.bar(range(len(bucket_order)), sub["n_categories"], color=colors, width=0.7)
        ax.set_xticks(range(len(bucket_order)))
        ax.set_xticklabels(bucket_labels, rotation=45, ha="right", fontsize=9, color=NAVY)
        ax.set_title(f"DS{ds_id}  {name}", loc="left", fontsize=12, color=NAVY)
        total = int(sub["n_categories"].sum())
        ax.text(0.98, 0.95, f"{total:,} active", transform=ax.transAxes,
                ha="right", va="top", color=GRAY, fontsize=10, fontweight="bold")
        ax.set_yticks([])
        ax.set_ylabel("")
        # direct-label nonzero bars
        for i, b in enumerate(bars):
            v = int(b.get_height())
            if v > 0:
                ax.text(b.get_x() + b.get_width() / 2, b.get_height() + total * 0.03,
                        f"{v:,}", ha="center", va="bottom", fontsize=9, color=NAVY)

    fig.tight_layout(rect=[0, 0, 1, 0.92])
    out = HERE / "ti_999_chart_staleness_by_ds.png"
    fig.savefig(out, dpi=200, bbox_inches="tight", facecolor="#FAFAFA")
    print(f"wrote {out}")


def chart_stale_exposure():
    df = pd.read_csv(OUTPUTS / "ti_999_stale_exposure_30d_2026_05_28.csv").set_index("subset")
    total = df.loc["all_active", "total_spend_30d"]
    stale_3p = df.loc["uses_stale_3p_any", "total_spend_30d"]
    liveramp = df.loc["uses_liveramp_any", "total_spend_30d"]
    other_interest = max(stale_3p + liveramp - (df.loc["uses_sharethis_any", "total_spend_30d"] +
                                                 df.loc["uses_dstillery_any", "total_spend_30d"] +
                                                 liveramp), 0)
    # Simpler: three stacks — stale_3p, liveramp (fresh), no_interest
    # Per the bucket query: 35.7% interest-using total = $14.36M. Of that, $7.73M touches stale 3P.
    # Recompute clean:
    bucket_df = pd.read_csv(OUTPUTS / "ti_999_campaign_buckets_30d_2026_05_28.csv")
    spend_by_bucket = bucket_df.set_index("bucket")["total_spend_30d"]
    interest_total = spend_by_bucket["interest_only"] + spend_by_bucket["interest_mixed"]
    no_interest = spend_by_bucket["no_interest"]

    # Stack: no_interest, interest_using_fresh_only, interest_using_with_stale
    # interest_using_with_stale = stale_3p (campaigns that touch DS17 or DS18) — but those campaigns
    # also count in interest buckets. To avoid double counting, derive: interest_total = stale_3p_spend + (interest_using - stale_3p_spend)
    # However stale-3P-touching campaigns CAN also touch liveramp. So splitting cleanly is messy.
    # Cleaner display: two-tier bar showing TOTAL $40.3M with $7.73M stale-3P exposure annotated.

    fig, ax = plt.subplots(figsize=(12, 5.5), dpi=200)
    fig.suptitle("$7.7M / month flows through stale 3P segments — ~$93M annualized",
                 fontsize=14, fontweight="bold", color=NAVY, x=0.05, ha="left", y=0.97)
    fig.text(0.05, 0.89,
             "Spend on campaigns referencing ShareThis or Dstillery (both >2yr stale). "
             "30-day window ending 2026-05-28.",
             color=GRAY, fontsize=10)

    # Horizontal stacked bar
    segments = [
        ("No interest segments", no_interest, GRAY),
        ("Interest, no stale-3P", interest_total - stale_3p, NAVY),
        ("Touches stale 3P (ShareThis/Dstillery)", stale_3p, ACCENT_RED),
    ]
    left = 0
    for label, val, color in segments:
        ax.barh([0], [val], left=[left], color=color, height=0.45)
        pct = val / total * 100
        if val / total > 0.05:
            ax.text(left + val / 2, 0, f"${val / 1e6:.2f}M\n{pct:.1f}%",
                    ha="center", va="center", color="white", fontsize=11, fontweight="bold")
        else:
            ax.text(left + val / 2, -0.4, f"${val / 1e6:.2f}M ({pct:.1f}%)",
                    ha="center", va="top", color=ACCENT_RED, fontsize=10, fontweight="bold")
        left += val

    # legend via annotations to the right
    ax.set_xlim(0, total * 1.05)
    ax.set_ylim(-0.7, 0.7)
    ax.set_yticks([])
    ax.set_xticks([])
    ax.spines["bottom"].set_visible(False)
    ax.spines["left"].set_visible(False)

    # bottom labels (stagger vertically to avoid collision on narrow segments)
    cur = 0
    for i, (label, val, color) in enumerate(segments):
        y_off = -0.50 if i == len(segments) - 1 else -0.40
        # wrap long labels
        wrapped = label.replace(" (", "\n(") if "(" in label else label
        ax.text(cur + val / 2, y_off, wrapped, ha="center", va="top",
                color=color, fontsize=10, fontweight="bold" if i == len(segments) - 1 else "normal")
        cur += val

    fig.tight_layout(rect=[0, 0, 1, 0.85])
    out = HERE / "ti_999_chart_stale_exposure.png"
    fig.savefig(out, dpi=200, bbox_inches="tight", facecolor="#FAFAFA")
    print(f"wrote {out}")


def chart_within_advertiser_kpi():
    df = pd.read_csv(OUTPUTS / "ti_999_within_advertiser_kpi_2026_05_28.csv")
    df = df.set_index("bucket")
    interest_cr = df.loc["interest", "conversion_rate"] * 100
    no_interest_cr = df.loc["no_interest", "conversion_rate"] * 100
    ratio = no_interest_cr / interest_cr

    fig, ax = plt.subplots(figsize=(11, 6), dpi=200)
    fig.suptitle(
        f"Within the same {int(df.loc['interest', 'n_advertisers']):,} advertisers, "
        f"no-interest converts at {ratio:.1f}x interest's rate",
        fontsize=14, fontweight="bold", color=NAVY, x=0.05, ha="left", y=0.97)
    fig.text(0.05, 0.87,
             "Removes advertiser-mix selection. Gap is likely funnel-stage (prospecting vs retargeting), "
             "not segment-quality.\n30-day window ending 2026-05-28.",
             color=GRAY, fontsize=10)

    labels = ["Interest-using\ncampaigns", "No-interest\ncampaigns"]
    values = [interest_cr, no_interest_cr]
    colors = [ACCENT_RED, GRAY]
    bars = ax.bar(range(2), values, color=colors, width=0.45)

    for i, b in enumerate(bars):
        ax.text(b.get_x() + b.get_width() / 2, b.get_height() + 0.005,
                f"{values[i]:.3f}%", ha="center", va="bottom",
                color=NAVY, fontsize=14, fontweight="bold")
        n_camps_imp_spend = (
            f"{int(df.iloc[i]['impressions_30d']) / 1e6:.0f}M impressions  ·  "
            f"${df.iloc[i]['total_spend_30d'] / 1e6:.2f}M spend"
        )
        ax.text(b.get_x() + b.get_width() / 2, -0.020,
                n_camps_imp_spend, ha="center", va="top",
                color=GRAY, fontsize=9)

    ax.set_xticks(range(2))
    ax.set_xticklabels(labels, fontsize=12, color=NAVY)
    ax.set_ylim(0, max(values) * 1.30)
    ax.set_yticks([])
    ax.set_ylabel("")
    ax.text(-0.05, 1.02, "Conversion rate", transform=ax.transAxes,
            fontsize=10, color=GRAY)
    fig.tight_layout(rect=[0, 0, 1, 0.80])
    out = HERE / "ti_999_chart_within_advertiser_kpi.png"
    fig.savefig(out, dpi=200, bbox_inches="tight", facecolor="#FAFAFA")
    print(f"wrote {out}")


def chart_stale_vs_fresh_kpi():
    df = pd.read_csv(OUTPUTS / "ti_999_stale_vs_fresh_kpi_2026_05_28.csv")
    df = df.set_index("bucket")
    order = ["a_no_interest", "b_only_fresh_liveramp", "c_only_stale_3p", "d_fresh_and_stale_mix"]
    labels = [
        "No interest\nsegments",
        "Only fresh\nLiveRamp",
        "Only stale 3P\n(ShareThis/Dstillery)",
        "Fresh + stale\nmixed",
    ]
    cr = [df.loc[b, "conversion_rate"] * 100 for b in order]
    fresh_cr = df.loc["b_only_fresh_liveramp", "conversion_rate"] * 100
    stale_cr = df.loc["c_only_stale_3p", "conversion_rate"] * 100
    gap_pct = (fresh_cr - stale_cr) / fresh_cr * 100

    fig, ax = plt.subplots(figsize=(12, 6.5), dpi=200)
    fig.suptitle(
        f"Stale-only campaigns convert {gap_pct:.0f}% worse than fresh-only LiveRamp",
        fontsize=14, fontweight="bold", color=NAVY, x=0.05, ha="left", y=0.97)
    fig.text(0.05, 0.88,
             "Conversion rate per audience-composition bucket. "
             "Direction supports the 'freshness matters' hypothesis (small-n caveat on stale-only).\n"
             "30-day window ending 2026-05-28.",
             color=GRAY, fontsize=10)

    colors = [GRAY, NAVY, ACCENT_RED, "#7F8C8D"]
    bars = ax.bar(range(4), cr, color=colors, width=0.55)

    for i, b in enumerate(bars):
        ax.text(b.get_x() + b.get_width() / 2, b.get_height() + 0.004,
                f"{cr[i]:.3f}%", ha="center", va="bottom",
                color=NAVY, fontsize=12, fontweight="bold")
        ax.text(b.get_x() + b.get_width() / 2, -0.014,
                f"{int(df.loc[order[i], 'n_campaigns']):,} camps\n"
                f"${df.loc[order[i], 'total_spend_30d'] / 1e6:.2f}M spend",
                ha="center", va="top", color=GRAY, fontsize=9)

    ax.set_xticks(range(4))
    ax.set_xticklabels(labels, fontsize=11, color=NAVY)
    ax.set_ylim(0, max(cr) * 1.20)
    ax.set_yticks([])
    ax.set_ylabel("")
    ax.text(-0.05, 1.02, "Conversion rate", transform=ax.transAxes,
            fontsize=10, color=GRAY)
    fig.tight_layout(rect=[0, 0, 1, 0.80])
    out = HERE / "ti_999_chart_stale_vs_fresh_kpi.png"
    fig.savefig(out, dpi=200, bbox_inches="tight", facecolor="#FAFAFA")
    print(f"wrote {out}")


def chart_top_advertisers_stale():
    df = pd.read_csv(OUTPUTS / "ti_999_top_advertisers_stale_2026_05_28.csv")
    df = df.head(15).iloc[::-1].reset_index(drop=True)  # smallest on bottom, largest on top
    total_stale_spend = 7725333.81  # from finding 4
    top_n_pct = df["stale_spend"].sum() / total_stale_spend * 100

    fig, ax = plt.subplots(figsize=(12, 9), dpi=200)
    fig.suptitle(
        f"Top 15 advertisers concentrate {top_n_pct:.0f}% of stale-3P exposure",
        fontsize=14, fontweight="bold", color=NAVY, x=0.05, ha="left", y=0.97)
    fig.text(0.05, 0.91,
             "$7.73M total monthly stale-3P spend exposure. "
             "Top 5 alone ≈ 42%. WGU is the single largest at $1.4M / month.\n"
             "30-day window ending 2026-05-28.",
             color=GRAY, fontsize=10)

    # color top 5 in accent red, rest navy
    colors = [ACCENT_RED if i >= len(df) - 5 else NAVY for i in range(len(df))]
    bars = ax.barh(range(len(df)), df["stale_spend"] / 1e6, color=colors, height=0.7)

    for i, b in enumerate(bars):
        ax.text(b.get_width() + 0.02, i, f"${b.get_width():.2f}M",
                va="center", ha="left", color=NAVY, fontsize=10, fontweight="bold")

    ax.set_yticks(range(len(df)))
    ax.set_yticklabels(df["company_name"].str.slice(0, 40), fontsize=10, color=NAVY)
    ax.set_xlabel("Stale-3P spend exposure, 30 days ($M)", color=GRAY, fontsize=10)
    ax.set_xlim(0, df["stale_spend"].max() / 1e6 * 1.20)
    ax.set_xticks([])
    ax.spines["bottom"].set_visible(False)
    fig.tight_layout(rect=[0, 0, 1, 0.89])
    out = HERE / "ti_999_chart_top_advertisers_stale.png"
    fig.savefig(out, dpi=200, bbox_inches="tight", facecolor="#FAFAFA")
    print(f"wrote {out}")


def chart_1p_vs_3p_buckets():
    df = pd.read_csv(OUTPUTS / "ti_999_1p_vs_3p_buckets_2026_05_28.csv")
    df = df.set_index("bucket")
    order = ["d_neither_1p_nor_3p", "a_1p_only", "b_3p_only", "c_both_1p_and_3p"]
    labels = [
        "Neither\n(retargeting / MNTN)",
        "1P only\n(CRM upload)",
        "3P only\n(LiveRamp / ShareThis / Dstillery)",
        "Both\n1P + 3P",
    ]
    cr = [df.loc[b, "conversion_rate"] * 100 for b in order]
    n_camps = [int(df.loc[b, "n_campaigns"]) for b in order]
    spend = [df.loc[b, "total_spend_30d"] / 1e6 for b in order]
    avg_3p = [df.loc[b, "avg_n_3p_dscids"] for b in order]
    avg_1p = [df.loc[b, "avg_n_1p_dscids"] for b in order]

    # Headline ratio: 1P-only beats 3P-only
    onep_cr = df.loc["a_1p_only", "conversion_rate"] * 100
    threep_cr = df.loc["b_3p_only", "conversion_rate"] * 100
    lift = (onep_cr - threep_cr) / threep_cr * 100

    fig, ax = plt.subplots(figsize=(13, 6.5), dpi=200)
    fig.suptitle(
        f"1P-only campaigns convert {lift:.0f}% better than 3P-only — "
        f"and layering both is the worst",
        fontsize=14, fontweight="bold", color=NAVY, x=0.05, ha="left", y=0.97)
    fig.text(0.05, 0.88,
             "Conversion rate per audience-composition bucket. Average dscid counts under each bar show targeting volume.\n"
             "30-day window ending 2026-05-28.",
             color=GRAY, fontsize=10)

    colors = [GRAY, NAVY, ACCENT_RED, "#7F8C8D"]
    bars = ax.bar(range(4), cr, color=colors, width=0.55)

    for i, b in enumerate(bars):
        ax.text(b.get_x() + b.get_width() / 2, b.get_height() + 0.005,
                f"{cr[i]:.3f}%", ha="center", va="bottom",
                color=NAVY, fontsize=12, fontweight="bold")
        # Footer with detail
        ax.text(b.get_x() + b.get_width() / 2, -0.018,
                f"{n_camps[i]:,} camps · ${spend[i]:.2f}M spend\n"
                f"avg {avg_1p[i]:.1f} 1P  +  {avg_3p[i]:.1f} 3P  dscids",
                ha="center", va="top", color=GRAY, fontsize=9)

    ax.set_xticks(range(4))
    ax.set_xticklabels(labels, fontsize=11, color=NAVY)
    ax.set_ylim(0, max(cr) * 1.20)
    ax.set_yticks([])
    ax.set_ylabel("")
    ax.text(-0.03, 1.02, "Conversion rate", transform=ax.transAxes,
            fontsize=10, color=GRAY)
    fig.tight_layout(rect=[0, 0, 1, 0.80])
    out = HERE / "ti_999_chart_1p_vs_3p_buckets.png"
    fig.savefig(out, dpi=200, bbox_inches="tight", facecolor="#FAFAFA")
    print(f"wrote {out}")


def chart_spend_tier_3p_usage():
    df = pd.read_csv(OUTPUTS / "ti_999_advertiser_tiers_2026_05_28.csv")
    # Tier display order: enterprise first (top)
    tier_labels = {
        "a_enterprise_100K+": "Enterprise\n($100K+ in 30d)",
        "b_mid_20K_100K":     "Mid-market\n($20-100K)",
        "c_smb_5K_20K":       "SMB\n($5-20K)",
        "d_micro_under_5K":   "Micro\n(<$5K)",
    }
    df = df.set_index("spend_tier")
    order = ["a_enterprise_100K+", "b_mid_20K_100K", "c_smb_5K_20K", "d_micro_under_5K"]

    n_advs = [int(df.loc[t, "n_advertisers"]) for t in order]
    tier_spend = [df.loc[t, "tier_total_spend"] / 1e6 for t in order]
    pct_3p = [df.loc[t, "pct_advs_use_3p"] for t in order]
    pct_1p = [df.loc[t, "pct_advs_use_1p"] for t in order]
    pct_stale = [df.loc[t, "pct_advs_use_stale_3p"] for t in order]

    fig, axes = plt.subplots(1, 2, figsize=(16, 7), dpi=200, gridspec_kw={"width_ratios": [1.1, 1]})
    fig.suptitle(
        "Enterprise advertisers use 3P most (62%) — but every tier is ~50% 3P-user",
        fontsize=14, fontweight="bold", color=NAVY, x=0.04, ha="left", y=0.98)
    fig.text(0.04, 0.92,
             "Each tier defined by 30-day spend. Top 77 advertisers ('enterprise') account for 50.6% of all MNTN spend.",
             color=GRAY, fontsize=10)

    # Left chart: % of advertisers using 1P / 3P / stale-3P per tier
    ax = axes[0]
    x = list(range(len(order)))
    width = 0.27
    b1 = ax.bar([xi - width for xi in x], pct_1p, width=width, color=NAVY, label="1P (CRM/IP)")
    b2 = ax.bar(x,                          pct_3p, width=width, color=ACCENT_RED, label="3P (LiveRamp/ShareThis/Dstillery)")
    b3 = ax.bar([xi + width for xi in x], pct_stale, width=width, color="#E67E22", label="Stale 3P (ShareThis/Dstillery)")
    for grp in (b1, b2, b3):
        for r in grp:
            h = r.get_height()
            ax.text(r.get_x() + r.get_width() / 2, h + 1.5,
                    f"{h:.0f}%", ha="center", va="bottom", color=NAVY, fontsize=9, fontweight="bold")
    ax.set_xticks(x)
    ax.set_xticklabels([tier_labels[t] for t in order], fontsize=10, color=NAVY)
    ax.set_ylim(0, 80)
    ax.set_yticks([])
    ax.legend(loc="upper right", frameon=False, fontsize=9)
    ax.set_title("% of advertisers in tier that use each DS family",
                 loc="left", fontsize=11, color=GRAY, pad=10)
    # bottom annotation
    for i, t in enumerate(order):
        ax.text(i, -8,
                f"{n_advs[i]:,} advs\n${tier_spend[i]:.1f}M tier spend",
                ha="center", va="top", color=GRAY, fontsize=9)

    # Right chart: tier share of total spend (enterprise at top)
    ax2 = axes[1]
    total = sum(tier_spend)
    spend_pct = [s / total * 100 for s in tier_spend]
    colors_right = ["#C0392B", "#2C3E50", "#7F8C8D", "#95A5A6"]
    y = list(range(len(order)))
    bars = ax2.barh(y, spend_pct, color=colors_right, height=0.6)
    for i, b in enumerate(bars):
        ax2.text(b.get_width() + 0.7, b.get_y() + b.get_height() / 2,
                 f"{spend_pct[i]:.1f}%   (${tier_spend[i]:.1f}M)",
                 va="center", ha="left", color=NAVY, fontsize=10, fontweight="bold")
    ax2.set_yticks(y)
    ax2.set_yticklabels([tier_labels[t].replace("\n", " ") for t in order],
                       fontsize=10, color=NAVY)
    ax2.invert_yaxis()  # enterprise at top
    ax2.set_xticks([])
    ax2.set_xlim(0, max(spend_pct) * 1.35)
    ax2.spines["bottom"].set_visible(False)
    ax2.set_title("Spend share by tier (of $40.3M total)",
                  loc="left", fontsize=11, color=GRAY, pad=10)

    fig.tight_layout(rect=[0, 0, 1, 0.85])
    out = HERE / "ti_999_chart_spend_tier_3p_usage.png"
    fig.savefig(out, dpi=200, bbox_inches="tight", facecolor="#FAFAFA")
    print(f"wrote {out}")


def chart_ip_overlap():
    df = pd.read_csv(OUTPUTS / "ti_999_ip_overlap_2026_05_26.csv").set_index("metric")
    n_1p_only = float(df.loc["n_ips_1p_only", "value"])
    n_both = float(df.loc["n_ips_1p_and_3p", "value"])
    n_3p_only = float(df.loc["n_ips_3p_only", "value"])
    pct_3p_in_1p = float(df.loc["pct_3p_overlapping_1p", "value"])

    total = n_1p_only + n_both + n_3p_only

    fig, ax = plt.subplots(figsize=(13, 5.5), dpi=200)
    fig.suptitle(
        f"{pct_3p_in_1p:.0f}% of 3P IPs are already in CRM — 3P brings ~28% incremental reach",
        fontsize=14, fontweight="bold", color=NAVY, x=0.04, ha="left", y=0.97)
    fig.text(0.04, 0.89,
             "Single-day IP-set composition (2026-05-26): IPs in DS4 (CRM) vs IPs in DS17/18/35 (3P interest).\n"
             "If an advertiser already has CRM, most of 3P's reach is duplicative.",
             color=GRAY, fontsize=10)

    segments = [
        ("1P CRM only", n_1p_only, NAVY),
        ("In both 1P + 3P", n_both, ACCENT_RED),
        ("3P only", n_3p_only, "#7F8C8D"),
    ]
    left = 0
    for label, val, color in segments:
        ax.barh([0], [val], left=[left], color=color, height=0.45)
        pct = val / total * 100
        ax.text(left + val / 2, 0,
                f"{val/1e6:.0f}M\n{pct:.1f}%",
                ha="center", va="center", color="white",
                fontsize=11, fontweight="bold")
        left += val

    # bottom labels
    cur = 0
    for label, val, color in segments:
        ax.text(cur + val / 2, -0.45, label, ha="center", va="top",
                color=color, fontsize=11, fontweight="bold")
        cur += val

    ax.set_xlim(0, total * 1.02)
    ax.set_ylim(-0.7, 0.7)
    ax.set_yticks([])
    ax.set_xticks([])
    ax.spines["bottom"].set_visible(False)
    ax.spines["left"].set_visible(False)
    fig.tight_layout(rect=[0, 0, 1, 0.83])
    out = HERE / "ti_999_chart_ip_overlap.png"
    fig.savefig(out, dpi=200, bbox_inches="tight", facecolor="#FAFAFA")
    print(f"wrote {out}")


if __name__ == "__main__":
    chart_bucket_spend_share()
    chart_staleness_by_ds()
    chart_stale_exposure()
    chart_within_advertiser_kpi()
    chart_stale_vs_fresh_kpi()
    chart_top_advertisers_stale()
    chart_1p_vs_3p_buckets()
    chart_spend_tier_3p_usage()
    chart_ip_overlap()
