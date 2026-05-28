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


if __name__ == "__main__":
    chart_bucket_spend_share()
    chart_staleness_by_ds()
    chart_stale_exposure()
