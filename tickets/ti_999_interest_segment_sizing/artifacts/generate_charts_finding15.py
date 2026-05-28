"""Generate Finding 15 PNG charts for TI-999 presentation.

Tufte principles: high data-ink ratio, direct labels, accent color only on the
key insight, one number per chart where possible.

Reads: outputs/*.csv (Pass 1, 2, 3, 6, 7, 9, 10, 11, 12, 12b results)
Writes: artifacts/ti_999_chart_finding15_*.png at 200 DPI.
"""
from pathlib import Path
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
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
    "text.parse_math": False,
    "mathtext.default": "regular",
})

ACCENT = "#C0392B"      # accent red for key insight
NAVY = "#2C3E50"        # supporting data
GRAY = "#95A5A6"        # context
GREEN = "#27AE60"       # positive
ORANGE = "#E67E22"      # warning


def save(fig, name):
    out = HERE / f"ti_999_chart_finding15_{name}.png"
    fig.savefig(out, dpi=200, bbox_inches="tight", facecolor="#FAFAFA")
    plt.close(fig)
    print(f"  wrote {out.name}")


def chart_8bucket_venn():
    """8-bucket Venn — % campaigns vs % spend per bucket."""
    data = [
        ("nothing",            11365, 73.2, 14.485, 35.9),
        ("MM only",              574,  3.7,  1.818,  4.5),
        ("1P only",             1292,  8.3,  7.654, 18.9),
        ("3P only",              858,  5.5,  5.240, 13.0),
        ("MM + 3P",              717,  4.6,  3.391,  8.4),
        ("MM + 1P",              320,  2.1,  2.017,  5.0),
        ("1P + 3P",              251,  1.6,  4.518, 11.2),
        ("MM + 1P + 3P",         152,  1.0,  1.272,  3.1),
    ]
    df = pd.DataFrame(data, columns=["bucket","n_camps","pct_camps","spend_M","pct_spend"])

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5.5))

    # Left: % campaigns
    colors = [GRAY] * len(df)
    df["color"] = colors
    ax1.barh(df["bucket"][::-1], df["pct_camps"][::-1], color=df["color"][::-1], edgecolor="none")
    for i, (b, p) in enumerate(zip(df["bucket"][::-1], df["pct_camps"][::-1])):
        ax1.text(p + 1, i, f"{p:.1f}%", va="center", fontsize=10, color=NAVY)
    ax1.set_title("% of all active campaigns", loc="left", color=NAVY)
    ax1.set_xlim(0, 80)
    ax1.set_xticks([])

    # Right: % spend — highlight 3P-touching cohorts in accent
    colors = [
        GRAY, GRAY, GRAY,
        ACCENT,   # 3P only
        ACCENT,   # MM + 3P
        GRAY,     # MM + 1P
        ACCENT,   # 1P + 3P
        ACCENT,   # MM + 1P + 3P
    ]
    df["color2"] = colors
    ax2.barh(df["bucket"][::-1], df["pct_spend"][::-1], color=df["color2"][::-1], edgecolor="none")
    for i, (b, p) in enumerate(zip(df["bucket"][::-1], df["pct_spend"][::-1])):
        ax2.text(p + 0.5, i, f"{p:.1f}%", va="center", fontsize=10, color=NAVY)
    ax2.set_title("% of 30d spend ($40.4M)", loc="left", color=NAVY)
    ax2.set_xlim(0, 45)
    ax2.set_xticks([])

    fig.suptitle("3P-touching cohorts (red) hold 36% of MNTN spend (~$172M/yr)",
                 fontsize=14, fontweight="bold", color=NAVY, y=1.00)
    fig.text(0.5, -0.04, "Window: 30d ending 2026-05-28 · 15,529 active campaigns · $40.42M",
             ha="center", fontsize=9, color=GRAY)
    plt.tight_layout()
    save(fig, "01_8bucket_venn")


def chart_or_vs_and_delivery():
    """Delivery distribution per OR/AND/EXCL pattern — unscored share per pattern."""
    patterns = [
        ("MM only\n(baseline)",                  4.2, 393, "$1.82M", GRAY),
        ("MM AND 3P\n(intersect/narrow)",         8.4,  35, "$0.16M", NAVY),
        ("MM AND NOT 1P\n(CRM suppress)",         6.7, 287, "$1.88M", NAVY),
        ("MM OR 3P\n(union/expand)",             14.1, 372, "$2.15M", ACCENT),
        ("MM OR 3P + AND 3P\n(hybrid)",          56.0,  34, "$0.45M", ACCENT),
    ]
    df = pd.DataFrame(patterns, columns=["label","unscored_pct","n_camps","spend","color"])

    fig, ax = plt.subplots(figsize=(11, 6))
    bars = ax.barh(df["label"][::-1], df["unscored_pct"][::-1], color=df["color"][::-1], edgecolor="none")
    for i, (lab, p, n, sp) in enumerate(zip(df["label"][::-1], df["unscored_pct"][::-1],
                                             df["n_camps"][::-1], df["spend"][::-1])):
        ax.text(p + 1, i, f"{p:.1f}%   ({n} camps, {sp})", va="center", fontsize=10, color=NAVY)

    ax.set_xlim(0, 70)
    ax.set_xticks([])
    ax.set_title("OR-additive inclusion (red) brings 3.3x more unscored delivery than baseline",
                 loc="left", color=NAVY)
    fig.text(0.5, -0.03,
             "% of impressions on IPs with household_score = -1 (unscored). 2026-05-26 delivery, ~61M imps.",
             ha="center", fontsize=9, color=GRAY)
    plt.tight_layout()
    save(fig, "02_or_vs_and_delivery")


def chart_fico_ceiling():
    """FICO: same scored ceiling, different overflow."""
    fig, ax = plt.subplots(figsize=(11, 5.5))

    campaigns = ["FICO MM_only\n(525934)", "FICO MM + 3P OR\n(325113)"]
    scored = [71.5, 60.1]      # thousands
    unscored = [0.3, 236.4]    # thousands
    spend = ["$41.7K", "$168.5K"]

    x = range(len(campaigns))
    bar1 = ax.bar(x, scored, color=NAVY, width=0.55, label="Scored impressions (MM)")
    bar2 = ax.bar(x, unscored, bottom=scored, color=ACCENT, width=0.55, label="Unscored impressions (3P overflow)")

    for i, (s, u, sp) in enumerate(zip(scored, unscored, spend)):
        ax.text(i, s/2, f"{s:.1f}K", ha="center", va="center", fontsize=12, color="white", fontweight="bold")
        if u > 2:
            ax.text(i, s + u/2, f"{u:.1f}K", ha="center", va="center", fontsize=12, color="white", fontweight="bold")
        ax.text(i, -25, sp, ha="center", fontsize=11, color=NAVY, fontweight="bold")
        ax.text(i, -40, "(30d spend)", ha="center", fontsize=9, color=GRAY)

    ax.set_xticks(x)
    ax.set_xticklabels(campaigns, fontsize=10)
    ax.set_ylabel("Impressions on 2026-05-26 (thousands)", color=NAVY)
    ax.set_ylim(-50, 320)
    ax.set_yticks([0, 50, 100, 150, 200, 250, 300])
    ax.legend(loc="upper left", frameon=False, fontsize=10)

    ax.set_title("FICO's MM ceiling is ~60-72K scored imps/day — 4x the budget overflows to 3P unscored",
                 loc="left", color=NAVY)
    plt.tight_layout()
    save(fig, "03_fico_ceiling")


def chart_ceiling_bound_distribution():
    """Of MM+3P_incl_only campaigns: % ceiling-bound vs not."""
    fig, ax = plt.subplots(figsize=(11, 5))

    cats = ["Ceiling-bound\n(>50% unscored)", "Partial overflow\n(10-50%)", "Below ceiling\n(<10%)"]
    pct_camps = [17.7, 6.0, 76.3]
    pct_spend = [26.3, 3.2, 70.5]
    colors = [ACCENT, ORANGE, GRAY]

    x = range(len(cats))
    width = 0.35
    b1 = ax.bar([i - width/2 for i in x], pct_camps, width, color=colors, alpha=0.7, label="% of campaigns", edgecolor="none")
    b2 = ax.bar([i + width/2 for i in x], pct_spend, width, color=colors, alpha=1.0, label="% of spend", edgecolor="none")

    for i, (pc, ps) in enumerate(zip(pct_camps, pct_spend)):
        ax.text(i - width/2, pc + 1.5, f"{pc:.1f}%", ha="center", fontsize=11, color=NAVY)
        ax.text(i + width/2, ps + 1.5, f"{ps:.1f}%", ha="center", fontsize=11, color=NAVY)

    ax.set_xticks(x)
    ax.set_xticklabels(cats, fontsize=10)
    ax.set_yticks([])
    ax.set_ylim(0, 90)
    ax.legend(loc="upper right", frameon=False, fontsize=10)

    ax.set_title("Only 17.7% of MM+3P campaigns actually overflow into 3P — 76% have unused 3P clauses",
                 loc="left", color=NAVY)
    fig.text(0.5, -0.02, "Cohort: 430 of 609 MM+3P_incl_only campaigns with ≥100 delivered impressions on 2026-05-26.",
             ha="center", fontsize=9, color=GRAY)
    plt.tight_layout()
    save(fig, "04_ceiling_bound_distribution")


def chart_efficiency_per_bucket():
    """Cost per conversion across the major prospecting buckets."""
    data = [
        ("Nothing\n(retargeting/RTC)", 16, GRAY),
        ("MM only",                     51, GRAY),
        ("1P only",                     26, GRAY),
        ("MM + 3P (OR)",                45, GRAY),
        ("3P only",                     75, ACCENT),
        ("MM + 1P (CRM suppress)",     107, ORANGE),
        ("1P + 3P\n(anti-pattern)",    192, ACCENT),
    ]
    df = pd.DataFrame(data, columns=["bucket","cost_conv","color"])

    fig, ax = plt.subplots(figsize=(11, 5.5))
    bars = ax.barh(df["bucket"][::-1], df["cost_conv"][::-1], color=df["color"][::-1], edgecolor="none")
    for i, (b, c) in enumerate(zip(df["bucket"][::-1], df["cost_conv"][::-1])):
        ax.text(c + 3, i, f"${c}", va="center", fontsize=11, color=NAVY)
    ax.set_xlim(0, 220)
    ax.set_xticks([])
    ax.set_title("Layering 1P + 3P without MM costs $192/conversion — 3.8x worse than MM_only",
                 loc="left", color=NAVY)
    fig.text(0.5, -0.02,
             "Spend-weighted cost per conversion across prospecting cohorts (30d ending 2026-05-28).",
             ha="center", fontsize=9, color=GRAY)
    plt.tight_layout()
    save(fig, "05_efficiency_per_bucket")


def chart_segment_cvr_quintiles():
    """The 350x CVR spread across LiveRamp dscid quintiles."""
    quintiles = ["Q5\ntop 20%", "Q4", "Q3", "Q2", "Q1\nbottom 20%"]
    cvr = [0.140, 0.016, 0.007, 0.002, 0.0004]
    spend_pct = [13.9, 14.1, 13.1, 14.7, 12.4]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5))

    # CVR
    colors = [GREEN, NAVY, NAVY, ORANGE, ACCENT]
    bars = ax1.bar(quintiles, cvr, color=colors, edgecolor="none")
    for i, (q, c) in enumerate(zip(quintiles, cvr)):
        ax1.text(i, c + 0.005, f"{c*100:.3f}%", ha="center", fontsize=10, color=NAVY)
    ax1.set_title("Top LiveRamp segments convert at 0.140% — bottom at 0.0004% (350x spread)",
                  loc="left", color=NAVY, fontsize=12)
    ax1.set_ylabel("Conversion rate", color=NAVY)
    ax1.set_yticks([])
    ax1.set_ylim(0, 0.17)

    # Spend %
    ax2.bar(quintiles, spend_pct, color=GRAY, edgecolor="none")
    for i, (q, s) in enumerate(zip(quintiles, spend_pct)):
        ax2.text(i, s + 0.5, f"{s:.1f}%", ha="center", fontsize=10, color=NAVY)
    ax2.set_title("…but buyers spend equally on each tier — no quality signal",
                  loc="left", color=NAVY, fontsize=12)
    ax2.set_ylabel("Share of LiveRamp-touching spend", color=NAVY)
    ax2.set_yticks([])
    ax2.set_ylim(0, 17)

    fig.text(0.5, -0.03,
             "1,005 LiveRamp dscids with ≥100K weighted impressions support. Equal-attribution proxy.",
             ha="center", fontsize=9, color=GRAY)
    plt.tight_layout()
    save(fig, "06_segment_cvr_quintiles")


def chart_counterfactual():
    """Current vs hypothetical conversions — the prize."""
    fig, ax = plt.subplots(figsize=(11, 5.5))

    quintiles = ["Q5\ntop 20%", "Q4", "Q3", "Q2", "Q1\nbottom 20%", "Unranked\n(low support)"]
    actual = [79.95, 13.54, 5.23, 2.15, 0.51, 75.28]      # in thousands
    hypothetical = [86.83, 110.32, 111.22, 148.35, 126.11, 169.13]  # if all at top-Q CVR

    x = range(len(quintiles))
    width = 0.4
    ax.bar([i - width/2 for i in x], actual, width, color=GRAY, label="Actual conversions (30d)", edgecolor="none")
    ax.bar([i + width/2 for i in x], hypothetical, width, color=ACCENT, label="If all imps at top-Q CVR", edgecolor="none")

    for i, (a, h) in enumerate(zip(actual, hypothetical)):
        ax.text(i - width/2, a + 3, f"{a:.0f}K", ha="center", fontsize=9, color=NAVY)
        ax.text(i + width/2, h + 3, f"{h:.0f}K", ha="center", fontsize=9, color=NAVY)

    ax.set_xticks(x)
    ax.set_xticklabels(quintiles)
    ax.set_yticks([])
    ax.set_ylim(0, 200)
    ax.legend(loc="upper left", frameon=False, fontsize=10)

    ax.set_title("Bottom-2 quintile spend ($3.65M/30d) delivers 2.7K conv — would deliver 274K at top-Q quality",
                 loc="left", color=NAVY, fontsize=12)
    fig.text(0.5, -0.02,
             "50% substitution (bottom-2 → top-Q): ~136K incremental conv/month → ~1.6M/year.",
             ha="center", fontsize=9, color=GRAY)
    plt.tight_layout()
    save(fig, "07_counterfactual")


def main():
    print("Generating Finding 15 charts...")
    chart_8bucket_venn()
    chart_or_vs_and_delivery()
    chart_fico_ceiling()
    chart_ceiling_bound_distribution()
    chart_efficiency_per_bucket()
    chart_segment_cvr_quintiles()
    chart_counterfactual()
    print("Done.")


if __name__ == "__main__":
    main()
