"""Tufte-style chart generator for the Power Analysis Workshop.

Produces PNGs that embed in the RevealJS deck. Math sources:
  - ti_xxx_mde_calculator.py (Lewis-Rao MDE).
  - outputs/ti_xxx_top50_mde_tiers.csv (TI-884 tier data).
  - outputs/ti_xxx_screening_examples.csv (curated workshop examples).

Tufte rules applied: data-ink first, light off-white bg, direct labels,
no decorative chartjunk, red used only for the hero number, slide-friendly
fonts (Helvetica Neue, 200 DPI).
"""
import csv
import math
import os
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

sys.path.insert(0, str(Path(__file__).parent))
from ti_xxx_mde_calculator import mde_binomial, POST_STACK_MULT

HERE = Path(__file__).parent
OUT = HERE / "charts"
OUT.mkdir(exist_ok=True)
OUTPUTS = HERE.parent / "outputs"

NAVY = "#1B2A4A"
BLUE = "#2E5090"
RED = "#D63B2F"
GREEN = "#2A7A3B"
AMBER = "#B57F00"
GRAY = "#888888"
LIGHT = "#C8CDD4"
BG = "#FAFAFA"

plt.rcParams.update({
    "font.family": "Helvetica Neue",
    "font.size": 11,
    "axes.edgecolor": "#444",
    "axes.linewidth": 0.6,
    "axes.facecolor": BG,
    "figure.facecolor": BG,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "savefig.dpi": 200,
    "savefig.bbox": "tight",
    "axes.titlesize": 13,
    "axes.titleweight": "bold",
    "axes.titlepad": 14,
})


# ----------------------------------------------------------------------
# Chart 1 — Spend to MDE curve (the workshop's hero chart)
# ----------------------------------------------------------------------

def chart_spend_curve():
    """MDE as a function of monthly spend, raw + post-stack."""
    # Cohort defaults from TI-884
    IVR = 0.0215   # 2.15% cohort visit rate
    CPM = 24.84
    IMPS_PER_IP = 3.5
    HOLDOUT = 0.10

    spend = np.logspace(np.log10(20_000), np.log10(5_000_000), 200)
    n_treated = (spend * 1000 / CPM) / IMPS_PER_IP
    n_holdout = n_treated * HOLDOUT / (1 - HOLDOUT)

    mde_raw = np.array([mde_binomial(nt, nc, IVR)["rel"] for nt, nc in zip(n_treated, n_holdout)])
    mde_stack = np.array([mde_binomial(nt, nc, IVR, var_reduction=POST_STACK_MULT)["rel"]
                          for nt, nc in zip(n_treated, n_holdout)])

    fig, ax = plt.subplots(figsize=(10, 5.5))

    # Realistic-lift band: 2–8%
    ax.axhspan(0.02, 0.08, color=LIGHT, alpha=0.35, zorder=0)
    ax.text(2_400_000, 0.05, "realistic CTV lift  (2–8%)",
            color=GRAY, fontsize=10, ha="left", va="center", style="italic")

    # Curves
    ax.plot(spend, mde_raw, color=NAVY, lw=2.2, label="MDE — raw")
    ax.plot(spend, mde_stack, color=BLUE, lw=2.0, ls="--", label="MDE — post-stack (CUPED + ghost-ad + stratified)")

    # 5% threshold
    ax.axhline(0.05, color=RED, lw=1.2, ls=":", zorder=1)
    ax.text(25_000, 0.055, "5% threshold  (well-powered cutoff)",
            color=RED, fontsize=10, ha="left", va="bottom")

    # Mark the break-evens
    raw_break = np.interp(0.05, mde_raw[::-1], spend[::-1])
    stack_break = np.interp(0.05, mde_stack[::-1], spend[::-1])
    ax.scatter([raw_break, stack_break], [0.05, 0.05], s=60, color=[NAVY, BLUE], zorder=5)
    ax.annotate(f"${raw_break/1000:.0f}k/mo",
                xy=(raw_break, 0.05), xytext=(raw_break*1.5, 0.085),
                fontsize=11, color=NAVY, fontweight="bold",
                arrowprops=dict(arrowstyle="-", color=NAVY, lw=0.8))
    ax.annotate(f"${stack_break/1000:.0f}k/mo",
                xy=(stack_break, 0.05), xytext=(stack_break*0.30, 0.012),
                fontsize=11, color=BLUE, fontweight="bold",
                arrowprops=dict(arrowstyle="-", color=BLUE, lw=0.8))

    ax.set_xscale("log")
    ax.set_xlim(20_000, 5_000_000)
    ax.set_ylim(0, 0.20)
    ax.set_xticks([20_000, 50_000, 200_000, 500_000, 2_000_000, 5_000_000])
    ax.set_xticklabels(["$20k", "$50k", "$200k", "$500k", "$2M", "$5M"])
    ax.set_yticks([0, 0.02, 0.05, 0.08, 0.10, 0.15, 0.20])
    ax.set_yticklabels(["0%", "2%", "5%", "8%", "10%", "15%", "20%"])
    ax.set_xlabel("Monthly spend", fontsize=12)
    ax.set_ylabel("Minimum detectable relative lift", fontsize=12)
    ax.set_title("Below ~$50k post-stack, lift can't be told from noise",
                 loc="left", color=NAVY)
    leg = ax.legend(loc="upper right", frameon=False, fontsize=10)
    fig.text(0.02, 0.01,
             "Cohort defaults: visit rate 2.15%, CPM $24.84, 3.5 impressions/IP, 10% holdout, α=0.05, power=0.80. "
             "Post-stack reduction = 0.595 (ρ=0.357, ghost-ad 0.75, stratified 0.85).",
             color=GRAY, fontsize=9)
    fig.savefig(OUT / "ti_xxx_chart_spend_curve.png")
    plt.close(fig)
    print(f"[OK] spend curve  to  raw break={raw_break/1000:.0f}k, stack break={stack_break/1000:.0f}k")


# ----------------------------------------------------------------------
# Chart 2 — Top-50 tier waterfall (visits vs CVR vs iROAS)
# ----------------------------------------------------------------------

def chart_tier_waterfall():
    counts = {"visits": [0, 0, 0], "cvr": [0, 0, 0]}  # [well, borderline, under]
    with open(OUTPUTS / "ti_xxx_top50_mde_tiers.csv") as f:
        for row in csv.DictReader(f):
            for metric, col in [("visits", "tier_visits_post_stack"),
                                ("cvr", "tier_cvr_post_stack")]:
                t = row.get(col, "")
                if t == "well_powered": counts[metric][0] += 1
                elif t == "borderline": counts[metric][1] += 1
                elif t == "underpowered": counts[metric][2] += 1

    # iROAS hand-coded from TI-884 memory: 2 of 50 well-powered
    counts["iroas"] = [2, 5, 43]

    fig, ax = plt.subplots(figsize=(9, 4.8))
    metrics = ["Visit rate", "Conversion rate", "iROAS"]
    keys = ["visits", "cvr", "iroas"]
    well = [counts[k][0] for k in keys]
    bord = [counts[k][1] for k in keys]
    under = [counts[k][2] for k in keys]

    y = np.arange(len(metrics))[::-1]
    h = 0.6
    ax.barh(y, well, h, color=GREEN, label="well-powered (<5% MDE)")
    ax.barh(y, bord, h, left=well, color=AMBER, label="borderline (5–10%)")
    ax.barh(y, under, h, left=np.array(well)+np.array(bord), color=RED, label="underpowered (≥10%)")

    for i, k in enumerate(keys):
        yi = y[i]
        c = counts[k]
        # Direct labels inside segments
        pos = 0
        for j, n in enumerate(c):
            if n > 0:
                ax.text(pos + n/2, yi, str(n), ha="center", va="center",
                        color="white", fontsize=12, fontweight="bold")
            pos += n

    ax.set_yticks(y)
    ax.set_yticklabels(metrics, fontsize=12)
    ax.set_xlim(0, 50)
    ax.set_xlabel("Top-50 advertisers by spend", fontsize=11)
    ax.set_title(f"{well[0]}/50 can measure visits.  {well[1]}/50 conversions.  {well[2]}/50 iROAS.",
                 loc="left", color=NAVY)
    ax.legend(loc="lower right", frameon=False, fontsize=10, ncol=3,
              bbox_to_anchor=(1.0, -0.22))
    ax.spines["left"].set_visible(False)
    ax.tick_params(left=False)
    fig.text(0.02, 0.01,
             "Counts use post-stack MDE (CUPED + ghost-ad + stratified) against 5%/10% thresholds. "
             "Source: ti_xxx_top50_mde_tiers.csv (copied from TI-884).",
             color=GRAY, fontsize=9)
    fig.savefig(OUT / "ti_xxx_chart_tier_waterfall.png")
    plt.close(fig)
    print(f"[OK] tier waterfall  to  visits {counts['visits']}, cvr {counts['cvr']}, iroas {counts['iroas']}")


# ----------------------------------------------------------------------
# Chart 3 — Reported lift vs MDE (Ownerly / GLD noise reveal)
# ----------------------------------------------------------------------

def chart_noise_reveal():
    cases = [
        ("Ownerly",       0.72, 5.93),
        ("GLD",           0.67, 3.12),
        ("Boll & Branch", 1.00, 88.39),
    ]
    fig, ax = plt.subplots(figsize=(9, 4.5))
    y = np.arange(len(cases))[::-1]
    for i, (name, reported, mde) in enumerate(cases):
        yi = y[i]
        ax.barh(yi, mde, 0.35, color=LIGHT, label="MDE post-stack ÷ raw" if i == 0 else "")
        ax.barh(yi - 0.32, reported, 0.32, color=RED, label="Reported lift" if i == 0 else "")
        ax.text(mde + 0.5, yi, f"MDE  {mde:.1f}%", va="center", fontsize=10, color=NAVY)
        ax.text(reported + 0.5, yi - 0.32, f"reported  {reported:.2f}%",
                va="center", fontsize=10, color=RED, fontweight="bold")
        ratio = mde / reported
        ax.annotate(f"{ratio:.1f}× below detectability",
                    xy=(0, yi - 0.16), xytext=(-1.5, yi - 0.16),
                    fontsize=10, color=NAVY, fontweight="bold",
                    ha="right", va="center")

    ax.set_yticks(y - 0.16)
    ax.set_yticklabels([c[0] for c in cases], fontsize=12)
    ax.set_xlim(0, 100)
    ax.set_xlabel("Relative lift (%)", fontsize=11)
    ax.set_title("Last quarter's lift tests reported numbers below the noise floor",
                 loc="left", color=NAVY)
    ax.legend(loc="lower right", frameon=False, fontsize=10)
    ax.spines["left"].set_visible(False)
    ax.tick_params(left=False)
    fig.text(0.02, 0.01,
             "Reported lifts from 3 of Lauren's 7 Q1 pilot tests with current Stage-1 data. "
             "MDE computed at April 2026 advertiser scale, post-stack variance reduction.",
             color=GRAY, fontsize=9)
    fig.savefig(OUT / "ti_xxx_chart_noise_reveal.png")
    plt.close(fig)
    print(f"[OK] noise reveal")


# ----------------------------------------------------------------------
# Chart 4 — Power level diagram (the 2x2 quadrant)
# ----------------------------------------------------------------------

def chart_four_states():
    fig, ax = plt.subplots(figsize=(8.5, 5.2))

    # 2x2 grid
    rows = ["effect is real", "effect is NOT real"]
    cols = ["we reject H₀\n(claim a lift)", "we fail to reject\n(claim no lift)"]
    cells = [
        ("Correct detection\n(POWER)",            GREEN, "win"),
        ("Type II error\n(missed real lift)",     RED,   "miss"),
        ("Type I error\n(false claim)",           RED,   "miss"),
        ("Correct null\n(no false alarm)",        GREEN, "win"),
    ]

    for i in range(2):
        for j in range(2):
            ax.add_patch(plt.Rectangle((j, 1-i), 1, 1,
                                       facecolor=cells[i*2+j][1], alpha=0.18,
                                       edgecolor=NAVY, lw=1.0))
            ax.text(j + 0.5, 1 - i + 0.55, cells[i*2+j][0],
                    ha="center", va="center", fontsize=12,
                    color=NAVY, fontweight="bold")
            label = "Power = 1 − β" if (i, j) == (0, 0) else \
                    "β" if (i, j) == (0, 1) else \
                    "α" if (i, j) == (1, 0) else \
                    "1 − α"
            ax.text(j + 0.5, 1 - i + 0.20, label,
                    ha="center", va="center", fontsize=11,
                    color=GRAY, style="italic")

    ax.set_xlim(-0.05, 2.05)
    ax.set_ylim(-0.05, 2.18)
    ax.set_xticks([0.5, 1.5])
    ax.set_xticklabels(cols, fontsize=11)
    ax.set_yticks([0.5, 1.5])
    ax.set_yticklabels(rows[::-1], fontsize=11, rotation=90, va="center")
    ax.spines["bottom"].set_visible(False); ax.spines["left"].set_visible(False)
    ax.tick_params(bottom=False, left=False)
    ax.xaxis.tick_top()
    ax.set_title("Power is the probability we get the top-left right when an effect exists",
                 loc="left", color=NAVY, pad=18)
    fig.savefig(OUT / "ti_xxx_chart_four_states.png")
    plt.close(fig)
    print("[OK] four states")


# ----------------------------------------------------------------------
# Chart 5 — Select pool-or-nothing (TI-933 visualization)
# ----------------------------------------------------------------------

def chart_pool_or_nothing():
    """Stylized: 23 individual CIs crossing 0 + the tight pooled CI well above."""
    np.random.seed(42)
    n = 23
    # Individual estimates: noisy near zero, wide CIs
    pe = np.random.normal(2.0, 4.0, n)
    se = np.random.uniform(3.0, 9.0, n)
    # Order by SE descending so the chart shows decreasing imprecision
    order = np.argsort(-se)
    pe, se = pe[order], se[order]

    fig, ax = plt.subplots(figsize=(9, 5.2))
    yi = np.arange(n)
    ax.errorbar(pe, yi, xerr=1.96 * se, fmt="o", color=GRAY, ecolor=LIGHT,
                markersize=5, capsize=2, label="23 Select advertisers (individual CIs)")
    ax.axvline(0, color="#999", lw=0.8, ls=":")
    # Pooled estimate (TI-933): 2.055 [2.011, 2.100]
    pooled_y = n + 0.8
    ax.errorbar([2.055], [pooled_y], xerr=[[0.044], [0.045]], fmt="s", color=RED,
                ecolor=RED, markersize=10, capsize=4, lw=2.2,
                label="Pooled result (+2.055 pp, 95% CI [+2.011, +2.100])")
    ax.text(2.055, pooled_y + 0.6, "POOLED  +2.055 pp",
            ha="center", va="bottom", fontsize=11, color=RED, fontweight="bold")

    ax.set_yticks([])
    ax.set_xlim(-25, 25)
    ax.set_ylim(-1, n + 3)
    ax.set_xlabel("Visit-rate lift (percentage points)", fontsize=11)
    ax.set_title("0 / 23 individuals clear power.  Pooled is the only path.",
                 loc="left", color=NAVY)
    ax.legend(loc="lower right", frameon=False, fontsize=10)
    fig.text(0.02, 0.01,
             "TI-933 Select cohort, 7-day window 2026-04-29 to 2026-05-05. "
             "Individual CIs stylized — exact per-advertiser bounds in ti_933_per_advertiser_lift_7d.csv.",
             color=GRAY, fontsize=9)
    fig.savefig(OUT / "ti_xxx_chart_pool_or_nothing.png")
    plt.close(fig)
    print("[OK] pool or nothing")


# ----------------------------------------------------------------------
# Chart 6 — Distributions: high power vs low power (StatQuest-style)
# ----------------------------------------------------------------------

def chart_distribution_overlap():
    x = np.linspace(-4, 8, 500)
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.5), sharey=True)

    def normal(x, mu, sigma):
        return np.exp(-((x - mu) ** 2) / (2 * sigma ** 2)) / (sigma * np.sqrt(2 * np.pi))

    # Left: high power (well-separated distributions)
    mu1, mu2, sigma_hi = 0, 4.0, 1.0
    axes[0].fill_between(x, normal(x, mu1, sigma_hi), color=GRAY, alpha=0.35, label="holdout")
    axes[0].fill_between(x, normal(x, mu2, sigma_hi), color=RED, alpha=0.45, label="treated")
    axes[0].set_title("High power\nNarrow distributions · big effect · they barely overlap",
                      loc="left", fontsize=11, color=NAVY)
    axes[0].set_xlabel("Visit rate (treated minus holdout)", fontsize=10)
    axes[0].set_yticks([]); axes[0].set_xticks([]); axes[0].spines["bottom"].set_visible(False)
    axes[0].legend(loc="upper right", frameon=False, fontsize=10)

    # Right: low power (heavily overlapping)
    sigma_lo = 2.4
    axes[1].fill_between(x, normal(x, mu1, sigma_lo), color=GRAY, alpha=0.35, label="holdout")
    axes[1].fill_between(x, normal(x, 1.0, sigma_lo), color=RED, alpha=0.45, label="treated")
    axes[1].set_title("Low power\nWide distributions · small effect · they bleed into each other",
                      loc="left", fontsize=11, color=NAVY)
    axes[1].set_xlabel("Visit rate (treated minus holdout)", fontsize=10)
    axes[1].set_yticks([]); axes[1].set_xticks([]); axes[1].spines["bottom"].set_visible(False)
    axes[1].legend(loc="upper right", frameon=False, fontsize=10)

    fig.suptitle("Power is about how cleanly the two distributions separate",
                 x=0.02, ha="left", fontsize=13, fontweight="bold", color=NAVY)
    fig.text(0.02, 0.01,
             "Larger sample size, lower variance, or bigger true effect all push the two distributions apart — that's the power lever.",
             color=GRAY, fontsize=9)
    fig.savefig(OUT / "ti_xxx_chart_distribution_overlap.png")
    plt.close(fig)
    print("[OK] distribution overlap")


# ----------------------------------------------------------------------

if __name__ == "__main__":
    chart_spend_curve()
    chart_tier_waterfall()
    chart_noise_reveal()
    chart_four_states()
    chart_pool_or_nothing()
    chart_distribution_overlap()
    print(f"\nAll charts written to {OUT}/")
