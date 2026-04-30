"""TI-884 chart generation — Tufte-compliant, exec-quality.

Reads:
  ../outputs/ti_884_top50_mde_tiers.csv
  ../outputs/ti_884_spend_threshold_curve.csv
  ../outputs/ti_884_lauren_validation.csv

Writes (../artifacts):
  ti_884_chart_spend_curve.png    — Al's deliverable: spend → MDE
  ti_884_chart_top50_mde.png      — top-50 advertiser MDE bar chart
  ti_884_chart_lauren_validation.png — reported lift vs MDE
  ti_884_chart_visits_vs_cvr.png  — comparison of measurement difficulty
"""
import csv
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

ROOT = Path(__file__).parent.parent
OUT = ROOT / "outputs"
ART = ROOT / "artifacts"

# Tufte color palette (per revealjs_guide.md)
NAVY = "#1B2A4A"
BLUE = "#2E5090"
GRAY = "#888888"
LIGHT_GRAY = "#C8CDD4"
RED = "#D63B2F"
BG = "#FAFAFA"

plt.rcParams.update({
    "font.family": "Helvetica Neue",
    "font.size": 10,
    "axes.facecolor": BG,
    "figure.facecolor": BG,
    "savefig.facecolor": BG,
    "axes.edgecolor": "#666666",
    "axes.linewidth": 0.6,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.titleweight": "bold",
    "axes.titlesize": 13,
    "axes.titlepad": 16,
    "xtick.color": "#444444",
    "ytick.color": "#444444",
    "xtick.major.size": 0,
    "ytick.major.size": 0,
})


def read_csv(path):
    with open(path) as f:
        return list(csv.DictReader(f))


def f(v, default=None):
    """Cast a CSV string to float, returning default on inf/empty."""
    if v in ("", "inf", None):
        return default
    try:
        return float(v)
    except ValueError:
        return default


# ---------------- Chart 1: Spend → MDE curve ----------------

def chart_spend_curve():
    rows = read_csv(OUT / "ti_884_spend_threshold_curve.csv")
    spends = [f(r["monthly_spend"]) for r in rows]
    mde_v_raw = [f(r["mde_visits_rel_pct"]) for r in rows]
    mde_v_stack = [f(r["mde_visits_rel_pct_post_stack"]) for r in rows]

    fig, ax = plt.subplots(figsize=(10, 5.5), dpi=200)
    ax.plot(spends, mde_v_raw, color=NAVY, linewidth=2.5, marker="o", markersize=5,
            label="Raw MDE (no variance reduction)")
    ax.plot(spends, mde_v_stack, color=BLUE, linewidth=2, marker="o", markersize=4,
            linestyle="--", alpha=0.85, label="Post-stack MDE (CUPED + ghost-ad + stratified)")

    # Reference lines for "realistic CTV lift" range and stakeholder thresholds
    ax.axhspan(2, 8, alpha=0.10, color=GRAY, zorder=0)
    ax.text(spends[-1], 5, "  realistic CTV lift band (2–8%)",
            fontsize=8.5, color=GRAY, va="center", ha="right", style="italic")

    ax.axhline(5, color=GRAY, linewidth=0.5, linestyle=":", alpha=0.5)
    ax.text(45_000, 5, "5% (well-powered threshold)",
            fontsize=8, color=GRAY, va="bottom", ha="left")

    # Annotate threshold crossings — find first spend where MDE drops below 5%
    for series, label, color in [(mde_v_raw, "raw", NAVY), (mde_v_stack, "post-stack", BLUE)]:
        for s, m in zip(spends, series):
            if m and m <= 5:
                ax.annotate(f"${s/1000:.0f}k",
                            xy=(s, m), xytext=(s, m - 1.5),
                            fontsize=9, color=color, ha="center", weight="bold",
                            arrowprops=dict(arrowstyle="-", color=color, lw=0.8))
                break

    ax.set_xscale("log")
    ax.set_xticks([50_000, 100_000, 200_000, 500_000, 1_000_000, 2_000_000, 5_000_000])
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x/1000:.0f}k" if x < 1e6 else f"${x/1e6:.0f}M"))
    ax.set_yticks([0, 2, 5, 8, 10, 15])
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda y, _: f"{y:.0f}%"))
    ax.set_ylim(0, 12)
    ax.set_xlabel("Monthly spend (Stage 1, log scale)", fontsize=10, color="#444444")
    ax.set_ylabel("Minimum detectable effect (relative)", fontsize=10, color="#444444")
    ax.set_title("Visit-rate measurability emerges around $200–500k/month",
                 loc="left", color=NAVY)
    ax.text(0, 1.03, "Cohort medians: IVR=2.15%, CPM=$24.84, 3.5 imps/IP",
            transform=ax.transAxes, fontsize=8.5, color=GRAY, style="italic")
    ax.legend(loc="upper right", frameon=False, fontsize=9)
    ax.grid(True, axis="y", linestyle=":", linewidth=0.4, color="#CCCCCC")

    plt.tight_layout()
    out = ART / "ti_884_chart_spend_curve.png"
    plt.savefig(out, bbox_inches="tight")
    plt.close()
    print(f"  wrote {out}")


# ---------------- Chart 2: Top-50 advertiser MDE bar chart ----------------

ADV_NAMES = {
    31357: "WGU", 30506: "Vivint", 31276: "Ferguson Home", 37775: "Zazzle",
    49868: "AID 49868", 31455: "Ancient Nutrition", 34143: "First Watch",
    36232: "AID 36232", 34838: "Clayton Homes", 51660: "AID 51660",
    40563: "Northern Tool", 34249: "AID 34249", 32404: "National Univ.",
    41034: "AID 41034", 42097: "Gruns", 34835: "AID 34835",
    54196: "AID 54196", 38422: "Signature Hardware", 37056: "AID 37056",
    38059: "AID 38059", 38652: "AID 38652", 34114: "AID 34114",
    9090: "AID 9090", 33389: "AID 33389", 57322: "AID 57322",
    41057: "AID 41057", 37115: "AID 37115", 37158: "AID 37158",
    32147: "AID 32147", 49753: "AID 49753",
}


def chart_top50_mde():
    rows = [r for r in read_csv(OUT / "ti_884_top50_mde_tiers.csv")
            if f(r["mde_visits_rel_pct"]) is not None]
    rows.sort(key=lambda r: f(r["monthly_spend"]) or 0, reverse=True)
    rows = rows[:30]  # top 30 fits cleanly on a slide

    aids = [int(r["advertiser_id"]) for r in rows]
    labels = [ADV_NAMES.get(a, str(a)) for a in aids]
    mde_raw = [f(r["mde_visits_rel_pct"], 0) for r in rows]

    # color by tier
    colors = [NAVY if m < 5 else BLUE if m < 10 else RED for m in mde_raw]

    fig, ax = plt.subplots(figsize=(10, 8), dpi=200)
    y = list(range(len(rows)))
    ax.barh(y, mde_raw, color=colors, height=0.65)

    # data labels
    for i, m in enumerate(mde_raw):
        ax.text(m + 0.15, i, f"{m:.1f}%", va="center", fontsize=8.5, color="#444444")

    ax.set_yticks(y)
    ax.set_yticklabels(labels, fontsize=8.5)
    ax.invert_yaxis()
    ax.set_xticks([0, 5, 10, 15])
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.0f}%"))
    ax.axvline(5, color=GRAY, linewidth=0.5, linestyle=":")
    ax.set_xlabel("Visit-rate MDE (raw, no variance reduction)", fontsize=10, color="#444444")
    ax.set_title("Top-30 advertisers: most are well-powered for visit-rate measurement",
                 loc="left", color=NAVY)
    ax.text(0, 1.02, "Stage 1 only · April 2026 · 10% holdout · α=0.05, power=0.80",
            transform=ax.transAxes, fontsize=8.5, color=GRAY, style="italic")
    ax.set_xlim(0, max(mde_raw) * 1.15)

    # legend swatches
    swatches = [
        plt.Rectangle((0, 0), 1, 1, color=NAVY),
        plt.Rectangle((0, 0), 1, 1, color=BLUE),
        plt.Rectangle((0, 0), 1, 1, color=RED),
    ]
    ax.legend(swatches, ["well-powered (<5%)", "borderline (5–10%)", "underpowered (>10%)"],
              loc="lower right", frameon=False, fontsize=9)

    plt.tight_layout()
    out = ART / "ti_884_chart_top50_mde.png"
    plt.savefig(out, bbox_inches="tight")
    plt.close()
    print(f"  wrote {out}")


# ---------------- Chart 3: Lauren cross-validation ----------------

def chart_lauren_validation():
    rows = read_csv(OUT / "ti_884_lauren_validation.csv")
    measurable = [r for r in rows if r["in_top50"] == "True" or f(r.get("treated_ips_stage1"))]
    if not measurable:
        print("  no measurable Lauren tests; skipping chart")
        return

    fig, ax = plt.subplots(figsize=(9, 5.5), dpi=200)
    names = [r["test_name"] for r in measurable]
    lifts = [f(r["reported_lift_pct"], 0) for r in measurable]
    mdes_raw = [f(r["mde_visits_rel_pct_raw"], 0) for r in measurable]
    mdes_stack = [f(r["mde_visits_rel_pct_post_stack"], 0) for r in measurable]

    y = list(range(len(measurable)))
    bar_h = 0.32
    ax.barh([i + bar_h/2 for i in y], mdes_raw, color=GRAY, height=bar_h, label="MDE (raw)")
    ax.barh([i - bar_h/2 for i in y], mdes_stack, color=LIGHT_GRAY, height=bar_h,
            label="MDE (post-stack)")
    ax.scatter(lifts, y, color=RED, s=120, zorder=10, label="Reported lift", marker="D")

    for i, (l, m) in enumerate(zip(lifts, mdes_raw)):
        ax.text(l + 1, i, f"  {l:.2f}%", va="center", fontsize=9, color=RED, weight="bold")
        ax.text(m + 1, i + bar_h/2, f"{m:.1f}%", va="center", fontsize=8.5, color="#444444")

    ax.set_yticks(y)
    ax.set_yticklabels(names, fontsize=10)
    ax.invert_yaxis()
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.0f}%"))
    ax.set_xlabel("Visit-rate effect size (relative)", fontsize=10, color="#444444")
    ax.set_title("Reported lifts sit far below detection threshold",
                 loc="left", color=NAVY)
    ax.text(0, 1.02, "Lauren's completed tests vs current MDE at April 2026 scale (Stage 1)",
            transform=ax.transAxes, fontsize=8.5, color=GRAY, style="italic")
    ax.legend(loc="lower right", frameon=False, fontsize=9)
    ax.set_xlim(0, max(max(mdes_raw), max(lifts)) * 1.25)

    plt.tight_layout()
    out = ART / "ti_884_chart_lauren_validation.png"
    plt.savefig(out, bbox_inches="tight")
    plt.close()
    print(f"  wrote {out}")


# ---------------- Chart 4: Visits vs CVR comparison ----------------

def chart_visits_vs_cvr():
    rows = read_csv(OUT / "ti_884_top50_mde_tiers.csv")
    rows = [r for r in rows
            if f(r["mde_visits_rel_pct"]) is not None
            and f(r["mde_cvr_rel_pct"]) is not None]

    visits = sorted(f(r["mde_visits_rel_pct"]) for r in rows)
    cvrs = sorted(f(r["mde_cvr_rel_pct"]) for r in rows)
    cvrs = [min(c, 100) for c in cvrs]  # clip extreme outliers for display

    fig, ax = plt.subplots(figsize=(9, 5), dpi=200)

    pcts = [10, 25, 50, 75, 90]
    def percentile(xs, p):
        i = int(len(xs) * p / 100)
        return xs[min(i, len(xs) - 1)]
    v_pcts = [percentile(visits, p) for p in pcts]
    c_pcts = [percentile(cvrs, p) for p in pcts]

    x = list(range(len(pcts)))
    bw = 0.38
    ax.bar([xi - bw/2 for xi in x], v_pcts, color=NAVY, width=bw, label="Visit-rate MDE")
    ax.bar([xi + bw/2 for xi in x], c_pcts, color=RED, width=bw, label="Conversion-rate MDE")

    for xi, v in zip(x, v_pcts):
        ax.text(xi - bw/2, v + 1, f"{v:.1f}%", ha="center", fontsize=8.5, color=NAVY)
    for xi, c in zip(x, c_pcts):
        ax.text(xi + bw/2, c + 1, f"{c:.0f}%", ha="center", fontsize=8.5, color=RED)

    ax.set_xticks(x)
    ax.set_xticklabels([f"P{p}" for p in pcts])
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda y, _: f"{y:.0f}%"))
    ax.set_ylabel("Relative MDE", fontsize=10, color="#444444")
    ax.set_title("CVR is much harder to measure than visits at the same scale",
                 loc="left", color=NAVY)
    ax.text(0, 1.02, "Top-50 advertiser distribution · Stage 1 · April 2026 · raw MDE",
            transform=ax.transAxes, fontsize=8.5, color=GRAY, style="italic")
    ax.legend(loc="upper left", frameon=False, fontsize=9)
    ax.set_ylim(0, max(c_pcts) * 1.15)

    plt.tight_layout()
    out = ART / "ti_884_chart_visits_vs_cvr.png"
    plt.savefig(out, bbox_inches="tight")
    plt.close()
    print(f"  wrote {out}")


def main():
    chart_spend_curve()
    chart_top50_mde()
    chart_lauren_validation()
    chart_visits_vs_cvr()


if __name__ == "__main__":
    main()
