"""TI-917 — Spend → MDE curve, regenerated with consistent title + annotations.

Source TI-884 chart had an inconsistency: title said "$200-500k/month" (the
comfortable-measurement zone) but annotations marked $50k / $150k (the 5%
crossings). Two different operating points mashed together.

This version locks the title to the precise 5% crossings shown by the
annotations, keeps the realistic-CTV-lift band as context, and explicitly
calls out that this is built on the OBSERVED cohort medians (3.5 imps/IP,
$24.84 CPM, 2.15% IVR). The educational table on the recommendation slide
uses round numbers (10 imps/IP, $25 CPM, 2% IVR) — the talk track explains
the adjustment math.

Output: artifacts/ti_917_chart_spend_curve.png
"""
import csv
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from pathlib import Path

THIS_DIR = Path(__file__).resolve().parent
TI917_ROOT = THIS_DIR.parent
TI884_OUT = TI917_ROOT.parent / "ti_884_power_sample_size_analysis" / "outputs"
SRC_CSV = TI884_OUT / "ti_884_spend_threshold_curve.csv"
OUT = THIS_DIR / "ti_917_chart_spend_curve.png"

NAVY = "#1B2A4A"
BLUE = "#5A7DB5"
GRAY = "#888888"
BG = "#FAFAFA"

mpl.rcParams.update({
    "text.parse_math": False,  # treat $ as a literal dollar sign, not math mode
    "font.family": ["Helvetica Neue", "Helvetica", "Arial", "sans-serif"],
    "font.size": 11,
    "axes.edgecolor": "#444",
    "axes.linewidth": 0.7,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.facecolor": BG,
    "figure.facecolor": BG,
    "savefig.facecolor": BG,
    "savefig.dpi": 200,
})


def f(v, default=None):
    if v in ("", None):
        return default
    try:
        return float(v)
    except ValueError:
        return default


def main():
    rows = list(csv.DictReader(open(SRC_CSV)))
    spends = [f(r["monthly_spend"]) for r in rows]
    raw = [f(r["mde_visits_rel_pct"]) for r in rows]
    stack = [f(r["mde_visits_rel_pct_post_stack"]) for r in rows]

    # Find first crossing of 5% MDE for each curve
    def first_crossing(spends, mde):
        for s, m in zip(spends, mde):
            if m is not None and m <= 5:
                return s, m
        return None, None

    s_raw, _ = first_crossing(spends, raw)
    s_stack, _ = first_crossing(spends, stack)

    fig, ax = plt.subplots(figsize=(11, 5.6))

    # CTV lift band (shaded)
    ax.axhspan(2, 8, alpha=0.10, color=GRAY, zorder=0)
    ax.text(spends[-1], 5.0, "  realistic CTV lift band (2–8%)",
            fontsize=8.5, color=GRAY, va="center", ha="right", style="italic")

    # 5% threshold line — make it more prominent
    ax.axhline(5, color=GRAY, linewidth=1.0, linestyle="--", alpha=0.7, zorder=1)
    ax.text(45_000, 5.15, "5% well-powered threshold",
            fontsize=9, color=GRAY, va="bottom", ha="left", weight="bold")

    # Curves
    ax.plot(spends, raw, color=NAVY, linewidth=2.5, marker="o", markersize=5,
            label="Raw — no variance reduction", zorder=3)
    ax.plot(spends, stack, color=BLUE, linewidth=2, marker="o", markersize=4,
            linestyle="--", alpha=0.95,
            label="Post-stack — CUPED + ghost-ad + stratified (40% SE reduction)", zorder=3)

    # Annotated crossings
    for s, color, label in [(s_stack, BLUE, "post-stack"), (s_raw, NAVY, "raw")]:
        if s is not None:
            ax.annotate(f"${s/1000:.0f}k", xy=(s, 5), xytext=(s, 5 - 1.7),
                        fontsize=10, color=color, ha="center", weight="bold",
                        arrowprops=dict(arrowstyle="-", color=color, lw=0.9), zorder=4)

    ax.set_xscale("log")
    ax.set_xticks([50_000, 100_000, 200_000, 500_000, 1_000_000, 2_000_000, 5_000_000])
    ax.xaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, _: f"${x/1000:.0f}k" if x < 1e6 else f"${x/1e6:.0f}M"))
    ax.set_yticks([0, 2, 5, 8, 10, 12])
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda y, _: f"{y:.0f}%"))
    ax.set_ylim(0, 12)
    ax.set_xlabel("Monthly Stage 1 spend (log scale)", fontsize=11, color="#444")
    ax.set_ylabel("Minimum detectable effect (relative)", fontsize=11, color="#444")
    ax.grid(True, axis="y", linestyle=":", linewidth=0.4, color="#CCCCCC")
    ax.legend(loc="upper right", frameon=False, fontsize=9)

    # Coherent title + subtitle
    fig.suptitle(
        f"Visit-rate MDE crosses 5% at ${s_raw/1000:.0f}k raw, ${s_stack/1000:.0f}k post-stack",
        fontsize=14, color=NAVY, fontweight="bold", x=0.02, y=0.98, ha="left",
        parse_math=False,
    )
    fig.text(
        0.02, 0.93,
        "Chart uses observed cohort medians: IVR=2.15%, CPM=$24.84, 3.5 imps/IP, 10% holdout.\n"
        "Recommendation table (next slide) uses round teaching numbers (10 imps/IP, $25 CPM) — "
        "adjust per advertiser by × (advertiser_cpm / table_cpm) × (advertiser_imps / table_imps).",
        ha="left", va="top", fontsize=8.5, color=GRAY,
        parse_math=False,
    )

    plt.tight_layout(rect=(0, 0, 1, 0.85))
    plt.savefig(OUT, bbox_inches="tight")
    plt.close()
    print(f"[OK] {OUT.name}")


if __name__ == "__main__":
    main()
