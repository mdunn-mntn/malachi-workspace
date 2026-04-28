"""TI-837 Phase 2 — generate Phase 1 vs Phase 2 comparison charts.

Two charts:
1. Wedge ratio: Phase 1 (1.24×, 0.62×) vs Phase 2 (0.96× IVW / 0.30× median)
2. Pooling-method comparison at peak: IVW / mean / median / sample-weighted

Style follows the Tufte rules in documentation/docs/presentation_playbook.md
and matches the Phase 1 chart aesthetic (off-white #FAFAFA bg, Helvetica,
direct labeling, red for hero number).
"""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import rcParams

ARTIFACTS = "/Users/malachi/Developer/work/mntn/workspace/tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/artifacts"

COLOR_HERO = "#D63B2F"
COLOR_NAVY = "#1B2A4A"
COLOR_BLUE = "#2E5090"
COLOR_MID = "#5A7DB5"
COLOR_LIGHT = "#A8BDD9"
COLOR_MUTED = "#C8CDD4"
COLOR_TEXT = "#222222"
COLOR_LIGHT_TEXT = "#666666"

rcParams.update({
    "font.family": "Helvetica Neue",
    "font.size": 12,
    "axes.facecolor": "#FAFAFA",
    "figure.facecolor": "#FAFAFA",
    "axes.edgecolor": "#FAFAFA",
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.spines.left": False,
    "axes.spines.bottom": False,
    "xtick.bottom": False,
    "ytick.left": False,
})


def chart_wedge_phase1_vs_phase2():
    """Side-by-side wedge ratio comparison: HIGH and PEAK across phases."""
    fig, ax = plt.subplots(figsize=(11, 6), dpi=200)

    # Data
    labels = ["High", "Peak\n(IVW)", "Peak\n(Median)"]
    p1_vals = [1.24, 0.62, 0.62]   # Phase 1: only IVW reported; show as same
    p2_vals = [0.96, 1.00, 0.30]   # Phase 2

    x = list(range(len(labels)))
    bar_w = 0.36

    bars1 = ax.bar([i - bar_w/2 for i in x], p1_vals, bar_w,
                   color=COLOR_MUTED, edgecolor="none",
                   label="Phase 1 (7 advertisers)")
    bars2 = ax.bar([i + bar_w/2 for i in x], p2_vals, bar_w,
                   color=[COLOR_NAVY, COLOR_LIGHT, COLOR_HERO],
                   edgecolor="none",
                   label="Phase 2 (30 advertisers)")

    # Reference line at 1.0
    ax.axhline(1.0, color="#999", linestyle="--", linewidth=0.8, zorder=0)
    ax.text(2.5, 1.02, "1.0× = clickpass matches guid",
            color="#999", fontsize=9, ha="right", va="bottom")

    # Direct labels
    for b, v in zip(bars1, p1_vals):
        ax.text(b.get_x() + b.get_width()/2, v + 0.04,
                f"{v:.2f}×", ha="center", va="bottom",
                fontsize=10, color=COLOR_LIGHT_TEXT)
    for b, v, color in zip(bars2, p2_vals,
                            [COLOR_NAVY, COLOR_LIGHT, COLOR_HERO]):
        ax.text(b.get_x() + b.get_width()/2, v + 0.04,
                f"{v:.2f}×", ha="center", va="bottom",
                fontsize=11, color=color, fontweight="bold")

    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=11, color=COLOR_TEXT)
    ax.set_yticks([0, 0.5, 1.0, 1.5])
    ax.set_yticklabels(["0", "0.5×", "1.0×", "1.5×"], fontsize=10)
    ax.set_ylim(0, 1.6)
    ax.tick_params(colors="#999")
    ax.set_ylabel("clickpass-ATT / guid-ATT (wedge)",
                  fontsize=10, color=COLOR_LIGHT_TEXT)

    # Title
    ax.text(0, 1.18,
            "The high-intent wedge collapsed.  The peak under-credit got stronger.",
            transform=ax.transAxes, fontsize=15, fontweight="bold",
            color=COLOR_NAVY, va="bottom")
    ax.text(0, 1.10,
            "Phase 1 (gray) vs Phase 2 (color). Peak shows two methods because "
            "IVW collapses to noise floor at peak.",
            transform=ax.transAxes, fontsize=10, color=COLOR_LIGHT_TEXT,
            va="bottom")

    # Legend (manually positioned, colored swatches as text)
    ax.text(0.02, -0.18,
            "Phase 1 (7 adv, IVW)", transform=ax.transAxes,
            fontsize=10, color=COLOR_LIGHT_TEXT, va="top")
    ax.text(0.30, -0.18,
            "Phase 2 (30 adv)", transform=ax.transAxes,
            fontsize=10, color=COLOR_NAVY, va="top", fontweight="bold")

    plt.tight_layout(rect=[0, 0, 1, 0.92])
    out = f"{ARTIFACTS}/ti_837_chart_phase1_vs_phase2_wedge.png"
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="#FAFAFA")
    plt.close()
    print(f"wrote {out}")


def chart_peak_pooling_methods():
    """Phase 2 peak-tier ATT under different pooling methods.

    Shows clickpass + guid bars side-by-side for each method, with the
    wedge ratio annotated.
    """
    fig, ax = plt.subplots(figsize=(11, 6), dpi=200)

    methods = ["IVW\n(default)", "Arithmetic\nmean", "Median",
               "Sample-size\nweighted"]
    cp = [0.215, 0.835, 0.360, 1.016]
    gd = [0.216, 2.549, 1.185, 2.957]
    wedges = [c/g for c, g in zip(cp, gd)]

    x = list(range(len(methods)))
    bar_w = 0.36

    bars_cp = ax.bar([i - bar_w/2 for i in x], cp, bar_w,
                     color=COLOR_MID, edgecolor="none")
    bars_gd = ax.bar([i + bar_w/2 for i in x], gd, bar_w,
                     color=COLOR_NAVY, edgecolor="none")

    # Direct labels
    for b, v in zip(bars_cp, cp):
        ax.text(b.get_x() + b.get_width()/2, v + 0.06,
                f"+{v:.2f}pp", ha="center", va="bottom",
                fontsize=9, color=COLOR_BLUE)
    for b, v in zip(bars_gd, gd):
        ax.text(b.get_x() + b.get_width()/2, v + 0.06,
                f"+{v:.2f}pp", ha="center", va="bottom",
                fontsize=10, color=COLOR_NAVY, fontweight="bold")

    # Wedge annotations under each method
    for i, w in enumerate(wedges):
        if w > 0.5:
            color = "#888"
        else:
            color = COLOR_HERO
        ax.text(i, -0.3, f"wedge: {w:.2f}×",
                ha="center", va="top", fontsize=10,
                color=color, fontweight="bold" if w < 0.5 else "normal")

    ax.set_xticks(x)
    ax.set_xticklabels(methods, fontsize=10, color=COLOR_TEXT)
    ax.set_yticks([0, 1, 2, 3])
    ax.set_yticklabels(["0", "+1pp", "+2pp", "+3pp"], fontsize=10)
    ax.set_ylim(-0.6, 3.5)
    ax.tick_params(colors="#999")
    ax.set_ylabel("Peak-intent ATT (percentage points)",
                  fontsize=10, color=COLOR_LIGHT_TEXT)

    # Title
    ax.text(0, 1.18,
            "Pooling method matters at peak.  IVW hides the under-credit.",
            transform=ax.transAxes, fontsize=15, fontweight="bold",
            color=COLOR_NAVY, va="bottom")
    ax.text(0, 1.10,
            "Phase 2 (30 advertisers, peak-intent tier). Clickpass (lighter) "
            "vs guid (darker). 3 of 4 methods show clickpass under-crediting "
            "guid by ~3×.",
            transform=ax.transAxes, fontsize=10, color=COLOR_LIGHT_TEXT,
            va="bottom")

    # Mini-legend (colored swatches as text)
    ax.text(0.02, -0.50,
            "clickpass", transform=ax.transAxes,
            fontsize=10, color=COLOR_MID, va="top", fontweight="bold")
    ax.text(0.18, -0.50,
            "guid", transform=ax.transAxes,
            fontsize=10, color=COLOR_NAVY, va="top", fontweight="bold")

    plt.tight_layout(rect=[0, 0, 1, 0.92])
    out = f"{ARTIFACTS}/ti_837_chart_peak_pooling_methods.png"
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="#FAFAFA")
    plt.close()
    print(f"wrote {out}")


if __name__ == "__main__":
    chart_wedge_phase1_vs_phase2()
    chart_peak_pooling_methods()
