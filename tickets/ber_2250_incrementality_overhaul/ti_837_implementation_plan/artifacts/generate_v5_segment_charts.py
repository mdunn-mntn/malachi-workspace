"""TI-837 v5 — segment comparison charts.

Charts:
1. By-segment headline: lift bars across 4 segments × 3 tiers (guid IVW)
2. Wedge per segment at high intent
3. Per-advertiser retargeting (the +21pp finding)

Tufte rules: red for hero number, navy for primary, light off-white bg, no
chart-junk, direct labeling.
"""
import json
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import rcParams
from pathlib import Path

ROOT = Path("/Users/malachi/Developer/work/mntn/workspace/tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan")
OUTPUTS = ROOT / "outputs"
ARTIFACTS = ROOT / "artifacts"

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

SEGS = [
    ("rtg", "Retargeting only", COLOR_HERO),
    ("all", "All campaigns combined", COLOR_BLUE),
    ("prosp", "Prospecting (all stages)", COLOR_NAVY),
    ("stage1", "Stage 1 only", COLOR_MUTED),
]


def load_meta(seg):
    return json.load(open(OUTPUTS / f"ti_837_meta_analysis_30adv_v5_segment_{seg}_2026_04_20_to_26.json"))


def chart_by_segment_headline():
    """4 segments × 1 tier (high) — guid IVW + sample-weighted comparison."""
    fig, ax = plt.subplots(figsize=(12, 6.5), dpi=200)

    # IVW values
    metas = {seg: load_meta(seg) for seg, _, _ in SEGS}

    seg_labels, ivw_atts, sw_atts, colors = [], [], [], []
    for seg, label, color in SEGS:
        d = metas[seg]
        ivw = d["per_tier_ivw"]["high"]["guid"]["att"] * 100
        # sample-weighted
        cells = [c for c in d["per_cell"]
                 if c["intent_tier"] == "high" and c["outcome"] == "guid" and c["passes_n_gate"]]
        n_t = sum(c["n_treated"] for c in cells)
        n_h = sum(c["n_holdout"] for c in cells)
        wt_t = sum(c["rate_treated"] * c["n_treated"] for c in cells) / n_t if n_t else 0
        wt_h = sum(c["rate_holdout"] * c["n_holdout"] for c in cells) / n_h if n_h else 0
        sw = (wt_t - wt_h) * 100
        seg_labels.append(label)
        ivw_atts.append(ivw)
        sw_atts.append(sw)
        colors.append(color)

    # Bars side by side: IVW (filled) and Sample-weighted (hatched)
    x = list(range(len(SEGS)))
    bar_w = 0.36

    bars_ivw = ax.bar([i - bar_w/2 for i in x], ivw_atts, bar_w,
                      color=colors, edgecolor="none", label="IVW pool")
    bars_sw = ax.bar([i + bar_w/2 for i in x], sw_atts, bar_w,
                     color=colors, edgecolor="white", linewidth=0.5,
                     hatch="//", alpha=0.55, label="Sample-weighted")

    # Direct labels on bars
    for b, v, color in zip(bars_ivw, ivw_atts, colors):
        offset = 0.5 if v >= 0 else -0.5
        va = "bottom" if v >= 0 else "top"
        ax.text(b.get_x() + b.get_width()/2, v + offset,
                f"{v:+.2f}pp", ha="center", va=va,
                fontsize=10, color=color, fontweight="bold")
    for b, v, color in zip(bars_sw, sw_atts, colors):
        offset = 0.5 if v >= 0 else -0.5
        va = "bottom" if v >= 0 else "top"
        ax.text(b.get_x() + b.get_width()/2, v + offset,
                f"{v:+.2f}pp", ha="center", va=va,
                fontsize=9, color=color, alpha=0.85)

    ax.axhline(0, color="#999", linewidth=0.6)
    ax.set_xticks(x)
    ax.set_xticklabels(seg_labels, fontsize=10, color=COLOR_TEXT)
    ax.set_ylabel("High-intent guid-ATT (percentage points)",
                  fontsize=10, color=COLOR_LIGHT_TEXT)
    ax.tick_params(colors="#999")

    # title
    ax.text(0, 1.18,
            "Retargeting drives the lift.  Pure prospecting drives almost none.",
            transform=ax.transAxes, fontsize=15, fontweight="bold",
            color=COLOR_NAVY, va="bottom")
    ax.text(0, 1.10,
            "High-intent guid-ATT across 4 campaign segmentations. "
            "IVW pool (solid) and sample-weighted (hatched).",
            transform=ax.transAxes, fontsize=10, color=COLOR_LIGHT_TEXT,
            va="bottom")

    plt.tight_layout(rect=[0, 0, 1, 0.92])
    out = ARTIFACTS / "ti_837_chart_segment_headline_v5.png"
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="#FAFAFA")
    plt.close()
    print(f"wrote {out}")


def chart_segment_x_tier():
    """4 segments × 3 tiers grid — guid IVW lift per cell."""
    fig, ax = plt.subplots(figsize=(11, 6.5), dpi=200)

    metas = {seg: load_meta(seg) for seg, _, _ in SEGS}

    tiers = ["high", "peak", "mid"]
    n_seg = len(SEGS)
    bar_w = 0.20
    x_base = list(range(len(tiers)))

    for i, (seg, label, color) in enumerate(SEGS):
        d = metas[seg]
        atts = []
        for t in tiers:
            if t in d["per_tier_ivw"]:
                atts.append(d["per_tier_ivw"][t]["guid"]["att"] * 100)
            else:
                atts.append(0)
        offset = (i - n_seg/2 + 0.5) * bar_w
        bars = ax.bar([xi + offset for xi in x_base], atts, bar_w,
                      color=color, edgecolor="none", label=label)
        for b, v in zip(bars, atts):
            yoff = 0.4 if v >= 0 else -0.4
            va = "bottom" if v >= 0 else "top"
            label_color = color if abs(v) > 0.5 else "#999"
            ax.text(b.get_x() + b.get_width()/2, v + yoff,
                    f"{v:+.1f}", ha="center", va=va,
                    fontsize=8, color=label_color)

    ax.axhline(0, color="#999", linewidth=0.6)
    ax.set_xticks(x_base)
    ax.set_xticklabels(["High intent", "Peak performance", "Mid intent"],
                       fontsize=11, color=COLOR_TEXT)
    ax.set_ylabel("guid-ATT (percentage points, IVW pool)",
                  fontsize=10, color=COLOR_LIGHT_TEXT)
    ax.tick_params(colors="#999")
    ax.legend(loc="upper right", frameon=False, fontsize=9)

    ax.text(0, 1.15,
            "Lift profile by tier — segment matters more than tier.",
            transform=ax.transAxes, fontsize=15, fontweight="bold",
            color=COLOR_NAVY, va="bottom")
    ax.text(0, 1.07,
            "Retargeting (red) shows large positive lift across high + peak. "
            "Stage 1 prospecting (gray) shows zero or slightly negative lift. "
            "Mid intent is at the noise floor across all segments.",
            transform=ax.transAxes, fontsize=10, color=COLOR_LIGHT_TEXT,
            va="bottom")

    plt.tight_layout(rect=[0, 0, 1, 0.88])
    out = ARTIFACTS / "ti_837_chart_segment_x_tier_v5.png"
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="#FAFAFA")
    plt.close()
    print(f"wrote {out}")


def chart_segment_wedge():
    """Wedge ratio per segment at high intent."""
    fig, ax = plt.subplots(figsize=(11, 5.5), dpi=200)

    metas = {seg: load_meta(seg) for seg, _, _ in SEGS}
    seg_labels, wedges, colors = [], [], []
    for seg, label, color in SEGS:
        d = metas[seg]
        if "high" not in d["per_tier_ivw"]:
            continue
        g = d["per_tier_ivw"]["high"]["guid"]["att"]
        c = d["per_tier_ivw"]["high"]["clickpass"]["att"]
        if abs(g) < 1e-9:
            wedge = 0  # undefined
        else:
            wedge = c / g
        seg_labels.append(label)
        wedges.append(max(-2.0, min(3.0, wedge)))   # cap for display
        colors.append(color)

    x = list(range(len(seg_labels)))
    bars = ax.bar(x, wedges, 0.5, color=colors, edgecolor="none")
    for b, v in zip(bars, wedges):
        ax.text(b.get_x() + b.get_width()/2, v + (0.06 if v >= 0 else -0.08),
                f"{v:.2f}×", ha="center",
                va="bottom" if v >= 0 else "top",
                fontsize=11, color=b.get_facecolor(), fontweight="bold")

    ax.axhline(1.0, color=COLOR_NAVY, linestyle="--", linewidth=0.8, alpha=0.5)
    ax.text(len(seg_labels) - 0.4, 1.04, "1.0× = clickpass matches guid",
            color="#888", fontsize=9, ha="right")
    ax.axhline(0, color="#bbb", linewidth=0.5)

    ax.set_xticks(x)
    ax.set_xticklabels(seg_labels, fontsize=10, color=COLOR_TEXT)
    ax.set_ylabel("clickpass-ATT / guid-ATT  (high intent, IVW)",
                  fontsize=10, color=COLOR_LIGHT_TEXT)
    ax.tick_params(colors="#999")

    ax.text(0, 1.15,
            "Attribution wedge by segment — clickpass over- or under-credits real lift.",
            transform=ax.transAxes, fontsize=14, fontweight="bold",
            color=COLOR_NAVY, va="bottom")
    ax.text(0, 1.07,
            "Negative wedge (Stage 1) means clickpass shows positive lift while guid shows zero/negative. "
            ">1× means clickpass over-credits; <1× means clickpass under-credits.",
            transform=ax.transAxes, fontsize=9, color=COLOR_LIGHT_TEXT,
            va="bottom")

    plt.tight_layout(rect=[0, 0, 1, 0.88])
    out = ARTIFACTS / "ti_837_chart_segment_wedge_v5.png"
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="#FAFAFA")
    plt.close()
    print(f"wrote {out}")


if __name__ == "__main__":
    chart_by_segment_headline()
    chart_segment_x_tier()
    chart_segment_wedge()
