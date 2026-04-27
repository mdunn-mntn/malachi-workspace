"""TI-837: Tufte-style charts for the incrementality lift presentation.

Inputs:
    outputs/ti_837_per_cell_table.csv               — per-(advertiser, tier, outcome) cells
    outputs/ti_837_meta_analysis_*.json             — IVW pools (per-tier + MNTN-overall)

Outputs (in artifacts/):
    ti_837_chart_money_per_tier_with_wedge.png   — per-tier guid-ATT bar with CI + clickpass overlay (THE money chart)
    ti_837_chart_per_advertiser_high_intent.png  — high-intent guid-ATT per advertiser (descending) with CI
    ti_837_chart_wedge_ratio_per_tier.png        — clickpass-ATT / guid-ATT ratio per tier
    ti_837_chart_mntn_overall_headline.png       — MNTN-overall headline with CI

Tufte principles enforced:
    - No gridlines, no borders, no chart-junk
    - Direct labeling on bars (no legends)
    - One accent color for the headline finding (red), navy for support, gray for context
    - Linear scales (no log compression)
    - One-line annotation per chart stating the implication

Usage:
    python generate_charts.py \
        --csv outputs/ti_837_per_cell_table.csv \
        --meta outputs/ti_837_meta_analysis_2026_04_20_to_26.json \
        --out-dir artifacts/
"""
import argparse
import csv
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import rcParams


# ---------- Tufte styling ----------
COLOR_HERO = "#D63B2F"      # red — the one number per chart that matters
COLOR_NAVY = "#1B2A4A"      # navy — supporting/primary data
COLOR_BLUE = "#2E5090"      # secondary
COLOR_MID = "#5A7DB5"
COLOR_LIGHT = "#A8BDD9"
COLOR_MUTED = "#C8CDD4"     # context/baseline
COLOR_TEXT = "#222222"
COLOR_TEXT_LIGHT = "#666666"
BACKGROUND = "#FAFAFA"

TIER_LABEL = {"high": "High Intent", "peak": "Peak Intent",
              "mid": "Mid Intent", "max_reach": "Max Reach"}
TIER_ORDER = ["high", "peak", "mid", "max_reach"]


def setup_style():
    rcParams.update({
        "font.family": "Helvetica Neue, Helvetica, Arial, sans-serif",
        "font.size": 11,
        "axes.facecolor": BACKGROUND,
        "figure.facecolor": BACKGROUND,
        "savefig.facecolor": BACKGROUND,
        "axes.edgecolor": COLOR_TEXT_LIGHT,
        "axes.linewidth": 0.6,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.spines.left": False,
        "axes.spines.bottom": True,
        "axes.titlepad": 14,
        "axes.titleweight": "bold",
        "axes.titlesize": 14,
        "axes.labelsize": 11,
        "axes.labelcolor": COLOR_TEXT,
        "xtick.color": COLOR_TEXT,
        "ytick.color": COLOR_TEXT,
        "xtick.bottom": False,
        "ytick.left": False,
        "xtick.major.size": 0,
        "ytick.major.size": 0,
    })


def load_cells(csv_path):
    rows = []
    with open(csv_path) as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append({
                "advertiser_id": int(row["advertiser_id"]),
                "advertiser_name": row["advertiser_name"],
                "intent_tier": row["intent_tier"],
                "outcome": row["outcome"],
                "att": float(row["att"]),
                "se": float(row["se"]),
                "ci_low": float(row["ci_low"]),
                "ci_high": float(row["ci_high"]),
                "ci_half_width_pp": float(row["ci_half_width_pp"]),
                "passes_n_gate": row["passes_n_gate"] == "true",
                "n_treated": int(row["n_treated"]),
                "n_holdout": int(row["n_holdout"]),
                "rate_treated": float(row["rate_treated"]),
                "rate_holdout": float(row["rate_holdout"]),
            })
    return rows


# ---------- Chart 1: per-tier guid-ATT with clickpass overlay (the wedge) ----------
def chart_money_per_tier_wedge(meta, out_path):
    fig, ax = plt.subplots(figsize=(11, 6.2))
    per_tier = meta["per_tier_ivw"]
    tiers = [t for t in TIER_ORDER if t in per_tier]
    x = list(range(len(tiers)))
    width = 0.35

    guid_atts = [per_tier[t]["guid"]["att"] * 100 for t in tiers]
    guid_lows = [per_tier[t]["guid"]["ci_low"] * 100 for t in tiers]
    guid_highs = [per_tier[t]["guid"]["ci_high"] * 100 for t in tiers]
    cp_atts = [per_tier[t]["clickpass"]["att"] * 100 for t in tiers]
    cp_lows = [per_tier[t]["clickpass"]["ci_low"] * 100 for t in tiers]
    cp_highs = [per_tier[t]["clickpass"]["ci_high"] * 100 for t in tiers]

    cp_bars = ax.bar([xi - width / 2 for xi in x], cp_atts, width=width,
                     color=COLOR_MUTED, edgecolor="none", zorder=2,
                     label="Clickpass-attributed")
    guid_bars = ax.bar([xi + width / 2 for xi in x], guid_atts, width=width,
                       color=COLOR_NAVY, edgecolor="none", zorder=2,
                       label="True total visits (guid)")

    ax.errorbar([xi - width / 2 for xi in x], cp_atts,
                yerr=[[a - lo for a, lo in zip(cp_atts, cp_lows)],
                      [hi - a for a, hi in zip(cp_atts, cp_highs)]],
                fmt="none", ecolor=COLOR_TEXT_LIGHT, lw=1.0, capsize=3, zorder=3)
    ax.errorbar([xi + width / 2 for xi in x], guid_atts,
                yerr=[[a - lo for a, lo in zip(guid_atts, guid_lows)],
                      [hi - a for a, hi in zip(guid_atts, guid_highs)]],
                fmt="none", ecolor=COLOR_TEXT_LIGHT, lw=1.0, capsize=3, zorder=3)

    for b, v in zip(cp_bars, cp_atts):
        ax.text(b.get_x() + b.get_width() / 2, v + 0.04, f"{v:+.2f}pp",
                ha="center", va="bottom", color=COLOR_TEXT_LIGHT,
                fontsize=10, fontweight="bold")
    for b, v in zip(guid_bars, guid_atts):
        ax.text(b.get_x() + b.get_width() / 2, v + 0.04, f"{v:+.2f}pp",
                ha="center", va="bottom", color=COLOR_NAVY,
                fontsize=10, fontweight="bold")

    ax.axhline(0, color=COLOR_TEXT_LIGHT, lw=0.6, zorder=1)
    ax.set_xticks(x)
    ax.set_xticklabels([TIER_LABEL[t] for t in tiers], fontsize=12)
    ax.set_ylabel("Visit-rate lift (percentage points)", fontsize=11)
    ax.set_title("Clickpass overstates real lift at every intent tier",
                 loc="left", color=COLOR_NAVY, pad=8)
    ax.text(0, 1.05,
            "MNTN attribution captures more visits than targeting actually causes — "
            "the gap is the share that would have happened anyway.",
            transform=ax.transAxes, fontsize=10.5, color=COLOR_TEXT_LIGHT,
            ha="left")
    legend = ax.legend(loc="upper right", frameon=False, fontsize=10)

    ax.set_ylim(min(0, min(guid_lows + cp_lows) * 1.4),
                max(guid_highs + cp_highs) * 1.18)
    plt.tight_layout()
    plt.savefig(out_path, dpi=200)
    plt.close(fig)


# ---------- Chart 2: per-advertiser high-intent guid-ATT (descending) ----------
def chart_per_advertiser_high_intent(cells, out_path):
    sub = [c for c in cells if c["intent_tier"] == "high" and c["outcome"] == "guid"]
    sub.sort(key=lambda c: c["att"], reverse=True)
    names = [c["advertiser_name"] for c in sub]
    atts = [c["att"] * 100 for c in sub]
    los = [c["ci_low"] * 100 for c in sub]
    his = [c["ci_high"] * 100 for c in sub]

    fig, ax = plt.subplots(figsize=(11, 6.2))
    y = list(range(len(sub)))
    bars = ax.barh(y, atts, color=COLOR_NAVY, edgecolor="none", zorder=2)
    ax.errorbar(atts, y,
                xerr=[[a - lo for a, lo in zip(atts, los)],
                      [hi - a for a, hi in zip(atts, his)]],
                fmt="none", ecolor=COLOR_TEXT_LIGHT, lw=1.0, capsize=3, zorder=3)
    for b, v in zip(bars, atts):
        ax.text(v + 0.05, b.get_y() + b.get_height() / 2,
                f"{v:+.2f}pp", va="center", ha="left",
                color=COLOR_NAVY, fontsize=10.5, fontweight="bold")
    ax.set_yticks(y)
    ax.set_yticklabels(names, fontsize=11.5)
    ax.invert_yaxis()
    ax.axvline(0, color=COLOR_TEXT_LIGHT, lw=0.6, zorder=1)
    ax.set_xlabel("High-intent guid-visit ATT (percentage points)", fontsize=11)
    ax.set_title("Targeting lift varies 30× across advertisers — but every one is positive",
                 loc="left", color=COLOR_NAVY, pad=8)
    ax.text(0, 1.05,
            "Per-advertiser guid-visit ATT at the high-intent tier (95% CI). "
            "Every advertiser shows a real positive lift; magnitude tracks vertical fit.",
            transform=ax.transAxes, fontsize=10.5, color=COLOR_TEXT_LIGHT, ha="left")
    ax.set_xlim(0, max(his) * 1.18)
    plt.tight_layout()
    plt.savefig(out_path, dpi=200)
    plt.close(fig)


# ---------- Chart 3: wedge ratio (clickpass / guid) per tier ----------
def chart_wedge_ratio(meta, out_path):
    per_tier = meta["per_tier_ivw"]
    tiers = [t for t in TIER_ORDER if t in per_tier]
    ratios = []
    labels = []
    for t in tiers:
        cp = per_tier[t]["clickpass"]["att"]
        gu = per_tier[t]["guid"]["att"]
        if gu == 0 or gu < 0:
            ratios.append(None)
            labels.append("n/a")
            continue
        r = cp / gu
        ratios.append(r)
        labels.append(f"{r:.2f}×")

    plotted = [(t, r, l) for t, r, l in zip(tiers, ratios, labels) if r is not None]
    fig, ax = plt.subplots(figsize=(10, 5.6))
    if plotted:
        x = list(range(len(plotted)))
        vals = [r for _, r, _ in plotted]
        bars = ax.bar(x, vals, color=COLOR_HERO, edgecolor="none", zorder=2,
                      width=0.45)
        ax.axhline(1, color=COLOR_TEXT_LIGHT, lw=0.7, ls="--", zorder=1)
        ax.text(len(plotted) - 1, 1.02, "1× = clickpass matches guid",
                color=COLOR_TEXT_LIGHT, fontsize=9, ha="right", va="bottom")
        for b, lbl in zip(bars, labels[:len(plotted)]):
            ax.text(b.get_x() + b.get_width() / 2, b.get_height() + 0.05,
                    lbl, ha="center", va="bottom",
                    color=COLOR_HERO, fontsize=12, fontweight="bold")
        ax.set_xticks(x)
        ax.set_xticklabels([TIER_LABEL[t] for t, _, _ in plotted], fontsize=12)
        ax.set_ylabel("Clickpass-ATT ÷ Guid-ATT", fontsize=11)
        ax.set_ylim(0, max(vals) * 1.25)
    ax.set_title("MNTN credits 1.1–1.2 visits for every 1 it actually causes",
                 loc="left", color=COLOR_HERO, pad=8)
    ax.text(0, 1.05,
            "Ratio of clickpass-attributed lift to true (guid-traffic) lift. "
            "Anything above 1× is over-credit; anything below 1× is under-credit.",
            transform=ax.transAxes, fontsize=10.5, color=COLOR_TEXT_LIGHT, ha="left")
    plt.tight_layout()
    plt.savefig(out_path, dpi=200)
    plt.close(fig)


# ---------- Chart 4: MNTN-overall headline ----------
def chart_overall_headline(meta, out_path):
    overall = meta["mntn_overall_ivw"]
    g = overall["guid"]
    c = overall["clickpass"]

    fig, ax = plt.subplots(figsize=(11, 5.6))
    bars = ax.bar([0, 1], [c["att"] * 100, g["att"] * 100],
                  color=[COLOR_MUTED, COLOR_NAVY],
                  edgecolor="none", width=0.5, zorder=2)
    ax.errorbar([0, 1],
                [c["att"] * 100, g["att"] * 100],
                yerr=[[(c["att"] - c["ci_low"]) * 100, (g["att"] - g["ci_low"]) * 100],
                      [(c["ci_high"] - c["att"]) * 100, (g["ci_high"] - g["att"]) * 100]],
                fmt="none", ecolor=COLOR_TEXT_LIGHT, lw=1.0, capsize=4, zorder=3)
    for b, label, color in zip(bars,
                                [f"{c['att']*100:+.3f}pp\n(clickpass)",
                                 f"{g['att']*100:+.3f}pp\n(guid)"],
                                [COLOR_TEXT_LIGHT, COLOR_NAVY]):
        ax.text(b.get_x() + b.get_width() / 2, b.get_height() + 0.02,
                label, ha="center", va="bottom",
                color=color, fontsize=12.5, fontweight="bold")
    ax.set_xticks([0, 1])
    ax.set_xticklabels(["Clickpass\n(MNTN-credited)", "Guid\n(true total)"], fontsize=12)
    ax.set_ylabel("MNTN-overall ATT (pp)", fontsize=11)
    ax.set_title("Overall MNTN incrementality (IVW-pooled across 7 advertisers × 3 tiers)",
                 loc="left", color=COLOR_NAVY, pad=8)
    ax.set_ylim(0, max(c["ci_high"], g["ci_high"]) * 100 * 1.25)
    plt.tight_layout()
    plt.savefig(out_path, dpi=200)
    plt.close(fig)


# ---------- main ----------
def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", required=True, help="per-cell CSV from compute_att.py")
    parser.add_argument("--meta", required=True, help="meta-analysis JSON from compute_att.py")
    parser.add_argument("--out-dir", required=True, help="artifacts/ output directory")
    args = parser.parse_args()

    setup_style()
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    cells = load_cells(args.csv)
    meta = json.loads(Path(args.meta).read_text())

    chart_money_per_tier_wedge(meta, out / "ti_837_chart_money_per_tier_with_wedge.png")
    print("wrote ti_837_chart_money_per_tier_with_wedge.png")

    chart_per_advertiser_high_intent(cells, out / "ti_837_chart_per_advertiser_high_intent.png")
    print("wrote ti_837_chart_per_advertiser_high_intent.png")

    chart_wedge_ratio(meta, out / "ti_837_chart_wedge_ratio_per_tier.png")
    print("wrote ti_837_chart_wedge_ratio_per_tier.png")

    chart_overall_headline(meta, out / "ti_837_chart_mntn_overall_headline.png")
    print("wrote ti_837_chart_mntn_overall_headline.png")


if __name__ == "__main__":
    main()
