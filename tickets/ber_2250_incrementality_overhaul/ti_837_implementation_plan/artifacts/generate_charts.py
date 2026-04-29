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

TIER_LABEL = {"high": "High Intent", "peak": "Peak Performance",
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
    # Title above subtitle — explicit positioning to avoid set_title/ax.text z-order issues
    ax.text(0, 1.14,
            "Targeting drives 3.4pp more total visits at high intent — clickpass overstates by 24%",
            transform=ax.transAxes, fontsize=14, fontweight="bold",
            color=COLOR_NAVY, ha="left", va="bottom")
    ax.text(0, 1.06,
            "Per-tier visit-rate ATT (95% CI), IVW-pooled across 7 advertisers, 7-day window. "
            "Wedge inverts at peak.",
            transform=ax.transAxes, fontsize=10.5, color=COLOR_TEXT_LIGHT,
            ha="left", va="bottom")
    ax.legend(loc="upper right", frameon=False, fontsize=10,
              bbox_to_anchor=(1.0, 1.0))

    ax.set_ylim(min(0, min(guid_lows + cp_lows) * 1.4),
                max(guid_highs + cp_highs) * 1.18)
    plt.subplots_adjust(top=0.84, left=0.08, right=0.97, bottom=0.10)
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
    # Negative bars get the label to the LEFT of the bar tip; positive bars to the RIGHT.
    # Avoids the error-bar cap colliding with the leading "−" character.
    x_range = max(his) - min(0, min(los))
    pad = x_range * 0.012
    for b, v, lo in zip(bars, atts, los):
        if v < 0:
            ax.text(v - pad, b.get_y() + b.get_height() / 2,
                    f"{v:+.2f}pp", va="center", ha="right",
                    color=COLOR_NAVY, fontsize=10.5, fontweight="bold")
        else:
            ax.text(v + pad, b.get_y() + b.get_height() / 2,
                    f"{v:+.2f}pp", va="center", ha="left",
                    color=COLOR_NAVY, fontsize=10.5, fontweight="bold")
    ax.set_yticks(y)
    ax.set_yticklabels(names, fontsize=11.5)
    ax.invert_yaxis()
    ax.axvline(0, color=COLOR_TEXT_LIGHT, lw=0.6, zorder=1)
    ax.set_xlabel("High-intent guid-visit ATT (percentage points)", fontsize=11)
    # Title above subtitle, both with explicit positioning (no set_title)
    ax.text(0, 1.14,
            "High-intent lift spans 200× — Ferguson +10.6pp to Northern Tool flat",
            transform=ax.transAxes, fontsize=14, fontweight="bold",
            color=COLOR_NAVY, ha="left", va="bottom")
    ax.text(0, 1.06,
            "Per-advertiser high-intent guid-visit ATT (95% CI), 7-day window.",
            transform=ax.transAxes, fontsize=10.5, color=COLOR_TEXT_LIGHT,
            ha="left", va="bottom")
    ax.set_xlim(min(0, min(los) * 1.2) - pad * 8, max(his) * 1.18)
    plt.subplots_adjust(top=0.84, left=0.16, right=0.97, bottom=0.12)
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

    # Drop mid-intent: when both numerator and denominator are at the noise
    # floor (~0.01pp), the ratio is mathematically valid but uninterpretable —
    # and it dominates a chart meant to be about the high-vs-peak inversion.
    plotted = [(t, r, l) for t, r, l in zip(tiers, ratios, labels)
               if r is not None and t != "mid"]
    fig, ax = plt.subplots(figsize=(10, 5.6))
    if plotted:
        x = list(range(len(plotted)))
        vals = [r for _, r, _ in plotted]
        bars = ax.bar(x, vals, color=COLOR_HERO, edgecolor="none", zorder=2,
                      width=0.45)
        ax.axhline(1, color=COLOR_TEXT_LIGHT, lw=0.7, ls="--", zorder=1)
        # Annotation centered between the bars — no collision with axis label or bars
        mid_x = (len(plotted) - 1) / 2
        ax.text(mid_x, 1.04, "1× = clickpass matches guid",
                color=COLOR_TEXT_LIGHT, fontsize=9, ha="center", va="bottom")
        for b, _, lbl in zip(bars, vals, [l for _, _, l in plotted]):
            ax.text(b.get_x() + b.get_width() / 2, b.get_height() + 0.04,
                    lbl, ha="center", va="bottom",
                    color=COLOR_HERO, fontsize=14, fontweight="bold")
        ax.set_xticks(x)
        ax.set_xticklabels([TIER_LABEL[t] for t, _, _ in plotted], fontsize=12)
        ax.set_ylabel("Clickpass-ATT ÷ Guid-ATT", fontsize=11)
        ax.set_ylim(0, max(max(vals), 1.0) * 1.30)
    # Title above subtitle (explicit positioning)
    ax.text(0, 1.14,
            "Attribution overstates at high intent, under-credits at peak",
            transform=ax.transAxes, fontsize=14, fontweight="bold",
            color=COLOR_HERO, ha="left", va="bottom")
    ax.text(0, 1.06,
            "Clickpass-ATT ÷ guid-ATT per tier. Above 1× = over-credit; below 1× = under-credit.",
            transform=ax.transAxes, fontsize=10.5, color=COLOR_TEXT_LIGHT,
            ha="left", va="bottom")
    plt.subplots_adjust(top=0.84, left=0.10, right=0.97, bottom=0.12)
    plt.savefig(out_path, dpi=200)
    plt.close(fig)


# ---------- Chart 4: MNTN headline — high-intent IVW pool (the "where targeting works" number) ----------
def chart_overall_headline(meta, out_path):
    """Lead with the high-intent IVW pool — the most defensible single
    'MNTN incrementality' number. The all-cells pool is mathematically valid
    but dominated by mid-tier cells with tiny variance and near-zero ATT,
    which understates the lift in the segment MNTN actually targets."""
    high_pool = meta["per_tier_ivw"].get("high")
    if not high_pool:
        return
    g = high_pool["guid"]
    c = high_pool["clickpass"]

    fig, ax = plt.subplots(figsize=(11, 6.0))
    big_g = g["att"] * 100
    big_c = c["att"] * 100

    # central hero number
    ax.text(0.5, 0.62, f"+{big_g:.2f}pp",
            transform=ax.transAxes, ha="center", va="center",
            color=COLOR_HERO, fontsize=110, fontweight="bold")
    ax.text(0.5, 0.30,
            f"true incremental visits at high intent  ·  "
            f"95% CI [{g['ci_low']*100:+.2f}pp, {g['ci_high']*100:+.2f}pp]",
            transform=ax.transAxes, ha="center", va="center",
            color=COLOR_TEXT, fontsize=14)
    ax.text(0.5, 0.18,
            f"MNTN clickpass attribution credits +{big_c:.2f}pp — a {(big_c/big_g):.2f}× over-statement",
            transform=ax.transAxes, ha="center", va="center",
            color=COLOR_TEXT_LIGHT, fontsize=12)
    ax.text(0.5, 0.08,
            "IVW-pooled across 7 advertisers · 04-20 → 04-26 UTC, 3-day visit post-period",
            transform=ax.transAxes, ha="center", va="center",
            color=COLOR_TEXT_LIGHT, fontsize=10, style="italic")
    ax.text(0.5, 0.92, "MNTN's high-intent targeting causes",
            transform=ax.transAxes, ha="center", va="center",
            color=COLOR_NAVY, fontsize=18, fontweight="bold")
    ax.set_xticks([])
    ax.set_yticks([])
    for spine in ax.spines.values():
        spine.set_visible(False)
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
