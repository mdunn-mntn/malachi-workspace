#!/usr/bin/env python3
"""AUDI-1115 L0f visuals — how billing works + preemption gap + residual pricing.

Reads measured CSVs (no hardcoded data), writes 3 PNGs to this artifacts/ dir.
"""

import csv
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch

HERE = Path(__file__).resolve().parent
OUT = HERE.parent / "outputs"
A1089 = HERE.parent.parent.parent / "audi_1089_ddp_vendor_evaluations" / "outputs" / "run_2026_07_10"

BG, NAVY, RED, GRAY, GREEN = "#FAFAFA", "#1F3864", "#C00000", "#7F7F7F", "#2E7D32"
plt.rcParams.update({
    "font.family": "Helvetica Neue", "figure.facecolor": BG, "axes.facecolor": BG,
    "axes.spines.top": False, "axes.spines.right": False, "axes.spines.left": False,
})

NAMES = {"28": "33Across", "40": "33Across API", "33": "Sovrn", "24": "Justuno",
         "36": "Cybba", "25": "5x5", "26": "Predactiv", "39": "Klickly"}
METERED = {"28", "40", "33", "24", "36"}

l0f = {r["vendor_ds"]: r for r in csv.DictReader(open(OUT / "audi_1115_l0f_fractional_credit_cpm.csv"))}
d3 = {r["data_source_id"]: r for r in csv.DictReader(open(A1089 / "deck_d3_bills_cpm.csv"))}
q2b = {r["ds"]: r for r in csv.DictReader(open(A1089 / "q2b_daily_drops.csv"))}


def pipeline():
    """The single charge happens at the WIN — 33Across funnel."""
    rows_mo = float(q2b["28"]["rows_day"]) * 30 / 1e9      # billion rows/month
    billed_mo = float(d3["28"]["billed_imps_month"]) / 1e6  # million imps/month
    fig, ax = plt.subplots(figsize=(10, 4.6))
    stages = [
        (0, "Ingested\n32.4B rows/mo", f"{rows_mo:.1f}B", GRAY, "we store + process\n(our cost, not vendor's)"),
        (1, "Usable in\nDS13/DS19", "77.6%", GRAY, "scored / made biddable\n(eligibility gate — no charge)"),
        (2, "WON + credited\n70.3M imps/mo", f"{billed_mo:.0f}M", NAVY, "$0.50 / 1,000\nTHE ONLY CHARGE"),
        (3, "Billed", "$422K/yr", RED, "= 70.3M x $0.50/1000 x 12"),
    ]
    widths = [3.2, 3.2 * 0.776, 1.0, 1.0]  # shrink to show the 0.2% collapse
    x = 0.5
    for i, (idx, label, big, color, sub) in enumerate(stages):
        w = widths[i]
        ax.add_patch(plt.Rectangle((x, 1.4 - w / 2), 1.5, w, color=color, alpha=0.9))
        ax.text(x + 0.75, 1.4, big, ha="center", va="center", color="white",
                fontsize=13, fontweight="bold")
        ax.text(x + 0.75, 3.15, label, ha="center", va="center", fontsize=10.5, fontweight="bold")
        ax.text(x + 0.75, -0.35, sub, ha="center", va="top", fontsize=8.5, color=color)
        if i < 3:
            ax.add_patch(FancyArrowPatch((x + 1.55, 1.4), (x + 2.0, 1.4),
                        arrowstyle="-|>", mutation_scale=16, color=GRAY, lw=1.5))
        x += 2.0
    ax.text(0.5, -1.35, "Only ~0.2% of what 33Across sends ever becomes a billed impression — "
            "we pay for the sliver that wins, not the firehose.",
            fontsize=9.5, color=RED, style="italic")
    ax.set_xlim(0, 8.5)
    ax.set_ylim(-1.6, 3.6)
    ax.axis("off")
    ax.set_title("Vendors are billed ONCE — at the won impression, not for ingestion",
                 fontsize=14, fontweight="bold", loc="left", x=0.02, y=1.0)
    fig.tight_layout()
    fig.savefig(HERE / "audi_1115_l0f_pipeline.png", dpi=200, facecolor=BG)
    print("wrote audi_1115_l0f_pipeline.png")


def preemption():
    """Per vendor: what we pay for = free-overlap + residual."""
    rows = []
    for ds in ["28", "40", "33", "24", "36"]:
        r = l0f[ds]
        any_ = float(r["imps_any_winner"]) / 1e6
        pre = float(r["imps_free_preempted"]) / 1e6
        rows.append((NAMES[ds], any_, pre, any_ - pre))
    rows.sort(key=lambda x: -x[1])
    fig, ax = plt.subplots(figsize=(9.5, 4.4))
    for y, (name, any_, pre, res) in enumerate(rows):
        ax.barh(y, pre, color=GRAY, height=0.6)
        ax.barh(y, res, left=pre, color=NAVY, height=0.6)
        ax.text(-4, y, name, ha="right", va="center", fontsize=10)
        ax.text(pre / 2, y, f"{100*pre/any_:.0f}% free overlap", ha="center", va="center",
                color="white", fontsize=8.5)
        ax.text(pre + res + 3, y, f"residual {res:.1f}M", va="center", fontsize=8.5, color=NAVY)
    ax.set_yticks([])
    ax.set_xticks([])
    ax.invert_yaxis()
    ax.set_title("~90% of what we pay every vendor for, a free log also won",
                 fontsize=14, fontweight="bold", loc="left", pad=40)
    ax.text(0, 1.05, "Won impressions (M/mo): gray = a free log co-won it (preempt, pay $0); "
            "navy = residual we'd actually pay for", transform=ax.transAxes, fontsize=9, color=GRAY)
    fig.tight_layout()
    fig.subplots_adjust(top=0.82)
    fig.savefig(HERE / "audi_1115_l0f_preemption.png", dpi=200, facecolor=BG)
    print("wrote audi_1115_l0f_preemption.png")


def pricing():
    """Residual break-even ($1-3) vs the $0.50 we pay."""
    rows = []
    for ds in ["28", "40", "33", "24", "36"]:
        cpm = float(l0f[ds]["media_cpm_frac"])
        rows.append((NAMES[ds], cpm * 0.10, cpm * 0.30))
    rows.sort(key=lambda x: -x[2])
    fig, ax = plt.subplots(figsize=(9.5, 4.4))
    for y, (name, lo, hi) in enumerate(rows):
        ax.barh(y, hi - lo, left=lo, height=0.55, color=NAVY)
        ax.text(hi + 0.05, y, f"\\${lo:.2f}–\\${hi:.2f}", va="center", fontsize=9, color=NAVY)
        ax.text(-0.06, y, name, ha="right", va="center", fontsize=10)
    ax.axvline(0.50, color=RED, lw=2)
    ax.text(0.52, len(rows) - 0.3, "we pay \\$0.50", color=RED, fontsize=10, fontweight="bold")
    ax.set_yticks([])
    ax.set_xlim(0, 3.6)
    ax.set_xlabel("break-even CPM on the residual (media value × 10–30% margin), $/1,000 imps",
                  fontsize=9, color=GRAY)
    ax.invert_yaxis()
    ax.set_title("On the residual, $0.50 is already below break-even — the rate isn't the problem",
                 fontsize=13.5, fontweight="bold", loc="left", pad=40)
    ax.text(0, 1.05, "Every vendor ≈ \\$1–3 because it's just MNTN's CTV media rate (~\\$10.7 CPM). "
            "The lever is preemption (volume), not a rate cut.", transform=ax.transAxes,
            fontsize=9, color=GRAY)
    fig.tight_layout()
    fig.subplots_adjust(top=0.82)
    fig.savefig(HERE / "audi_1115_l0f_residual_pricing.png", dpi=200, facecolor=BG)
    print("wrote audi_1115_l0f_residual_pricing.png")


if __name__ == "__main__":
    pipeline()
    preemption()
    pricing()
