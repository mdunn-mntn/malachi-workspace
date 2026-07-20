#!/usr/bin/env python3
"""AUDI-1089 billing-review deck charts (Tufte: data-ink, direct labels, one accent, no titles).
Data from ../outputs/*.csv. Titles live in the deck, not the image. No mathtext '$' (escaped).
Outputs 200-DPI PNGs next to this script."""
import csv, os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "..", "outputs")

for fam in ("Helvetica Neue", "Helvetica", "Arial"):
    if any(fam in f.name for f in font_manager.fontManager.ttflist):
        plt.rcParams["font.family"] = fam; break
plt.rcParams.update({"figure.facecolor": "#FAFAFA", "axes.facecolor": "#FAFAFA",
                     "savefig.facecolor": "#FAFAFA", "axes.edgecolor": "#CCCCCC",
                     "font.size": 13, "axes.spines.top": False, "axes.spines.right": False})
NAVY, RED, GRAY, MUTED = "#1B2A4A", "#D63B2F", "#8A9099", "#C8CDD4"


def load(name):
    with open(os.path.join(OUT, name)) as f:
        return list(csv.DictReader(f))


def chart_preemption_proof():
    rows = load("audi_1089_preemption_proof_winner_mix.csv")
    rows.sort(key=lambda r: float(r["impressions"]), reverse=True)
    labels = [r["label"] for r in rows]
    imps = [float(r["impressions"]) / 1e6 for r in rows]
    cpm = [float(r["tv_cpm"]) for r in rows]
    # accent RED only on the gap bar (free+paid, billed 0.50); paid-only navy; free/neither muted
    colors = []
    for r in rows:
        if r["winner_mix"] == "free_and_paid":
            colors.append(RED)
        elif r["tv_cpm"] == "0.50":
            colors.append(NAVY)
        else:
            colors.append(MUTED)
    fig, ax = plt.subplots(figsize=(9.2, 4.4))
    y = range(len(labels))
    ax.barh(y, imps, color=colors, height=0.62)
    ax.set_yticks(list(y)); ax.set_yticklabels(labels, fontsize=12)
    ax.invert_yaxis()
    ax.set_xlabel("June won impressions (millions)", fontsize=11, color="#555")
    ax.set_xlim(0, max(imps) * 1.28)
    for i, (v, c) in enumerate(zip(imps, cpm)):
        tag = "\\$0.50 billed" if c == 0.50 else "\\$0 (free)"
        weight = "bold" if rows[i]["winner_mix"] == "free_and_paid" else "normal"
        col = RED if rows[i]["winner_mix"] == "free_and_paid" else "#333"
        ax.text(v + max(imps) * 0.015, i, f"{v:,.0f}M   {tag}", va="center",
                fontsize=11, color=col, fontweight=weight)
    ax.tick_params(length=0)
    fig.tight_layout()
    p = os.path.join(HERE, "audi_1089_billing_review_preemption_proof.png")
    fig.savefig(p, dpi=200, bbox_inches="tight"); plt.close(fig)
    print("wrote", p)


def chart_waterfall():
    rows = load("audi_1089_preemption_by_vendor.csv")
    total_bill = sum(float(r["bill_yr"]) for r in rows)
    total_after = sum(float(r["bill_after"]) for r in rows)
    cut = total_bill - total_after
    # simple 3-bar waterfall: current -> (-preemption) -> residual
    fig, ax = plt.subplots(figsize=(8.6, 4.6))
    xs = [0, 1, 2]
    # current bill
    ax.bar(0, total_bill / 1000, color=NAVY, width=0.6)
    # residual sits at bottom; the cut floats above it in red
    ax.bar(1, cut / 1000, bottom=total_after / 1000, color=RED, width=0.6)
    ax.bar(2, total_after / 1000, color=NAVY, width=0.6)
    ax.set_xticks(xs)
    ax.set_xticklabels(["Metered bill\ntoday", "Free-log\npreemption", "Residual bill\n(keep all data)"],
                       fontsize=11.5)
    ax.set_ylabel("\\$ / year (thousands)", fontsize=11, color="#555")
    ax.set_ylim(0, total_bill / 1000 * 1.16)
    ax.text(0, total_bill / 1000 + 12, f"\\${total_bill/1000:,.0f}K", ha="center",
            fontsize=14, fontweight="bold", color=NAVY)
    ax.text(1, (total_after + cut) / 1000 + 12, f"−\\${cut/1000:,.0f}K", ha="center",
            fontsize=14, fontweight="bold", color=RED)
    ax.text(1, (total_after + cut / 2) / 1000, "33.7%", ha="center", va="center",
            fontsize=11, color="white", fontweight="bold")
    ax.text(2, total_after / 1000 + 12, f"\\${total_after/1000:,.0f}K", ha="center",
            fontsize=14, fontweight="bold", color=NAVY)
    ax.tick_params(length=0)
    for s in ("left",):
        ax.spines[s].set_visible(False)
    ax.set_yticks([])
    fig.tight_layout()
    p = os.path.join(HERE, "audi_1089_billing_review_waterfall.png")
    fig.savefig(p, dpi=200, bbox_inches="tight"); plt.close(fig)
    print("wrote", p)


if __name__ == "__main__":
    chart_preemption_proof()
    chart_waterfall()
