#!/usr/bin/env python3
"""TI-1027 — score-tier composition of each vendor's DELIVERED IPs (cost_impression_log household_score, 7d).
Answers: of each vendor's IPs we bid on, what % land in HI / PP / Mid / Max-Reach / unscored?
Data: outputs/ti_1027_vendor_score_tiers_7d.csv"""
import csv, os
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager

OUT = os.path.join(os.path.dirname(__file__), "..", "outputs")
ART = os.path.dirname(__file__)
for f in ["Helvetica Neue", "Helvetica", "Arial"]:
    if any(f.lower() in n.lower() for n in {fp.name for fp in font_manager.fontManager.ttflist}):
        plt.rcParams["font.family"] = f; break
plt.rcParams.update({"figure.facecolor": "#FAFAFA", "axes.facecolor": "#FAFAFA",
                     "savefig.facecolor": "#FAFAFA", "axes.edgecolor": "#CCCCCC", "axes.grid": False})

rows = list(csv.DictReader(open(os.path.join(OUT, "ti_1027_vendor_score_tiers_7d.csv"))))
rows.sort(key=lambda r: int(r["hi_10000"]) / int(r["delivered_ips"]), reverse=True)

# tiers high->low with colors
TIERS = [("hi_10000", "High Intent (10000)", "#1B5E36"),
         ("pp_8000", "Peak Performance (8000)", "#5B8A72"),
         ("high_grad", "High graduated (6666-9999)", "#A9C5A0"),
         ("mid", "Mid Intent (3333-6665)", "#E0B040"),
         ("maxreach", "Max Reach (1-3332)", "#CDB7A0"),
         ("unscored_delivered", "Unscored / no-intent", "#D1A7AD")]
names = [r["partner"] for r in rows]
y = range(len(names))
fig, ax = plt.subplots(figsize=(10.5, 5.6))
left = [0.0] * len(rows)
for key, label, color in TIERS:
    vals = [100 * int(r[key]) / int(r["delivered_ips"]) for r in rows]
    ax.barh(list(y), vals, left=left, color=color, label=label, edgecolor="white", linewidth=0.4)
    for i, (v, l) in enumerate(zip(vals, left)):
        if v >= 6:
            ax.text(l + v / 2, i, f"{v:.0f}", va="center", ha="center", fontsize=8.5,
                    color="white" if color in ("#1B5E36", "#5B8A72", "#E0B040") else "#444")
    left = [a + b for a, b in zip(left, vals)]
ax.set_yticks(list(y))
ax.set_yticklabels([("5x5" if n == "5x5" else n) for n in names], fontsize=10)
for i, n in enumerate(names):
    if n == "5x5":
        ax.get_yticklabels()[i].set_fontweight("bold"); ax.get_yticklabels()[i].set_color("#C00000")
ax.set_xlim(0, 100); ax.set_xticks([0, 25, 50, 75, 100])
ax.set_xlabel("% of delivered IPs", fontsize=10)
for sp in ("top", "right"): ax.spines[sp].set_visible(False)
ax.invert_yaxis()
ax.legend(ncol=3, fontsize=8, loc="lower center", bbox_to_anchor=(0.5, -0.22), frameon=False)
ax.set_title("Scored ≠ high-value — but 5x5's households score as high as any vendor's",
             fontsize=13, fontweight="bold", loc="left", pad=26)
ax.text(0, 1.012, "Tier mix of each vendor's IPs that MNTN served an impression to · 7-day window · household score is a property of the household, ~uniform across vendors",
        transform=ax.transAxes, color="#666", fontsize=8.5, va="top")
fig.tight_layout(); fig.savefig(os.path.join(ART, "ti_1027_chart_score_tiers.png"), dpi=200, bbox_inches="tight")
print("wrote ti_1027_chart_score_tiers.png")
