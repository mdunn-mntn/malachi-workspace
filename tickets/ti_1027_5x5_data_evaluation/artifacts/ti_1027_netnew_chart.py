#!/usr/bin/env python3
"""TI-1027: 5x5 IP-domain pairs vs the FREE internal baseline (augmentor + guid).
Are we paying for data we already get free? Data: outputs/ti_1027_netnew_vs_free_5x5.csv"""
import csv, os
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager
OUT = os.path.join(os.path.dirname(__file__), "..", "outputs"); ART = os.path.dirname(__file__)
for f in ["Helvetica Neue", "Helvetica", "Arial"]:
    if any(f.lower() in n.lower() for n in {fp.name for fp in font_manager.fontManager.ttflist}):
        plt.rcParams["font.family"] = f; break
plt.rcParams.update({"figure.facecolor": "#FAFAFA", "axes.facecolor": "#FAFAFA",
                     "savefig.facecolor": "#FAFAFA", "axes.edgecolor": "#CCC"})
GREEN, GRAY, AMBER = "#1B5E36", "#9AA5B1", "#CDB7A0"
rows = list(csv.DictReader(open(os.path.join(OUT, "ti_1027_netnew_vs_free_5x5.csv"))))
segs = [("Already in our FREE logs\n(augmentor + guid)", float(rows[0]["pct"]), GRAY),
        ("Net-new AND classifiable\n(what we pay 5x5 for)", float(rows[1]["pct"]), GREEN),
        ("Net-new but unclassifiable", float(rows[2]["pct"]), AMBER)]
fig, ax = plt.subplots(figsize=(10, 2.9)); left = 0
for label, v, c in segs:
    ax.barh([0], [v], left=left, color=c, edgecolor="white")
    ax.text(left+v/2, 0, f"{v:.0f}%", ha="center", va="center", fontsize=14, fontweight="bold",
            color="white" if c in (GREEN, GRAY) else "#444")
    left += v
ax.set_xlim(0, 100); ax.set_ylim(-0.5, 0.5); ax.set_xticks([]); ax.set_yticks([])
for s in ("top", "right", "left", "bottom"): ax.spines[s].set_visible(False)
ax.set_title("Are we paying for data we already get free? No — 72% is net-new AND usable",
             fontsize=14, fontweight="bold", loc="left", pad=30)
ax.text(0, 1.18, "5x5's 33M daily IP-domain pairs vs our FREE internal logs (augmentor bidstream + guid pixel) · only 18% already free",
        transform=ax.transAxes, color="#666", fontsize=9.5, va="top")
yL = -0.46
for i, (label, v, c) in enumerate(segs):
    ax.add_patch(plt.Rectangle((i*0.345, yL-0.05), 0.016, 0.07, transform=ax.transAxes, color=c, clip_on=False))
    ax.text(i*0.345+0.022, yL, label, transform=ax.transAxes, fontsize=7.8, color="#444", va="center")
fig.tight_layout(); fig.savefig(os.path.join(ART, "ti_1027_chart_netnew_vs_free.png"), dpi=200, bbox_inches="tight")
print("wrote ti_1027_chart_netnew_vs_free.png")
