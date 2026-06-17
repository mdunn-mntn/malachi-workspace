#!/usr/bin/env python3
"""TI-1027: 30-day targeting-window recency — how much of 5x5's data is irreplaceable.
Data: outputs/ti_1027_recency_30d_5x5.csv"""
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
RED, NAVY, GRAY = "#D1495B", "#1B3A5B", "#C9CDD3"

d = {r["metric"]: r["value"] for r in csv.DictReader(open(os.path.join(OUT, "ti_1027_recency_30d_5x5.csv")))}
sole = float(d["pct_sole_in_window"])              # 69.8
sole_or_fresh = float(d["pct_sole_or_freshest"])   # 95.4
fresh_nonsole = sole_or_fresh - sole               # 25.6
other_fresher = 100 - sole_or_fresh                # 4.6

fig, ax = plt.subplots(figsize=(10, 2.9))
segs = [("Sole — no other vendor has it in 30 days", sole, RED),
        ("5x5 is the freshest / tied", fresh_nonsole, NAVY),
        ("Another vendor delivers it fresher", other_fresher, GRAY)]
left = 0
for label, v, c in segs:
    ax.barh([0], [v], left=left, color=c, edgecolor="white")
    if v >= 4:
        ax.text(left+v/2, 0, f"{v:.0f}%", ha="center", va="center", fontsize=13, fontweight="bold",
                color="white" if c != GRAY else "#444")
    left += v
ax.set_xlim(0, 100); ax.set_ylim(-0.5, 0.5); ax.set_yticks([]); ax.set_xticks([])
for s in ("top","right","left","bottom"): ax.spines[s].set_visible(False)
ax.set_title("Within the 30-day targeting window, ~70% of 5x5's data is irreplaceable",
             fontsize=14, fontweight="bold", loc="left", pad=30)
ax.text(0, 1.18, "5x5's 754M household→site pairs · only ~5% is genuinely covered fresher by another vendor (the 7-day snapshot overstated overlap)",
        transform=ax.transAxes, color="#666", fontsize=9.5, va="top")
# legend
yL = -0.42
for i,(label,v,c) in enumerate(segs):
    ax.add_patch(plt.Rectangle((i*0.34, yL-0.05), 0.018, 0.08, transform=ax.transAxes, color=c, clip_on=False))
    ax.text(i*0.34+0.025, yL, label, transform=ax.transAxes, fontsize=8.3, color="#444", va="center")
fig.tight_layout(); fig.savefig(os.path.join(ART, "ti_1027_chart_recency.png"), dpi=200, bbox_inches="tight")
print("wrote ti_1027_chart_recency.png")
