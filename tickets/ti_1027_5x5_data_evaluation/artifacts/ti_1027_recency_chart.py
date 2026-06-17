#!/usr/bin/env python3
"""TI-1027: 30-day targeting-window recency — sole / freshest / tied / other-fresher for 5x5's IP-domain pairs.
Data: outputs/ti_1027_recency_30d_5x5.csv"""
import csv, os
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager
OUT = os.path.join(os.path.dirname(__file__), "..", "outputs"); ART = os.path.dirname(__file__)
for f in ["Helvetica Neue", "Helvetica", "Arial"]:
    if any(f.lower() in n.lower() for n in {fp.name for fp in font_manager.fontManager.ttflist}):
        plt.rcParams["font.family"] = f; break
plt.rcParams.update({"figure.facecolor": "#FAFAFA", "axes.facecolor": "#FAFAFA",
                     "savefig.facecolor": "#FAFAFA", "axes.edgecolor": "#CCCCCC"})
RED, GREEN, NAVY, GRAY = "#D1495B", "#1B5E36", "#3E6D99", "#C9CDD3"
d = {r["segment"]: float(r["pct"]) for r in csv.DictReader(open(os.path.join(OUT, "ti_1027_recency_30d_5x5.csv")))}
# (label, pct, color, hatch)
segs = [("Sole — no other vendor has it in 30 days", d["sole"], RED, None),
        ("5x5 delivered it freshest", d["5x5_freshest"], GREEN, None),
        ("Tied — another vendor same-day (co-fresh)", d["tied"], NAVY, "///"),
        ("Another vendor delivered it fresher", d["other_fresher"], GRAY, None)]
fig, ax = plt.subplots(figsize=(10, 3.1))
left = 0
for label, v, c, hatch in segs:
    ax.barh([0], [v], left=left, color=c, edgecolor="white", hatch=hatch)
    if v >= 4:
        ax.text(left+v/2, 0, f"{v:.0f}%", ha="center", va="center", fontsize=13, fontweight="bold",
                color="white" if c in (RED, NAVY) else "#444")
    left += v
# small-segment callout for 5x5 freshest (1.2%)
ax.annotate(f"{d['5x5_freshest']:.0f}%", xy=(d["sole"]+d["5x5_freshest"]/2, 0.42),
            ha="center", fontsize=9, color=GREEN, fontweight="bold")
ax.set_xlim(0, 100); ax.set_ylim(-0.5, 0.6); ax.set_yticks([]); ax.set_xticks([])
for s in ("top", "right", "left", "bottom"): ax.spines[s].set_visible(False)
ax.set_title("Within the 30-day targeting window, ~70% of 5x5's data is irreplaceable",
             fontsize=14, fontweight="bold", loc="left", pad=32)
ax.text(0, 1.20, "5x5's 754M household-site pairs · sole-or-freshest = 71% · only ~5% genuinely covered fresher; 24% is a same-day tie",
        transform=ax.transAxes, color="#666", fontsize=9.3, va="top")
# legend (2x2)
yL = -0.40
for i, (label, v, c, hatch) in enumerate(segs):
    col = i % 2; row = i // 2
    xx = 0.02 + col*0.50; yy = yL - row*0.12
    ax.add_patch(plt.Rectangle((xx, yy-0.045), 0.018, 0.075, transform=ax.transAxes, color=c, hatch=hatch, clip_on=False))
    ax.text(xx+0.026, yy, f"{label}  ({v:.1f}%)", transform=ax.transAxes, fontsize=8.0, color="#444", va="center")
fig.tight_layout(); fig.savefig(os.path.join(ART, "ti_1027_chart_recency.png"), dpi=200, bbox_inches="tight")
print("wrote ti_1027_chart_recency.png")
