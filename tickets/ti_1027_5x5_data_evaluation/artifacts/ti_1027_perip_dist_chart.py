#!/usr/bin/env python3
"""TI-1027: distribution of ADDITIONAL unique domains 5x5 adds per IP (not the mean).
Data: outputs/ti_1027_perip_additional_domains_dist.csv"""
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
RED, NAVY, GRAY = "#D1495B", "#1B3A5B", "#9AA5B1"
rows = list(csv.DictReader(open(os.path.join(OUT, "ti_1027_perip_additional_domains_dist.csv"))))
labels = ["0\n(redundant)", "+1", "+2–3", "+4–9", "+10+"]
vals = [float(r["pct"]) for r in rows]; cols = [GRAY, RED, NAVY, NAVY, NAVY]
fig, ax = plt.subplots(figsize=(9, 5))
b = ax.bar(labels, vals, color=cols, width=0.62)
for bar, v in zip(b, vals):
    ax.text(bar.get_x()+bar.get_width()/2, v+1, f"{v:.0f}%", ha="center", fontsize=13, fontweight="bold", color=bar.get_facecolor())
ax.set_ylim(0, 82); ax.set_yticks([])
for s in ("top", "right", "left"): ax.spines[s].set_visible(False)
ax.set_xlabel("Additional unique domains 5x5 adds for that IP", fontsize=10)
ax.set_title("For 85% of households it sees, 5x5 adds a net-new domain — but usually just one",
             fontsize=13, fontweight="bold", loc="left", pad=26)
ax.text(0, 1.012, "Distribution across the 20.8M IPs 5x5 saw on 2026-06-15 · median +1, p90 +2 · broad but shallow",
        transform=ax.transAxes, color="#666", fontsize=9, va="top")
fig.tight_layout(); fig.savefig(os.path.join(ART, "ti_1027_chart_perip_dist.png"), dpi=200)
print("wrote ti_1027_chart_perip_dist.png")
