#!/usr/bin/env python3
"""TI-1027: are vendors additive or redundant per shared IP? union vs best-single domains by # vendors.
Data: outputs/ti_1027_perip_additivity.csv, ti_1027_pair_multiplicity.csv"""
import csv, os
import numpy as np
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager
OUT = os.path.join(os.path.dirname(__file__), "..", "outputs"); ART = os.path.dirname(__file__)
for f in ["Helvetica Neue", "Helvetica", "Arial"]:
    if any(f.lower() in n.lower() for n in {fp.name for fp in font_manager.fontManager.ttflist}):
        plt.rcParams["font.family"] = f; break
plt.rcParams.update({"figure.facecolor": "#FAFAFA", "axes.facecolor": "#FAFAFA",
                     "savefig.facecolor": "#FAFAFA", "axes.edgecolor": "#CCC"})
NAVY, RED, GRAY = "#1B3A5B", "#D1495B", "#9AA5B1"
rows = [r for r in csv.DictReader(open(os.path.join(OUT, "ti_1027_perip_additivity.csv"))) if 2 <= int(r["n_vendors"]) <= 6]
x = np.arange(len(rows)); w = 0.38
best = [float(r["avg_best_single_vendor"]) for r in rows]
union = [float(r["avg_union_domains"]) for r in rows]
fig, ax = plt.subplots(figsize=(9.2, 5.2))
ax.bar(x-w/2, best, w, color=GRAY, label="Best single vendor")
ax.bar(x+w/2, union, w, color=RED, label="All vendors combined (union)")
for i, (b, u, r) in enumerate(zip(best, union, rows)):
    ax.text(i-w/2, b+0.3, f"{b:.1f}", ha="center", fontsize=9, color="#555")
    ax.text(i+w/2, u+0.3, f"{u:.1f}", ha="center", fontsize=9.5, color=RED, fontweight="bold")
    ax.text(i, u+1.4, f"{float(r['lift_vs_best_single']):.2f}x", ha="center", fontsize=10.5, fontweight="bold", color=NAVY)
ax.set_xticks(x); ax.set_xticklabels([f"{r['n_vendors']} vendors" for r in rows])
ax.set_ylim(0, 24); ax.set_yticks([])
for s in ("top", "right", "left"): ax.spines[s].set_visible(False)
ax.set_xlabel("IPs grouped by how many vendors see them", fontsize=10)
ax.legend(frameon=False, fontsize=9, loc="upper left")
ax.set_title("Stacking vendors IS additive — each adds net-new domains per IP", fontsize=13.5, fontweight="bold", loc="left", pad=26)
ax.text(0, 1.012, "Distinct domains per IP: best single vendor vs all combined · overlap only ~15-29% · 76% of all IP-domain pairs come from ONE vendor",
        transform=ax.transAxes, color="#666", fontsize=8.8, va="top")
fig.tight_layout(); fig.savefig(os.path.join(ART, "ti_1027_chart_additivity.png"), dpi=200)
print("wrote ti_1027_chart_additivity.png")
