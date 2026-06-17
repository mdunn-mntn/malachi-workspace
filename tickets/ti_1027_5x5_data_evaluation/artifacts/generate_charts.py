#!/usr/bin/env python3
"""TI-1027 — 5x5 evaluation charts. Tufte-style, data from outputs/ CSVs."""
import csv, os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager

OUT = os.path.join(os.path.dirname(__file__), "..", "outputs")
ART = os.path.dirname(__file__)

for f in ["Helvetica Neue", "Helvetica", "Arial"]:
    if any(f.lower() in n.lower() for n in {fp.name for fp in font_manager.fontManager.ttflist}):
        plt.rcParams["font.family"] = f; break
plt.rcParams.update({"figure.facecolor": "#FAFAFA", "axes.facecolor": "#FAFAFA",
                     "savefig.facecolor": "#FAFAFA", "axes.edgecolor": "#CCCCCC",
                     "axes.grid": False, "font.size": 11})
RED, NAVY, GRAY = "#D1495B", "#1B3A5B", "#9AA5B1"

def read(name):
    with open(os.path.join(OUT, name)) as fh:
        return list(csv.DictReader(fh))

def strip(ax):
    for s in ("top", "right"): ax.spines[s].set_visible(False)

# --- Chart 1: leverage ratio — scale vs unique MM contribution ---
fig, ax = plt.subplots(figsize=(8, 4.6))
labels = ["Share of raw\nsite-visit data", "Share of unique MM-usable\ndomain signal"]
vals = [3.6, 12.2]
bars = ax.bar(labels, vals, color=[GRAY, RED], width=0.55)
for b, v in zip(bars, vals):
    ax.text(b.get_x()+b.get_width()/2, v+0.3, f"{v:.1f}%", ha="center", va="bottom",
            fontsize=14, fontweight="bold", color=b.get_facecolor())
ax.set_ylim(0, 15); ax.set_ylabel("% of total"); strip(ax); ax.set_yticks([])
ax.annotate("", xy=(1, 12.2), xytext=(0, 3.6),
            arrowprops=dict(arrowstyle="->", color=NAVY, lw=1.5, connectionstyle="arc3,rad=-0.3"))
ax.text(0.5, 9.2, "≈ 3.4× its weight", ha="center", color=NAVY, fontsize=12, fontstyle="italic")
ax.set_title("5x5 punches 3.4× above its scale in MNTN Matched", fontsize=14, fontweight="bold", loc="left", pad=26)
ax.text(0, 1.012, "Unique, classified domains contributed vs share of raw records · 7-day window",
        transform=ax.transAxes, color="#666", fontsize=9.5, va="top")
fig.tight_layout(); fig.savefig(os.path.join(ART, "ti_1027_chart_leverage.png"), dpi=200); plt.close(fig)

# --- Chart 2: vendor comparison — unique classified domains, by billing type ---
rows = read("ti_1027_vendor_uniqueness_comparison_7d.csv")
rows = [r for r in rows if r["data_source_id"] not in ("23", "30")]  # external DDPs only
rows.sort(key=lambda r: int(r["unique_classified"]), reverse=True)
names = [r["partner"] for r in rows]
vals = [int(r["unique_classified"]) for r in rows]
def color(r):
    if r["data_source_id"] == "25": return RED
    return NAVY if r["billing_type"] == "flat_fee" else GRAY
cols = [color(r) for r in rows]
fig, ax = plt.subplots(figsize=(9, 5))
y = range(len(names))
ax.barh(list(y), vals, color=cols)
ax.invert_yaxis(); ax.set_yticks(list(y)); ax.set_yticklabels(names, fontsize=10)
for i, v in enumerate(vals):
    ax.text(v + max(vals)*0.01, i, f"{v:,}", va="center", fontsize=9.5,
            color=cols[i], fontweight="bold" if rows[i]["data_source_id"]=="25" else "normal")
strip(ax); ax.set_xticks([])
ax.set_title("5x5 is the #2 unique-domain DDP — the per-use CPM vendors are redundant",
             fontsize=13.5, fontweight="bold", loc="left", pad=26)
ax.text(0, 1.012, "Unique classified domains contributed to MNTN Matched · navy/red = flat-fee, gray = $0.50 CPM (per-use)",
        transform=ax.transAxes, color="#666", fontsize=9.5, va="top")
fig.tight_layout(); fig.savefig(os.path.join(ART, "ti_1027_chart_vendor_comparison.png"), dpi=200); plt.close(fig)

# --- Chart 3: vertical dependence on 5x5-unique domains (B2B story) ---
rows = read("ti_1027_vertical_dependence_7d.csv")[:12]
names = [r["vertical_name"].replace("B2B - ", "B2B · ") for r in rows]
vals = [float(r["pct_dependent_on_5x5"]) for r in rows]
cols = [RED if n.startswith("B2B") else NAVY for n in names]
fig, ax = plt.subplots(figsize=(9, 5.4))
y = range(len(names))
ax.barh(list(y), vals, color=cols)
ax.invert_yaxis(); ax.set_yticks(list(y)); ax.set_yticklabels(names, fontsize=10)
for i, v in enumerate(vals):
    ax.text(v + 0.3, i, f"{v:.0f}%", va="center", fontsize=9.5, color=cols[i])
strip(ax); ax.set_xticks([])
ax.set_title("If 5x5 is dropped, B2B verticals lose the most fresh domain coverage",
             fontsize=13.5, fontweight="bold", loc="left", pad=26)
ax.text(0, 1.012, "% of each vertical's classified domains that ONLY 5x5 provides · red = B2B-audience verticals (our customers' targeting)",
        transform=ax.transAxes, color="#666", fontsize=9.5, va="top")
fig.tight_layout(); fig.savefig(os.path.join(ART, "ti_1027_chart_vertical_dependence.png"), dpi=200); plt.close(fig)

print("charts written:", [f for f in os.listdir(ART) if f.endswith(".png")])
