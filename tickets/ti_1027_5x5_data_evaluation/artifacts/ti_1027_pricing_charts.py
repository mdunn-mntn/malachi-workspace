#!/usr/bin/env python3
"""TI-1027: (1) per-vendor touched-spend comparison, (2) definitive pricing (monthly + CPM).
Data: outputs/ti_1027_vendor_spend_comparison_2026-06-15.csv"""
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
RED, NAVY, GRAY, GREEN = "#D1495B", "#1B3A5B", "#9AA5B1", "#5B8A72"

# --- Chart 1: per-vendor touched media spend/day ---
rows = list(csv.DictReader(open(os.path.join(OUT, "ti_1027_vendor_spend_comparison_2026-06-15.csv"))))
rows.sort(key=lambda r: int(r["media_spend_day"]), reverse=True)
names = [r["partner"] for r in rows]
vals = [int(r["media_spend_day"])/1000 for r in rows]
def col(r): return GREEN if r["internal"]=="1" else (RED if r["data_source_id"]=="25" else NAVY)
cols = [col(r) for r in rows]
fig, ax = plt.subplots(figsize=(9.5, 5))
y = range(len(names))
ax.barh(list(y), vals, color=cols)
ax.invert_yaxis(); ax.set_yticks(list(y))
ax.set_yticklabels([("5x5" if n=="5x5" else n) for n in names], fontsize=10)
for i,n in enumerate(names):
    if n=="5x5": ax.get_yticklabels()[i].set_fontweight("bold"); ax.get_yticklabels()[i].set_color(RED)
for i,v in enumerate(vals):
    ax.text(v+4, i, f"${v:.0f}K", va="center", fontsize=9.5, color=cols[i],
            fontweight="bold" if rows[i]["data_source_id"]=="25" else "normal")
ax.set_xticks([]); [ax.spines[s].set_visible(False) for s in ("top","right")]
ax.set_title("On 'touched spend' every big vendor looks the same — that's the overlap",
             fontsize=13, fontweight="bold", loc="left", pad=26)
ax.text(0,1.012,"Media spend/day on impressions to households each vendor observed · they overlap heavily (don't sum to total) · green=internal, red=5x5",
        transform=ax.transAxes, color="#666", fontsize=8.5, va="top")
fig.tight_layout(); fig.savefig(os.path.join(ART,"ti_1027_chart_spend_comparison.png"), dpi=200); plt.close(fig)

# --- Chart 2: definitive pricing in the two contract structures ---
fig, ax = plt.subplots(figsize=(10, 5.2)); ax.axis("off")
ax.text(0, 1.0, "What we should pay for 5x5 — in the two structures Kale uses", fontsize=15, fontweight="bold", color=NAVY)
ax.text(0, 0.93, "Same fair value, three views. Anchor the negotiation on the monthly rate + a volume minimum.", fontsize=10, color="#555")
# Monthly column
ax.add_patch(plt.Rectangle((0.0,0.12),0.46,0.72, fill=True, color="#EAF0EA", ec="#CCC"))
ax.text(0.23,0.78,"MONTHLY RATE  (+ min volume)", ha="center", fontsize=12, fontweight="bold", color=GREEN)
mt = [("Floor","~$3K / mo","we'd happily pay"),
      ("FAIR","$15K–50K / mo","≈ $25–30K anchor ask"),
      ("Walk-away",">~$525K / mo","= the CPM ceiling")]
yy=0.66
for a,b,c in mt:
    ax.text(0.04,yy,a,fontsize=10.5,fontweight="bold",color=NAVY); ax.text(0.30,yy,b,fontsize=11,fontweight="bold")
    ax.text(0.04,yy-0.045,c,fontsize=8.5,color="#666"); yy-=0.135
ax.text(0.23,0.165,"Min volume: ≥2.5B rows/mo  AND  ≥25M unique IP×domain pairs/day",
        ha="center", fontsize=8.3, color="#444", style="italic")
# CPM column
ax.add_patch(plt.Rectangle((0.52,0.12),0.46,0.72, fill=True, color="#EEF1F5", ec="#CCC"))
ax.text(0.75,0.78,"CPM  (per 1,000 impressions)", ha="center", fontsize=12, fontweight="bold", color=NAVY)
ct = [("On MATCHED impr","≤ $0.50 CPM","peer parity — fair"),
      ("On ALL touched impr","$0.02–0.05 CPM","~95% is redundant"),
      ("Walk-away",">$0.10 on touched","= overpaying for overlap")]
yy=0.66
for a,b,c in ct:
    ax.text(0.56,yy,a,fontsize=10.5,fontweight="bold",color=NAVY); ax.text(0.82,yy,b,fontsize=11,fontweight="bold")
    ax.text(0.56,yy-0.045,c,fontsize=8.5,color="#666"); yy-=0.135
ax.text(0.75,0.165,"Insist CPM is billed on matched/incremental impressions, not all touched",
        ha="center", fontsize=8.3, color="#444", style="italic")
ax.text(0,0.04,"Reconciliation: $25K/mo  ≈  $0.024 CPM on all touched impr  ≈  $0.50 CPM on matched-only impr — same dollars.",
        fontsize=9, color=RED, fontweight="bold")
fig.savefig(os.path.join(ART,"ti_1027_chart_pricing.png"), dpi=200, bbox_inches="tight"); plt.close(fig)
print("wrote ti_1027_chart_spend_comparison.png, ti_1027_chart_pricing.png")
