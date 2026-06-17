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

# --- Chart 2: definitive pricing (two contract structures). No big title — the slide H2 carries it.
fig, ax = plt.subplots(figsize=(10, 5.0)); ax.axis("off"); ax.set_xlim(0, 1); ax.set_ylim(0, 1)
ax.text(0.5, 0.965, "Same fair value, two views — anchor on the monthly rate + a volume minimum",
        ha="center", fontsize=11.5, color="#555")
def _box(x0, header, hcolor, fill, rows, footer):
    ax.add_patch(plt.Rectangle((x0, 0.15), 0.46, 0.72, fill=True, color=fill, ec="#BBB", lw=1))
    ax.text(x0+0.23, 0.805, header, ha="center", fontsize=12.5, fontweight="bold", color=hcolor)
    yy = 0.685
    for a, b, c in rows:
        ax.text(x0+0.035, yy, a, fontsize=11, fontweight="bold", color=NAVY)
        ax.text(x0+0.425, yy, b, ha="right", fontsize=12, fontweight="bold", color=NAVY)
        ax.text(x0+0.035, yy-0.052, c, fontsize=8.8, color="#666")
        yy -= 0.165
    ax.text(x0+0.23, 0.195, footer, ha="center", fontsize=8.0, color="#444", style="italic")
_box(0.0, "MONTHLY RATE  (+ min volume)", GREEN, "#EAF0EA",
     [("Floor", "~$3K/mo", "we'd happily pay"),
      ("FAIR", "$15–50K/mo", "≈ $25–30K anchor ask"),
      ("Walk-away", ">~$525K/mo", "= the CPM ceiling")],
     "min: ≥2.5B rows/mo  AND  ≥25M unique IP×domain pairs/day")
_box(0.52, "CPM  (per 1,000 impressions)", NAVY, "#EEF1F5",
     [("On MATCHED impr", "≤ $0.50", "peer parity — fair"),
      ("On ALL touched", "$0.02–0.05", "~95% is redundant"),
      ("Walk-away", ">$0.10", "overpaying for overlap")],
     "insist CPM bills on matched impr, not all touched")
ax.text(0.5, 0.04, "Reconciliation:  $25K/mo  ≈  $0.024 CPM (all touched)  ≈  $0.50 CPM (matched-only)  —  same dollars.",
        ha="center", fontsize=9.5, color=RED, fontweight="bold")
fig.savefig(os.path.join(ART, "ti_1027_chart_pricing.png"), dpi=200, bbox_inches="tight"); plt.close(fig)
print("wrote ti_1027_chart_spend_comparison.png, ti_1027_chart_pricing.png")
