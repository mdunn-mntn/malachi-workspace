#!/usr/bin/env python3
"""TI-1027 Phase 2 charts: (1) layered uniqueness reframe, (2) willingness-to-pay scale.
Data: outputs/ti_1027_layered_uniqueness_5x5.csv, ti_1027_wtp_anchor_5x5.csv"""
import csv, os
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager
from matplotlib.ticker import FuncFormatter

OUT = os.path.join(os.path.dirname(__file__), "..", "outputs")
ART = os.path.dirname(__file__)
for f in ["Helvetica Neue", "Helvetica", "Arial"]:
    if any(f.lower() in n.lower() for n in {fp.name for fp in font_manager.fontManager.ttflist}):
        plt.rcParams["font.family"] = f; break
plt.rcParams.update({"figure.facecolor": "#FAFAFA", "axes.facecolor": "#FAFAFA",
                     "savefig.facecolor": "#FAFAFA", "axes.edgecolor": "#CCCCCC", "axes.grid": False})
RED, NAVY, GRAY, GREEN = "#D1495B", "#1B3A5B", "#9AA5B1", "#5B8A72"

# --- Chart 1: layered uniqueness ---
rows = {r["grain"]: r for r in csv.DictReader(open(os.path.join(OUT, "ti_1027_layered_uniqueness_5x5.csv")))}
order = [("ip", "Unique IPs\n(reach)"), ("domain", "Unique domains"), ("ip_domain_pair", "Unique (IP×domain)\nevents (data values)")]
vals = [float(rows[k]["pct_unique"]) for k, _ in order]
labels = [l for _, l in order]
cols = [GRAY, NAVY, RED]
fig, ax = plt.subplots(figsize=(8.2, 5))
bars = ax.bar(labels, vals, color=cols, width=0.6)
for b, v in zip(bars, vals):
    ax.text(b.get_x()+b.get_width()/2, v+1.5, f"{v:.0f}%", ha="center", va="bottom", fontsize=15,
            fontweight="bold", color=b.get_facecolor())
ax.set_ylim(0, 90); ax.set_yticks([]);
for sp in ("top","right","left"): ax.spines[sp].set_visible(False)
ax.set_title("5x5's value is unique DATA, not unique reach", fontsize=14, fontweight="bold", loc="left", pad=26)
ax.text(0, 1.012, "% of 5x5's data unique to it (no other vendor reports it), by grain · IPs we mostly already see; the site-visits we don't",
        transform=ax.transAxes, color="#666", fontsize=9, va="top")
fig.tight_layout(); fig.savefig(os.path.join(ART, "ti_1027_chart_layered_uniqueness.png"), dpi=200); plt.close(fig)

# --- Chart 2: willingness-to-pay scale (log $) ---
w = {r["metric"]: r["value"] for r in csv.DictReader(open(os.path.join(OUT, "ti_1027_wtp_anchor_5x5.csv")))}
cpm = float(w["peer_cpm_usd"])
floor = int(w["impr_5x5_unique_ip"]) * 365 * cpm / 1000        # ~ $40K
ceiling = int(w["impr_5x5_touched"]) * 365 * cpm / 1000        # ~ $6.3M
fair_lo, fair_hi = 150_000, 600_000
fig, ax = plt.subplots(figsize=(10, 3.4))
ax.set_xscale("log"); ax.set_xlim(2e4, 1.2e7); ax.set_ylim(0, 1)
ax.axhline(0.45, color="#DDD", lw=2, zorder=1)
# fair band
ax.axvspan(fair_lo, fair_hi, ymin=0.30, ymax=0.60, color=GREEN, alpha=0.35, zorder=2)
def mark(x, label, sub, color, dy=0.62):
    ax.plot([x], [0.45], "o", color=color, ms=11, zorder=4)
    ax.annotate(label, (x, dy), ha="center", fontsize=11, fontweight="bold", color=color)
    ax.annotate(sub, (x, dy-0.13), ha="center", fontsize=8.5, color="#555")
mark(floor, f"Floor ~${floor/1e3:.0f}K", "incremental reach only", GRAY)
mark((fair_lo*fair_hi)**0.5, "Fair $150–600K", "unique B2B data signal", GREEN, dy=0.18)
mark(ceiling, f"Walk-away ~${ceiling/1e6:.1f}M", "CPM-equiv of all touched impr", RED)
ax.set_yticks([])
for sp in ("top","right","left"): ax.spines[sp].set_visible(False)
ax.xaxis.set_major_formatter(FuncFormatter(lambda v,_: f"${v/1e6:.0f}M" if v>=1e6 else f"${v/1e3:.0f}K"))
ax.set_title("What 5x5 is worth per year — place the flat fee on this scale", fontsize=13.5, fontweight="bold", loc="left", pad=24)
ax.text(0, 1.04, "Annual willingness-to-pay · log scale · renew ≤ fair, renegotiate above, walk only near the ceiling",
        transform=ax.transAxes, color="#666", fontsize=9, va="top")
fig.tight_layout(); fig.savefig(os.path.join(ART, "ti_1027_chart_wtp_scale.png"), dpi=200); plt.close(fig)
print(f"floor=${floor:,.0f}  ceiling=${ceiling:,.0f}")
print("wrote ti_1027_chart_layered_uniqueness.png, ti_1027_chart_wtp_scale.png")
