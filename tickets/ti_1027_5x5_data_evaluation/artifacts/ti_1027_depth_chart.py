#!/usr/bin/env python3
"""TI-1027: per-IP depth — raw volume vs unique domains per IP. Data: outputs/ti_1027_per_ip_depth.csv"""
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
rows = list(csv.DictReader(open(os.path.join(OUT, "ti_1027_per_ip_depth.csv"))))

# scatter: x = raw events (log), y = unique domains per IP, size = IPs
fig, ax = plt.subplots(figsize=(10, 5.8))
def col(r): return GREEN if r["partner"] in ("augmentor_log","guid_log") else (RED if r["data_source_id"]=="25" else NAVY)
for r in rows:
    x = int(r["events"]); y = float(r["unique_domains_per_ip"]); s = 40 + int(r["ips"])/2.0e5
    ax.scatter(x, y, s=s, color=col(r), alpha=0.85, edgecolor="white", linewidth=0.8, zorder=3)
    nm = "5x5" if r["data_source_id"]=="25" else r["partner"].replace("_log","")
    off = 1.10 if r["partner"] not in ("33Across",) else 1.10
    ax.annotate(f"{nm}", (x, y*1.05+0.04), ha="center", fontsize=9.3,
                color=col(r), fontweight="bold" if r["data_source_id"]=="25" else "normal", zorder=4)
ax.set_xscale("log"); ax.set_xlim(1e6, 1.5e9); ax.set_ylim(0, 3.4)
ax.set_xlabel("Raw events / day (log)", fontsize=10)
ax.set_ylabel("UNIQUE site-visits (domains) per IP", fontsize=10)
for sp in ("top","right"): ax.spines[sp].set_visible(False)
from matplotlib.ticker import FuncFormatter
ax.xaxis.set_major_formatter(FuncFormatter(lambda v,_: f"{v/1e6:.0f}M" if v<1e9 else f"{v/1e9:.0f}B"))
ax.annotate("33Across: biggest feed, shallowest unique depth\n(repeat visits to common domains)",
            (8.34e8, 0.65), xytext=(1.2e8, 0.25), fontsize=8.5, color=GRAY,
            arrowprops=dict(arrowstyle="->", color=GRAY, lw=1))
ax.set_title("Raw volume ≠ value — what counts is unique site-visits per household",
             fontsize=13.5, fontweight="bold", loc="left", pad=26)
ax.text(0, 1.012, "Each IP: how many DISTINCT domains the vendor uniquely contributes · bubble = # IPs · green=internal, red=5x5 · 2026-06-15",
        transform=ax.transAxes, color="#666", fontsize=8.8, va="top")
fig.tight_layout(); fig.savefig(os.path.join(ART, "ti_1027_chart_depth.png"), dpi=200); plt.close(fig)
print("wrote ti_1027_chart_depth.png")
