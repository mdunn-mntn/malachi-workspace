"""AUDI-1070 — Avon delivered-intent tracks the budget and recovers (not a one-way decline).
Reads outputs/avon_score_distribution.csv. Bars = blended intent (advertiser_household_score,
unscored counted as 0); line = monthly spend. Scores logged from Jun 2025 (CIL onset)."""
import csv
import numpy as np
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager
for fam in ["Helvetica Neue", "Helvetica", "Arial"]:
    if any(fam in f.name for f in font_manager.fontManager.ttflist):
        plt.rcParams["font.family"] = fam; break
plt.rcParams.update({"figure.facecolor": "#FAFAFA", "axes.facecolor": "#FAFAFA", "savefig.facecolor": "#FAFAFA"})
D = "tickets/audi_1070_yoy_decline_caraway_avon_hexclad/"
NAVY, RED, GREEN, GRAY = "#27496D", "#D63B2F", "#2E8B57", "#9AA0A6"

rows = []
with open(D + "outputs/avon_score_distribution.csv") as f:
    for r in csv.DictReader(f):
        if not r.get("mo"): continue
        rows.append(r)
# keep only months with logged scores (avg present)
rows = [r for r in rows if r["avg_unscored_as_0"] not in ("", None)]
mo = [r["mo"] for r in rows]
blend = [float(r["avg_unscored_as_0"]) for r in rows]
spend = [float(r["spend"]) / 1000 for r in rows]
hi = [float(r["pct_hi8k"]) for r in rows]

fig, ax = plt.subplots(figsize=(12, 5.8))
xs = np.arange(len(mo))
cols = [GREEN if b >= 9000 else (GRAY if b >= 6000 else RED) for b in blend]
ax.bar(xs, blend, 0.62, color=cols, zorder=2)
for i, b in enumerate(blend):
    ax.text(i, b + 180, f"{b:,.0f}", ha="center", fontsize=9, color="#333", zorder=3)
ax.set_ylim(0, 11000); ax.set_ylabel("Blended intent  (avg MM score, unscored = 0)", color=NAVY)
ax.set_xticks(xs); ax.set_xticklabels(mo, fontsize=9, rotation=35, ha="right")
# spend line on secondary axis
ax2 = ax.twinx()
ax2.plot(xs, spend, color=NAVY, lw=2.2, marker="o", ms=5, zorder=4)
ax2.set_ylim(0, max(spend) * 1.7); ax2.set_ylabel("Monthly spend ($k)", color=NAVY)
# annotations — keep only the peak-budget callout and the May anomaly
def idx(m): return mo.index(m) if m in mo else None
if idx("2025-11") is not None:
    j = idx("2025-11")
    ax2.annotate("peak budget $37k —\nintent dips (still scored, just lower)", xy=(j, spend[j]),
                 xytext=(j - 1.7, max(spend) * 1.45), fontsize=8.8, color=NAVY, ha="center",
                 arrowprops=dict(arrowstyle="->", color=NAVY, lw=1))
if idx("2026-05") is not None:
    j = idx("2026-05")
    ax.annotate("May: 65% unscored\n(anomaly — flagged)", xy=(j, blend[j]),
                xytext=(j - 0.2, 6700), fontsize=8.8, color=GRAY, ha="center", fontweight="bold",
                arrowprops=dict(arrowstyle="->", color=GRAY, lw=1))
# color legend (direct, no box)
ax.text(0.0, -0.30, "Green = high intent (≥9,000)   ·   Gray = mid   ·   Red = low/diluted   ·   navy line = spend",
        transform=ax.transAxes, fontsize=8.8, color="#555")
ax.set_title("MM targeting didn't degrade — delivered intent tracks the budget and recovers",
             fontsize=13.5, fontweight="bold", loc="left", y=1.07, color=NAVY)
ax.text(0, 1.015, "Avon, advertiser_household_score (MM-tuned), unscored counted as 0. High-spend months dip into "
        "lower-intent inventory then snap back when spend normalizes — reversible, not a secular decline.",
        transform=ax.transAxes, color="#666", fontsize=9.2)
for s in ["top"]: ax.spines[s].set_visible(False); ax2.spines[s].set_visible(False)
plt.tight_layout(); plt.savefig(D + "artifacts/audi_1070_avon_score_distribution.png", dpi=200, bbox_inches="tight")
print("wrote avon_score_distribution.png  months=", len(mo))
