"""AUDI-1070 — Why the API/MoM chart (~9x) differs from the UI/BigQuery (~22-26x):
SCOPE. The API chart is prospecting-only (S1). The UI cards blend all stages, and
mid-funnel (S2/S3) re-serve warm audiences at ROAS ~49-68, lifting the all-stages
number. Every scope is UP YoY. Reads outputs/avon_stage_split.csv."""
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

rows = {}
with open(D + "outputs/avon_stage_split.csv") as f:
    for r in csv.DictReader(f):
        rows[(r["stage"], r["yr"])] = r
def roas(stage, yr): return float(rows[(stage, yr)]["roas_lt_tv"])

stages = [("prospecting_S1", "Prospecting\n(S1)"), ("midfunnel_S2", "Mid-funnel\n(S2)"),
          ("mtplus_S3", "MT-Plus\n(S3)"), ("aid_wide_all", "ALL STAGES\n(AID-wide)")]
labels = [s[1] for s in stages]
r25 = [roas(s[0], "2025") for s in stages]
r26 = [roas(s[0], "2026") for s in stages]

fig, ax = plt.subplots(figsize=(12, 6))
x = np.arange(len(stages)); w = 0.38
b1 = ax.bar(x - w/2, r25, w, color=GRAY, label="Jan–May 2025")
b2 = ax.bar(x + w/2, r26, w, color=NAVY, label="Jan–May 2026")
for i in range(len(stages)):
    ax.text(x[i] - w/2, r25[i] + 1.0, f"{r25[i]:.0f}×", ha="center", fontsize=10, color=GRAY)
    ax.text(x[i] + w/2, r26[i] + 1.0, f"{r26[i]:.0f}×", ha="center", fontsize=10.5, color=NAVY, fontweight="bold")
# scope callouts
ax.annotate("the API / MoM chart\nshows ONLY this  (~9–10×)", xy=(0 + w/2, r26[0]), xytext=(0.15, 40),
            fontsize=10, color=RED, ha="center", fontweight="bold",
            arrowprops=dict(arrowstyle="->", color=RED, lw=1.4))
ax.annotate("the UI cards show\nTHIS blend  (22–26×)", xy=(3 + w/2, r26[3]), xytext=(2.55, 52),
            fontsize=10, color=GREEN, ha="center", fontweight="bold",
            arrowprops=dict(arrowstyle="->", color=GREEN, lw=1.4))
ax.set_xticks(x); ax.set_xticklabels(labels, fontsize=11)
ax.set_ylim(0, 78); ax.set_ylabel("ROAS  (last-touch + last-TV-touch)")
ax.legend(frameon=False, loc="upper left", fontsize=10.5)
ax.set_title("The API chart isn't wrong — it's prospecting-only. The UI blends all stages.",
             fontsize=14, fontweight="bold", loc="left", y=1.07, color=NAVY)
ax.text(0, 1.015, "Avon, Jan–May. Mid-funnel (S2/S3) re-serves warm audiences at ROAS ~49–68, lifting the all-stages number to ~24×. "
        "Every stage is up or flat YoY. Proof of scope: the chart's spend bars sum to ~$57k/$47k = prospecting, not the $73k/$64k AID-wide.",
        transform=ax.transAxes, color="#666", fontsize=9.0)
for s in ["top", "right"]: ax.spines[s].set_visible(False)
plt.tight_layout(); plt.savefig(D + "artifacts/audi_1070_avon_source_reconciliation.png", dpi=200, bbox_inches="tight")
print("wrote avon_source_reconciliation.png")
print(f"S1 {r25[0]:.1f}->{r26[0]:.1f} | S2 {r25[1]:.1f}->{r26[1]:.1f} | S3 {r25[2]:.1f}->{r26[2]:.1f} | AID {r25[3]:.1f}->{r26[3]:.1f}")
