"""INCR-75 — Tufte funnel/waterfall chart for Jira/Slack/deck.
Reads ../outputs/incr_75_funnel_counts.csv and incr_75_final_tiered.csv.
Output: ../artifacts/incr_75_chart_funnel.png (200 DPI, #FAFAFA, Helvetica Neue).
"""
import csv
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "outputs"
ART = ROOT / "artifacts"

for fam in ("Helvetica Neue", "Helvetica", "Arial"):
    if any(fam in f.name for f in font_manager.fontManager.ttflist):
        plt.rcParams["font.family"] = fam
        break
plt.rcParams.update({"figure.facecolor": "#FAFAFA", "axes.facecolor": "#FAFAFA",
                     "savefig.facecolor": "#FAFAFA", "axes.edgecolor": "#CCCCCC"})

NAVY, RED, GRAY, GREEN, AMBER = "#1F3A5F", "#C0392B", "#9AA0A6", "#3E7D44", "#C9962E"

funnel = list(csv.DictReader(open(OUT / "incr_75_funnel_counts.csv")))
steps = [s for s in funnel if s["step"] != "99"]
final = list(csv.DictReader(open(OUT / "incr_75_final_tiered.csv")))
tiers = {t: sum(1 for x in final if x["final_tier"] == t) for t in ("Top", "Mid", "Low")}

labels = ["All delivering\n(trailing 30d)", "Clean & active", "Not B2B", "Measurable IVR\n= ELIGIBLE"]
remaining = [int(s["remaining"]) for s in steps]
removed = [int(s["removed"]) for s in steps]

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5.6), gridspec_kw={"width_ratios": [2.3, 1]})
fig.subplots_adjust(top=0.76, wspace=0.18)
fig.suptitle("2,009 active advertisers to 1,287 eligible incrementality-test candidates",
             x=0.012, y=0.985, ha="left", fontsize=14.5, fontweight="bold", color=NAVY)
fig.text(0.012, 0.915,
         "Hard filters only (clean/active, not-B2B, measurable IVR); spend, IVR-position and power are scored "
         "within the eligible set, not cut.  Window: trailing 30d.",
         ha="left", fontsize=9.5, color="#666")

# ---- left: funnel bars ----
y = range(len(labels))[::-1]
colors = [GRAY, GRAY, NAVY, GREEN]
ax1.barh(list(y), remaining, color=colors, height=0.62, zorder=3)
for yi, lab, rem, rmv in zip(list(y), labels, remaining, removed):
    ax1.text(rem + 25, yi, f"{rem:,}", va="center", ha="left", fontsize=12, fontweight="bold", color="#222")
    if rmv > 0:
        ax1.text(rem - 25, yi, f"−{rmv:,}", va="center", ha="right", fontsize=10, color="white", fontweight="bold")
ax1.set_yticks(list(y)); ax1.set_yticklabels(labels, fontsize=10.5)
ax1.set_xlim(0, max(remaining) * 1.18)
for sp in ("top", "right", "bottom"):
    ax1.spines[sp].set_visible(False)
ax1.set_xticks([])
ax1.set_title("Advertiser funnel  (count remaining; −removed)", fontsize=11.5, fontweight="bold",
              color="#333", loc="left", pad=10)

# ---- right: tier split of the eligible ----
tnames = ["Top", "Mid", "Low"]
tvals = [tiers[t] for t in tnames]
tcolors = [GREEN, AMBER, GRAY]
bars = ax2.bar(tnames, tvals, color=tcolors, width=0.62, zorder=3)
for b, v in zip(bars, tvals):
    ax2.text(b.get_x() + b.get_width() / 2, v + 6, f"{v:,}", ha="center", va="bottom",
             fontsize=12, fontweight="bold", color="#222")
for sp in ("top", "right", "left"):
    ax2.spines[sp].set_visible(False)
ax2.set_yticks([])
ax2.set_ylim(0, max(tvals) * 1.22)
ax2.set_title("Eligible set by value tier", fontsize=11.5, fontweight="bold", color="#333", loc="left", pad=10)
ax2.text(0.0, -0.13, "Top = run first (powered at 5% IVR, mid-spend, movable).",
         transform=ax2.transAxes, fontsize=8.5, color="#666", ha="left")

path = ART / "incr_75_chart_funnel.png"
fig.savefig(path, dpi=200, bbox_inches="tight")
print(f"wrote {path}")
