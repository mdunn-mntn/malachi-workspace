#!/usr/bin/env python3
"""TI-1026 deck charts (Tufte: max data-ink, direct labels, one accent color, finding-as-title).
Outputs PNGs to artifacts/ti_1026_chart_*.png. Data from the analysis (see summary.md §4.9/§4.10)."""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager
from pathlib import Path

HERE = Path(__file__).resolve().parent
NAVY = "#1F3864"; RED = "#C00000"; GREY = "#9AA0A6"; GREEN = "#375623"; BG = "#FAFAFA"
plt.rcParams.update({
    "font.family": ["Helvetica Neue", "Helvetica", "Arial", "DejaVu Sans"],
    "figure.facecolor": BG, "axes.facecolor": BG, "savefig.facecolor": BG,
    "axes.edgecolor": "#CCCCCC", "axes.linewidth": 0.8, "font.size": 13,
})

def tufte(ax):
    for s in ("top", "right", "left"):
        ax.spines[s].set_visible(False)
    ax.tick_params(length=0)
    ax.set_yticks([])
    ax.grid(False)

# ---- Chart 1: visit rate by MNTN score band (targeting works) ----
fig, ax = plt.subplots(figsize=(9, 4.6), dpi=200)
bands = ["Unscored\n(-1)", "Low\n0-3.3k", "Mid\n3.3-6.5k", "High\n6.5-10k", "Top (10000)\nHigh-Intent"]
vr = [0.463, 0.063, 0.198, 0.192, 1.354]
colors = [GREY, NAVY, NAVY, NAVY, RED]
bars = ax.bar(bands, vr, color=colors, width=0.66)
for b, v in zip(bars, vr):
    ax.text(b.get_x() + b.get_width()/2, v + 0.03, f"{v:.2f}%", ha="center", va="bottom",
            fontsize=12, fontweight="bold", color=RED if v > 1 else NAVY)
tufte(ax)
ax.set_ylim(0, 1.55)
ax.text(0, 1.02, "Our targeting works: visit rate is 7x higher for MNTN's top-intent households",
        transform=ax.transAxes, fontsize=15, fontweight="bold", color=NAVY)
ax.text(0, 0.955, "Orange Theory CTV campaign · visit rate by MNTN household-score band · 30 days, per served IP",
        transform=ax.transAxes, fontsize=10.5, color=GREY)
plt.tight_layout(rect=[0, 0, 1, 0.93]); plt.savefig(HERE/"ti_1026_chart_score_vr.png"); plt.close()

# ---- Chart 2: OTF vs CTV peer benchmark ----
fig, ax = plt.subplots(figsize=(9, 4.6), dpi=200)
labels = ["Orange Theory", "Peer median", "Peer top quartile"]
vals = [0.18, 0.91, 1.99]
colors = [RED, NAVY, NAVY]
bars = ax.barh(labels[::-1], vals[::-1], color=colors[::-1], height=0.6)
for b, v in zip(bars, vals[::-1]):
    ax.text(v + 0.03, b.get_y() + b.get_height()/2, f"{v:.2f}%", va="center",
            fontsize=12, fontweight="bold", color=NAVY)
tufte(ax)
ax.set_xticks([])
ax.set_xlim(0, 2.2)
ax.text(0, 1.05, "But Orange Theory's visit rate sits in the bottom ~15% of comparable CTV campaigns",
        transform=ax.transAxes, fontsize=15, fontweight="bold", color=NAVY)
ax.text(0, 0.975, "Blended visit rate vs 814 CTV scored-prospecting campaigns (30d). OTF 0.18% = ~15th percentile; median 0.91% (5x higher)",
        transform=ax.transAxes, fontsize=10, color=GREY)
plt.tight_layout(rect=[0, 0, 1, 0.92]); plt.savefig(HERE/"ti_1026_chart_benchmark.png"); plt.close()

# ---- Chart 3: delivery composition by score gate (why 3P can't help) ----
fig, ax = plt.subplots(figsize=(9, 4.6), dpi=200)
camps = ["Main campaign\n(score gate ON, HHST=6501)", "No-gate campaign\n(HHST=0)"]
scored = [82.3, 0.04]
midband = [16.2, 0.0]
unscored = [1.5, 99.96]
ax.barh(camps[::-1], [scored[1], scored[0]], color=GREEN, height=0.5, label="Scored >=6501 (targeted)")
ax.barh(camps[::-1], [midband[1], midband[0]], left=[scored[1], scored[0]], color="#A9D08E", height=0.5, label="Scored 1-6500")
ax.barh(camps[::-1], [unscored[1], unscored[0]],
        left=[scored[1]+midband[1], scored[0]+midband[0]], color=RED, height=0.5, label="UNSCORED (3P-only / no intent)")
# direct labels
ax.text(82.3/2, 1, "82% scored", ha="center", va="center", color="white", fontsize=11, fontweight="bold")
ax.text(99.96/2, 0, "99.96% UNSCORED", ha="center", va="center", color="white", fontsize=11, fontweight="bold")
tufte(ax); ax.set_xticks([])
ax.set_xlim(0, 100)
ax.legend(loc="lower center", bbox_to_anchor=(0.5, -0.22), ncol=3, frameon=False, fontsize=9)
ax.text(0, 1.06, "The score gate decides what 3P does: filter it out, or buy unscored junk",
        transform=ax.transAxes, fontsize=15, fontweight="bold", color=NAVY)
ax.text(0, 0.985, "Delivered impressions by household score, last 14 days. Gate ON -> 3P-only (unscored) IPs filtered (1.5%). Gate OFF -> 99.96% unscored.",
        transform=ax.transAxes, fontsize=9.5, color=GREY)
plt.tight_layout(rect=[0, 0, 1, 0.92]); plt.savefig(HERE/"ti_1026_chart_hhst_delivery.png"); plt.close()

print("charts written:", *[p.name for p in HERE.glob("ti_1026_chart_*.png")])

# ---- Chart 4: the sizing funnel — how much each filter removes ----
fig, ax = plt.subplots(figsize=(9.2, 4.8), dpi=200)
stages = ["MNTN Matched\nkeyword universe", "Inside 7-mi\nstudio fence", "...& income/age\neligible (≈)", "Score gate keeps\nHigh-Intent → bidder"]
vals = [4.58, 2.09, 1.49, None]  # millions; last is qualitative
ypos = list(range(len(stages)))[::-1]
barcolors = [NAVY, NAVY, "#A9A9A9", BG]
for i, (st, v) in enumerate(zip(stages, vals)):
    y = ypos[i]
    if v is not None:
        ax.barh(y, v, color=barcolors[i], height=0.6)
        lbl = f"{v:.2f}M" + ("  (~46%)" if i==1 else ("  (≈)" if i==2 else ""))
        ax.text(v+0.06, y, lbl, va="center", fontsize=12, fontweight="bold", color=NAVY if i<2 else "#666")
    ax.text(-0.08, y, st, va="center", ha="right", fontsize=10.5, color="#333")
# removal annotations — placed in the gaps between bars, left-aligned, no collision
ax.text(0.05, 2.5, "↓  geo removes ~2.5M  (≈54%) — the biggest filter",
        fontsize=10.5, color=RED, fontweight="bold", va="center")
ax.text(0.05, 1.5, "↓  income/age exclusions remove ~1.3M  (≈29%)",
        fontsize=10.5, color=RED, fontweight="bold", va="center")
ax.text(2.4, 0, "score gate keeps only high-intent →\ncampaign reaches ~464K IPs / 14d",
        fontsize=9.5, color="#666", va="center")
tufte(ax); ax.set_xticks([]); ax.set_yticks([]); ax.set_xlim(-2.2, 5.6); ax.set_ylim(-0.6, 3.7)
for s in ("bottom",): ax.spines[s].set_visible(False)
ax.text(-2.2, 1.12, "The sizing funnel: geo halves the audience; income/age exclusions cut another third",
        transform=ax.transAxes, fontsize=14, fontweight="bold", color=NAVY)
ax.text(-2.2, 1.05, "MNTN Matched households (national, daily) → after each filter. 3P adds nothing under the score gate. (≈ = combined-stage estimate)",
        transform=ax.transAxes, fontsize=9.5, color=GREY)
plt.tight_layout(rect=[0.02, 0, 1, 0.85]); plt.savefig(HERE/"ti_1026_chart_funnel.png"); plt.close()
print("funnel chart written")

# ---- Chart 5: availability — fresh audience keeps arriving (not pool-exhausted) ----
import json as _json
daily = _json.loads((HERE.parent/"outputs"/"ti_1026_availability_daily.json").read_text())
days = [r["day"][5:] for r in daily]            # MM-DD
new_ips = [int(r["new_ips"] or 0)/1000 for r in daily]   # K
cum = [int(r["cumulative_reach"] or 0)/1e6 for r in daily]  # M
fig, ax = plt.subplots(figsize=(9.6, 4.7), dpi=200)
ax.bar(range(len(days)), new_ips, color=NAVY, width=0.7, label="New (fresh) IPs reached per day (K)")
ax2 = ax.twinx()
ax2.plot(range(len(days)), cum, color=RED, linewidth=2.4, label="Cumulative reach (M)")
ax.set_ylim(0, 100); ax2.set_ylim(0, 2.0)
for s in ("top",): ax.spines[s].set_visible(False); ax2.spines[s].set_visible(False)
ax.set_xticks(range(0, len(days), 3)); ax.set_xticklabels([days[i] for i in range(0,len(days),3)], fontsize=8, rotation=0)
ax.tick_params(length=0); ax2.tick_params(length=0)
ax.set_ylabel("new IPs / day (K)", fontsize=9, color=NAVY); ax2.set_ylabel("cumulative reach (M)", fontsize=9, color=RED)
# annotate pause + frequency
pause_i = days.index("06-02") if "06-02" in days else None
if pause_i is not None:
    ax.annotate("delivery pause\n(not audience-out)", xy=(pause_i, 1), xytext=(pause_i-3, 55),
                fontsize=8.5, color="#666", ha="center",
                arrowprops=dict(arrowstyle="->", color="#999", lw=1))
ax.text(0.0, 1.10, "Availability is healthy: fresh households keep arriving; frequency stays ~3.7 over 90d",
        transform=ax.transAxes, fontsize=13.5, fontweight="bold", color=NAVY)
ax.text(0.0, 1.03, "Campaign 319137 · daily new (never-served) IPs + cumulative reach. New-IP flow never collapses → not pool-exhausted; spend has room.",
        transform=ax.transAxes, fontsize=9, color=GREY)
ax.legend(loc="upper right", frameon=False, fontsize=8.5, bbox_to_anchor=(0.72,1.0))
ax2.legend(loc="upper right", frameon=False, fontsize=8.5, bbox_to_anchor=(0.99,1.0))
plt.tight_layout(rect=[0,0,1,0.9]); plt.savefig(HERE/"ti_1026_chart_availability.png"); plt.close()
print("availability chart written")
