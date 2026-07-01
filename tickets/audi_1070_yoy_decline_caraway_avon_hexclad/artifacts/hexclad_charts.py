"""AUDI-1070 HexClad charts: (1) visit-rate by intent tier (PP vs HI clincher),
(2) HI->PP tier-shift over time, (3) the paradox decomposition (spend up, OV down)."""
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
NAVY, RED, GREEN, GRAY, AMBER = "#27496D", "#D63B2F", "#2E8B57", "#9AA0A6", "#C77B30"

# ---- Chart 1: visit rate by tier ----
tiers = [("HI (10000)\nvertical + keyword", 3.842, GREEN), ("PP (8000)\nvertical only", 1.187, RED),
         ("Mid\n(3333–6665)", 1.129, GRAY), ("unscored", 0.578, GRAY), ("Max Reach\n(1–3332)", 0.277, GRAY)]
fig, ax = plt.subplots(figsize=(11, 5.8))
xs = np.arange(len(tiers))
ax.bar(xs, [t[1] for t in tiers], 0.62, color=[t[2] for t in tiers])
for i, t in enumerate(tiers):
    ax.text(i, t[1] + 0.08, f"{t[1]:.2f}%", ha="center", fontsize=12, fontweight="bold", color=t[2])
ax.annotate("Peak Performance visits at\n1.19% — just 31% of High-Intent",
            xy=(1, 1.187), xytext=(2.2, 3.0), fontsize=10.5, color=RED, ha="center", fontweight="bold",
            arrowprops=dict(arrowstyle="->", color=RED, lw=1.3))
ax.set_xticks(xs); ax.set_xticklabels([t[0] for t in tiers], fontsize=10)
ax.set_ylim(0, 4.4); ax.set_ylabel("Visit rate (per served household)")
ax.set_title("Only High-Intent converts — every tier below it visits ~3× worse",
             fontsize=14, fontweight="bold", loc="left", y=1.06, color=NAVY)
ax.text(0, 1.015, "HexClad prospecting, Jan–May 2026, per-IP visit rate by delivered intent tier. HI = in vertical AND keyword; "
        "PP = in vertical, NOT keyword. As delivery moved HI to PP, the blended rate fell toward the ~1% floor.",
        transform=ax.transAxes, color="#666", fontsize=9)
for s in ["top", "right"]: ax.spines[s].set_visible(False)
plt.tight_layout(); plt.savefig(D + "artifacts/audi_1070_hexclad_visit_rate_by_tier.png", dpi=200, bbox_inches="tight")
print("wrote hexclad_visit_rate_by_tier.png")
plt.close()

# ---- Chart 2: HI->PP tier shift over time ----
rows = []
with open(D + "outputs/hexclad_tier_shift_monthly.csv") as f:
    for r in csv.DictReader(f): rows.append(r)
mo = [r["mo"] for r in rows]
HI = [float(r["pct_HI_10k"]) for r in rows]
PP = [float(r["pct_PP_8k"]) for r in rows]
Mid = [float(r["pct_Mid"]) for r in rows]
un = [float(r["pct_unscored"]) for r in rows]
fig, ax = plt.subplots(figsize=(12.5, 5.8))
x = np.arange(len(mo))
ax.bar(x, HI, 0.7, label="High-Intent (10k)", color=GREEN)
ax.bar(x, PP, 0.7, bottom=HI, label="Peak Performance (8k)", color=RED)
ax.bar(x, Mid, 0.7, bottom=[a+b for a,b in zip(HI,PP)], label="Mid", color=AMBER)
ax.bar(x, un, 0.7, bottom=[a+b+c for a,b,c in zip(HI,PP,Mid)], label="unscored", color=GRAY)
for i in range(len(mo)):
    if PP[i] >= 8: ax.text(i, HI[i]+PP[i]/2, f"{PP[i]:.0f}", ha="center", va="center", fontsize=8.5, color="white", fontweight="bold")
ax.axvline(6.5, color="#333", lw=1, ls="--"); ax.text(6.6, 104, "2026+", fontsize=9, color="#333")
ax.annotate("PP 0% to 34%", xy=(11, 92), xytext=(9.3, 112), fontsize=11, color=RED, fontweight="bold",
            arrowprops=dict(arrowstyle="->", color=RED, lw=1.3))
ax.set_xticks(x); ax.set_xticklabels(mo, fontsize=8.5, rotation=35, ha="right")
ax.set_ylim(0, 118); ax.set_ylabel("% of delivered impressions")
ax.legend(frameon=False, loc="lower left", ncol=4, fontsize=9, bbox_to_anchor=(0,-0.28))
ax.set_title("Delivery fell out of High-Intent into Peak Performance",
             fontsize=14, fontweight="bold", loc="left", y=1.06, color=NAVY)
ax.text(0, 1.015, "HexClad prospecting delivery by intent tier. 2025 was ~95%+ pure High-Intent; through 2026 up to 34% shifted "
        "into Peak Performance (vertical-only) as spend scaled and the HI pool ran dry.", transform=ax.transAxes, color="#666", fontsize=9)
for s in ["top", "right"]: ax.spines[s].set_visible(False)
plt.tight_layout(); plt.savefig(D + "artifacts/audi_1070_hexclad_tier_shift.png", dpi=200, bbox_inches="tight")
print("wrote hexclad_tier_shift.png")
plt.close()

# ---- Chart 3: the paradox decomposition ----
M = [("Spend", +45, "in"), ("Impressions", +33, "in"), ("Reach (HH)", +22, "in"),
     ("Visit rate", -54, "bad"), ("Visits", -39, "bad"), ("Conversions", -50, "bad"),
     ("AOV", -2, "flat"), ("Order Value", -51, "bad"), ("ROAS", -66, "bad")]
fig, ax = plt.subplots(figsize=(12.5, 5.8))
xs = np.arange(len(M))
cols = {"in": NAVY, "bad": RED, "flat": GRAY}
ax.axhline(0, color="#444", lw=1)
for i, (lab, v, k) in enumerate(M):
    ax.bar(i, v, 0.62, color=cols[k])
    ax.text(i, v + (3 if v >= 0 else -6), f"{v:+d}%", ha="center", fontsize=10.5, fontweight="bold", color=cols[k])
ax.set_xticks(xs); ax.set_xticklabels([m[0] for m in M], fontsize=9.5, rotation=20, ha="right")
ax.set_ylim(-78, 60); ax.set_ylabel("2026 vs 2025 (% change)")
ax.set_title("The paradox: spend +45%, but order value −51%",
             fontsize=14, fontweight="bold", loc="left", y=1.06, color=NAVY)
ax.text(0, 1.015, "HexClad prospecting, last-touch (Mike's report). More spend & reach, but the visit rate collapsed -54% "
        "= half the conversions at a flat AOV = order value & ROAS halved. Saturation would keep OV flat; it didn't.",
        transform=ax.transAxes, color="#666", fontsize=9)
for s in ["top", "right"]: ax.spines[s].set_visible(False)
plt.tight_layout(); plt.savefig(D + "artifacts/audi_1070_hexclad_paradox.png", dpi=200, bbox_inches="tight")
print("wrote hexclad_paradox.png")
