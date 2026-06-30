"""AUDI-1070 Avon visits waterfall: why raw visits fell -16% Jan-May YoY.
Visits = (Spend / CPM) x VisitRate  ->  -16% = -12.5% (less spend) - 4.5% (higher CPM)
+ 0.4% (visit rate, flat). Decomposition is multiplicative & exact (to rounding)."""
import numpy as np
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager
for fam in ["Helvetica Neue", "Helvetica", "Arial"]:
    if any(fam in f.name for f in font_manager.fontManager.ttflist):
        plt.rcParams["font.family"] = fam; break
plt.rcParams.update({"figure.facecolor": "#FAFAFA", "axes.facecolor": "#FAFAFA", "savefig.facecolor": "#FAFAFA"})
D = "tickets/audi_1070_yoy_decline_caraway_avon_hexclad/"
NAVY, RED, GREEN = "#27496D", "#D63B2F", "#2E8B57"

V25, V26 = 526929, 443049
# multiplicative steps from 2025 visits
c0 = V25
c1 = c0 * (63967/73078)          # less spend -12.5%
c2 = c1 / (12.42/11.88)          # higher CPM +4.5% (fewer imps/$)
c3 = c2 * (8.60/8.57)            # visit rate flat +0.4%  (~= V26)
steps = [
    ("Jan–May 2025\nvisits", 0, c0, NAVY, f"{c0:,.0f}"),
    ("Less spend\n−12.5%", c1, c0, RED, f"{c1-c0:,.0f}"),
    ("Higher CPM\n+4.5%", c2, c1, RED, f"{c2-c1:,.0f}"),
    ("Visit rate\nflat (+0.4%)", c2, c3, GREEN, f"+{c3-c2:,.0f}"),
    ("Jan–May 2026\nvisits", 0, V26, NAVY, f"{V26:,.0f}"),
]
fig, ax = plt.subplots(figsize=(11, 5.4))
for i, (lab, lo, hi, col, txt) in enumerate(steps):
    ax.bar(i, hi - lo, bottom=lo, width=0.62, color=col, edgecolor="white")
    ytxt = hi + 9000 if hi >= lo else lo + 9000
    ax.text(i, max(hi, lo) + 9000, txt, ha="center", fontsize=10.5, fontweight="bold",
            color=col if i in (1, 2, 3) else "#222")
# connector lines
conn = [c0, c1, c2, c3]
for i in range(len(conn) - 1):
    ax.plot([i + 0.31, i + 1 - 0.31], [conn[i], conn[i]], color="#bbb", lw=1, ls="--")
ax.plot([3 + 0.31, 4 - 0.31], [c3, c3], color="#bbb", lw=1, ls="--")
ax.set_xticks(range(5)); ax.set_xticklabels([s[0] for s in steps], fontsize=9.5)
ax.set_ylim(0, V25 * 1.12); ax.set_ylabel("Visits")
ax.set_title("Why visits fell −16%: less spend + higher CPM — not quality",
             fontsize=14, fontweight="bold", loc="left", y=1.07)
ax.text(0, 1.01, "Visits = (Spend ÷ CPM) × Visit rate. The drop is 78% lower budget + 22% higher CPM; visit rate held flat.",
        transform=ax.transAxes, color="#666", fontsize=10)
ax.get_yaxis().set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v/1000:.0f}k"))
for s in ["top", "right"]: ax.spines[s].set_visible(False)
plt.tight_layout(); plt.savefig(D + "artifacts/audi_1070_avon_visits_waterfall.png", dpi=200, bbox_inches="tight")
print("wrote avon_visits_waterfall.png ; 2026 reconstructed =", round(c3), "vs actual", V26)
