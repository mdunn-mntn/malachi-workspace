"""AUDI-1070 — Mike's April example explained. Avon April 2026 vs April 2025:
2x spend for the same revenue -> ROAS halved. ROAS = Revenue/Spend, so the halving
is arithmetic, identical in first-touch and last-touch."""
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

# April 2025 -> 2026
spend25, spend26 = 9142, 18369
rev25, rev26 = 311749, 302174
labels = ["Spend", "Revenue", "ROAS"]
idx = [spend26/spend25*100, rev26/rev25*100, (rev26/spend26)/(rev25/spend25)*100]
cols = [RED, GREEN, NAVY]
detail = [f"${spend25/1000:.1f}k → ${spend26/1000:.1f}k", f"${rev25/1000:.0f}k → ${rev26/1000:.0f}k",
          "34× → 16×  (LT)\n16× → 7.5× (FT)"]
fig, ax = plt.subplots(figsize=(9.5, 5.6))
xs = np.arange(3)
ax.axhline(100, color="#444", lw=1)
bars = ax.bar(xs, idx, 0.6, color=cols)
for i, (b, v, d) in enumerate(zip(bars, idx, detail)):
    ax.text(i, v + 4, f"{v-100:+.0f}%", ha="center", fontsize=14, fontweight="bold", color=cols[i])
    ax.text(i, -14, d, ha="center", fontsize=9.5, color="#555")
ax.set_xticks(xs); ax.set_xticklabels(labels, fontsize=13, fontweight="bold")
ax.set_ylim(0, 220); ax.set_ylabel("April 2026 vs April 2025 (2025 = 100)")
ax.set_title("Mike's April example: 2× the spend for the same revenue → ROAS halved",
             fontsize=14, fontweight="bold", loc="left", y=1.08)
ax.text(0, 1.015, "Avon. ROAS = Revenue ÷ Spend, so 0.97 ÷ 2.01 = 0.48. The halving is arithmetic — "
        "identical in first-touch & last-touch — and it's a single high-spend month.",
        transform=ax.transAxes, color="#666", fontsize=9.3)
for s in ["top", "right"]: ax.spines[s].set_visible(False)
ax.margins(y=0.1)
plt.subplots_adjust(bottom=0.16)
plt.savefig(D + "artifacts/audi_1070_avon_april.png", dpi=200, bbox_inches="tight")
print("wrote avon_april.png  | spend +%d%% | rev %+d%% | roas %+d%%" % (idx[0]-100, idx[1]-100, idx[2]-100))
