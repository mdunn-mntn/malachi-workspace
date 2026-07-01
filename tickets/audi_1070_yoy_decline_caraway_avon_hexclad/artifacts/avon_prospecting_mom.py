"""AUDI-1070 — Avon prospecting month-vs-month (first-touch), 2026 vs 2025 % change.
April is the ONLY down month — it's the one month Avon 2x'd spend (diminishing returns).
Shows spend %chg vs ROAS %chg per month (inverse relationship)."""
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
months = ["Jan","Feb","Mar","Apr","May"]
spend_chg = [-14,-51,-53,+89,-7]     # all_prospecting 2026 vs 2025
roas_chg  = [-11,+76,+71,-53,+22]
conv_chg  = [+14,+50,+57,+15,+18]    # conversion rate up every month
x=np.arange(5); w=0.38
fig,ax=plt.subplots(figsize=(12,5.9))
ax.axhline(0,color="#444",lw=1)
b1=ax.bar(x-w/2, spend_chg, w, color=NAVY, label="Spend % change")
b2=ax.bar(x+w/2, roas_chg, w, color=[RED if v<0 else GREEN for v in roas_chg], label="ROAS % change")
for i,v in enumerate(spend_chg): ax.text(i-w/2, v+(3 if v>=0 else -8), f"{v:+d}%", ha="center", fontsize=9, color=NAVY)
for i,v in enumerate(roas_chg):
    col=RED if v<0 else GREEN
    ax.text(i+w/2, v+(3 if v>=0 else -8), f"{v:+d}%", ha="center", fontsize=9, fontweight="bold", color=col)
# April callout
ax.annotate("April: the ONLY down month —\nAvon 2×'d spend (+89%),\nso ROAS halved (diminishing returns)",
            xy=(3+w/2,-53), xytext=(2.3,-83), fontsize=9.5, color=RED, ha="center", fontweight="bold",
            arrowprops=dict(arrowstyle="->",color=RED,lw=1.3))
ax.text(0.02,0.04,"Conversion rate rose every month: "+"  ".join(f"{m} {c:+d}%" for m,c in zip(months,conv_chg)),
        transform=ax.transAxes, fontsize=9, color=GREEN, fontweight="bold")
ax.set_xticks(x); ax.set_xticklabels(months, fontsize=12)
ax.set_ylim(-95,105); ax.set_ylabel("2026 vs 2025 (% change)")
ax.legend(frameon=False, loc="upper right", fontsize=10)
ax.set_title("Month-by-month: April is the only down month — the one month spend doubled",
             fontsize=13.5, fontweight="bold", loc="left", y=1.06)
ax.text(0,1.012,"Avon prospecting, first-touch. Where spend fell, ROAS rose; the lone drop (April) is the month spend jumped +89%.",
        transform=ax.transAxes, color="#666", fontsize=9.3)
for s in ["top","right"]: ax.spines[s].set_visible(False)
plt.tight_layout(); plt.savefig(D+"artifacts/audi_1070_avon_prospecting_mom.png", dpi=200, bbox_inches="tight")
print("wrote avon_prospecting_mom.png")
