"""AUDI-1070 HexClad: HI supply is NOT budget-driven. Distinct HI IPs served per month
vs spend — at constant spend HI supply swings 2-3x (Jan 79% vs Feb 30%)."""
import numpy as np
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager
for fam in ["Helvetica Neue","Helvetica","Arial"]:
    if any(fam in f.name for f in font_manager.fontManager.ttflist):
        plt.rcParams["font.family"]=fam; break
plt.rcParams.update({"figure.facecolor":"#FAFAFA","axes.facecolor":"#FAFAFA","savefig.facecolor":"#FAFAFA"})
D="tickets/audi_1070_yoy_decline_caraway_avon_hexclad/"
NAVY,RED,GREEN,GRAY="#27496D","#D63B2F","#2E8B57","#9AA0A6"
mo=["25-06","07","08","09","10","11","12","26-01","02","03","04","05"]
hi_ips=[1.45,2.49,2.69,3.25,3.93,3.29,0.86,3.15,1.67,1.79,2.16,2.22]  # millions
spend=[52,120,123,131,224,765,408,152,185,161,153,167]  # $K
pct_hi=[98,97,98,93,96,30,15,79,30,45,42,57]
x=np.arange(len(mo))
fig,ax=plt.subplots(figsize=(12.5,5.9))
cols=[GREEN if p>=80 else (GRAY if p>=45 else RED) for p in pct_hi]
ax.bar(x,hi_ips,0.62,color=cols,zorder=2)
for i,v in enumerate(hi_ips): ax.text(i,v+0.08,f"{v:.1f}M",ha="center",fontsize=8.5,color="#333")
ax2=ax.twinx()
ax2.plot(x,spend,color=NAVY,lw=2.2,marker="o",ms=5,zorder=4)
ax2.set_ylim(0,900); ax2.set_ylabel("Monthly spend ($K)",color=NAVY)
ax.set_ylim(0,4.6); ax.set_ylabel("Distinct High-Intent IPs served (millions)")
# annotate the constant-spend divergence
ax.annotate("Jan '26: $152K → 3.15M HI (79%)",xy=(7,3.15),xytext=(6.0,4.25),fontsize=9,color=GREEN,ha="center",
            arrowprops=dict(arrowstyle="->",color=GREEN,lw=1.1))
ax.annotate("Feb '26: MORE spend ($185K),\nHALF the HI (1.67M, 30%)",xy=(8,1.67),xytext=(9.6,3.6),fontsize=9,color=RED,ha="center",fontweight="bold",
            arrowprops=dict(arrowstyle="->",color=RED,lw=1.2))
ax.set_xticks(x); ax.set_xticklabels(mo,fontsize=9)
ax.set_title("It's High-Intent SUPPLY, not budget — same spend, 2–3× swings in HI",
             fontsize=13.5,fontweight="bold",loc="left",y=1.06,color=NAVY)
ax.text(0,1.015,"HexClad prospecting: distinct HI-scored households served (bars, colored by HI share) vs spend (line). "
        "A bigger budget on a fixed pool would give a STEADY HI count; instead it swings with supply, independent of spend.",
        transform=ax.transAxes,color="#666",fontsize=8.8)
for s in ["top"]: ax.spines[s].set_visible(False); ax2.spines[s].set_visible(False)
plt.tight_layout(); plt.savefig(D+"artifacts/audi_1070_hexclad_supply.png",dpi=200,bbox_inches="tight")
print("wrote hexclad_supply.png")
