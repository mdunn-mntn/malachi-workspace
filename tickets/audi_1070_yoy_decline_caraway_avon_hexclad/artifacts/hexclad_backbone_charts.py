"""AUDI-1070 HexClad backbone charts: (1) served-IP tier composition 2025 vs 2026
(HI 95%->49% = disproves 'staying in HI'); (2) HHST gate trajectory (loosened in 2026)."""
import numpy as np
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager
for fam in ["Helvetica Neue","Helvetica","Arial"]:
    if any(fam in f.name for f in font_manager.fontManager.ttflist):
        plt.rcParams["font.family"]=fam; break
plt.rcParams.update({"figure.facecolor":"#FAFAFA","axes.facecolor":"#FAFAFA","savefig.facecolor":"#FAFAFA"})
D="tickets/audi_1070_yoy_decline_caraway_avon_hexclad/"
NAVY,RED,GREEN,GRAY,AMBER="#27496D","#D63B2F","#2E8B57","#9AA0A6","#C77B30"

# ---- Chart 1: composition 2025 vs 2026 (horizontal stacked) ----
# tiers: HI, PP, Mid, unscored (MaxReach folded into unscored/low)
segs=[("High-Intent (10k)",GREEN),("Peak Performance (8k)",RED),("Mid",AMBER),("unscored / low",GRAY)]
y2025=[95,0,2,3]; y2026=[49,17,10,24]
fig,ax=plt.subplots(figsize=(12,3.9))
for i,(row,label) in enumerate([(y2025,"2025\n(first scored data)"),(y2026,"2026\n(Jan–May)")]):
    left=0
    for (name,col),v in zip(segs,row):
        ax.barh(i,v,left=left,color=col,edgecolor="white",height=0.55)
        if v>=4: ax.text(left+v/2,i,f"{v:.0f}%",ha="center",va="center",color="white",fontsize=11,fontweight="bold")
        left+=v
ax.set_yticks([0,1]); ax.set_yticklabels(["2025\n(1st scored)","2026\n(Jan–May)"],fontsize=10.5)
ax.set_xlim(0,100); ax.set_xlabel("% of served households")
handles=[plt.Rectangle((0,0),1,1,color=c) for _,c in segs]
ax.legend(handles,[s[0] for s in segs],frameon=False,ncol=4,fontsize=9.5,loc="lower center",bbox_to_anchor=(0.5,-0.5))
ax.annotate("",xy=(49,1),xytext=(95,0),arrowprops=dict(arrowstyle="->",color=NAVY,lw=1.6))
ax.text(72,0.5,"High-Intent\n95% → 49%",fontsize=11,color=NAVY,fontweight="bold",ha="center")
ax.set_title("HexClad did NOT stay in High-Intent — its HI share roughly halved",
             fontsize=14,fontweight="bold",loc="left",y=1.1,color=NAVY)
ax.text(0,1.16,"Served-household intent-tier composition, prospecting. (Jan–May 2025 predates score logging; 2025 = the first "
        "scored months, H2-2025, which ran ~95% High-Intent.)",transform=ax.transAxes,color="#666",fontsize=9)
for s in ["top","right","left"]: ax.spines[s].set_visible(False)
ax.tick_params(left=False)
plt.tight_layout(); plt.savefig(D+"artifacts/audi_1070_hexclad_composition.png",dpi=200,bbox_inches="tight")
print("wrote hexclad_composition.png"); plt.close()

# ---- Chart 2: HHST gate trajectory ----
mo=["2025-01","02","03","04","05","06","09","10","11","2026-01","02","03","04","05"]
hhst=[6666,6666,6666,6666,6666,6666,6666,6666,10000,10000,10000,4500,6300,6666]
p10=[6666,6666,6666,6666,4800,6666,6400,6666,3334,10000,10000,3600,3900,5401]
x=np.arange(len(mo))
fig,ax=plt.subplots(figsize=(12,5.2))
ax.plot(x,hhst,color=NAVY,lw=2.3,marker="o",ms=5,label="median HHST")
ax.fill_between(x,p10,hhst,color=NAVY,alpha=0.08)
ax.axhline(10000,color=GREEN,lw=1,ls=":"); ax.text(0,10150,"10000 = HI only",fontsize=8.5,color=GREEN)
ax.axhline(8000,color=RED,lw=1,ls=":"); ax.text(0,8150,"8000 = PP",fontsize=8.5,color=RED)
ax.axhline(6666,color="#888",lw=1,ls=":"); ax.text(0,6716,"6666 = HI+PP floor (Mid gated)",fontsize=8.5,color="#888")
ax.axhline(3333,color=GRAY,lw=1,ls=":"); ax.text(0,3483,"3333 = Mid floor",fontsize=8.5,color=GRAY)
ax.axvline(8.5,color="#333",lw=1,ls="--"); ax.text(8.6,11200,"2026",fontsize=9,color="#333")
ax.annotate("gate dropped to 4500\n(opened Mid) to fill the +45% budget",xy=(11,4500),xytext=(9.0,1800),
            fontsize=9.5,color=RED,ha="center",fontweight="bold",arrowprops=dict(arrowstyle="->",color=RED,lw=1.3))
ax.set_xticks(x); ax.set_xticklabels(mo,fontsize=8.5,rotation=35,ha="right")
ax.set_ylim(0,11800); ax.set_ylabel("Household Score Threshold (the gate)")
ax.set_title("The intent gate loosened in 2026 to keep spending",
             fontsize=14,fontweight="bold",loc="left",y=1.06,color=NAVY)
ax.text(0,1.015,"HexClad prospecting HHST (min household_score the bidder will serve). Steady at 6666 in 2025; in 2026 it swung "
        "to 10000 then collapsed to 3333–4500, admitting Mid-intent — the signature of a budget outrunning the High-Intent pool.",
        transform=ax.transAxes,color="#666",fontsize=8.8)
for s in ["top","right"]: ax.spines[s].set_visible(False)
plt.tight_layout(); plt.savefig(D+"artifacts/audi_1070_hexclad_hhst.png",dpi=200,bbox_inches="tight")
print("wrote hexclad_hhst.png")
