"""Caraway: cumulative distinct HI reached + brand-new share = SATURATION confirmation.
New-share falls 100%->35% (fresh HI dries up, running on refresh); cumulative -> 16.5M (churn-inflated pool)."""
import numpy as np, matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager
for fam in ["Helvetica Neue","Helvetica","Arial"]:
    if any(fam in f.name for f in font_manager.fontManager.ttflist):
        plt.rcParams["font.family"]=fam; break
plt.rcParams.update({"figure.facecolor":"#FAFAFA","axes.facecolor":"#FAFAFA","savefig.facecolor":"#FAFAFA"})
D="tickets/audi_1070_yoy_decline_caraway_avon_hexclad/"
NAVY,RED,GREEN,GRAY,AMBER="#27496D","#D63B2F","#2E8B57","#9AA0A6","#C77B30"
mo=["Jun","Jul","Aug","Sep","Oct","Nov","Dec","Jan","Feb","Mar","Apr","May","Jun"]; yr=["'25"]*7+["'26"]*6
cum=[1.08,2.27,3.8,5.64,7.39,8.99,9.27,11.59,13.1,14.43,15.51,16.34,16.45]
newsh=[100,71.8,74.4,64.1,58.6,51.7,36,59.5,42.2,37.9,37.1,34.7,20.2]
x=np.arange(len(mo))
fig,ax=plt.subplots(figsize=(13,5.7))
ax.bar(x,cum,width=0.6,color=GRAY,alpha=0.55,zorder=2)
for i,v in enumerate(cum): ax.text(i,v+0.2,f"{v:.1f}",ha="center",fontsize=7,color="#666")
ax.set_ylim(0,19); ax.set_ylabel("Cumulative distinct HI households reached (M)",color="#666")
ax2=ax.twinx()
ax2.plot(x,newsh,color=NAVY,lw=2.5,marker="o",ms=5,zorder=5)
ax2.axhline(50,color=RED,lw=1,ls=":"); ax2.text(0.1,52,"50% — half the reach is now RE-served HI",color=RED,fontsize=8)
ax2.set_ylim(0,108); ax2.set_ylabel("Brand-new share of HI reach (%)",color=NAVY)
for i in [0,4,5,10]: ax2.text(i,newsh[i]+3,f"{newsh[i]:.0f}%",ha="center",fontsize=8,color=NAVY,fontweight="bold")
ax2.annotate("~Oct-Nov '25: crosses 50% ->\nmajority of HI reach is now RECYCLED\n(re-targeting the lower end of HI)",
             xy=(5,51.7),xytext=(8.2,80),fontsize=8.5,color=RED,ha="center",fontweight="bold",
             arrowprops=dict(arrowstyle="->",color=RED,lw=1.3))
ax.axvspan(5.5,6.5,color=RED,alpha=0.05); ax.text(6,1.0,"Dec\ngate off",ha="center",fontsize=6.5,color=RED)
ax.set_xticks(x); ax.set_xticklabels([f"{m}\n{y}" for m,y in zip(mo,yr)],fontsize=7.5)
for s in ["top"]: ax.spines[s].set_visible(False); ax2.spines[s].set_visible(False)
plt.tight_layout(); plt.savefig(D+"artifacts/caraway_saturation.png",dpi=200,bbox_inches="tight"); print("wrote caraway_saturation.png")
