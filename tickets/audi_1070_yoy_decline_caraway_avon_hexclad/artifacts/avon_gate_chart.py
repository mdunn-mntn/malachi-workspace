"""Avon prospecting HI-share by month + gate events. Same holiday gate-removal (Nov 19) as HexClad,
but Avon RE-GATED Jan 6 (10000) -> recovered to 99.9% HI. That's why Avon is healthy (HexClad never re-gated)."""
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
hi=[99.9,100,100,100,96.7,52.9,8.2,79.0,99.9,99.9,97.8,68.9,9.7]
x=np.arange(len(mo))
cols=[GREEN if h>=90 else (AMBER if h>=50 else RED) for h in hi]
fig,ax=plt.subplots(figsize=(13,5.6))
ax.bar(x,hi,width=0.62,color=cols,zorder=3)
for i,h in enumerate(hi): ax.text(i,h+1.5,f"{h:.0f}",ha="center",fontsize=8.5,fontweight="bold",color="#333")
ax.annotate("Nov 19: gate REMOVED (->0/-1)\nholiday spend spike",xy=(5,52.9),xytext=(3.0,72),fontsize=8.5,color=RED,ha="center",fontweight="bold",arrowprops=dict(arrowstyle="->",color=RED,lw=1.3))
ax.annotate("Jan 6: RE-GATED to 10,000\n-> recovers to 99.9% HI\n(HexClad NEVER did this)",xy=(7,79),xytext=(9.4,55),fontsize=8.5,color=GREEN,ha="center",fontweight="bold",arrowprops=dict(arrowstyle="->",color=GREEN,lw=1.3))
ax.annotate("May-Jun: loosening\n+ Fangorn onset",xy=(11,68.9),xytext=(11.4,88),fontsize=8,color=AMBER,ha="center",arrowprops=dict(arrowstyle="->",color=AMBER,lw=1.1))
ax.set_xticks(x); ax.set_xticklabels([f"{m}\n{y}" for m,y in zip(mo,yr)],fontsize=7.5)
ax.set_ylim(0,116); ax.set_ylabel("HI-share of prospecting delivery (%, RTC-excluded)")
ax.set_yticks([0,25,50,75,100])
for s in ["top","right"]: ax.spines[s].set_visible(False)
ax.text(2,-11,"mostly HI 97-100% (gate held)",ha="center",fontsize=8,color=GREEN,style="italic")
plt.tight_layout(); plt.savefig(D+"artifacts/avon_gate.png",dpi=200,bbox_inches="tight"); print("wrote avon_gate.png")
