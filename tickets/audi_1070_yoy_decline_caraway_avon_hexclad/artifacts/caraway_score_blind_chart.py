"""Caraway: the score stayed MAXED (~10000) while VR halved -> household_score is binary and BLIND
to the within-HI quality gradient. Aug'25 (highest score, lowest VR) and Mar'26 (perfect score, low VR)."""
import numpy as np, matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager
for fam in ["Helvetica Neue","Helvetica","Arial"]:
    if any(fam in f.name for f in font_manager.fontManager.ttflist):
        plt.rcParams["font.family"]=fam; break
plt.rcParams.update({"figure.facecolor":"#FAFAFA","axes.facecolor":"#FAFAFA","savefig.facecolor":"#FAFAFA"})
D="tickets/audi_1070_yoy_decline_caraway_avon_hexclad/"
NAVY,RED,GREEN,GRAY="#27496D","#D63B2F","#2E8B57","#9AA0A6"
mo=["Jun","Jul","Aug","Sep","Oct","Nov","Dec","Jan","Feb","Mar","Apr","May","Jun"]
yr=["'25"]*7+["'26"]*6
sc=[10000,9968,9995,9543,9858,9598,7070,9725,9603,10000,9926,8560,8705]  # avg scored-only
vr=[0.341,0.368,0.127,0.226,0.36,0.328,0.235,0.122,0.141,0.146,0.142,0.165,0.157]
x=np.arange(len(mo))
fig,ax=plt.subplots(figsize=(13,5.7))
ax.plot(x,sc,color=GREEN,lw=2.4,marker="s",ms=6,zorder=4,label="avg score of scored IPs")
ax.axhline(10000,color=GREEN,lw=1,ls=":",alpha=0.6)
ax.set_ylim(6500,10400); ax.set_ylabel("Avg household_score (scored IPs)",color=GREEN)
ax.text(0.2,10120,"score pinned at ~10,000 (the flag is binary)",color=GREEN,fontsize=9,fontweight="bold")
ax2=ax.twinx()
ax2.plot(x,vr,color=NAVY,lw=2.4,marker="o",ms=5,zorder=5,label="visit rate")
ax2.set_ylim(0,0.42); ax2.set_ylabel("Prospecting visit rate (%)",color=NAVY)
ax2.annotate("Aug '25: score 9,995 (highest)\nVR 0.13% (lowest)",xy=(2,0.127),xytext=(3.6,0.05),fontsize=8.5,color=RED,ha="center",fontweight="bold",arrowprops=dict(arrowstyle="->",color=RED,lw=1.2))
ax2.annotate("Mar '26: score 10,000 (perfect)\nVR 0.15%",xy=(9,0.146),xytext=(10.3,0.30),fontsize=8.5,color=RED,ha="center",fontweight="bold",arrowprops=dict(arrowstyle="->",color=RED,lw=1.2))
ax.set_xticks(x); ax.set_xticklabels([f"{m}\n{y}" for m,y in zip(mo,yr)],fontsize=7.5)
for s in ["top"]: ax.spines[s].set_visible(False); ax2.spines[s].set_visible(False)
plt.tight_layout(); plt.savefig(D+"artifacts/caraway_score_blind.png",dpi=200,bbox_inches="tight"); print("wrote caraway_score_blind.png")
