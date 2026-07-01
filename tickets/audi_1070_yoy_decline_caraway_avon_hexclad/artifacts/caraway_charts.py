"""AUDI-1070 Caraway — the signature chart: HI-share HELD high (gate worked, stayed in High-Intent)
while VR COLLAPSED as spend scaled = within-HI over-scaling (pacing ceiling), NOT gate removal."""
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

mo=["Jun","Jul","Aug","Sep","Oct","Nov","Dec","Jan","Feb","Mar","Apr","May","Jun"]
yr=["'25"]*7+["'26"]*6
hi=[98.9,99.1,99.8,90.3,92.9,80.3,18.6,84.9,85.9,99.9,96.4,64.6,50.9]
vr=[0.341,0.368,0.127,0.226,0.36,0.328,0.235,0.122,0.141,0.146,0.142,0.165,0.157]
spend=[69.0,68.2,94.6,119.1,137.7,200.0,151.3,169.9,149.1,126.8,103.6,135.3,105.8]  # $K
x=np.arange(len(mo))

fig,ax=plt.subplots(figsize=(13,5.9))
cols=[GREEN if h>=80 else (AMBER if h>=50 else RED) for h in hi]
ax.bar(x,hi,width=0.62,color=cols,alpha=0.9,zorder=2)
for i,h in enumerate(hi): ax.text(i,h+1.5,f"{h:.0f}",ha="center",fontsize=7.5,color="#555")
ax.set_ylim(0,112); ax.set_ylabel("HI-share of prospecting delivery (%)",color=GREEN)
ax.set_yticks([0,25,50,75,100])
# VR line on secondary axis
ax2=ax.twinx()
ax2.plot(x,vr,color=NAVY,lw=2.4,marker="o",ms=5,zorder=5)
ax2.set_ylim(0,0.42); ax2.set_ylabel("Prospecting visit rate (%)",color=NAVY)
for i in [0,1,9,10]: ax2.text(i,vr[i]+0.012,f"{vr[i]:.2f}%",ha="center",fontsize=8,color=NAVY,fontweight="bold")
# the key comparison annotation
ax2.annotate("Jul '25: 99% HI -> 0.37% VR",xy=(1,0.368),xytext=(3.4,0.405),fontsize=8.5,color=NAVY,ha="center",
             arrowprops=dict(arrowstyle="->",color=NAVY,lw=1.1))
ax2.annotate("Mar '26: 99.9% HI -> 0.15% VR\nSAME HI-share, HALF the visit rate",xy=(9,0.146),xytext=(11.0,0.345),
             fontsize=8.5,color=RED,ha="center",fontweight="bold",arrowprops=dict(arrowstyle="->",color=RED,lw=1.3))
ax.set_xticks(x); ax.set_xticklabels([f"{m}\n{y}" for m,y in zip(mo,yr)],fontsize=7.5)
for s in ["top"]: ax.spines[s].set_visible(False); ax2.spines[s].set_visible(False)
plt.tight_layout(); plt.savefig(D+"artifacts/caraway_signature.png",dpi=200,bbox_inches="tight")
print("wrote caraway_signature.png")
