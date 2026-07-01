"""AUDI-1070 HexClad pacing: the live 30-day HI pool tops at ~3.8M (half the 7M nominal) and
peaks in Oct 2025; brand-new share of reach falls (running on refresh) = the flow ceiling biting."""
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

mo   =["Jun","Jul","Aug","Sep","Oct","Nov","Dec","Jan","Feb","Mar","Apr","May"]
pool =[1.06,2.44,2.61,3.21,3.81,3.22,0.77,3.09,1.85,1.61,2.10,2.05]   # live 30d HI pool (M)
newsh=[100,79.7,60.2,58.2,53.7,44.9,32.2,55.3,35.6,37.9,41.3,39.6]     # brand-new share (%)
gate =[False,False,False,False,False,True,True,False,False,False,False,False]  # Nov/Dec gate-confounded
x=np.arange(len(mo))

fig,ax=plt.subplots(figsize=(13,5.9))
cols=[GREEN if (i<=4) else (RED if gate[i] else GRAY) for i in range(len(mo))]
ax.bar(x,pool,width=0.62,color=cols,zorder=3)
for i,v in enumerate(pool): ax.text(i,v+0.12,f"{v:.1f}M",ha="center",fontsize=9,color="#333")
# ceiling lines
ax.axhline(7.0,color="#888",lw=1.4,ls="--",zorder=2)
ax.text(11.4,7.05,"~7M nominal (cumulative lifetime)",fontsize=8.5,color="#888",ha="right",va="bottom")
ax.axhline(3.81,color=RED,lw=1.2,ls=":",zorder=2)
ax.text(0.0,3.9,"real live ceiling ~3.8M (30-day pool = ~half of 7M)",fontsize=8.5,color=RED,va="bottom")
# brand-new share line (secondary)
ax2=ax.twinx()
ax2.plot(x,newsh,color=NAVY,lw=2.0,marker="o",ms=4,zorder=5)
ax2.set_ylim(0,105); ax2.set_ylabel("Brand-new share of HI reach (%)",color=NAVY)
for i in [0,4,7,11]: ax2.text(i,newsh[i]+3,f"{newsh[i]:.0f}%",ha="center",fontsize=8,color=NAVY)
# Oct inflection
ax.annotate("OCT: ceiling bites\nspend $224K (~$7.2K/day) > sustainable ~$5K/day\n-> re-serving; new-share 100%->54%; reach/$ rolls over",
            xy=(4,3.81),xytext=(6.4,5.9),fontsize=8.8,color=RED,ha="center",fontweight="bold",
            arrowprops=dict(arrowstyle="->",color=RED,lw=1.4))
ax.axvline(4.5,color=RED,lw=1,ls="--",alpha=0.5)
ax.text(2,-0.75,"clean HI-only gate (Jun-Oct)",ha="center",fontsize=8,color=GREEN,style="italic")
ax.text(5.5,-0.75,"gate removed",ha="center",fontsize=8,color=RED,style="italic")
ax.set_xticks(x); ax.set_xticklabels([m+(" '25" if i<7 else " '26") for i,m in enumerate(mo)],fontsize=8.5,rotation=30,ha="right")
ax.set_ylim(0,7.7); ax.set_ylabel("Live 30-day HI pool (distinct HI IPs, millions)")
for s in ["top"]: ax.spines[s].set_visible(False); ax2.spines[s].set_visible(False)
plt.tight_layout(); plt.savefig(D+"artifacts/audi_1070_hexclad_pacing.png",dpi=200,bbox_inches="tight")
print("wrote pacing.png")
