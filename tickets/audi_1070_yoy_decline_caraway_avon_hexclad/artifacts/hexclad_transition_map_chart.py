"""AUDI-1070 HexClad: Jun->Dec 2025 monthly transition map — the 'case for changes'.
Monthly prospecting HI-share (imps-weighted), colored by regime, annotated with gate + the Nov 11 pivot."""
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager
for fam in ["Helvetica Neue","Helvetica","Arial"]:
    if any(fam in f.name for f in font_manager.fontManager.ttflist):
        plt.rcParams["font.family"]=fam; break
plt.rcParams.update({"figure.facecolor":"#FAFAFA","axes.facecolor":"#FAFAFA","savefig.facecolor":"#FAFAFA"})
D="tickets/audi_1070_yoy_decline_caraway_avon_hexclad/"
NAVY,RED,GREEN,GRAY,AMBER="#27496D","#D63B2F","#2E8B57","#9AA0A6","#C77B30"

mo   =["Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
hi   =[100.0,98.1,99.9,95.3,97.5,22.5,10.7]          # imps-weighted HI% (prospecting)
gate =["6666","6657","6666","6300-6666","6666 to 10000","10000 to REMOVED","-1 (no gate)"]
imps =[2.57,5.44,5.41,5.75,9.61,34.53,18.12]          # monthly imps (M)
x=range(len(mo))
cols=[GREEN if h>=90 else (AMBER if h>=50 else RED) for h in hi]

fig,ax=plt.subplots(figsize=(12.5,5.8))
bars=ax.bar(x,hi,width=0.62,color=cols,zorder=3)
for i,(h,g,im) in enumerate(zip(hi,gate,imps)):
    ax.text(i,h+1.5,f"{h:.0f}%",ha="center",fontsize=11,fontweight="bold",color="#333")
    ax.text(i,-7,f"gate {g}",ha="center",fontsize=7.2,color="#555")
    ax.text(i,-12,f"{im:.1f}M imps",ha="center",fontsize=7,color="#999")
# regime shading
ax.axvspan(-0.5,4.5,color=GREEN,alpha=0.05,zorder=0)
ax.axvspan(4.5,6.5,color=RED,alpha=0.05,zorder=0)
ax.text(2,108,"CLEAN HI-ONLY REGIME  (gate ~6666, 95-100% HI)",ha="center",fontsize=9.5,color=GREEN,fontweight="bold")
ax.text(5.5,108,"GATE REMOVED",ha="center",fontsize=9.5,color=RED,fontweight="bold")
# the pivot arrow
ax.annotate("Nov 11: HHST gate REMOVED (to 0/-1)\n100% HI to 13% HI overnight; +20× holiday volume",
            xy=(5,22.5),xytext=(3.15,60),fontsize=9.5,color=RED,ha="center",fontweight="bold",
            arrowprops=dict(arrowstyle="->",color=RED,lw=1.5))
ax.annotate("Dec: never re-gated\n(-1 all month, ~11% HI)",xy=(6,10.7),xytext=(6.05,42),fontsize=8.5,color=RED,ha="center",
            arrowprops=dict(arrowstyle="->",color=RED,lw=1.1))
ax.text(3,-19,"Sep 18-27: brief Mid loosening",ha="center",fontsize=7,color=AMBER,style="italic")
ax.text(4,-19,"Oct 4: seasonal camps launch",ha="center",fontsize=7,color="#777",style="italic")
ax.set_xticks(list(x)); ax.set_xticklabels([m+" '25" for m in mo],fontsize=10)
ax.set_ylim(-22,116); ax.set_ylabel("Monthly HI-share of prospecting delivery (%)")
ax.set_yticks([0,25,50,75,100])
for s in ["top","right"]: ax.spines[s].set_visible(False)
ax.tick_params(bottom=False)
plt.tight_layout(); plt.savefig(D+"artifacts/audi_1070_hexclad_transition_map.png",dpi=200,bbox_inches="tight")
print("wrote transition_map.png")
