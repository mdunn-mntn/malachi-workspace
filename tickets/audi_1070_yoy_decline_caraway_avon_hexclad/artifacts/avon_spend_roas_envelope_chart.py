"""AUDI-1070 Avon (31921) — why prospecting ROAS swings 4.3x-16.8x even though it stayed gated-HI.
The tell: AOV is flat ($47-56 every month) -> swings are conversions-per-DOLLAR, not basket size, not audience.
ROAS moves INVERSELY with spend (diminishing returns in the finite HI pool): lowest-spend months (~$7k) hit 16x,
the highest-spend month (Nov $25.6k) sits at 5.9x. Same saturation mechanic as Caraway; Avon just runs at the
efficient low-spend end. Small conversion volume + seasonality fill the scatter. Stable HI-share != stable ROAS.
Data: outputs/avon_mom_lt_decomposition.csv (last-touch prospecting, obj 1,5,6; reproduces the client MoM chart)."""
import csv, datetime as dt
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager
for fam in ["Helvetica Neue","Helvetica","Arial"]:
    if any(fam in f.name for f in font_manager.fontManager.ttflist):
        plt.rcParams["font.family"]=fam; break
plt.rcParams.update({"figure.facecolor":"#FAFAFA","axes.facecolor":"#FAFAFA","savefig.facecolor":"#FAFAFA"})
D="tickets/audi_1070_yoy_decline_caraway_avon_hexclad/"
NAVY,RED,GREEN,GRAY,AMBER="#27496D","#D63B2F","#2E8B57","#9AA0A6","#C77B30"

rows=list(csv.DictReader(open(D+"outputs/avon_mom_lt_decomposition.csv")))
def mlabel(mo):
    y,m=mo.split("-"); return dt.date(int(y),int(m),1).strftime("%b '%y")
sp=[float(r["spend"])/1000 for r in rows]
roas=[float(r["roas_lt"]) for r in rows]
conv=[float(r["conv_lt"]) for r in rows]
aov=[float(r["aov_lt"]) for r in rows]
lab=[mlabel(r["mo"]) for r in rows]

fig,(axL,axR)=plt.subplots(1,2,figsize=(15,6.2),width_ratios=[1.32,1.0])

# ===== LEFT: spend vs ROAS scatter — the diminishing-returns envelope =====
sizes=[c/6 for c in conv]  # bubble size = conversion volume
sc=axL.scatter(sp, roas, s=sizes, c=roas, cmap="RdYlGn", vmin=4, vmax=17,
               edgecolor="white", linewidth=1.1, zorder=4, alpha=0.95)
for x,y,l in zip(sp,roas,lab):
    axL.annotate(l,(x,y),xytext=(4,4),textcoords="offset points",fontsize=7.2,color="#444")
# envelope guide (downward): highest achievable ROAS falls as spend rises
axL.annotate("", xy=(25.6,6.4), xytext=(7.0,17.2),
             arrowprops=dict(arrowstyle="-",color=RED,lw=1.4,ls="--",alpha=0.6))
axL.text(9.6,15.2,"efficiency ceiling falls as spend rises\n(diminishing returns in the finite HI pool)",
         fontsize=8.4,color=RED,rotation=-24,style="italic")
axL.text(7.0,17.9,"low spend (~$7k) →\ncream of HI, ~16x",fontsize=8.2,color=GREEN,fontweight="bold")
axL.text(20.2,4.7,"Nov '25 push\n$25.6k → 5.9x",fontsize=8.2,color=RED,fontweight="bold",ha="center")
axL.set_xlabel("Monthly prospecting spend ($k)",fontsize=10)
axL.set_ylabel("ROAS (last-touch)",fontsize=10)
axL.set_xlim(3,28); axL.set_ylim(3.2,18.6)
axL.set_title("Avon ROAS swings track SPEND, not audience",fontsize=12.5,fontweight="bold",color=NAVY,loc="left",pad=9)
axL.text(0,1.005,"bubble size = conversion volume; color = ROAS",transform=axL.transAxes,fontsize=8,color="#777")
for s in ["top","right"]: axL.spines[s].set_visible(False)

# ===== RIGHT: AOV is flat — the swing is NOT basket size =====
x=list(range(len(rows)))
axR.plot(x,aov,color=NAVY,lw=2.0,marker="o",ms=4,zorder=4)
axR.axhspan(47,56,color=GREEN,alpha=0.08,zorder=0)
axR.set_ylim(0,70)
for xi,a in zip(x,aov):
    if xi%3==0: axR.annotate(f"${a:.0f}",(xi,a),xytext=(0,7),textcoords="offset points",fontsize=7.5,ha="center",color=NAVY)
axR.set_xticks(x[::2]); axR.set_xticklabels([lab[i] for i in x[::2]],fontsize=7.5,rotation=45,ha="right")
axR.set_ylabel("AOV ($)",fontsize=10)
axR.set_title("AOV is flat ($47–56) all 17 months",fontsize=12.5,fontweight="bold",color=NAVY,loc="left",pad=9)
axR.text(0,1.005,"→ swings are conversions-per-dollar, not basket size, not audience quality",
         transform=axR.transAxes,fontsize=8,color="#777")
for s in ["top","right"]: axR.spines[s].set_visible(False)

fig.suptitle("Avon (31921) — stable HI-share ≠ stable ROAS: the swings are spend-efficiency + small-volume noise",
             fontsize=13.5,fontweight="bold",color=NAVY,x=0.012,ha="left",y=1.02)
plt.tight_layout()
plt.savefig(D+"artifacts/avon_spend_roas_envelope.png",dpi=200,bbox_inches="tight")
print("wrote avon_spend_roas_envelope.png")
