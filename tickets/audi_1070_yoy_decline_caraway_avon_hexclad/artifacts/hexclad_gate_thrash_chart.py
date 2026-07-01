"""AUDI-1070 HexClad (34611) — CORRECTED gate narrative (Johnny's pushback, 2026-07-01).
NOT 'set to 0 and never reverted'. The main flagship (446801) gate was THRASHED: ~6666 (Jul-Oct, 93-98% HI)
-> REMOVED Nov 14 (holiday, HI crashes to 11-15%) -> RESTORED 10000 Jan 5 (HI recovers to 80%) -> OFF again Feb 5
(31%) -> oscillates constantly Mar-Jun (ramps 3333->6666 then drops to 0). Delivery HI-share tracks the gate every
time. The performance problem = repeated, extended UNGATED stretches (esp. the Nov-Dec holiday, when spend & volume
also exploded: 28M/19M imps vs ~5M baseline), NOT a permanent 0.
Data: outputs/hexclad_446801_gate_history_daily.csv, outputs/hexclad_446801_monthly_hishare.csv."""
import csv, datetime as dt
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Patch
from matplotlib import font_manager
for fam in ["Helvetica Neue","Helvetica","Arial"]:
    if any(fam in f.name for f in font_manager.fontManager.ttflist):
        plt.rcParams["font.family"]=fam; break
plt.rcParams.update({"figure.facecolor":"#FAFAFA","axes.facecolor":"#FAFAFA","savefig.facecolor":"#FAFAFA"})
D="tickets/audi_1070_yoy_decline_caraway_avon_hexclad/"
NAVY,RED,GREEN,GRAY,AMBER="#27496D","#D63B2F","#2E8B57","#B8BDC2","#C77B30"
def dn(s): return mdates.date2num(dt.date.fromisoformat(s))

gate=[(r["d"], int(r["threshold"])) for r in csv.DictReader(open(D+"outputs/hexclad_446801_gate_history_daily.csv"))]
gate.sort()
gx=[dn(d) for d,_ in gate]; gy=[max(v,0) for _,v in gate]

hi=[(r["mo"], float(r["pct_hi"])) for r in csv.DictReader(open(D+"outputs/hexclad_446801_monthly_hishare.csv"))]
def mmid(mo):
    y,m=mo.split("-"); return dn(f"{y}-{m:0>2}-15")
hx=[mmid(m) for m,_ in hi]; hy=[v for _,v in hi]

fig,ax=plt.subplots(figsize=(15,6.4))

# delivery HI-share on secondary axis
ax2=ax.twinx()
ax2.plot(hx,hy,color=NAVY,lw=2.1,marker="o",ms=6,zorder=6)
for x,y in zip(hx,hy):
    ax2.annotate(f"{y:.0f}%",(x,y),xytext=(0,8),textcoords="offset points",fontsize=7.6,ha="center",color=NAVY,fontweight="bold")
ax2.set_ylim(0,108); ax2.set_ylabel("delivered HI-share (%, monthly)",color=NAVY,fontsize=9.5)
ax2.tick_params(axis="y",labelcolor=NAVY,labelsize=8); ax2.set_yticks([0,25,50,75,100])
ax2.text(dn("2025-07-06"),86,"delivered HI-share (navy) tracks the gate",color=NAVY,fontsize=8.5)

# gate step, colored by regime: gated(>=6600) green, no-gate(<=0) red, mid/continuous amber
def regime(v): return GREEN if v>=6600 else (RED if v<=0 else AMBER)
ax.step(gx,gy,where="post",color="#555",lw=0.7,alpha=0.35,zorder=3)
for i in range(1,len(gate)):
    x0,x1=gx[i-1],gx[i]; y0=gy[i-1]
    ax.plot([x0,x1],[y0,y0],color=regime(y0),lw=2.6,zorder=5,solid_capstyle="butt")
# last point
ax.plot([gx[-1],dn("2026-06-30")],[gy[-1],gy[-1]],color=regime(gy[-1]),lw=2.6,zorder=5)

ax.set_ylim(-600,11400); ax.set_ylabel("HHST gate — flagship 446801 (min score to serve)",fontsize=9.5)
ax.set_yticks([0,3333,6666,8000,10000])
ax.set_yticklabels(["0  (no gate)","3333  Mid","6666  HI+Peak","8000  Peak","10000  HI-only"],fontsize=8)

def ann(x,y,txt,color,tx,ty,ha="center"):
    ax.annotate(txt,xy=(dn(x),y),xytext=(dn(tx),ty),fontsize=8.5,color=color,ha=ha,fontweight="bold",
                arrowprops=dict(arrowstyle="->",color=color,lw=1.3))
ann("2025-09-01",6666,"Jul-Oct: gated ~6666\n(93-98% HI)",GREEN,"2025-07-20",3300)
ann("2025-11-16",0,"Nov 14: gate REMOVED\nholiday blowout (28M/19M imps)\nHI -> 11-15%",RED,"2025-09-25",1150,"center")
ann("2026-01-05",10000,"Jan 5: RESTORED 10000\nHI recovers to 80%",GREEN,"2025-12-05",8600,"center")
ann("2026-02-05",0,"Feb 5: OFF again (31%)",RED,"2026-01-20",600,"left")
ann("2026-04-15",4500,"Mar-Jun: THRASHED\nramps 3333->6666 then drops to 0,\nrepeatedly (Fangorn-era)",AMBER,"2026-03-20",9600,"center")

# shade the two big ungated stretches
ax.axvspan(dn("2025-11-14"),dn("2026-01-05"),color=RED,alpha=0.05,zorder=0)
ax.axvspan(dn("2026-02-05"),dn("2026-02-26"),color=RED,alpha=0.05,zorder=0)

ax.xaxis.set_major_formatter(mdates.DateFormatter("%b '%y"))
ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
ax.set_xlim(dn("2025-06-25"),dn("2026-07-08"))
plt.setp(ax.get_xticklabels(),fontsize=8)
for s in ["top"]: ax.spines[s].set_visible(False)
ax2.spines["top"].set_visible(False)

leg=[Patch(fc=GREEN,label="gated >=6600 (HI)"),Patch(fc=RED,label="no gate (<=0)"),Patch(fc=AMBER,label="mid / continuous (Fangorn ramp)")]
ax.legend(handles=leg,frameon=False,ncol=3,fontsize=8.5,loc="upper center",bbox_to_anchor=(0.5,1.10))
ax.set_title("HexClad (34611) — the gate was THRASHED, not 'set to 0 and never reverted'",
             fontsize=13,fontweight="bold",color=NAVY,loc="left",pad=24)

plt.tight_layout()
plt.savefig(D+"artifacts/hexclad_gate_thrash.png",dpi=200,bbox_inches="tight")
print("wrote hexclad_gate_thrash.png")
