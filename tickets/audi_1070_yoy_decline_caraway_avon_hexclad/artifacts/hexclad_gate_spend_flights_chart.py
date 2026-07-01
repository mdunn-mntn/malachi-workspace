"""AUDI-1070 HexClad (34611) — HHST gate + daily SPEND + FLIGHT boundaries on one time axis (Johnny/Tofer request).
Shows: does a new flight coincide with the gate changing? Short flights (<=72h) run ungated 45% of their days vs 28%
for long flights (Tofer's manual short-flight HHST=0 practice — a tendency, not a rule; he misses some). The big
damage = the Nov-Dec holiday MEGA-flights ($112k/$165k/$409k/$180k budgets) running during the gate-off stretch.
Data: outputs/hexclad_446801_gate_history_daily.csv, _daily_spend.csv, outputs/hexclad_flights.csv."""
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

gate=sorted((r["d"],int(r["threshold"])) for r in csv.DictReader(open(D+"outputs/hexclad_446801_gate_history_daily.csv")))
gx=[dn(d) for d,_ in gate]; gy=[max(v,0) for _,v in gate]
spend=sorted((r["d"],float(r["spend"] or 0)) for r in csv.DictReader(open(D+"outputs/hexclad_446801_daily_spend.csv")))
sx=[dn(d) for d,_ in spend]; sy=[v/1000 for _,v in spend]  # $k
flights=[r for r in csv.DictReader(open(D+"outputs/hexclad_flights.csv")) if r["status_id"]=="3"]

fig,(axA,axB)=plt.subplots(2,1,figsize=(16,8.4),height_ratios=[2.3,1.0],sharex=True)
fig.suptitle("HexClad (34611) — HHST gate vs daily spend vs flight boundaries",
             fontsize=15,fontweight="bold",color=NAVY,x=0.008,ha="left",y=0.99)
fig.text(0.008,0.945,"Do new flights move the gate? Short flights (<=72h, red) run ungated 45% of days vs 28% for long — Tofer's manual HHST=0-on-short-flights (a tendency, not a rule). Big damage = the Nov-Dec holiday mega-flights.",
         fontsize=9.3,color="#666",ha="left")

# ---- Panel A: gate step + daily spend area ----
axA2=axA.twinx()
axA2.fill_between(sx,sy,0,color=AMBER,alpha=0.18,zorder=1)
axA2.plot(sx,sy,color=AMBER,lw=0.9,alpha=0.7,zorder=2)
axA2.set_ylabel("daily spend ($k)",color=AMBER,fontsize=9.5); axA2.tick_params(axis="y",labelcolor=AMBER,labelsize=8)
axA2.set_ylim(0,max(sy)*1.15)

def regime(v): return GREEN if v>=6600 else (RED if v<=0 else AMBER)
pts=list(zip(gx,gy))
for i in range(1,len(pts)):
    axA.plot([pts[i-1][0],pts[i][0]],[pts[i-1][1],pts[i-1][1]],color=regime(pts[i-1][1]),lw=2.6,zorder=5,solid_capstyle="butt")
axA.set_ylim(-900,11200); axA.set_yticks([0,3333,6666,8000,10000])
axA.set_yticklabels(["0 no gate","3333","6666 HI+Pk","8000","10000 HI"],fontsize=8)
axA.set_ylabel("HHST gate (flagship 446801)",fontsize=9.5)

# flight-start ticks at the bottom of panel A (red short / gray long)
for f in flights:
    c=RED if int(f["dur_hours"])<=72 else GRAY
    axA.plot([dn(f["start_d"]),dn(f["start_d"])],[-900,-350],color=c,lw=1.0,alpha=0.85,zorder=4)
axA.text(dn("2025-07-05"),-1500,"flight starts (red = short <=72h)",fontsize=7.6,color="#777")

# annotate the holiday mega-flights + key gate events
axA.annotate("Nov 14: holiday flight 882000 starts\n$112k -> $165k -> $409k blowout\ngate REMOVED, spend explodes",
             xy=(dn("2025-11-16"),200),xytext=(dn("2025-08-20"),4200),fontsize=8.4,color=RED,fontweight="bold",ha="center",
             arrowprops=dict(arrowstyle="->",color=RED,lw=1.3))
axA.annotate("Jan 5: gate RESTORED 10000",xy=(dn("2026-01-05"),10000),xytext=(dn("2025-12-02"),8300),
             fontsize=8.2,color=GREEN,fontweight="bold",ha="center",arrowprops=dict(arrowstyle="->",color=GREEN,lw=1.2))
axA.annotate("Feb 5: off again",xy=(dn("2026-02-05"),150),xytext=(dn("2026-02-16"),2400),
             fontsize=8.2,color=RED,fontweight="bold",ha="center",arrowprops=dict(arrowstyle="->",color=RED,lw=1.2))

# ---- Panel B: flight ribbon (each flight a bar colored by duration; label mega-budget) ----
for f in flights:
    s,e=dn(f["start_d"]),dn(f["end_d"]); e=max(e,s+0.6)
    short=int(f["dur_hours"])<=72
    axB.barh(0, e-s, left=s, height=0.5, color=(RED if short else GREEN), edgecolor="white", linewidth=0.5, zorder=3)
    if int(f["budget"])>=75000:
        axB.text((s+e)/2, 0.42, f"${int(f['budget'])//1000}k", ha="center", fontsize=7, color=NAVY, rotation=45, fontweight="bold")
axB.set_ylim(-0.5,0.9); axB.set_yticks([]); axB.set_ylabel("flights",fontsize=9.5)
axB.text(dn("2025-11-05"),-0.42,"holiday mega-flights",fontsize=7.8,color=NAVY,ha="center")

axB.xaxis.set_major_formatter(mdates.DateFormatter("%b '%y"))
axB.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
axB.set_xlim(dn("2025-06-28"),dn("2026-07-05"))
plt.setp(axB.get_xticklabels(),fontsize=8)
for sp in ["top","right"]: axB.spines[sp].set_visible(False)
for sp in ["top"]: axA.spines[sp].set_visible(False)
axA2.spines["top"].set_visible(False)

leg=[Patch(fc=GREEN,label="gate gated / flight >72h"),Patch(fc=RED,label="gate off / flight <=72h (short)"),
     Patch(fc=AMBER,label="daily spend / mid gate")]
axB.legend(handles=leg,frameon=False,ncol=3,fontsize=8.5,loc="upper center",bbox_to_anchor=(0.5,-0.35))

plt.tight_layout(rect=[0,0.02,1,0.93])
plt.savefig(D+"artifacts/hexclad_gate_spend_flights.png",dpi=190,bbox_inches="tight")
print("wrote hexclad_gate_spend_flights.png")
