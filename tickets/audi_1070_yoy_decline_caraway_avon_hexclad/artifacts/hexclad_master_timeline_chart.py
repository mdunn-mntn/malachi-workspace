"""AUDI-1070 HexClad MASTER change timeline — every lever on one canvas:
delivery HI-share (outcome) + HHST gate + audience data-sources + campaigns + scoring, Jul2025-Jun2026.
The single 'full picture' visual: the Nov-11 gate removal is the pivot; audience HI substrate stayed intact."""
import datetime as dt
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager
import matplotlib.dates as mdates
for fam in ["Helvetica Neue","Helvetica","Arial"]:
    if any(fam in f.name for f in font_manager.fontManager.ttflist):
        plt.rcParams["font.family"]=fam; break
plt.rcParams.update({"figure.facecolor":"#FAFAFA","axes.facecolor":"#FAFAFA","savefig.facecolor":"#FAFAFA"})
D="tickets/audi_1070_yoy_decline_caraway_avon_hexclad/"
NAVY,RED,GREEN,GRAY,AMBER="#27496D","#D63B2F","#2E8B57","#9AA0A6","#C77B30"
def dd(s): return dt.date.fromisoformat(s)

fig,ax=plt.subplots(figsize=(14.5,7.6))
x0,x1=dd("2025-06-20"),dd("2025-07-01")  # placeholder
# regime shading
ax.axvspan(dd("2025-07-01"),dd("2025-11-11"),color=GREEN,alpha=0.06)
ax.axvspan(dd("2025-11-11"),dd("2025-12-31"),color=RED,alpha=0.06)
ax.axvspan(dd("2025-12-31"),dd("2026-06-30"),color=AMBER,alpha=0.06)
ax.text(dd("2025-09-05"),5.75,"CLEAN HI-ONLY (gate ~6,666–10,000, 95–100% HI)",color=GREEN,fontsize=9,ha="center",fontweight="bold")
ax.text(dd("2025-12-05"),5.75,"GATE REMOVED\n(max-reach)",color=RED,fontsize=8.5,ha="center",fontweight="bold")
ax.text(dd("2026-03-20"),5.75,"GATE THRASHED 51× (0 to 10,000) — HI-share swings with each flip",color=AMBER,fontsize=9,ha="center",fontweight="bold")

# --- lane 5: delivery HI-share line (outcome) ---
mo=[ "2025-07-15","2025-08-15","2025-09-15","2025-10-15","2025-11-15","2025-12-15",
     "2026-01-15","2026-02-15","2026-03-15","2026-04-15","2026-05-15"]
hi=[98,100,95,98,22,11,55,28,39,36,33]
mx=[dd(m) for m in mo]
def toy(v): return 4.55+ (v/100.0)*1.15   # scale 0-100 into y 4.55-5.70
ax.plot(mx,[toy(v) for v in hi],color=NAVY,lw=2.0,marker="o",ms=4,zorder=5)
for m,v in zip(mx,hi): ax.text(m,toy(v)+0.04,f"{v}%",fontsize=7,color=NAVY,ha="center")
ax.text(dd("2025-06-25"),toy(50),"Delivery\nHI-share",fontsize=8.5,color=NAVY,fontweight="bold",va="center",ha="right")

# --- lanes: (name, y) ---
lanes={"HHST gate":3.4,"Audience (data sources)":2.4,"Campaigns":1.5,"Scoring engine":0.7}
for name,y in lanes.items():
    ax.axhline(y,color="#ddd",lw=0.8,zorder=0)
    ax.text(dd("2025-06-25"),y,name,fontsize=8.5,fontweight="bold",va="center",ha="right",color="#333")

# events: (date, lane_y, label, color, dy)
E=[
 ("2025-07-02",3.4,"6,666\n(HI+PP)",GREEN,0.28),
 ("2025-10-21",3.4,"to 10,000\n(HI-only)",GREEN,0.28),
 ("2025-11-11",3.4,"REMOVED to 0",RED,-0.34),
 ("2025-12-05",3.4,"−1 all Dec",RED,0.26),
 ("2025-07-02",2.4,"DS{1,2,14,19,35}\n+RTC directive",NAVY,0.34),
 ("2025-09-24",2.4,"+DS13\nvertical",NAVY,-0.34),
 ("2025-10-29",2.4,"+DS21/34",NAVY,0.30),
 ("2026-02-18",2.4,"+DS4 CRM\n+DS16",NAVY,0.34),
 ("2026-03-04",2.4,"−DS1/−DS35\n(DS19 stays)",NAVY,-0.36),
 ("2026-06-03",2.4,"+DS46",NAVY,0.30),
 ("2025-10-04",1.5,"seasonals launch;\n446801 dark",GRAY,0.30),
 ("2025-11-14",1.5,"handoff to 446801\nmax-reach (11/15)",GRAY,-0.34),
 ("2025-11-27",1.5,"Black Friday\n~20× volume",RED,0.30),
 ("2026-01-05",0.7,"RTC fires\n(dormant 2025)",AMBER,0.30),
 ("2025-10-20",0.7,"pacing inflection:\nspend 40% over sustainable",AMBER,-0.32),
 ("2026-06-04",0.7,"Fangorn live\n(bucketed to continuous)",RED,0.30),
]
for ds,y,lab,col,dy in E:
    x=dd(ds)
    ax.plot([x],[y],"o",ms=6,color=col,zorder=6)
    ax.annotate(lab,xy=(x,y),xytext=(x,y+dy),fontsize=6.9,color=col,ha="center",
                va="bottom" if dy>0 else "top",fontweight="bold",
                bbox=dict(boxstyle="round,pad=0.2",fc="white",ec=col,lw=0.6))
# the pivot line across all lanes
ax.axvline(dd("2025-11-11"),color=RED,lw=1.6,ls="--",alpha=0.7,zorder=2)

ax.set_ylim(0.1,6.0); ax.set_xlim(dd("2025-06-20"),dd("2026-06-30"))
ax.xaxis.set_major_formatter(mdates.DateFormatter("%b '%y"))
ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
ax.set_yticks([]);
for s in ["top","right","left"]: ax.spines[s].set_visible(False)
plt.setp(ax.get_xticklabels(),fontsize=8.5,rotation=0)
plt.tight_layout(); plt.savefig(D+"artifacts/audi_1070_hexclad_master_timeline.png",dpi=200,bbox_inches="tight")
print("wrote master_timeline.png")
