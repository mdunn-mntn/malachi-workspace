"""AUDI-1070 HexClad — the picture SEPARATED BY campaign_id. Gantt of the 5 Stage-1 prospecting
campaigns: active spans colored by gate/HI regime, showing the handoffs (225087->446801->seasonals->446801->+551235),
446801's 39-day dark gap, and the new Mar-2026 flight."""
import datetime as dt
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
NAVY,RED,GREEN,GRAY,AMBER,PURPLE="#27496D","#D63B2F","#2E8B57","#B8BDC2","#C77B30","#6A4C93"
def dn(s): return mdates.date2num(dt.date.fromisoformat(s))

# rows top->bottom
rows=["225087","446801","485933","485962","551235"]
ylab={"225087":"56914  ·  CTV Prospecting (early)  ·  $69K",
      "446801":"93373  ·  CTV Prospecting HIGH-INTENT  ·  $2.73M",
      "485933":"100739  ·  Cell A BAU (Oct '25)  ·  $140K",
      "485962":"100744  ·  Cell B SCALE UP (Oct '25)  ·  $245K",
      "551235":"111708  ·  Prospecting GENERAL INTEREST  ·  $42K"}
Y={c:len(rows)-1-i for i,c in enumerate(rows)}

# segments: (campaign, start, end, color, hatch)
segs=[
 ("225087","2025-06-01","2025-09-02",GREEN,None),
 ("446801","2025-07-03","2025-10-03",GREEN,None),
 ("446801","2025-10-04","2025-11-13",GRAY,"//"),
 ("446801","2025-11-14","2025-12-31",RED,None),
 ("446801","2026-01-01","2026-06-03",AMBER,None),
 ("446801","2026-06-04","2026-06-30",PURPLE,None),
 ("485933","2025-10-04","2025-10-20",GREEN,None),
 ("485933","2025-10-21","2025-11-10",GREEN,None),
 ("485933","2025-11-11","2025-12-31",RED,None),
 ("485962","2025-10-04","2025-11-10",GREEN,None),
 ("485962","2025-11-11","2026-01-04",RED,None),
 ("551235","2026-03-06","2026-06-03",AMBER,None),
 ("551235","2026-06-04","2026-06-30",PURPLE,None),
]
fig,ax=plt.subplots(figsize=(14.5,5.4))
for c,s,e,col,h in segs:
    ax.barh(Y[c], dn(e)-dn(s), left=dn(s), height=0.55, color=col, hatch=h,
            edgecolor="white", linewidth=0.6, zorder=3)
# 446801 dark label + 10000 markers
ax.text((dn("2025-10-04")+dn("2025-11-13"))/2, Y["446801"], "DARK 39d", ha="center", va="center", fontsize=7.5, color="#555", fontweight="bold")
for c in ["485933","485962"]:
    ax.text((dn("2025-10-21")+dn("2025-11-10"))/2, Y[c]+0.34, "gate 10,000", ha="center", fontsize=6.8, color=GREEN)
ax.axvline(dn("2025-11-11"), color=RED, lw=1.6, ls="--", alpha=0.75, zorder=4)
ax.text(dn("2025-11-11"), len(rows)-0.35, " Nov 11: gate removed", color=RED, fontsize=8.5, fontweight="bold", ha="left")
ax.axvline(dn("2026-03-06"), color=NAVY, lw=1.1, ls=":", alpha=0.6, zorder=4)
ax.text(dn("2026-03-06"), -0.78, "Mar 6: 'General Interest'\ncampaign + DS change", color=NAVY, fontsize=7.5, ha="center")
# scale-up callout
ax.annotate("Oct 'Scale Up' A/B test\n= the spend that outran the HI pool",
            xy=(dn("2025-10-15"), Y["485962"]-0.32), xytext=(dn("2025-08-05"), Y["485962"]-0.85),
            fontsize=7.3, color=AMBER, ha="center", fontweight="bold",
            arrowprops=dict(arrowstyle="->", color=AMBER, lw=1.1))

ax.set_yticks([Y[c] for c in rows]); ax.set_yticklabels([ylab[c] for c in rows], fontsize=8.5)
ax.set_ylim(-1.25, len(rows)-0.2)
ax.xaxis.set_major_formatter(mdates.DateFormatter("%b '%y"))
ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
ax.set_xlim(dn("2025-05-25"), dn("2026-07-05"))
plt.setp(ax.get_xticklabels(), fontsize=8, rotation=0)
for s in ["top","right","left"]: ax.spines[s].set_visible(False)
ax.tick_params(left=False)
leg=[Patch(fc=GREEN,label="HI-only gate (6,666–10,000)"),Patch(fc=RED,label="gate removed / max-reach"),
     Patch(fc=AMBER,label="gate thrashed (2026)"),Patch(fc=GRAY,hatch="//",label="dark (no delivery)"),
     Patch(fc=PURPLE,label="Fangorn (Jun '26)")]
ax.legend(handles=leg, frameon=False, ncol=5, fontsize=8, loc="upper center", bbox_to_anchor=(0.5,1.10))
plt.tight_layout(); plt.savefig(D+"artifacts/audi_1070_hexclad_campaign_gantt.png",dpi=200,bbox_inches="tight")
print("wrote campaign_gantt.png")
