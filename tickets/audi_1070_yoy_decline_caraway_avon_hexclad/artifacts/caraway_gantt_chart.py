"""AUDI-1070 Caraway — per-campaign Gantt (run times + gate/HI regime), like HexClad.
Shows: Jul2025-Apr2026 = ONE flagship campaign gated HI (single-campaign over-scaling, NOT a blend);
Dec-25 holiday gate-drop; May-13-2026 HANDOFF flagship->DMA cells (the 'one off/one on' pattern)."""
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

rows=["419915","490249","439156","613551","613591"]
ylab={"419915":"88892  ·  All-DMAs  ·  $29K",
      "490249":"101460  ·  Test Campaign  ·  $82K",
      "439156":"92099  ·  CTV Prospecting (FLAGSHIP)  ·  $1.33M",
      "613551":"123920  ·  High-DMA (new)  ·  $144K",
      "613591":"123929  ·  Low-DMA (new)  ·  $49K"}
Y={c:len(rows)-1-i for i,c in enumerate(rows)}

segs=[
 ("419915","2025-06-01","2025-06-14",GREEN,None),
 ("490249","2025-10-10","2025-10-28",GREEN,None),
 ("439156","2025-06-14","2025-11-30",GREEN,None),
 ("439156","2025-12-01","2025-12-31",RED,None),      # holiday gate-drop (18.6% HI)
 ("439156","2026-01-01","2026-05-13",GREEN,None),     # core window: gated HI, single campaign
 ("613551","2026-05-13","2026-06-30",AMBER,None),
 ("613591","2026-05-13","2026-06-30",AMBER,None),
]
fig,ax=plt.subplots(figsize=(14.5,5.2))
for c,s,e,col,h in segs:
    ax.barh(Y[c], dn(e)-dn(s), left=dn(s), height=0.55, color=col, hatch=h, edgecolor="white", linewidth=0.6, zorder=3)
# labels on flagship
ax.text((dn("2026-01-01")+dn("2026-05-13"))/2, Y["439156"], "gated HI, single campaign", ha="center", va="center", fontsize=7.5, color="white", fontweight="bold")
ax.text((dn("2025-12-01")+dn("2025-12-31"))/2, Y["439156"]+0.36, "Dec holiday\ngate-drop", ha="center", fontsize=6.8, color=RED)
# handoff line
ax.axvline(dn("2026-05-13"), color=NAVY, lw=1.5, ls="--", alpha=0.75, zorder=4)
ax.text(dn("2026-05-13"), len(rows)-0.3, " May 13: flagship OFF -> DMA cells ON", color=NAVY, fontsize=8.5, fontweight="bold", ha="left")
# core-window shading
ax.axvspan(dn("2026-01-01"), dn("2026-05-01"), color=GREEN, alpha=0.05, zorder=0)
ax.text(dn("2026-03-01"), -0.7, "core decline window\n(one campaign, gate held HI, VR still collapsed)", color=GREEN, fontsize=7.5, ha="center")

ax.set_yticks([Y[c] for c in rows]); ax.set_yticklabels([ylab[c] for c in rows], fontsize=8.5)
ax.set_ylim(-1.1, len(rows)-0.2)
ax.xaxis.set_major_formatter(mdates.DateFormatter("%b '%y"))
ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
ax.set_xlim(dn("2025-05-25"), dn("2026-07-05"))
plt.setp(ax.get_xticklabels(), fontsize=8, rotation=0)
for s in ["top","right","left"]: ax.spines[s].set_visible(False)
ax.tick_params(left=False)
leg=[Patch(fc=GREEN,label="HI-gated (80-100% HI)"),Patch(fc=RED,label="gate dropped (Dec holiday)"),Patch(fc=AMBER,label="DMA cells / Fangorn (44-57% HI)")]
ax.legend(handles=leg, frameon=False, ncol=3, fontsize=8, loc="upper center", bbox_to_anchor=(0.5,1.09))
plt.tight_layout(); plt.savefig(D+"artifacts/caraway_gantt.png",dpi=200,bbox_inches="tight")
print("wrote caraway_gantt.png")
