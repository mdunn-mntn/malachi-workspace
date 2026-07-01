"""AUDI-1070 Avon (31921) — campaign HHST gate over time, like Caraway/HexClad.
Avon's prospecting = ONE gated campaign (259556 Beeswax TV Prospecting); MT2/MT3/Ego/retargeting carry NO gate.
Story: gate held 6666/10000 (Jun-Nov 18) -> DROPPED to 0/-1 Nov 19 (holiday max-reach) -> RE-GATED 10000 Jan 6
(HexClad never did) -> recovered -> Fangorn-era continuous (9501/9401) from mid-May. That re-gate = why Avon is healthy.
Data: outputs/avon_gate_history_daily.csv (archive), outputs/diag_avon/03_gate_timeline_daily.csv (delivery HI-share),
outputs/diag_avon/01_campaign_census.csv (campaign run-times)."""
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

# ---------- load daily gate history (flagship 259556) ----------
gate=[(r["d"], int(r["threshold"])) for r in csv.DictReader(open(D+"outputs/avon_gate_history_daily.csv"))]
gate.sort()
gx=[dn(d) for d,_ in gate]
# clamp -1 (no gate) to 0 for plotting
gy=[max(v,0) for _,v in gate]

# ---------- load daily delivery HI-share (flagship) ----------
hi=[(r["d"], float(r["pct_hi"])) for r in csv.DictReader(open(D+"outputs/diag_avon/03_gate_timeline_daily.csv"))]
hi.sort()
hx=[dn(d) for d,_ in hi]; hy=[v for _,v in hi]

# ---------- flagship gate-regime segments (for the gantt bar) ----------
# derived from the change-points: gated (>0 & bucketed) green; no-gate red; Fangorn-continuous amber
FLAG_SEGS=[
 ("2025-06-01","2025-11-19",GREEN),   # gated 6666/10000
 ("2025-11-19","2026-01-06",RED),     # gate dropped to 0/-1 (holiday)
 ("2026-01-06","2026-05-19",GREEN),   # RE-GATED 10000
 ("2026-05-19","2026-06-30",AMBER),   # Fangorn-era continuous (9501/9401)
]

# ---------- campaign run-times (census) ----------
rows_c=list(csv.DictReader(open(D+"outputs/diag_avon/01_campaign_census.csv")))
def span(cid):
    r=[x for x in rows_c if x["campaign_id"]==cid][0]
    return r["first_day"], r["last_day"]

# gantt rows (top panel): flagship + the other prospecting campaigns + collapsed retargeting
gantt=[
 ("259556","Beeswax TV Prospecting  ·  S1  ·  $132K","FLAG"),
 ("330397","Multi-Touch  ·  S2  ·  $15K","PLAIN"),
 ("330396","Multi-Touch – Plus  ·  S3  ·  $8K","PLAIN"),
 ("259557","Beeswax TV Prospecting – Ego  ·  $0","PLAIN"),
 ("RETGT","6× TV Retargeting  ·  obj=4  ·  $56K","PLAIN"),
]
Yg={cid:len(gantt)-1-i for i,(cid,_,_) in enumerate(gantt)}

# ============================ FIGURE ============================
fig,(axT,axB)=plt.subplots(2,1,figsize=(14.5,8.2),height_ratios=[1.05,1.9],sharex=True)

# ---------- TOP: campaign gantt ----------
for cid,lab,kind in gantt:
    y=Yg[cid]
    if kind=="FLAG":
        for s,e,col in FLAG_SEGS:
            axT.barh(y, dn(e)-dn(s), left=dn(s), height=0.6, color=col, edgecolor="white", linewidth=0.7, zorder=3)
    else:
        if cid=="RETGT": s,e=("2025-06-01","2026-06-30")
        else: s,e=span(cid)
        axT.barh(y, dn(e)-dn(s), left=dn(s), height=0.6, color=GRAY, edgecolor="white", linewidth=0.7, zorder=3)
        axT.text(dn(e)+3, y, "no HHST gate", va="center", fontsize=7.3, color="#8A9099", style="italic")
axT.text(dn("2025-06-05"), Yg["259556"]+0.42, "the ONLY gated campaign  ->  gate story below", fontsize=8, color=NAVY, fontweight="bold")
axT.set_yticks([Yg[c] for c,_,_ in gantt]); axT.set_yticklabels([lab for _,lab,_ in gantt], fontsize=8.3)
axT.set_ylim(-0.7,len(gantt)-0.25)
for sp in ["top","right","left"]: axT.spines[sp].set_visible(False)
axT.tick_params(left=False)
axT.set_title("Avon (31921) — campaigns & the HHST intent gate over time",
              fontsize=13.5, fontweight="bold", color=NAVY, loc="left", pad=10)

# ---------- BOTTOM: gate step-line + delivery HI-share ----------
# delivery HI-share on secondary axis (context, light)
axB2=axB.twinx()
axB2.fill_between(hx, hy, 0, color=NAVY, alpha=0.04, zorder=1)
axB2.plot(hx, hy, color=NAVY, lw=1.7, alpha=0.85, zorder=6)
axB2.set_ylim(0,108); axB2.set_ylabel("HI-share of delivery (%)", color=NAVY, fontsize=9)
axB2.tick_params(axis="y", labelcolor=NAVY, labelsize=8)
axB2.set_yticks([0,25,50,75,100])
axB2.text(dn("2025-06-18"), 90, "delivery HI-share (navy) tracks the gate", color=NAVY, fontsize=8, alpha=0.9)

# gate step line, colored by regime
def seg_color(v): return RED if v<=0 else (AMBER if v not in (6666,10000) else GREEN)
axB.step(gx, gy, where="post", color="#555", lw=0.8, alpha=0.35, zorder=3)  # thin connective
# colored regime overlays: plot step per regime slice
pts=list(zip(gx,gy))
for s,e,col in FLAG_SEGS:
    sub=[(x,y) for x,y in pts if dn(s)<=x<=dn(e)]
    if sub:
        axB.step([p[0] for p in sub],[p[1] for p in sub], where="post", color=col, lw=3.0, zorder=5, solid_capstyle="butt")

axB.set_ylim(-500,11200); axB.set_ylabel("HHST gate (min household score to serve)", fontsize=9.5)
axB.set_yticks([0,3333,6666,8000,10000])
axB.set_yticklabels(["0  (no gate)","3333  Mid","6666  HI+Peak","8000  Peak","10000  HI-only"], fontsize=8)
axB.axhline(0, color=GRAY, lw=0.8, ls=":", zorder=1)

# annotations
def ann(x,y,txt,color,tx,ty,ha="center"):
    axB.annotate(txt, xy=(dn(x),y), xytext=(dn(tx),ty), fontsize=8.6, color=color, ha=ha, fontweight="bold",
                 arrowprops=dict(arrowstyle="->",color=color,lw=1.3))
ann("2025-08-01",6666,"Jun–Nov 18: gated 6666->10000\n(HI + Peak, ~97–100% HI delivery)",GREEN,"2025-07-05",3050)
ann("2025-11-19",0,"Nov 19: gate REMOVED -> 0/−1\nholiday max-reach (HI-share -> 8%)",RED,"2025-09-20",1250,"center")
ann("2026-01-06",10000,"Jan 6: RE-GATED -> 10,000\n(HexClad never did -> why Avon recovered)",GREEN,"2026-02-05",4600,"left")
ann("2026-05-25",9501,"mid-May+: Fangorn/RTC onset\n(continuous gate 9501/9401;\nHI-share metric shifts — beyond Jan–May window)",AMBER,"2026-03-25",7600,"center")
# holiday no-gate shading
axB.axvspan(dn("2025-11-19"), dn("2026-01-06"), color=RED, alpha=0.05, zorder=0)

axB.xaxis.set_major_formatter(mdates.DateFormatter("%b '%y"))
axB.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
axB.set_xlim(dn("2025-05-25"), dn("2026-07-08"))
plt.setp(axB.get_xticklabels(), fontsize=8, rotation=0)
for sp in ["top"]: axB.spines[sp].set_visible(False)
axB2.spines["top"].set_visible(False)

leg=[Patch(fc=GREEN,label="gated (6666 / 10000 — HI)"),Patch(fc=RED,label="no gate (0 / −1 — holiday)"),
     Patch(fc=AMBER,label="Fangorn continuous gate"),Patch(fc=GRAY,label="campaign w/ no HHST gate")]
axT.legend(handles=leg, frameon=False, ncol=4, fontsize=8, loc="upper center", bbox_to_anchor=(0.5,1.17))

plt.tight_layout()
plt.savefig(D+"artifacts/avon_gate_timeline.png",dpi=200,bbox_inches="tight")
print("wrote avon_gate_timeline.png")
