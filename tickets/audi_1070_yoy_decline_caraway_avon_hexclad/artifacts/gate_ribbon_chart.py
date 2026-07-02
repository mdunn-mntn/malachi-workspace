"""AUDI-1070 — per-campaign gate ribbon for The Bouqs (32147) + Kindred Bravely (35094).
Each ACTIVE prospecting campaign (obj=1, gateable) = a row; color shows the HHST gate state each day:
green = gated HI/Peak (>=6600), amber = mid/continuous (1-6599), red = NO gate (<=0). MT2/MT3 (obj 5/6) are
unscored BY DESIGN (no real gate) — noted, not drawn as 'off'. Data: outputs/diag_<slug>/percamp_gate_history.csv + 01_campaign_census.csv."""
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
def state_color(v):
    return RED if v<=0 else (GREEN if v>=6600 else AMBER)

def build(slug, title, min_imps=0.2):
    OUT=D+f"outputs/diag_{slug}/"
    # census: campaign_id -> (group_name, imps_M, obj)
    cen={}
    for r in csv.DictReader(open(OUT+"01_campaign_census.csv")):
        cen[r["campaign_id"]]=(r.get("group_name",""), float(r.get("imps_M",0) or 0), r.get("obj",""))
    # per-campaign daily gate
    rows=list(csv.DictReader(open(OUT+"percamp_gate_history.csv")))
    bycid={}
    for r in rows:
        bycid.setdefault(r["campaign_id"],[]).append((r["d"], int(r["threshold"]), r["objective_id"]))
    # keep obj=1 (gateable) campaigns with meaningful activity
    camps=[]
    for cid,evs in bycid.items():
        obj=evs[0][2]
        if obj!="1": continue
        imps=cen.get(cid,("",0,""))[1]
        if imps < min_imps and len(evs) < 25: continue   # drop tiny noise campaigns
        evs.sort()
        camps.append((cid,evs,imps))
    # sort by first date
    camps.sort(key=lambda c: c[1][0][0])
    n=len(camps)
    fig,ax=plt.subplots(figsize=(15, max(3.2, 0.52*n+2.2)))
    ylabels=[]
    for i,(cid,evs,imps) in enumerate(camps):
        y=n-1-i
        # forward-fill daily state across the record span, draw contiguous runs
        days=[e[0] for e in evs]; thr={e[0]:e[1] for e in evs}
        d0=dt.date.fromisoformat(days[0]); d1=dt.date.fromisoformat(days[-1])
        cur=None; run_start=None; last=None
        d=d0
        segs=[]
        while d<=d1:
            ds=d.isoformat()
            if ds in thr: last=thr[ds]
            c=state_color(last) if last is not None else GRAY
            if c!=cur:
                if cur is not None: segs.append((run_start,d,cur))
                cur=c; run_start=d
            d+=dt.timedelta(days=1)
        if cur is not None: segs.append((run_start,d1+dt.timedelta(days=1),cur))
        for s,e,c in segs:
            ax.barh(y, dn(e.isoformat())-dn(s.isoformat()), left=dn(s.isoformat()), height=0.62, color=c, edgecolor="white", linewidth=0.3, zorder=3)
        grp=cen.get(cid,("",0,""))[0]
        gl=(grp[:26]+"…") if len(grp)>27 else grp
        ylabels.append(f"{cid}  {gl}" if gl else cid)
    ax.set_yticks(range(n)); ax.set_yticklabels(ylabels[::-1], fontsize=7.8)
    ax.set_ylim(-0.6, n-0.4)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b '%y"))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
    ax.set_xlim(dn("2025-01-01"), dn("2026-06-05"))
    plt.setp(ax.get_xticklabels(), fontsize=8, rotation=0)
    for s in ["top","right","left"]: ax.spines[s].set_visible(False)
    ax.tick_params(left=False)
    # holiday marker
    ax.axvspan(dn("2025-11-19"), dn("2026-01-06"), color=RED, alpha=0.05, zorder=0)
    ax.text(dn("2025-12-12"), n-0.35, "holiday", fontsize=8, color=RED, ha="center", alpha=0.8)
    leg=[Patch(fc=GREEN,label="gated HI/Peak (>=6600)"),Patch(fc=AMBER,label="mid / continuous (1-6599)"),Patch(fc=RED,label="NO gate (<=0)")]
    ax.set_title(title, fontsize=13.5, fontweight="bold", color=NAVY, loc="left", pad=10)
    fig.text(0.5, 0.055, "Each row = an obj=1 stage-1 prospecting campaign; color = its HHST gate that day. Multi-Touch (obj 5/6) companions are unscored BY DESIGN (no gate) and omitted.", ha="center", fontsize=8.2, color="#666")
    fig.legend(handles=leg, frameon=False, ncol=3, fontsize=9, loc="lower center", bbox_to_anchor=(0.5, 0.0))
    plt.tight_layout(rect=[0, 0.085, 1, 0.96])
    out=D+f"artifacts/{slug}_gate_ribbon.png"
    plt.savefig(out,dpi=190,bbox_inches="tight"); plt.close(); print("wrote",out.split('/')[-1],f"({n} campaigns)")

build("bouqs","The Bouqs (32147) — prospecting campaigns & their intent gate (on/off) over time")
build("kindred","Kindred Bravely (35094) — prospecting campaigns & their intent gate (on/off) over time")
