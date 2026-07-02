"""AUDI-1070 Kindred Bravely (35094). Gate-removal/thrash + mix-shift; over-scaling DISPROVEN.
Even at +65% spend, within-HI VR held (~0.7-1.1%, r positive) => NOT saturation. ROAS -81% lens-invariant.
Charts: yoy (all metrics), not_saturation (decisive), gate (monthly HI-share), lens.
Data: outputs/diag_kindred/*.csv."""
import csv, datetime as dt
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from matplotlib import font_manager
for fam in ["Helvetica Neue","Helvetica","Arial"]:
    if any(fam in f.name for f in font_manager.fontManager.ttflist):
        plt.rcParams["font.family"]=fam; break
plt.rcParams.update({"figure.facecolor":"#FAFAFA","axes.facecolor":"#FAFAFA","savefig.facecolor":"#FAFAFA"})
D="tickets/audi_1070_yoy_decline_caraway_avon_hexclad/"
OUT=D+"outputs/diag_kindred/"; ART=D+"artifacts/"
NAVY,RED,GREEN,GRAY,AMBER="#27496D","#D63B2F","#2E8B57","#9AA0A6","#C77B30"
def mlabel(mo):
    y,m=mo.split("-"); return dt.date(int(y),int(m),1).strftime("%b'%y")
def fnan(s):
    try: return float(s)
    except (ValueError,TypeError): return np.nan

# ---------- 1) YoY all metrics ----------
rows=[r for r in csv.DictReader(open(OUT+"b_yoy_pct_all_metrics.csv"))]
order=["spend","imps","visits","visit_rate_pct","conv_rate_pct","cpm","aov","roas"]
lab={"spend":"Spend","imps":"Impressions","visits":"Visits","visit_rate_pct":"Visit rate","conv_rate_pct":"Conv rate","cpm":"CPM","aov":"AOV","roas":"ROAS"}
d={r["metric"]:float(r["yoy_pct"]) for r in rows}
vals=[d[m] for m in order]; labs=[lab[m] for m in order]
cols=[GREEN if v>0 else RED for v in vals]
y=np.arange(len(order))[::-1]
fig,ax=plt.subplots(figsize=(11,5.8))
ax.barh(y,vals,color=cols,zorder=3,height=0.62)
for yi,v in zip(y,vals):
    ax.text(v+(1.5 if v>=0 else -1.5), yi, f"{v:+.0f}%", va="center", ha="left" if v>=0 else "right", fontsize=9, fontweight="bold", color="#333")
ax.axvline(0,color="#333",lw=0.8)
ax.set_yticks(y); ax.set_yticklabels(labs,fontsize=10)
ax.set_xlim(-95,85); ax.set_xlabel("YoY % change (Jan-May 2025 -> 2026)")
for s in ["top","right","left"]: ax.spines[s].set_visible(False)
ax.tick_params(left=False)
ax.set_title("Spend UP +65%, results DOWN — an efficiency collapse",fontsize=13.5,fontweight="bold",color=NAVY,loc="left",pad=10)
ax.text(0,-0.13,"Kindred scaled spend +65% but visits fell −47%, visit rate −63%, and ROAS −81% (9.76 -> 1.81). AOV is flat (−2%) — so it's not a revenue-mix story.",transform=ax.transAxes,fontsize=8.5,color="#666")
plt.tight_layout(); plt.savefig(ART+"kindred_yoy.png",dpi=200,bbox_inches="tight"); print("kindred_yoy.png")

# ---------- 2) NOT SATURATION (decisive): within-HI VR flat vs spend rising ----------
whi=[r for r in csv.DictReader(open(OUT+"d_within_hi_vr_monthly.csv")) if r["mo"]!="NOTE" and r.get("within_hi_vr_pct","")]
spend_by_mo={r["mo"]:fnan(r["spend"]) for r in csv.DictReader(open(OUT+"a_monthly_continuous.csv"))}
mo=[r["mo"] for r in whi]; lbl=[mlabel(m) for m in mo]
vr=[float(r["within_hi_vr_pct"]) for r in whi]
sp=[spend_by_mo.get(m,np.nan)/1000 for m in mo]
floor=[r["below_100k_floor"]=="TRUE" for r in whi]
xx=np.arange(len(mo))
fig,ax=plt.subplots(figsize=(13,5.8))
ax.bar(xx,sp,width=0.6,color=AMBER,alpha=0.35,zorder=1,label="prospecting spend ($k)")
ax.set_ylabel("monthly prospecting spend ($k)",color=AMBER); ax.set_ylim(0,max(x for x in sp if not np.isnan(x))*1.25)
ax2=ax.twinx()
ax2.plot(xx,vr,color=GREEN,lw=2.6,marker="o",ms=6,zorder=5)
for i,(v,fl) in enumerate(zip(vr,floor)):
    ax2.annotate(f"{v:.2f}"+("*" if fl else ""),(i,v),xytext=(0,9),textcoords="offset points",ha="center",fontsize=7.8,color=GREEN,fontweight="bold")
ax2.set_ylabel("within-HI visit rate (%)",color=GREEN); ax2.set_ylim(0,2.4)
ax2.axhspan(0.6,1.2,color=GREEN,alpha=0.06,zorder=0)
ax.set_xticks(xx); ax.set_xticklabels(lbl,fontsize=8.5,rotation=45,ha="right")
for s in ["top"]: ax.spines[s].set_visible(False); ax2.spines[s].set_visible(False)
ax2.annotate("within-HI visit rate HOLDS ~0.7-1.1%\neven as spend climbs — the HI pool is NOT exhausted\n(Pearson r(spend, VR) POSITIVE)",xy=(11,0.85),xytext=(4.2,1.9),fontsize=9,color=GREEN,ha="center",fontweight="bold",arrowprops=dict(arrowstyle="->",color=GREEN,lw=1.3))
ax.set_title("Over-scaling DISPROVEN — spend rose, but the HI pool held",fontsize=13.5,fontweight="bold",color=NAVY,loc="left",pad=10)
ax.text(0,-0.16,"If +65% spend had drained a finite HI pool (Caraway), within-HI visit rate would FALL with spend. It doesn't — the highest-spend months are among the HIGHEST within-HI rates. So the fix is the gate, not capping spend. (* = below 100k-imp floor; scores start mid-May-2025)",transform=ax.transAxes,fontsize=8.2,color="#666")
plt.tight_layout(); plt.savefig(ART+"kindred_not_saturation.png",dpi=200,bbox_inches="tight"); print("kindred_not_saturation.png")

# ---------- 3) GATE: monthly HI-share with holiday + thrash ----------
sd=[r for r in csv.DictReader(open(OUT+"d_monthly_score_dist.csv"))]
mo=[mlabel(r["mo"]) for r in sd]; hi=[float(r["pct_HI"]) for r in sd]; uns=[float(r["pct_unscored"]) for r in sd]
xx=np.arange(len(mo))
cols=[GREEN if h>=50 else (AMBER if h>=25 else RED) for h in hi]
fig,ax=plt.subplots(figsize=(13,5.6))
ax.bar(xx,hi,width=0.62,color=cols,zorder=3)
for i,h in enumerate(hi): ax.text(i,h+1.5,f"{h:.0f}",ha="center",fontsize=8,fontweight="bold",color="#333")
ax.set_ylabel("HI-share of prospecting delivery (%)"); ax.set_ylim(0,72)
ax.set_xticks(xx); ax.set_xticklabels(mo,fontsize=8.5,rotation=45,ha="right")
for s in ["top","right"]: ax.spines[s].set_visible(False)
def find(m):
    return [r["mo"] for r in sd].index(m)
ax.annotate("Nov 19: gate REMOVED\nfor the holidays\nHI 96% -> 25% -> Dec 2.6%",xy=(find("2025-11"),25.1),xytext=(find("2025-08")+0.2,52),fontsize=8.6,color=RED,ha="center",fontweight="bold",arrowprops=dict(arrowstyle="->",color=RED,lw=1.3))
ax.annotate("Feb: DAILY thrash\n(10000<->0<->3334)",xy=(find("2026-02"),48.8),xytext=(find("2026-03")+0.4,64),fontsize=8.4,color=AMBER,ha="center",arrowprops=dict(arrowstyle="->",color=AMBER,lw=1.1))
ax.set_title("The holiday gate-removal — then a February thrash",fontsize=13.5,fontweight="bold",color=NAVY,loc="left",pad=10)
ax.text(0,-0.17,"Gate held ~55% HI Jun-Oct, dropped to 0/−1 Nov 19 (HI -> 2.6% by Dec), re-gated Jan 6, then oscillated 10000<->0<->Mid-Intent daily in Feb and loosened through May. ~45% unscored even in 'good' months = the mix that drags blended ROAS.",transform=ax.transAxes,fontsize=8.2,color="#666")
plt.tight_layout(); plt.savefig(ART+"kindred_gate.png",dpi=200,bbox_inches="tight"); print("kindred_gate.png")

# ---------- 4) LENS-INVARIANT ROAS ----------
fig,ax=plt.subplots(figsize=(8.5,5.4))
grp=["Last-touch","Last-touch\n+ competing (UI lens)"]
y25=[9.76,19.13]; y26=[1.81,3.53]
x=np.arange(2); w=0.36
ax.bar(x-w/2,y25,w,color=GRAY,label="Jan-May 2025",zorder=3)
ax.bar(x+w/2,y26,w,color=RED,label="Jan-May 2026",zorder=3)
for xi,a,b in zip(x,y25,y26):
    ax.text(xi-w/2,a+0.4,f"{a:.1f}",ha="center",fontsize=9,fontweight="bold",color="#555")
    ax.text(xi+w/2,b+0.4,f"{b:.1f}",ha="center",fontsize=9,fontweight="bold",color=RED)
    ax.text(xi,max(a,b)+2.0,"−81%",ha="center",fontsize=11,fontweight="bold",color=RED)
ax.set_xticks(x); ax.set_xticklabels(grp,fontsize=9.5)
ax.set_ylabel("Prospecting ROAS"); ax.set_ylim(0,23)
ax.legend(frameon=False,fontsize=9,loc="upper right")
for s in ["top","right"]: ax.spines[s].set_visible(False)
ax.set_title("The drop is real — not an attribution artifact",fontsize=13,fontweight="bold",color=NAVY,loc="left",pad=10)
ax.text(0,-0.14,"ROAS falls −81% under BOTH lenses (plain last-touch AND the client's last-touch+competing view). Not a reporting-lens or window trick.",transform=ax.transAxes,fontsize=8.5,color="#666")
plt.tight_layout(); plt.savefig(ART+"kindred_lens.png",dpi=200,bbox_inches="tight"); print("kindred_lens.png")
