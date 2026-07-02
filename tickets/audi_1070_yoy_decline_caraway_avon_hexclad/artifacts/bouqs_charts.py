"""AUDI-1070 The Bouqs (32147) — eCommerce Unit. Gate-removal/thrash (HexClad family), NOT over-scaling.
Decisive: within-HI VR ROSE (0.30->2.40) while HI-share COLLAPSED (55->4) => delivery left a HEALTHY pool.
Charts: yoy (prospecting vs AID-wide), pool_health (the decisive one), monthly_continuous.
Data: outputs/diag_bouqs/*.csv."""
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
OUT=D+"outputs/diag_bouqs/"; ART=D+"artifacts/"
NAVY,RED,GREEN,GRAY,AMBER="#27496D","#D63B2F","#2E8B57","#9AA0A6","#C77B30"
def mlabel(mo):
    y,m=mo.split("-"); return dt.date(int(y),int(m),1).strftime("%b'%y")

# ---------- 1) YoY: prospecting vs AID-wide ----------
yoy={ (r["scope"],r["metric"]):float(r["yoy_pct"]) for r in csv.DictReader(open(OUT+"extra_b_yoy_pct_change.csv")) }
mets=[("visits","Visits"),("visit_rate_pct","Visit rate"),("roas","ROAS"),("spend","Spend")]
prosp=[yoy[("prospecting_obj156",m)] for m,_ in mets]
aid=[yoy[("aid_wide_all_obj",m)] for m,_ in mets]
x=np.arange(len(mets)); w=0.38
fig,ax=plt.subplots(figsize=(11,5.6))
b1=ax.bar(x-w/2,prosp,w,color=RED,label="Prospecting (obj 1,5,6)",zorder=3)
b2=ax.bar(x+w/2,aid,w,color=GRAY,label="AID-wide (all campaigns)",zorder=3)
for bars in (b1,b2):
    for b in bars:
        v=b.get_height()
        ax.text(b.get_x()+b.get_width()/2, v+(1.5 if v>=0 else -3.5), f"{v:+.0f}%", ha="center", fontsize=9, fontweight="bold", color="#333")
ax.axhline(0,color="#333",lw=0.8)
ax.set_xticks(x); ax.set_xticklabels([l for _,l in mets],fontsize=10)
ax.set_ylabel("YoY % change (Jan–May 2025 -> 2026)")
ax.set_ylim(-65,25)
ax.legend(frameon=False,fontsize=9,loc="lower left")
for s in ["top","right"]: ax.spines[s].set_visible(False)
ax.set_title("The decline lives in PROSPECTING, not the account",fontsize=13,fontweight="bold",color=NAVY,loc="left",pad=10)
ax.text(0,-0.16,"Prospecting visits −55% on only −20% spend -> the visit rate itself collapsed. AID-wide is −14% because retargeting held.",transform=ax.transAxes,fontsize=8.5,color="#666")
plt.tight_layout(); plt.savefig(ART+"bouqs_yoy.png",dpi=200,bbox_inches="tight"); print("bouqs_yoy.png")

# ---------- 2) POOL HEALTH (decisive): HI-share collapse + within-HI VR rising ----------
rows=[r for r in csv.DictReader(open(OUT+"extra_d_score_dist_within_hi_vr.csv"))]
mo=[mlabel(r["mo"]) for r in rows]
hishare=[float(r["pct_HI"]) for r in rows]
whi=[float(r["within_HI_vr_pct"]) for r in rows]
xx=np.arange(len(mo))
fig,ax=plt.subplots(figsize=(13,5.8))
ax.bar(xx,hishare,width=0.6,color=GRAY,alpha=0.55,zorder=2,label="HI-share of delivery (%)")
for i,h in enumerate(hishare): ax.text(i,h+1.2,f"{h:.0f}",ha="center",fontsize=7.6,color="#777")
ax.set_ylabel("HI-share of delivery (%)",color="#777"); ax.set_ylim(0,90)
ax2=ax.twinx()
ax2.plot(xx,whi,color=GREEN,lw=2.6,marker="o",ms=6,zorder=5,label="within-HI visit rate (%)")
for i,v in enumerate(whi): ax2.annotate(f"{v:.2f}",(i,v),xytext=(0,9),textcoords="offset points",ha="center",fontsize=7.8,color=GREEN,fontweight="bold")
ax2.set_ylabel("within-HI visit rate (%)",color=GREEN); ax2.set_ylim(0,3.0)
ax.set_xticks(xx); ax.set_xticklabels(mo,fontsize=8.5,rotation=0)
for s in ["top"]: ax.spines[s].set_visible(False); ax2.spines[s].set_visible(False)
ax.annotate("HI-share COLLAPSES\n55% -> 4%",xy=(9.5,10),xytext=(6.6,55),fontsize=9,color="#555",ha="center",fontweight="bold",arrowprops=dict(arrowstyle="->",color="#888",lw=1.2))
ax2.annotate("but within-HI visit rate RISES\n0.30% -> 2.40% — the served-HI pool got BETTER",xy=(10,2.40),xytext=(3.4,2.55),fontsize=9,color=GREEN,ha="center",fontweight="bold",arrowprops=dict(arrowstyle="->",color=GREEN,lw=1.3))
ax.set_title("Not over-scaling — delivery LEFT a healthy pool",fontsize=13.5,fontweight="bold",color=NAVY,loc="left",pad=10)
ax.text(0,-0.15,"If the HI audience were exhausted, within-HI visit rate would FALL (Caraway). It ROSE. The households that clear the gate convert better than ever — the problem is the gate stopped asking for them.  (Jun'25->May'26; scores start mid-May-2025)",transform=ax.transAxes,fontsize=8.2,color="#666")
plt.tight_layout(); plt.savefig(ART+"bouqs_pool_health.png",dpi=200,bbox_inches="tight"); print("bouqs_pool_health.png")

# ---------- 3) Monthly continuous VR + ROAS ----------
rows=[r for r in csv.DictReader(open(OUT+"extra_a_monthly_continuous.csv"))]
def fnan(s):
    try: return float(s)
    except (ValueError, TypeError): return np.nan
mo=[mlabel(r["mo"]) for r in rows]
vr=[fnan(r["visit_rate_pct"]) for r in rows]; roas=[fnan(r["roas"]) for r in rows]
xx=np.arange(len(mo))
fig,ax=plt.subplots(figsize=(14,5.4))
ax.plot(xx,vr,color=NAVY,lw=2.1,marker="o",ms=4,label="visit rate (%)",zorder=4)
ax2=ax.twinx()
ax2.plot(xx,roas,color=AMBER,lw=1.9,marker="s",ms=3.5,label="ROAS",zorder=3)
ax.set_ylabel("visit rate (%)",color=NAVY); ax2.set_ylabel("ROAS",color=AMBER)
ax.set_xticks(xx[::2]); ax.set_xticklabels([mo[i] for i in range(0,len(mo),2)],fontsize=7.5,rotation=45,ha="right")
for s in ["top"]: ax.spines[s].set_visible(False); ax2.spines[s].set_visible(False)
# annotate the grind + Dec pause + Mar CPM spike
try:
    di=[r["mo"] for r in rows].index("2025-12"); ax.axvline(di,color=RED,lw=1,ls=":",alpha=0.6); ax.text(di,max(vr)*0.95,"Dec pause",fontsize=7.5,color=RED,ha="center")
except ValueError: pass
ax.set_title("Monthly continuous — a steady prospecting grind, punctuated by config events",fontsize=12.5,fontweight="bold",color=NAVY,loc="left",pad=10)
ax.text(0,-0.22,"Prospecting visit rate grinds 0.67%->0.23% and ROAS 2–3× -> ~1× over the full period; Dec-2025 full-account pause; Mar-2026 CPM spike ($19.62) on a clean re-gate (real HI-inventory cost).",transform=ax.transAxes,fontsize=8.2,color="#666")
ax.legend(frameon=False,fontsize=8.5,loc="upper right")
plt.tight_layout(); plt.savefig(ART+"bouqs_monthly.png",dpi=200,bbox_inches="tight"); print("bouqs_monthly.png")
