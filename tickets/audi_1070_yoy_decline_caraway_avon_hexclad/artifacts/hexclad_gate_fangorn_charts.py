"""AUDI-1070 HexClad: (1) daily HHST gate event-study — delivery HI-share snaps overnight
with every gate flip (proves steep drop-offs are config events, not gradual decline);
(2) Fangorn rule-out — 0% continuous scores through May 2026, flips June 4-5 (after the window)."""
import csv, datetime as dt
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

# ================= Chart 1: daily gate event-study =================
rows=list(csv.DictReader(open(D+"outputs/hexclad_daily_hishare_2026.csv")))
d=[dt.date.fromisoformat(r["d"]) for r in rows]
hi=[float(r["pct_10000"]) for r in rows]
imps=[int(r["imps"])/1000 for r in rows]  # thousands

fig,ax=plt.subplots(figsize=(13,5.6))
# shade HI-only windows (gate=10000)
for a,b in [("2026-01-06","2026-02-05"),("2026-02-27","2026-03-06")]:
    ax.axvspan(dt.date.fromisoformat(a),dt.date.fromisoformat(b),color=GREEN,alpha=0.07,zorder=0)
ax.plot(d,hi,color=NAVY,lw=2.1,zorder=3)
ax.fill_between(d,hi,color=NAVY,alpha=0.06,zorder=1)

events=[("2026-01-05","gate to 10,000\n(HI-only)",GREEN,1),
        ("2026-02-05","gate to 0\n(no gate)",RED,-1),
        ("2026-02-26","gate to 10,000",GREEN,1),
        ("2026-03-06","gate to 0",RED,-1)]
for ds,lab,col,side in events:
    x=dt.date.fromisoformat(ds)
    ax.axvline(x,color=col,lw=1.3,ls="--",zorder=2)
    ax.annotate(lab,xy=(x,50),xytext=(x,88 if side>0 else 40),
                fontsize=9,color=col,fontweight="bold",ha="center",
                bbox=dict(boxstyle="round,pad=0.25",fc="white",ec=col,lw=0.8))
ax.text(dt.date.fromisoformat("2026-01-20"),104,"HI-only delivery",color=GREEN,fontsize=9,ha="center",fontweight="bold")
ax.text(dt.date.fromisoformat("2026-02-14"),4,"gate open: 12% HI, 57% unscored",color=RED,fontsize=9,ha="center",fontweight="bold")
ax.set_ylim(-3,112); ax.set_ylabel("% of prospecting impressions that are High-Intent (score = 10,000)")
ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %-d"))
ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))
for s in ["top","right"]: ax.spines[s].set_visible(False)
plt.tight_layout(); plt.savefig(D+"artifacts/audi_1070_hexclad_gate_eventstudy.png",dpi=200,bbox_inches="tight")
print("wrote gate_eventstudy.png"); plt.close()

# ================= Chart 2: Fangorn rule-out =================
fr=list(csv.DictReader(open(D+"outputs/hexclad_fangorn_detector_monthly.csv")))
mo=[r["mo"] for r in fr]
cont=[float(r["pct_fangorn_high_8001_9999"]) for r in fr]
x=range(len(mo))
fig,ax=plt.subplots(figsize=(12,4.8))
cols=[GRAY]*len(mo); cols[-1]=RED  # June 2026 = Fangorn on
bars=ax.bar(x,cont,color=cols,width=0.66,zorder=3)
for i,v in enumerate(cont):
    ax.text(i,v+0.8,("0%" if v==0 else f"{v:.0f}%"),ha="center",fontsize=9,
            color=(RED if i==len(mo)-1 else "#888"),fontweight=("bold" if i==len(mo)-1 else "normal"))
ax.axvspan(-0.5,11.5,color=GREEN,alpha=0.06,zorder=0)
ax.text(5.5,32,"AUDI-1070 window: Jun 2025 – May 2026\n0.0% continuous scores every month = 100% bucketed, NOT Fangorn",
        color=GREEN,fontsize=9.5,ha="center",fontweight="bold")
ax.annotate("HexClad migrated to\nFangorn Jun 4–5, 2026",xy=(12,38.3),xytext=(9.7,44),
            fontsize=9.5,color=RED,ha="center",fontweight="bold",
            arrowprops=dict(arrowstyle="->",color=RED,lw=1.3))
ax.set_xticks(list(x)); ax.set_xticklabels([m[2:] if m.startswith("2025") else m[2:] for m in mo],fontsize=8.5,rotation=0)
ax.set_xticklabels(mo,fontsize=8,rotation=35,ha="right")
ax.set_ylim(0,50); ax.set_ylabel("% impressions with continuous (Fangorn) High score 8001–9999")
for s in ["top","right"]: ax.spines[s].set_visible(False)
plt.tight_layout(); plt.savefig(D+"artifacts/audi_1070_hexclad_fangorn.png",dpi=200,bbox_inches="tight")
print("wrote fangorn.png")
