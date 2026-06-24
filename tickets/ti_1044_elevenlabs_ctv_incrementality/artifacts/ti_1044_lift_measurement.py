"""TI-1044 — lift-measurement slide chart: IVR + CVR, clean ITT, with 95% CI + p.
Reads outputs/ti_1044_ghost_lift_itt.json (clean ITT: visitors=clickpass IVR, converters=CVR).
Forest chart + CSV. IVR is well-powered & significant; CVR ≈ 0.
"""
import json, math, csv
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
from scipy.stats import norm

BASE = "/Users/malachi/Developer/work/mntn/workspace/tickets/ti_1044_elevenlabs_ctv_incrementality"
plt.rcParams.update({"font.family":"Helvetica Neue, Helvetica, Arial, sans-serif",
    "figure.facecolor":"#FAFAFA","axes.facecolor":"#FAFAFA","axes.edgecolor":"#888",
    "axes.linewidth":0.8,"savefig.dpi":200,"axes.spines.top":False,"axes.spines.right":False})
NAVY,RED,MINT,GRAY="#1f3a5f","#c0392b","#16a085","#9aa0a6"

def load(name):
    d=json.load(open(f"{BASE}/outputs/{name}"))
    while isinstance(d,list) and d and isinstance(d[0],list): d=d[0]
    return {r["grp"]:r for r in d}

g=load("ti_1044_ghost_lift_itt_full.json"); t,c=g["treated"],g["control"]
n_t,n_c=int(t["ips"]),int(c["ips"])
def stats(key):
    nt,nc=int(t[key]),int(c[key]); p1,p2=nt/n_t,nc/n_c; rel=p1/p2-1
    se=math.sqrt((1-p1)/(p1*n_t)+(1-p2)/(p2*n_c))
    lo,hi=math.exp(math.log(p1/p2)-1.96*se)-1, math.exp(math.log(p1/p2)+1.96*se)-1
    pp=(nt+nc)/(n_t+n_c); z=(p1-p2)/math.sqrt(pp*(1-pp)*(1/n_t+1/n_c)); pv=2*(1-norm.cdf(abs(z)))
    return dict(t=p1*100,c=p2*100,lift=rel*100,lo=lo*100,hi=hi*100,p=pv)

rows=[("Attributed visits\n(clickpass)",stats("clickpass_visitors"),GRAY),
      ("Total site traffic\n(guid — TRUE visit lift)",stats("guid_visitors"),NAVY),
      ("Conversions (CVR)",stats("converters"),RED)]

fig,ax=plt.subplots(figsize=(9.8,4.8))
ys=[2,1,0]
for (lbl,r,col),y in zip(rows,ys):
    ax.plot([r["lo"],r["hi"]],[y,y],color=col,lw=3,zorder=2,solid_capstyle="round")
    ax.scatter([r["lift"]],[y],color=col,s=120,zorder=3)
    pstr = "p<0.001 · significant" if r["p"]<0.001 else f"p={r['p']:.2f} · n.s."
    ax.text(r["lift"], y+0.22, f"{r['lift']:+.0f}%", ha="center", va="bottom",
            fontsize=18, fontweight="bold", color=col)
    ax.text(r["lift"], y-0.26, pstr, ha="center", va="top", fontsize=10, color=col)
ax.axvline(0,color="#444",lw=1.3,ls="--")
ax.set_yticks(ys); ax.set_yticklabels([r[0] for r in rows],fontsize=12)
ax.set_ylim(-0.7,2.7); ax.set_xlim(-35,185)
ax.xaxis.set_major_formatter(FuncFormatter(lambda x,_:f"{x:+.0f}%"))
ax.set_xlabel("Incremental lift, served vs held-out households (clean ITT, 95% CI)")
ax.set_title("Incremental lift ≈ 0 — total visits and conversions.",
             fontsize=15,fontweight="bold",loc="left",pad=30)
ax.text(0,1.06,"Attributed (clickpass) is large only because it requires an impression — a credit metric, not causal. "
        "Holdout · win-selection removed · 6.6M households.",
        transform=ax.transAxes,fontsize=8.6,color="#555")
fig.tight_layout(); fig.savefig(f"{BASE}/artifacts/ti_1044_chart_lift_measurement.png"); plt.close(fig)

with open(f"{BASE}/outputs/ti_1044_lift_measurement.csv","w",newline="") as f:
    w=csv.writer(f); w.writerow(["metric","treated_rate_pct","control_rate_pct","lift_pct","ci_lo_pct","ci_hi_pct","p_value","significant","n_treated","n_control"])
    for lbl,r,_ in rows:
        w.writerow([lbl,round(r["t"],4),round(r["c"],4),round(r["lift"],1),round(r["lo"],1),round(r["hi"],1),
                    ("<0.001" if r["p"]<0.001 else round(r["p"],3)),r["p"]<0.05,n_t,n_c])
print(f"n_treated={n_t:,}  n_control={n_c:,}")
for lbl,r,_ in rows:
    print(f"  {lbl}: {r['lift']:+.0f}%  CI[{r['lo']:+.0f},{r['hi']:+.0f}]  p={r['p']:.4f}  {'SIG' if r['p']<.05 else 'n.s.'}")
print("chart -> artifacts/ti_1044_chart_lift_measurement.png")
