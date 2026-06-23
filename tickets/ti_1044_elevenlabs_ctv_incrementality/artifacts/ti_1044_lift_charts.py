"""TI-1044 ghost-ad lift charts (Tufte). Reads outputs/ JSONs, writes PNGs.
  A) attribution_vs_true: clickpass (attributed) vs guid (total traffic) visit lift — the illusion.
  B) conv_att_vs_itt: conversion lift ATT (served vs ghost, win-selection biased) vs clean ITT.
Numbers come from the canonical holdout-vs-served readout (same method as TI-837 / TI-933).
"""
import json, math
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
    data = json.load(open(f"{BASE}/outputs/{name}"))
    while isinstance(data, list) and len(data) and isinstance(data[0], list):  # unwrap [[...]]
        data = data[0]
    return {r["grp"]: r for r in data}

def lift_ci(num_t,n_t,num_c,n_c):
    p1,p2=num_t/n_t,num_c/n_c; rel=p1/p2-1
    se=math.sqrt((1-p1)/(p1*n_t)+(1-p2)/(p2*n_c))
    lo,hi=math.exp(math.log(p1/p2)-1.96*se)-1, math.exp(math.log(p1/p2)+1.96*se)-1
    pp=(num_t+num_c)/(n_t+n_c); z=(p1-p2)/math.sqrt(pp*(1-pp)*(1/n_t+1/n_c))
    return rel*100, lo*100, hi*100, 2*(1-norm.cdf(abs(z)))

full = load("ti_1044_ghost_lift_full.json")
t,c = full["treated"], full["control"]
nt,nc = int(t["ips"]), int(c["ips"])
cp = lift_ci(int(t["clickpass_visitors"]),nt,int(c["clickpass_visitors"]),nc)
gd = lift_ci(int(t["guid_visitors"]),nt,int(c["guid_visitors"]),nc)
cv = lift_ci(int(t["converters"]),nt,int(c["converters"]),nc)

# ---- Chart A: attribution vs true visit lift ----
fig,ax=plt.subplots(figsize=(8.6,5))
labels=["Attributed visits\n(clickpass)","Total site traffic\n(guid_log)"]
vals=[cp[0],gd[0]]; cols=[GRAY,MINT]
errs=[[cp[0]-cp[1]],[cp[2]-cp[0]]],[[gd[0]-gd[1]],[gd[2]-gd[0]]]
bars=ax.bar(labels,vals,color=cols,width=0.55,zorder=3)
ax.errorbar(labels,vals,yerr=[[cp[0]-cp[1],gd[0]-gd[1]],[cp[2]-cp[0],gd[2]-gd[0]]],
            fmt="none",ecolor="#333",capsize=5,lw=1.2,zorder=4)
for b,v in zip(bars,vals):
    ax.text(b.get_x()+b.get_width()/2, v+ (4 if v>=0 else -10), f"{v:+.0f}%", ha="center",
            va="bottom" if v>=0 else "top", fontsize=14, fontweight="bold", color=b.get_facecolor())
ax.axhline(0,color="#666",lw=1)
ax.yaxis.set_major_formatter(FuncFormatter(lambda y,_:f"{y:.0f}%"))
ax.set_ylabel("Visit-rate lift, served vs held-out (95% CI)")
ax.set_title("The 'visit lift' is attribution, not incrementality.",fontsize=15,fontweight="bold",loc="left",pad=26)
ax.text(0,1.04,"Our own ghost-ad holdout: attributed visits jump; total site traffic barely moves.",
        transform=ax.transAxes,fontsize=10,color="#555")
fig.tight_layout(); fig.savefig(f"{BASE}/artifacts/ti_1044_chart_attribution_vs_true.png"); plt.close(fig)

# ---- Chart B: conversion lift ATT vs ITT ----
out={"att":(cv[0],cv[1],cv[2],cv[3])}
try:
    itt=load("ti_1044_ghost_lift_itt.json"); it,ic=itt["treated"],itt["control"]
    iv=lift_ci(int(it["converters"]),int(it["ips"]),int(ic["converters"]),int(ic["ips"]))
    out["itt"]=iv
except Exception: pass
fig,ax=plt.subplots(figsize=(8.6,5))
keys=[("att","Served vs ghost\n(ATT — win-selection biased)",RED)]
if "itt" in out: keys.append(("itt","Targeted vs ghost\n(ITT — clean, pre-auction)",NAVY))
xs=[k[1] for k in keys]; vs=[out[k[0]][0] for k in keys]
lo=[out[k[0]][0]-out[k[0]][1] for k in keys]; hi=[out[k[0]][2]-out[k[0]][0] for k in keys]
bars=ax.bar(xs,vs,color=[k[2] for k in keys],width=0.5,zorder=3)
ax.errorbar(xs,vs,yerr=[lo,hi],fmt="none",ecolor="#333",capsize=5,lw=1.2,zorder=4)
for b,v,k in zip(bars,vs,keys):
    p=out[k[0]][3]
    ax.text(b.get_x()+b.get_width()/2, v+1.5, f"{v:+.0f}%\np={p:.3f}", ha="center", va="bottom",
            fontsize=12, fontweight="bold", color=b.get_facecolor())
ax.axhline(0,color="#666",lw=1)
ax.yaxis.set_major_formatter(FuncFormatter(lambda y,_:f"{y:.0f}%"))
ax.set_ylabel("Conversion-rate lift (95% CI)")
ax.set_title("Conversion 'lift' shrinks once you remove win-selection.",fontsize=15,fontweight="bold",loc="left",pad=26)
ax.text(0,1.04,"Serving your highest-value (auction-winning) households inflates the raw number.",
        transform=ax.transAxes,fontsize=10,color="#555")
fig.tight_layout(); fig.savefig(f"{BASE}/artifacts/ti_1044_chart_conv_att_vs_itt.png"); plt.close(fig)

print("charts written:")
print(f"  clickpass(attributed) visit lift: {cp[0]:+.0f}%  CI[{cp[1]:+.0f},{cp[2]:+.0f}] p={cp[3]:.4f}")
print(f"  guid(total) visit lift          : {gd[0]:+.0f}%  CI[{gd[1]:+.0f},{gd[2]:+.0f}] p={gd[3]:.4f}")
print(f"  conversion lift (ATT)           : {cv[0]:+.0f}%  CI[{cv[1]:+.0f},{cv[2]:+.0f}] p={cv[3]:.4f}")
if "itt" in out: print(f"  conversion lift (ITT, clean)    : {out['itt'][0]:+.0f}%  CI[{out['itt'][1]:+.0f},{out['itt'][2]:+.0f}] p={out['itt'][3]:.4f}")
