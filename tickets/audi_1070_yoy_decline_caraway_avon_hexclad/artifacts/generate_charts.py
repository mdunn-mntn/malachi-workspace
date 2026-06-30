"""AUDI-1070 charts (Tufte: max data-ink, direct labels, finding-as-title).
Reads outputs/*.csv (bq_run.sh format, footer-tolerant)."""
import numpy as np, pandas as pd
from io import StringIO
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager

for fam in ["Helvetica Neue", "Helvetica", "Arial"]:
    if any(fam in f.name for f in font_manager.fontManager.ttflist):
        plt.rcParams["font.family"] = fam; break
plt.rcParams.update({"figure.facecolor": "#FAFAFA", "axes.facecolor": "#FAFAFA",
                     "savefig.facecolor": "#FAFAFA", "axes.edgecolor": "#888",
                     "axes.grid": False, "font.size": 12})
DIR = "tickets/audi_1070_yoy_decline_caraway_avon_hexclad/"
RED, NAVY, GRAY, GREEN = "#D1495B", "#27496D", "#9AA0A6", "#2E8B57"

def load(path):
    raw = open(path).read().splitlines()
    s = next(i for i, l in enumerate(raw) if l.startswith("advertiser_id"))
    rows = []
    for l in raw[s:]:
        if l.strip() == "" or l.startswith(("---", "Waiting", "Bytes")): break
        rows.append(l)
    df = pd.read_csv(StringIO("\n".join(rows)))
    return df.apply(pd.to_numeric, errors="ignore")

# ---- Chart 1: per-AID YoY deltas (spend / VR / ROAS) ----
y = load(DIR + "outputs/q0_yoy_febmay.csv")
piv = y.pivot(index="advertiser_id", columns="period")
names = {31921: "Avon\n(flat spend)", 34611: "HexClad\n(+38% spend)", 40341: "Caraway\n(+119% spend)"}
order = [31921, 34611, 40341]
def delta(aid, col):
    a = piv.loc[aid, (col, "2025_FebMay")]; b = piv.loc[aid, (col, "2026_FebMay")]
    return (b / a - 1) * 100
metrics = ["spend", "VR", "ROAS"]
vals = {m: [delta(a, {"spend": "total_spend", "VR": "vr_pct", "ROAS": "roas"}[m]) for a in order] for m in metrics}
fig, ax = plt.subplots(figsize=(9, 5.2))
x = np.arange(len(order)); w = 0.26
for i, (m, c) in enumerate(zip(metrics, [NAVY, RED, "#8338EC"])):
    bars = ax.bar(x + (i - 1) * w, vals[m], w, label=m, color=c)
    for b in bars:
        h = b.get_height()
        ax.text(b.get_x() + b.get_width() / 2, h + (3 if h >= 0 else -3), f"{h:+.0f}%",
                ha="center", va="bottom" if h >= 0 else "top", fontsize=10, fontweight="bold")
ax.axhline(0, color="#444", lw=1)
ax.set_xticks(x); ax.set_xticklabels([names[a] for a in order])
ax.set_ylabel("YoY change (Feb–May 2026 vs 2025)")
ax.set_title("The decline scales with spend growth — flat-spend Avon didn't decline",
             fontsize=13.5, fontweight="bold", loc="left", y=1.12)
ax.text(0, 1.025, "Visit-rate & ROAS fall in proportion to how hard each advertiser scaled spend",
        transform=ax.transAxes, color=GRAY, fontsize=10.5)
ax.legend(frameon=False, loc="upper left", fontsize=11, handlelength=1.1)
ax.set_ylim(min(min(vals[m]) for m in metrics) * 1.18, max(max(vals[m]) for m in metrics) * 1.30)
for s in ["top", "right"]: ax.spines[s].set_visible(False)
plt.tight_layout(); plt.savefig(DIR + "artifacts/audi_1070_chart_per_aid_yoy.png", dpi=200, bbox_inches="tight"); plt.close()

# ---- Chart 2: cohort saturation gradient + the 3 AIDs ----
c = load(DIR + "outputs/q5_cohort.csv").apply(pd.to_numeric, errors="coerce").dropna(subset=["s25","s26","i25","i26","v25","v26"])
c["sg"] = c.s26 / c.s25
c["vr_ratio"] = (c.v26/c.i26)/(c.v25/c.i25)
c = c[(c.v25>0)&(c.v26>0)].replace([np.inf,-np.inf],np.nan).dropna(subset=["vr_ratio","sg"])
c["dec"] = pd.qcut(c.sg, 10, labels=False, duplicates="drop")
g = c.groupby("dec").agg(sg=("sg","median"), vr=("vr_ratio","median"))
fig, ax = plt.subplots(figsize=(9, 5.4))
ax.axhline(1.0, color=GRAY, lw=1, ls="--")
ax.plot(g["sg"], g["vr"], "-o", color=NAVY, lw=2.4, ms=7, label="cohort median (n=294)")
ax.text(g["sg"].iloc[0], g["vr"].iloc[0]+.04, "cut spend:\nVR rises", color=GREEN, fontsize=10, fontweight="bold")
ax.text(g["sg"].iloc[-1], g["vr"].iloc[-1]-.10, "grow spend:\nVR falls", color=RED, fontsize=10, fontweight="bold", ha="right")
labels = {40341:"Caraway", 34611:"HexClad", 31921:"Avon"}
for aid,nm in labels.items():
    r = c[c.advertiser_id==aid]
    if len(r):
        r=r.iloc[0]; ax.scatter(r["sg"], r["vr_ratio"], s=130, color=RED, zorder=5, edgecolor="white", lw=1.5)
        ax.annotate(f"{nm}\n(×{r['sg']:.1f} spend, VR ×{r['vr_ratio']:.2f})", (r["sg"], r["vr_ratio"]),
                    textcoords="offset points", xytext=(8,8), fontsize=9.5, fontweight="bold")
ax.set_xscale("log"); ax.set_xticks([0.3,0.5,1,2,4]); ax.set_xticklabels(["0.3×","0.5×","1×","2×","4×"])
ax.set_xlabel("YoY spend growth (log)"); ax.set_ylabel("YoY visit-rate ratio (1.0 = no change)")
ax.set_title("Visit-rate decline is spend-driven saturation, not Matched degradation",
             fontsize=13.5, fontweight="bold", loc="left", y=1.12)
ax.text(0,1.025,"Flat-spend advertisers' VR ROSE; only spend-growers declined; systemic MM degradation falsified",
        transform=ax.transAxes, color=GRAY, fontsize=10.5)
for s in ["top","right"]: ax.spines[s].set_visible(False)
plt.tight_layout(); plt.savefig(DIR+"artifacts/audi_1070_chart_saturation_gradient.png", dpi=200); plt.close()

# ---- Chart 3: reach expansion vs visits-per-user (the mechanism) ----
fig, axes = plt.subplots(1, 3, figsize=(11, 4.4), sharey=False)
reach = {31921:(2.36,1.68), 34611:(13.2,15.7), 40341:(4.43,10.06)}     # M unique users 25,26
vpu   = {31921:(0.192,0.213), 34611:(0.0273,0.0168), 40341:(0.0139,0.0044)}
titles = {31921:"Avon (flat)", 34611:"HexClad (+38%)", 40341:"Caraway (+119%)"}
for ax, aid in zip(axes, [31921,34611,40341]):
    r0,r1 = reach[aid]; v0,v1 = vpu[aid]
    ax2 = ax.twinx()
    ax.bar([0,1],[r0,r1], color=NAVY, width=.55, alpha=.85)
    ax2.plot([0,1],[v0,v1], "-o", color=RED, lw=2.5, ms=8)
    for i,val in enumerate([r0,r1]): ax.text(i, val, f"{val:.1f}M", ha="center", va="bottom", fontsize=9, color=NAVY, fontweight="bold")
    for i,val in enumerate([v0,v1]): ax2.text(i, val, f"{val:.3f}", ha="center", va="bottom", fontsize=9, color=RED, fontweight="bold")
    ax.set_xticks([0,1]); ax.set_xticklabels(["2025","2026"]); ax.set_title(titles[aid], fontsize=11, fontweight="bold")
    ax.set_ylim(0, max(r0,r1)*1.25); ax2.set_ylim(0, max(v0,v1)*1.3)
    for s in ["top"]: ax.spines[s].set_visible(False); ax2.spines[s].set_visible(False)
    ax.tick_params(axis="y", colors=NAVY); ax2.tick_params(axis="y", colors=RED)
axes[0].set_ylabel("unique users reached (M)", color=NAVY)
axes[-1].text(1.18, 0.5, "visits per user", color=RED, rotation=90, transform=axes[-1].transAxes, va="center")
fig.suptitle("The mechanism: scaling reached MORE users (navy) but each was far less likely to visit (red)",
             fontsize=13, fontweight="bold", x=0.02, ha="left")
plt.tight_layout(rect=[0,0,1,0.95]); plt.savefig(DIR+"artifacts/audi_1070_chart_reach_expansion.png", dpi=200); plt.close()
print("wrote 3 charts to artifacts/")
