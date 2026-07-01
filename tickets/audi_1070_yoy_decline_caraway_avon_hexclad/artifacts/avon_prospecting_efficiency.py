"""AUDI-1070 — Avon prospecting (all stages, first-touch) Jan-May 2025 vs 2026:
volume fell with the -18% budget; every efficiency metric improved."""
import numpy as np
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager
for fam in ["Helvetica Neue", "Helvetica", "Arial"]:
    if any(fam in f.name for f in font_manager.fontManager.ttflist):
        plt.rcParams["font.family"] = fam; break
plt.rcParams.update({"figure.facecolor": "#FAFAFA", "axes.facecolor": "#FAFAFA", "savefig.facecolor": "#FAFAFA"})
D = "tickets/audi_1070_yoy_decline_caraway_avon_hexclad/"
GRAY, GREEN = "#9AA0A6", "#2E8B57"
# (label, 2025, 2026, group, better) group vol/perf
M = [
 ("Spend", 56833, 46615, "vol", "?"),
 ("Impressions", 4479077, 3344501, "vol", "?"),
 ("Households", 1826270, 1144625, "vol", "?"),
 ("Verified visits", 272218, 187200, "vol", "?"),
 ("Conv. rate", 3.85, 5.02, "perf", "up"),
 ("Visit rate", 14.91, 16.35, "perf", "up"),
 ("CPA", 5.43, 4.96, "perf", "down"),
 ("ROAS", 9.39, 10.37, "perf", "up"),
]
fig, ax = plt.subplots(figsize=(12.5, 5.8))
ax.axhline(100, color="#444", lw=1)
for i,(lab,a,b,grp,better) in enumerate(M):
    idx=b/a*100; chg=(b/a-1)*100
    good=(grp=="perf") and ((better=="up" and chg>0) or (better=="down" and chg<0))
    col=GREEN if good else GRAY
    ax.bar(i, idx, 0.62, color=col)
    tag=f"{chg:+.0f}%"+("\n(cheaper)" if lab=="CPA" else "")
    ax.text(i, idx+2.5, tag, ha="center", fontsize=11, fontweight="bold", color=col if good else "#555")
ax.set_xticks(range(len(M))); ax.set_xticklabels([m[0] for m in M], fontsize=10.5)
ax.axvspan(-0.5,3.5,color="#9AA0A6",alpha=0.06); ax.axvspan(3.5,7.5,color="#2E8B57",alpha=0.06)
ax.text(1.5,138,"VOLUME (tracks the −18% budget)",ha="center",fontsize=10,fontweight="bold",color="#666")
ax.text(5.5,138,"EFFICIENCY (all better)",ha="center",fontsize=10,fontweight="bold",color=GREEN)
ax.set_ylim(0,146); ax.set_ylabel("2026 vs 2025 (2025 = 100)")
ax.set_title("Avon prospecting: −18% spend, but every efficiency metric improved",
             fontsize=13.5, fontweight="bold", loc="left", y=1.07)
ax.text(0,1.015,"First-touch (industry_standard), Jan–May 2025 vs 2026, all prospecting stages. Fewer dollars at a higher CPM "
        "cut volume; ROAS, visit rate, conversion rate and CPA all improved.", transform=ax.transAxes, color="#666", fontsize=9.3)
for s in ["top","right"]: ax.spines[s].set_visible(False)
plt.tight_layout(); plt.savefig(D+"artifacts/audi_1070_avon_prospecting_efficiency.png", dpi=200, bbox_inches="tight")
print("wrote avon_prospecting_efficiency.png")
