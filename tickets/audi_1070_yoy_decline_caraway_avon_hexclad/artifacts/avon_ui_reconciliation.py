"""AUDI-1070 — MNTN Reporting UI confirms Avon improved (Jan-May 2025 vs 2026).
Numbers transcribed from the Advertiser Reporting UI screenshots (advertiser 31921).
Volume fell with the -12.5% budget; every efficiency metric got better."""
import numpy as np
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager
for fam in ["Helvetica Neue", "Helvetica", "Arial"]:
    if any(fam in f.name for f in font_manager.fontManager.ttflist):
        plt.rcParams["font.family"] = fam; break
plt.rcParams.update({"figure.facecolor": "#FAFAFA", "axes.facecolor": "#FAFAFA", "savefig.facecolor": "#FAFAFA"})
D = "tickets/audi_1070_yoy_decline_caraway_avon_hexclad/"
GRAY, GREEN, RED, NAVY = "#9AA0A6", "#2E8B57", "#D63B2F", "#27496D"

# (label, 2025, 2026, group, better_is)  group: vol / perf
M = [
 ("Spend", 73077.81, 63966.61, "vol", "?"),
 ("Households", 2677801, 2001098, "vol", "?"),
 ("Impressions", 6151381, 5151784, "vol", "?"),
 ("Verified visits", 692888, 598436, "vol", "?"),
 ("Conv. rate", 4.42, 5.26, "perf", "up"),
 ("CPA", 2.39, 2.03, "perf", "down"),   # lower = better
 ("ROAS", 22.12, 26.36, "perf", "up"),
]
fig, ax = plt.subplots(figsize=(12, 5.6))
xs = np.arange(len(M))
ax.axhline(100, color="#444", lw=1)
for i, (lab, a, b, grp, better) in enumerate(M):
    idx = b / a * 100
    chg = (b / a - 1) * 100
    # "good" = ROAS/CVR up, CPA down. volume = neutral/gray.
    good = (grp == "perf") and ((better == "up" and chg > 0) or (better == "down" and chg < 0))
    col = GREEN if good else GRAY
    ax.bar(i, idx, 0.62, color=col)
    tag = f"{chg:+.0f}%"
    if lab == "CPA": tag += "\n(cheaper)"
    ax.text(i, idx + 2.5, tag, ha="center", fontsize=11, fontweight="bold", color=col if good else "#555")
ax.set_xticks(xs); ax.set_xticklabels([m[0] for m in M], fontsize=10.5)
ax.axvspan(-0.5, 3.5, color="#9AA0A6", alpha=0.06)
ax.axvspan(3.5, 6.5, color="#2E8B57", alpha=0.06)
ax.text(1.5, 132, "VOLUME (tracks the −12.5% budget)", ha="center", fontsize=10, fontweight="bold", color="#666")
ax.text(5.0, 132, "PERFORMANCE (all better)", ha="center", fontsize=10, fontweight="bold", color=GREEN)
ax.set_ylim(0, 140); ax.set_ylabel("2026 vs 2025 (2025 = 100)")
ax.set_title("MNTN's own Reporting UI confirms Avon improved — ROAS +19%, conv-rate +19%, CPA −15%",
             fontsize=13.5, fontweight="bold", loc="left", y=1.07)
ax.text(0, 1.015, "Avon Advertiser Reporting UI, Jan–May 2025 vs 2026. Spend & impressions match our BigQuery to the dollar. "
        "Volume fell with the budget; every efficiency metric got better.", transform=ax.transAxes, color="#666", fontsize=9.3)
for s in ["top", "right"]: ax.spines[s].set_visible(False)
plt.tight_layout(); plt.savefig(D + "artifacts/audi_1070_avon_ui_reconciliation.png", dpi=200, bbox_inches="tight")
print("wrote avon_ui_reconciliation.png")
print("ROAS methods YoY:  mean-of-monthly(FT chart) 8.94->8.74 (-2%) | UI aggregate 22.12->26.36 (+19%) | our last-touch aggregate 17.3->20.7 (+19%)")
