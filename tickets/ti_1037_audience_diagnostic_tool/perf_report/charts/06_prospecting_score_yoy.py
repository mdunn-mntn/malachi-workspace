"""Module 06 render — score-bucket distribution: two-period comparison.

Grouped bars: each MNTN score tier's share of prospecting impressions in an earlier vs later period.
NOTE: true Period 1 (Jan–May '25) has NO scores (household_score logging began 2025-06), so the
"earlier" window defaults to the earliest fully-scored 5 months (Jun–Oct '25) vs Jan–May '26.

Reads  outputs/<adv>/06_prospecting_score_buckets_monthly.csv   (monthly bucket counts)
Writes outputs/<adv>/06_prospecting_score_yoy.png
"""
import argparse
import csv
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from matplotlib import font_manager

for fam in ["Helvetica Neue", "Helvetica", "Arial"]:
    if any(fam in f.name for f in font_manager.fontManager.ttflist):
        plt.rcParams["font.family"] = fam
        break
plt.rcParams.update({"figure.facecolor": "#FAFAFA", "axes.facecolor": "#FAFAFA",
                     "savefig.facecolor": "#FAFAFA"})
NAVY, RED = "#27496D", "#C0392B"
# tier column, display label — no-data first, then worst→best (HI on the right, where the mass is)
TIERS = [("notlogged", "No score data\n(pre-2025-06)"), ("unscored", "Unscored\n(≤0)"),
         ("maxreach", "MaxReach\n(1–3332)"), ("mi", "Mid Intent\n(3333–6665)"),
         ("pp", "Peak Perf\n(6666–8000)"), ("hi", "High Intent\n(8001–10000)")]


def in_range(mo, lo, hi):
    return lo <= mo <= hi


def dist(rows, lo, hi):
    agg = {t: 0 for t, _ in TIERS}
    tot = 0
    for r in rows:
        if in_range(r["month"], lo, hi):
            tot += r["total"]
            for t, _ in TIERS:
                agg[t] += r[t]
    return {t: (100 * agg[t] / tot if tot else 0) for t, _ in TIERS}, tot


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default="outputs/kindred_35094/06_prospecting_score_buckets_monthly.csv")
    ap.add_argument("--out", default="outputs/kindred_35094/06_prospecting_score_yoy.png")
    ap.add_argument("--adv", default="Kindred Bravely (35094)")
    ap.add_argument("--pa", nargs=2, default=["2025-01", "2025-05"])   # standard P1 (no scores for this client)
    ap.add_argument("--pb", nargs=2, default=["2026-01", "2026-05"])   # standard P2
    ap.add_argument("--pa-label", default="Jan–May '25")
    ap.add_argument("--pb-label", default="Jan–May '26")
    a = ap.parse_args()

    rows = [{k: (v if k == "month" else int(v)) for k, v in
             {("month" if k == "mo" else k): v for k, v in r.items()}.items()}
            for r in csv.DictReader(open(a.csv))]

    da, na = dist(rows, a.pa[0], a.pa[1])
    db, nb = dist(rows, a.pb[0], a.pb[1])
    labels = [lab for _, lab in TIERS]
    va = [da[t] for t, _ in TIERS]
    vb = [db[t] for t, _ in TIERS]
    x = np.arange(len(TIERS))
    w = 0.4

    fig, ax = plt.subplots(figsize=(11, 5.6))
    b1 = ax.bar(x - w / 2, va, w, color=NAVY, label=a.pa_label, zorder=3)
    b2 = ax.bar(x + w / 2, vb, w, color=RED, label=a.pb_label, zorder=3)
    for bars, vals in ((b1, va), (b2, vb)):
        for rect, v in zip(bars, vals):
            ax.text(rect.get_x() + rect.get_width() / 2, v + 1.2, f"{v:.1f}%", ha="center",
                    va="bottom", fontsize=9, color="#333", fontweight="bold")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=9.5)
    ax.set_ylim(0, 108)
    ax.set_ylabel("% of prospecting impressions", fontsize=10, color="#555")
    ax.grid(axis="y", color="#DDD", lw=0.5, alpha=0.5)
    for s in ["top", "right"]:
        ax.spines[s].set_visible(False)
    ax.legend(frameon=False, fontsize=10, loc="upper center")
    ax.set_title(f"{a.adv} — Prospecting score distribution: two periods",
                 fontsize=14, fontweight="bold", loc="left", color="#222", pad=10)
    fig.text(0.5, 0.005, "RTC-excluded. For THIS client Period 1 (Jan–May '25) predates score logging "
             "(household_score column began 2025-06), so it reads as 'No score data' — only Period 2 is "
             "measurable. For advertisers scored in both windows this is a true side-by-side.",
             ha="center", fontsize=8, color="#888")
    plt.tight_layout(rect=[0, 0.03, 1, 1])
    plt.savefig(a.out, dpi=200, bbox_inches="tight")
    print(f"wrote {a.out}")
    if da["notlogged"] > 50:
        print(f"FINDING: Period 1 ({a.pa_label}) is {da['notlogged']:.0f}% 'No score data' (pre-2025-06 "
              f"logging) — not comparable. Period 2 ({a.pb_label}): HI {db['hi']:.0f}%, unscored "
              f"{db['unscored']:.1f}%. A true score YoY needs an advertiser scored in both windows.")
    else:
        print(f"FINDING: prospecting HI {da['hi']:.0f}% ({a.pa_label}) → {db['hi']:.0f}% ({a.pb_label}); "
              f"unscored {da['unscored']:.1f}% → {db['unscored']:.1f}%.")


if __name__ == "__main__":
    main()
