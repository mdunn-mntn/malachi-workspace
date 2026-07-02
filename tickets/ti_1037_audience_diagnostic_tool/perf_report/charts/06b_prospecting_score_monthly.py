"""Module 06b render — monthly score-bucket distribution (100% stacked).

One bar per month, stacked by MNTN score tier (share of prospecting impressions). Shows how delivery
composition shifts month to month — e.g. the Nov–Dec '25 holiday gate-OFF as an unscored spike.

Reads  outputs/<adv>/06_prospecting_score_buckets_monthly.csv
Writes outputs/<adv>/06b_prospecting_score_monthly.png
"""
import argparse
import csv
from datetime import datetime
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
# stack bottom→top: no-data (gray) at bottom, HI (best) next, unscored (worst) at top so gate-off
# spikes read at the top. `notlogged` (pre-2025-06) is a data-availability state, not an intent tier.
STACK = [("notlogged", "No score data (pre-2025-06)", "#C8CCD0"),
         ("hi", "High Intent (8001–10000)", "#1B6B4F"),
         ("pp", "Peak Perf (6666–8000)", "#5FA88A"),
         ("mi", "Mid Intent (3333–6665)", "#C9A227"),
         ("maxreach", "MaxReach (1–3332)", "#D98C4A"),
         ("unscored", "Unscored (≤0)", "#C0392B")]


def mlabel(ym):
    return datetime.strptime(ym, "%Y-%m").strftime("%b\n'%y")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default="outputs/kindred_35094/06_prospecting_score_buckets_monthly.csv")
    ap.add_argument("--out", default="outputs/kindred_35094/06b_prospecting_score_monthly.png")
    ap.add_argument("--adv", default="Kindred Bravely (35094)")
    a = ap.parse_args()

    rows = list(csv.DictReader(open(a.csv)))
    months = [r["mo"] for r in rows]
    tot = [int(r["total"]) for r in rows]
    pct = {t: [100 * int(r[t]) / int(r["total"]) if int(r["total"]) else 0 for r in rows]
           for t, _, _ in STACK}
    x = np.arange(len(months))

    fig, ax = plt.subplots(figsize=(1.2 + 0.78 * len(months), 6.4))
    bottom = np.zeros(len(months))
    for t, lab, color in STACK:
        vals = np.array(pct[t])
        ax.bar(x, vals, 0.82, bottom=bottom, color=color, label=lab, zorder=3,
               edgecolor="white", linewidth=0.4)
        # label a segment when it's big enough to read (dark text on the light-gray no-data band)
        tc = "#555" if t == "notlogged" else "white"
        for xi, (v, b) in enumerate(zip(vals, bottom)):
            if v >= 7:
                ax.text(xi, b + v / 2, f"{v:.0f}", ha="center", va="center", fontsize=7.5,
                        color=tc, fontweight="bold", zorder=5)
        bottom += vals

    ax.set_xticks(x)
    ax.set_xticklabels([mlabel(m) for m in months], fontsize=8)
    ax.set_ylim(0, 100)
    ax.set_ylabel("% of prospecting impressions", fontsize=10, color="#555")
    for s in ["top", "right", "left"]:
        ax.spines[s].set_visible(False)
    ax.tick_params(left=False)
    ax.set_yticks([0, 25, 50, 75, 100])
    ax.set_yticklabels(["0", "25", "50", "75", "100%"], fontsize=8)
    # handles in visual (top→bottom) order for the legend
    h, l = ax.get_legend_handles_labels()
    ax.legend(h[::-1], l[::-1], frameon=False, fontsize=8.5, loc="lower center",
              bbox_to_anchor=(0.5, -0.26), ncol=3)
    ax.set_title(f"{a.adv} — Prospecting score distribution by month",
                 fontsize=14, fontweight="bold", loc="left", color="#222", pad=10)
    plt.tight_layout(rect=[0, 0.1, 1, 1])
    plt.savefig(a.out, dpi=200, bbox_inches="tight")
    print(f"wrote {a.out}")
    worst = max(range(len(months)), key=lambda i: pct["unscored"][i])
    print(f"FINDING: HI dominates most months (~96–100%); unscored spikes to {pct['unscored'][worst]:.0f}% "
          f"in {months[worst]} (holiday gate-OFF), and {pct['unscored'][months.index('2025-11')]:.0f}% in "
          f"2025-11 — the score-level signature of the Dec gate-off (cf. modules 03/03b).")


if __name__ == "__main__":
    main()
