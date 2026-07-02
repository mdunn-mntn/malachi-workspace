"""Module 09 render — reach, frequency & HI recirculation (3 panels).

Panel 1: monthly HI households, NEW (green) vs RETURNING (amber) stacked, + brand-new-share line.
         Rising returning share / falling brand-new share = recirculation.
Panel 2: cumulative distinct HI reached — the pool-coverage curve (plateau = exhausting the pool).
Panel 3: overall reach (distinct IPs) + frequency (imps/IP) — rising frequency = re-serving the same IPs.
HI panels start Jun '25 (scores logged from then); reach/frequency covers the full window.

Reads  outputs/<adv>/09_prospecting_reach_recirculation.csv
Writes outputs/<adv>/09_prospecting_reach_recirculation.png
"""
import argparse
import csv
from datetime import datetime
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
from matplotlib import font_manager

for fam in ["Helvetica Neue", "Helvetica", "Arial"]:
    if any(fam in f.name for f in font_manager.fontManager.ttflist):
        plt.rcParams["font.family"] = fam
        break
plt.rcParams.update({"figure.facecolor": "#FAFAFA", "axes.facecolor": "#FAFAFA",
                     "savefig.facecolor": "#FAFAFA"})
NAVY, GREEN, AMBER, RED, GRAY = "#27496D", "#2E8B57", "#C77B30", "#D63B2F", "#9AA0A6"


def mnum(ym):
    return mdates.date2num(datetime.strptime(ym, "%Y-%m"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default="outputs/kindred_35094/09_prospecting_reach_recirculation.csv")
    ap.add_argument("--out", default="outputs/kindred_35094/09_prospecting_reach_recirculation.png")
    ap.add_argument("--adv", default="Kindred Bravely (35094)")
    ap.add_argument("--p1", nargs=2, default=["2025-01", "2025-05"])
    ap.add_argument("--p2", nargs=2, default=["2026-01", "2026-05"])
    a = ap.parse_args()

    rows = [{k: (v if k == "mo" else int(v)) for k, v in r.items()}
            for r in csv.DictReader(open(a.csv))]
    months = [r["mo"] for r in rows]
    x = [mnum(m) for m in months]
    hi = [r for r in rows if r["hi_reach"] > 0]           # scored months (Jun'25+)
    xh = [mnum(r["mo"]) for r in hi]

    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(13, 12))

    def bands(ax):
        for s, e in (a.p1, a.p2):
            ax.axvspan(mnum(s), mnum(e) + 20, color=NAVY, alpha=0.05, zorder=0)

    # ---- Panel 1: new vs returning HI + brand-new share ----
    bands(ax1)
    new = np.array([r["new_hi"] for r in hi]) / 1e3
    ret = np.array([r["returning_hi"] for r in hi]) / 1e3
    ax1.bar(xh, new, width=22, color=GREEN, label="new HI (first time)", zorder=3)
    ax1.bar(xh, ret, width=22, bottom=new, color=AMBER, label="returning HI (recirculated)", zorder=3)
    ax1.set_ylabel("HI households reached (000s)", fontsize=9, color="#555")
    axb = ax1.twinx()
    bns = [100 * r["new_hi"] / r["hi_reach"] for r in hi]
    axb.plot(xh, bns, color=NAVY, lw=2.2, marker="o", ms=4, zorder=5)
    axb.set_ylim(0, 108)
    axb.set_ylabel("brand-new share (%)", fontsize=9, color=NAVY)
    axb.annotate(f"{bns[0]:.0f}%", (xh[0], bns[0]), textcoords="offset points", xytext=(2, 6),
                 fontsize=8, color=NAVY, fontweight="bold")
    axb.annotate(f"{bns[-1]:.0f}%", (xh[-1], bns[-1]), textcoords="offset points", xytext=(2, 6),
                 fontsize=8, color=NAVY, fontweight="bold")
    ax1.set_title("HI households: new vs returning  (brand-new share falling = recirculation)",
                  fontsize=11, fontweight="bold", loc="left", color="#333")
    ax1.legend(frameon=False, fontsize=8.5, loc="upper left", ncol=2)

    # ---- Panel 2: cumulative HI reach ----
    bands(ax2)
    cum = np.cumsum([r["new_hi"] for r in hi]) / 1e6
    ax2.plot(xh, cum, color=NAVY, lw=2.4, marker="o", ms=4, zorder=4)
    ax2.fill_between(xh, cum, color=NAVY, alpha=0.08)
    ax2.annotate(f"{cum[-1]:.2f}M distinct HI", (xh[-1], cum[-1]), textcoords="offset points",
                 xytext=(-6, 8), ha="right", fontsize=9, color=NAVY, fontweight="bold")
    ax2.set_ylabel("cumulative distinct HI (M)", fontsize=9, color="#555")
    ax2.set_ylim(0, cum[-1] * 1.15)
    ax2.set_title("Cumulative HI reach  (still climbing = pool not yet exhausted)",
                  fontsize=11, fontweight="bold", loc="left", color="#333")

    # ---- Panel 3: reach + frequency (full window) ----
    bands(ax3)
    reach = np.array([r["reach"] for r in rows]) / 1e6
    freq = [r["imps"] / r["reach"] for r in rows]
    ax3.bar(x, reach, width=22, color=GRAY, alpha=0.5, zorder=2, label="reach (distinct IPs)")
    ax3.set_ylabel("reach (M distinct IPs)", fontsize=9, color="#555")
    axf = ax3.twinx()
    axf.plot(x, freq, color=RED, lw=2.2, marker="o", ms=4, zorder=5)
    axf.set_ylabel("frequency (imps / IP)", fontsize=9, color=RED)
    axf.set_ylim(0, max(freq) * 1.3)
    ax3.set_title("Reach & frequency  (frequency ~1.2–1.6: low re-serve per month)",
                  fontsize=11, fontweight="bold", loc="left", color="#333")
    ax3.legend(frameon=False, fontsize=8.5, loc="upper left")

    for ax in (ax1, ax2, ax3):
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b\n'%y"))
        ax.tick_params(axis="x", labelsize=7.5)
        ax.set_xlim(mnum(months[0]) - 18, mnum(months[-1]) + 20)
        for sp in ["top", "right"]:
            ax.spines[sp].set_visible(False)
    fig.suptitle(f"{a.adv} — Prospecting reach, frequency & HI recirculation", fontsize=15,
                 fontweight="bold", color="#222", x=0.01, ha="left", y=0.997)
    plt.tight_layout(rect=[0, 0, 1, 0.985])
    plt.savefig(a.out, dpi=190, bbox_inches="tight")
    print(f"wrote {a.out}")
    print(f"FINDING: brand-new HI share {bns[0]:.0f}% (Jun'25) -> {bns[-1]:.0f}% (May'26); returning "
          f"share rose to {100 - bns[-1]:.0f}%. Cumulative HI reach {cum[-1]:.1f}M and STILL climbing "
          f"(~{[r['new_hi'] for r in hi][-1]/1e3:.0f}k new/mo) -> recirculating but pool not exhausted. "
          f"Frequency ~{np.mean(freq):.1f} imps/IP (low). Need pool size (module 10) for coverage %.")


if __name__ == "__main__":
    main()
