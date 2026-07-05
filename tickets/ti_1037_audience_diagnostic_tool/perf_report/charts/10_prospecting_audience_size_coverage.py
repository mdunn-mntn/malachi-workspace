"""Module 10 render — addressable audience size & HI coverage (2 panels).

Panel 1: monthly addressable prospecting pool (total_audience_size) — the supply size and its trend
         (grew when 3P was added, then contracted). deliverable ≈ pool/5 (UI overstatement).
Panel 2: coverage — cumulative distinct HI reached (from module 09) vs the deliverable pool; the gap
         is remaining fresh-HI headroom, and coverage % rises as the pool contracts + reach accrues.

Reads  outputs/<adv>/10_prospecting_audience_size_coverage.csv   (pool)
       outputs/<adv>/09_prospecting_reach_recirculation.csv       (cumulative HI reach)
Writes outputs/<adv>/10_prospecting_audience_size_coverage.png
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
    return mdates.date2num(datetime.strptime(ym[:7], "%Y-%m"))  # accepts YYYY-MM or YYYY-MM-DD


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pool-csv", default="outputs/kindred_35094/10_prospecting_audience_size_coverage.csv")
    ap.add_argument("--reach-csv", default="outputs/kindred_35094/09_prospecting_reach_recirculation.csv")
    ap.add_argument("--out", default="outputs/kindred_35094/10_prospecting_audience_size_coverage.png")
    ap.add_argument("--adv", default="Kindred Bravely (35094)")
    ap.add_argument("--p1", nargs=2, default=["2025-01", "2025-05"])
    ap.add_argument("--p2", nargs=2, default=["2026-01", "2026-05"])
    a = ap.parse_args()

    pool = list(csv.DictReader(open(a.pool_csv)))
    pm = [p["mo"] for p in pool]
    xa = [mnum(m) for m in pm]
    addr = np.array([float(p["addressable_pool"]) for p in pool]) / 1e6
    deliv = np.array([float(p["deliverable_est"]) for p in pool]) / 1e6

    # cumulative HI reached from module 09 (may be absent / all-zero -> panel 2 degrades)
    cum_by_mo, run = {}, 0
    try:
        reach = list(csv.DictReader(open(a.reach_csv)))
        for r in reach:
            if int(r["hi_reach"]) > 0:
                run += int(r["new_hi"])
                cum_by_mo[r["mo"]] = run / 1e6
    except (FileNotFoundError, KeyError):
        pass
    hm = [m for m in pm if m in cum_by_mo]
    xh = [mnum(m) for m in hm]
    cum = [cum_by_mo[m] for m in hm]
    deliv_hm = [float(next(p for p in pool if p["mo"] == m)["deliverable_est"]) / 1e6 for m in hm]
    have_cov = len(hm) >= 1

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(13, 9))

    def bands(ax):
        for s, e in (a.p1, a.p2):
            ax.axvspan(mnum(s), mnum(e) + 20, color=NAVY, alpha=0.05, zorder=0)

    # ---- Panel 1: addressable pool ----
    bands(ax1)
    ax1.fill_between(xa, addr, color=NAVY, alpha=0.16, zorder=2)
    ax1.plot(xa, addr, color=NAVY, lw=2.4, marker="o", ms=4, zorder=3, label="addressable pool (UI)")
    ax1.plot(xa, deliv, color=NAVY, lw=1.4, ls="--", alpha=0.7, zorder=3, label="deliverable ≈ pool ÷ 5")
    # auto-detect the largest MoM pool jump (>=40% step up) -> likely a 3P/audience-expansion event
    jump_i = None
    if len(addr) >= 2:
        steps = [(addr[i] - addr[i - 1]) / max(addr[i - 1], 1e-9) for i in range(1, len(addr))]
        bi = int(np.argmax(steps)) + 1
        if steps[bi - 1] >= 0.40:
            jump_i = bi
    if jump_i is not None:
        ax1.annotate("audience expansion\n(pool jumps)", (xa[jump_i], addr[jump_i]),
                     textcoords="offset points", xytext=(6, -6), fontsize=8, color=NAVY)
    ax1.annotate(f"{addr[-1]:.0f}M", (xa[-1], addr[-1]), textcoords="offset points", xytext=(4, 4),
                 fontsize=9, color=NAVY, fontweight="bold")
    ax1.set_ylabel("addressable pool (M IPs)", fontsize=9, color="#555")
    ax1.set_ylim(0, max(addr) * 1.15)
    # dynamic contraction/expansion of pool from window peak to last month
    peak = max(addr)
    chg = (addr[-1] - peak) / peak * 100
    trend = f"contracted ~{abs(chg):.0f}% from peak" if chg < -3 else (
        f"expanded ~{chg:.0f}% from trough" if chg > 3 else "roughly flat")
    ax1.set_title(f"Addressable prospecting pool  ({trend} across the window)",
                  fontsize=11, fontweight="bold", loc="left", color="#333")
    ax1.legend(frameon=False, fontsize=8.5, loc="upper right")

    # ---- Panel 2: coverage ----
    bands(ax2)
    cov = None
    if have_cov:
        ax2.plot(xh, deliv_hm, color=NAVY, lw=1.8, ls="--", marker="s", ms=3, label="deliverable HI pool (≈ pool ÷5)")
        ax2.fill_between(xh, cum, color=GREEN, alpha=0.18, zorder=2)
        ax2.plot(xh, cum, color=GREEN, lw=2.4, marker="o", ms=4, zorder=3, label="cumulative distinct HI reached")
        cov = 100 * cum[-1] / deliv_hm[-1]
        ax2.annotate(f"{cum[-1]:.1f}M reached\n≈ {cov:.0f}% of {deliv_hm[-1]:.1f}M deliverable",
                     (xh[-1], cum[-1]), textcoords="offset points", xytext=(-8, 6), ha="right",
                     fontsize=9, color=GREEN, fontweight="bold")
        ax2.set_ylim(0, max(max(cum), max(deliv_hm)) * 1.2)
        ax2.legend(frameon=False, fontsize=8.5, loc="upper left")
    else:
        ax2.text(0.5, 0.5, "No HI (scored) reach data for this window\n(module 09 empty / pre-score era)",
                 transform=ax2.transAxes, ha="center", va="center", fontsize=11, color=GRAY)
    ax2.set_ylabel("HI households (M)", fontsize=9, color="#555")
    ax2.set_title("HI coverage  (cumulative distinct HI reached vs the deliverable pool)",
                  fontsize=11, fontweight="bold", loc="left", color="#333")

    for ax in (ax1, ax2):
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b\n'%y"))
        ax.tick_params(axis="x", labelsize=7.5)
        ax.set_xlim(mnum(pm[0]) - 18, mnum(pm[-1]) + 20)
        for sp in ["top", "right"]:
            ax.spines[sp].set_visible(False)
    fig.suptitle(f"{a.adv} — Prospecting audience size & HI coverage", fontsize=15,
                 fontweight="bold", color="#222", x=0.01, ha="left", y=0.995)
    fig.text(0.01, 0.005, "total_audience_size overstates deliverable by ~5x (data_knowledge); it's the TOTAL "
             f"addressable (keyword+3P), not HI-only, so HI coverage % is approximate. Pool floored {pm[0]}.",
             fontsize=7.5, color="#999")
    plt.tight_layout(rect=[0, 0.02, 1, 0.985])
    plt.savefig(a.out, dpi=190, bbox_inches="tight")
    print(f"wrote {a.out}")
    cov_txt = (f" Cumulative HI reached {cum[-1]:.1f}M ≈ {cov:.0f}% of the ~{deliv_hm[-1]:.1f}M deliverable."
               if have_cov else " (no HI reach data for window.)")
    print(f"FINDING: addressable pool {addr[0]:.0f}M({pm[0]})->{addr[-1]:.0f}M({pm[-1]}); "
          f"{trend} (peak {peak:.0f}M).{cov_txt}")


if __name__ == "__main__":
    main()
