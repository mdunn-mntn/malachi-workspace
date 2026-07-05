"""Module 05 render — monthly metric trends (small multiples).

One panel per metric, monthly value over the continuous window (own y-scale, since levels differ by
orders of magnitude — cleaner than a single log overlay). P1/P2 comparison bands shaded. Companion to
05b's MoM heatmap: the heatmap says WHERE a drastic move happened, these lines show the trajectory.

Reads  outputs/<adv>/05_prospecting_monthly_metrics.csv
Writes outputs/<adv>/05_prospecting_monthly_lines.png
"""
import argparse
import csv
from datetime import datetime
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib import font_manager

for fam in ["Helvetica Neue", "Helvetica", "Arial"]:
    if any(fam in f.name for f in font_manager.fontManager.ttflist):
        plt.rcParams["font.family"] = fam
        break
plt.rcParams.update({"figure.facecolor": "#FAFAFA", "axes.facecolor": "#FAFAFA",
                     "savefig.facecolor": "#FAFAFA"})
NAVY, RED = "#27496D", "#D63B2F"


def _k(v):   # compact number for y-ticks / annotations
    a = abs(v)
    if a >= 1e6:
        return f"{v/1e6:.1f}M"
    if a >= 1e3:
        return f"{v/1e3:.0f}k"
    return f"{v:.0f}"


def _sd(a, b):  # safe divide — NaN when the denominator is 0 (intermittent months plot as a gap)
    return a / b if b else float("nan")


def _f(fmt, v):  # safe format — em-dash for NaN/None
    return "—" if (v is None or v != v) else fmt(v)


# (label, value_fn, latest-value formatter)
METRICS = [
    ("Spend",            lambda r: r["spend"],                                    lambda v: f"${_k(v)}"),
    ("Impressions",      lambda r: r["impressions"],                              lambda v: _k(v)),
    ("CPM",              lambda r: _sd(1000 * r["spend"], r["impressions"]),      lambda v: f"${v:.0f}"),
    ("Visits",           lambda r: r["visits"],                                   lambda v: _k(v)),
    ("Visit rate %",     lambda r: _sd(100 * r["visits"], r["impressions"]),      lambda v: f"{v:.2f}%"),
    ("Conversions",      lambda r: r["conversions"],                              lambda v: _k(v)),
    ("Conv rate /visit %", lambda r: _sd(100 * r["conversions"], r["visits"]),    lambda v: f"{v:.1f}%"),
    ("Revenue",          lambda r: r["revenue"],                                  lambda v: f"${_k(v)}"),
    ("AOV",              lambda r: _sd(r["revenue"], r["conversions"]),           lambda v: f"${v:.0f}"),
    ("ROAS",             lambda r: _sd(r["revenue"], r["spend"]),                 lambda v: f"{v:.1f}×"),
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default="outputs/kindred_35094/05_prospecting_monthly_metrics.csv")
    ap.add_argument("--out", default="outputs/kindred_35094/05_prospecting_monthly_lines.png")
    ap.add_argument("--adv", default="Kindred Bravely (35094)")
    ap.add_argument("--p1", nargs=2, default=["2025-01", "2025-05"])
    ap.add_argument("--p2", nargs=2, default=["2026-01", "2026-05"])
    a = ap.parse_args()

    rows = [{k: float(v) if k != "month" else v for k, v in r.items()}
            for r in csv.DictReader(open(a.csv))]
    x = [mdates.date2num(datetime.strptime(r["month"], "%Y-%m")) for r in rows]

    def _month(s):   # accept YYYY-MM or YYYY-MM-DD (period tokens are full dates)
        return datetime.strptime(s[:7], "%Y-%m")

    def band(p):
        return (mdates.date2num(_month(p[0])),
                mdates.date2num(_month(p[1])) + 20)

    fig, axes = plt.subplots(5, 2, figsize=(14, 12.5), sharex=True)
    axes = axes.ravel()
    for i, (label, fn, fmt) in enumerate(METRICS):
        ax = axes[i]
        y = [fn(r) for r in rows]
        for p in (a.p1, a.p2):
            s, e = band(p)
            ax.axvspan(s, e, color=NAVY, alpha=0.05, zorder=0)
        ax.plot(x, y, color=NAVY, lw=2, marker="o", ms=4, zorder=3)
        ax.plot(x[-1], y[-1], marker="o", ms=6, color=RED, zorder=4)
        ax.annotate(_f(fmt, y[-1]), (x[-1], y[-1]), textcoords="offset points", xytext=(4, 4),
                    fontsize=8.5, color=RED, fontweight="bold")
        ax.annotate(_f(fmt, y[0]), (x[0], y[0]), textcoords="offset points", xytext=(2, 4),
                    fontsize=8, color="#888")
        ax.set_title(label, fontsize=11, fontweight="bold", loc="left", color="#333", pad=4)
        ax.margins(y=0.18)
        for sp in ["top", "right"]:
            ax.spines[sp].set_visible(False)
        ax.tick_params(labelsize=7.5)
        ax.grid(axis="y", color="#DDD", lw=0.5, alpha=0.5)

    for ax in axes[-2:]:
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b '%y"))
        plt.setp(ax.get_xticklabels(), rotation=0, fontsize=8)
    fig.suptitle(f"{a.adv} — Prospecting monthly metric trends", fontsize=15,
                 fontweight="bold", color="#222", x=0.01, ha="left", y=0.995)
    plt.tight_layout(rect=[0, 0, 1, 0.985])
    plt.savefig(a.out, dpi=190, bbox_inches="tight")
    print(f"wrote {a.out}")


if __name__ == "__main__":
    main()
