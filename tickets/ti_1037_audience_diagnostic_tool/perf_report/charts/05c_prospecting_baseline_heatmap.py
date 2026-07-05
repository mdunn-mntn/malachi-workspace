"""Module 05c render — %-vs-all-months-baseline heatmap (spike-robust flag map).

Companion to 05b. Instead of month-over-month (which double-counts a spike: a big + is followed by
a phantom - as it reverts), each cell = the month's % deviation from that metric's average across ALL
months. A one-month spike then shows up ONCE (as a spike above baseline); the revert month sits near
baseline instead of being flagged as a drop. Cells with |dev| >= --flag-pct are outlined.

Note: because some metrics trend over the window (e.g. ROAS 16x->2x), deviation-from-mean also shows
the regime (early months above the norm, late below) — that's the trend, not a one-month anomaly. Use
alongside 05b (local change) for the full picture; a trailing-baseline variant is available on request.

Reads  outputs/<adv>/05_prospecting_monthly_metrics.csv
Writes outputs/<adv>/05c_prospecting_baseline_heatmap.png
"""
import argparse
import csv
from datetime import datetime
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager, colormaps, colors
from matplotlib.patches import Rectangle

for fam in ["Helvetica Neue", "Helvetica", "Arial"]:
    if any(fam in f.name for f in font_manager.fontManager.ttflist):
        plt.rcParams["font.family"] = fam
        break
plt.rcParams.update({"figure.facecolor": "#FAFAFA", "axes.facecolor": "#FAFAFA",
                     "savefig.facecolor": "#FAFAFA"})
FLAG = "#111111"

METRICS = [
    ("Spend",            lambda r: r["spend"]),
    ("Impressions",      lambda r: r["impressions"]),
    ("CPM",              lambda r: 1000 * r["spend"] / r["impressions"] if r["impressions"] else None),
    ("Visits",           lambda r: r["visits"]),
    ("Visit rate",       lambda r: 100 * r["visits"] / r["impressions"] if r["impressions"] else None),
    ("Conversions",      lambda r: r["conversions"]),
    ("Conv rate /visit", lambda r: 100 * r["conversions"] / r["visits"] if r["visits"] else None),
    ("Revenue",          lambda r: r["revenue"]),
    ("AOV",              lambda r: r["revenue"] / r["conversions"] if r["conversions"] else None),
    ("ROAS",             lambda r: r["revenue"] / r["spend"] if r["spend"] else None),
]


def mlabel(ym):
    return datetime.strptime(ym, "%Y-%m").strftime("%b\n'%y")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default="outputs/kindred_35094/05_prospecting_monthly_metrics.csv")
    ap.add_argument("--out", default="outputs/kindred_35094/05c_prospecting_baseline_heatmap.png")
    ap.add_argument("--adv", default="Kindred Bravely (35094)")
    ap.add_argument("--flag-pct", type=float, default=40.0)
    ap.add_argument("--clamp", type=float, default=100.0)
    a = ap.parse_args()

    rows = [{k: float(v) if k != "month" else v for k, v in r.items()}
            for r in csv.DictReader(open(a.csv))]
    months = [r["month"] for r in rows]
    vals = [[fn(r) for r in rows] for _, fn in METRICS]
    # baseline = mean across all months; deviation of each month from it
    dev = []
    for row in vals:
        defined = [v for v in row if v is not None]
        base = sum(defined) / len(defined) if defined else None
        dev.append([None if (v is None or not base) else 100 * (v - base) / base for v in row])

    nrow, ncol = len(METRICS), len(months)
    fig, ax = plt.subplots(figsize=(1.9 + 0.72 * ncol, 1.4 + 0.52 * nrow))
    norm = colors.Normalize(vmin=-a.clamp, vmax=a.clamp)
    cmap = colormaps["RdBu"]     # red = below baseline, blue = above baseline
    flagged = []
    for ri, (label, _) in enumerate(METRICS):
        y = nrow - 1 - ri
        ax.text(-0.15, y + 0.5, label, ha="right", va="center", fontsize=10, color="#333")
        for ci in range(ncol):
            v = dev[ri][ci]
            face = "#EDEDED" if v is None else cmap(norm(max(-a.clamp, min(a.clamp, v))))
            ax.add_patch(Rectangle((ci, y), 1, 1, facecolor=face, edgecolor="white", lw=1.2, zorder=2))
            if v is not None and abs(v) >= a.flag_pct:
                ax.add_patch(Rectangle((ci + 0.04, y + 0.04), 0.92, 0.92, facecolor="none",
                                       edgecolor=FLAG, lw=2.2, zorder=4))
                flagged.append((label, months[ci], v))
            txt = "–" if v is None else f"{v:+.0f}"
            dark = v is not None and abs(norm(max(-a.clamp, min(a.clamp, v))) - 0.5) > 0.32
            ax.text(ci + 0.5, y + 0.5, txt, ha="center", va="center", fontsize=8.5,
                    color="white" if dark else "#222", zorder=5)

    for ci, ym in enumerate(months):
        ax.text(ci + 0.5, nrow + 0.12, mlabel(ym), ha="center", va="bottom", fontsize=8, color="#333")
    ax.set_xlim(-3.4, ncol + 0.1)
    ax.set_ylim(-0.5, nrow + 0.9)
    ax.axis("off")
    ax.set_title(f"{a.adv} — Prospecting: each month vs its all-months average  "
                 f"(outlined = |Δ| ≥ {a.flag_pct:.0f}%)",
                 fontsize=13.5, fontweight="bold", loc="left", color="#222", x=0.0, y=1.04)
    ax.text(-3.4, -0.35, f"cell = % above/below the metric's {ncol}-month mean · red = below, blue = above · "
            "spike-robust (no phantom revert flag)", fontsize=8.5, color="#777", va="center")

    plt.tight_layout()
    plt.savefig(a.out, dpi=200, bbox_inches="tight")
    print(f"wrote {a.out}")

    flagged.sort(key=lambda t: -abs(t[2]))
    top = "; ".join(f"{m} {mm} {v:+.0f}%" for m, mm, v in flagged[:6])
    print(f"FINDING: vs all-months average — {len(flagged)} month-cells beyond ±{a.flag_pct:.0f}%. "
          f"Biggest: {top}. Spike-robust: a one-off spike shows once, its revert month sits near baseline.")


if __name__ == "__main__":
    main()
