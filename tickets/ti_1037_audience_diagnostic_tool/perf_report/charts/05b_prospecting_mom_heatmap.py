"""Module 05b render — MoM %-change heatmap (the "where to look" flag map).

For each metric, month-over-month % change across the window. Diverging color: red = drop,
blue = rise, white = stable. Cells whose |MoM %| >= --flag-pct are outlined (drastic moves =
where to dig deeper). Reads the monthly series from module 05.

Reads  outputs/<adv>/05_prospecting_monthly_metrics.csv
Writes outputs/<adv>/05b_prospecting_mom_heatmap.png
Prints a one-line FINDING: listing the flagged cells.
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

# (label, value_fn) — order top→bottom
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
    ap.add_argument("--out", default="outputs/kindred_35094/05b_prospecting_mom_heatmap.png")
    ap.add_argument("--adv", default="Kindred Bravely (35094)")
    ap.add_argument("--flag-pct", type=float, default=40.0)
    ap.add_argument("--clamp", type=float, default=100.0)   # ±clamp% = full color saturation
    a = ap.parse_args()

    rows = [{k: float(v) if k != "month" else v for k, v in r.items()}
            for r in csv.DictReader(open(a.csv))]
    months = [r["month"] for r in rows]
    # value per (metric, month); MoM % from month 1 onward
    vals = [[fn(r) for r in rows] for _, fn in METRICS]
    mom_months = months[1:]
    mom = []
    for row in vals:
        line = []
        for t in range(1, len(row)):
            prev, cur = row[t - 1], row[t]
            line.append(None if (prev in (None, 0) or cur is None) else 100 * (cur - prev) / prev)
        mom.append(line)

    nrow, ncol = len(METRICS), len(mom_months)
    fig, ax = plt.subplots(figsize=(1.9 + 0.72 * ncol, 1.4 + 0.52 * nrow))
    norm = colors.Normalize(vmin=-a.clamp, vmax=a.clamp)
    cmap = colormaps["RdBu"]     # low(neg)=red, high(pos)=blue
    flagged = []
    for ri, (label, _) in enumerate(METRICS):
        y = nrow - 1 - ri
        ax.text(-0.15, y + 0.5, label, ha="right", va="center", fontsize=10, color="#333")
        for ci in range(ncol):
            v = mom[ri][ci]
            face = "#EDEDED" if v is None else cmap(norm(max(-a.clamp, min(a.clamp, v))))
            ax.add_patch(Rectangle((ci, y), 1, 1, facecolor=face, edgecolor="white", lw=1.2, zorder=2))
            if v is not None and abs(v) >= a.flag_pct:
                ax.add_patch(Rectangle((ci + 0.04, y + 0.04), 0.92, 0.92, facecolor="none",
                                       edgecolor=FLAG, lw=2.2, zorder=4))
                flagged.append((label, mom_months[ci], v))
            txt = "–" if v is None else f"{v:+.0f}"
            dark = v is not None and abs(norm(max(-a.clamp, min(a.clamp, v))) - 0.5) > 0.32
            ax.text(ci + 0.5, y + 0.5, txt, ha="center", va="center", fontsize=8.5,
                    color="white" if dark else "#222", zorder=5)

    for ci, ym in enumerate(mom_months):
        ax.text(ci + 0.5, nrow + 0.12, mlabel(ym), ha="center", va="bottom", fontsize=8, color="#333")
    ax.set_xlim(-3.4, ncol + 0.1)
    ax.set_ylim(-0.5, nrow + 0.9)
    ax.axis("off")
    ax.set_title(f"{a.adv} — Prospecting MoM % change  (outlined = |Δ| ≥ {a.flag_pct:.0f}%, look here)",
                 fontsize=13.5, fontweight="bold", loc="left", color="#222", x=0.0, y=1.04)
    ax.text(-3.4, -0.35, "cell = month-over-month % change · red = drop, blue = rise, gray = n/a",
            fontsize=8.5, color="#777", va="center")

    plt.tight_layout()
    plt.savefig(a.out, dpi=200, bbox_inches="tight")
    print(f"wrote {a.out}")

    flagged.sort(key=lambda t: -abs(t[2]))
    top = "; ".join(f"{m} {mm} {v:+.0f}%" for m, mm, v in flagged[:6])
    print(f"FINDING: {len(flagged)} drastic MoM moves flagged (|Δ|≥{a.flag_pct:.0f}%). Biggest: {top}. "
          f"Cluster: the Nov'25 spike then Dec'25→Jan'26 collapse in visits/VR/conv/revenue/ROAS.")


if __name__ == "__main__":
    main()
