"""Module 04 render — Prospecting P1-vs-P2 metrics table (+ %Δ).

Reads the two raw period-sum rows, derives every metric (spend, imps, CPM, visits, visit rate,
conversions, conv rate, revenue, AOV, ROAS) and the %Δ, and renders a clean 3-value-column table
(Period 1 | Period 2 | Δ%). %Δ is colored by whether the move is GOOD for that metric
(green good / red bad / gray = context metric). Emits both a PNG (for the report) and a markdown table.

Reads  outputs/<adv>/04_prospecting_yoy_metrics.csv   (2 rows: P1, P2 raw sums)
Writes outputs/<adv>/04_prospecting_yoy_metrics.png
       outputs/<adv>/04_prospecting_yoy_metrics.md
Prints a one-line FINDING: for the assembled report.
"""
import argparse
import csv
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager

for fam in ["Helvetica Neue", "Helvetica", "Arial"]:
    if any(fam in f.name for f in font_manager.fontManager.ttflist):
        plt.rcParams["font.family"] = fam
        break
plt.rcParams.update({"figure.facecolor": "#FAFAFA", "axes.facecolor": "#FAFAFA",
                     "savefig.facecolor": "#FAFAFA"})
NAVY, GREEN, RED, GRAY = "#27496D", "#2E8B57", "#D63B2F", "#666666"

M = lambda v: f"${v:,.0f}"
MM = lambda v: f"{v/1e6:.2f}M"
CNT = lambda v: f"{v:,.0f}"
P3 = lambda v: f"{v:.3f}%"
P2 = lambda v: f"{v:.2f}%"
M2 = lambda v: f"${v:,.2f}"
X = lambda v: f"{v:.2f}×"

# (label, formatter, good_direction, value_fn)  good: +1 up-is-good, -1 down-is-good, 0 context
METRICS = [
    ("Spend",           M,   0, lambda r: r["spend"]),
    ("Impressions",     MM,  0, lambda r: r["impressions"]),
    ("CPM",             M2, -1, lambda r: 1000 * r["spend"] / r["impressions"]),
    ("Visits",          CNT, 1, lambda r: r["visits"]),
    ("Visit rate",      P3,  1, lambda r: 100 * r["visits"] / r["impressions"]),
    ("Conversions",     CNT, 1, lambda r: r["conversions"]),
    ("Conv rate /visit", P2, 1, lambda r: 100 * r["conversions"] / r["visits"]),
    ("Revenue",         M,   1, lambda r: r["revenue"]),
    ("AOV",             M2,  1, lambda r: r["revenue"] / r["conversions"]),
    ("ROAS",            X,   1, lambda r: r["revenue"] / r["spend"]),
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default="outputs/kindred_35094/04_prospecting_yoy_metrics.csv")
    ap.add_argument("--out", default="outputs/kindred_35094/04_prospecting_yoy_metrics.png")
    ap.add_argument("--md",  default="outputs/kindred_35094/04_prospecting_yoy_metrics.md")
    ap.add_argument("--adv", default="Kindred Bravely (35094)")
    ap.add_argument("--p1-label", default="Jan–May '25")
    ap.add_argument("--p2-label", default="Jan–May '26")
    a = ap.parse_args()

    raw = {r["period"]: {k: float(v) for k, v in r.items() if k != "period"}
           for r in csv.DictReader(open(a.csv))}
    p1, p2 = raw["P1"], raw["P2"]

    table = []  # (label, fmt(v1), fmt(v2), delta_str, color)
    for label, fmt, good, fn in METRICS:
        v1, v2 = fn(p1), fn(p2)
        d = 100 * (v2 - v1) / v1 if v1 else None
        ds = "n/a" if d is None else f"{d:+.1f}%"
        color = GRAY if (good == 0 or d is None) else (GREEN if d * good > 0 else RED)
        table.append((label, fmt(v1), fmt(v2), ds, color))

    # ---- PNG ----
    n = len(table)
    fig, ax = plt.subplots(figsize=(8.6, 0.52 * n + 1.35))
    ax.axis("off")
    xL, xP1, xP2, xD = 0.02, 0.52, 0.75, 0.985     # label left; numbers right-aligned at these x
    yh = n + 0.15
    ax.text(xL, yh, "Metric", ha="left", va="center", fontsize=10, fontweight="bold", color=NAVY)
    ax.text(xP1, yh, f"Period 1\n{a.p1_label}", ha="right", va="center", fontsize=9.5, fontweight="bold", color=NAVY)
    ax.text(xP2, yh, f"Period 2\n{a.p2_label}", ha="right", va="center", fontsize=9.5, fontweight="bold", color=NAVY)
    ax.text(xD, yh, "Δ %", ha="right", va="center", fontsize=10, fontweight="bold", color=NAVY)
    ax.plot([0, 1], [n - 0.42, n - 0.42], color=NAVY, lw=1.3)

    for i, (label, s1, s2, ds, color) in enumerate(table):
        y = n - 1 - i
        if i % 2 == 0:
            ax.axhspan(y - 0.5, y + 0.5, xmin=0, xmax=1, color="#000000", alpha=0.03, zorder=0)
        ax.text(xL, y, label, ha="left", va="center", fontsize=10, color="#222")
        ax.text(xP1, y, s1, ha="right", va="center", fontsize=10, color="#222")
        ax.text(xP2, y, s2, ha="right", va="center", fontsize=10, color="#222")
        ax.text(xD, y, ds, ha="right", va="center", fontsize=10.5, color=color, fontweight="bold")
    ax.set_xlim(0, 1)
    ax.set_ylim(-0.6, n + 0.7)
    ax.set_title(f"{a.adv} — Prospecting: Period 1 vs Period 2",
                 fontsize=13.5, fontweight="bold", loc="left", color="#222", pad=12)
    plt.tight_layout()
    plt.savefig(a.out, dpi=200, bbox_inches="tight")
    print(f"wrote {a.out}")

    # ---- markdown ----
    md = [f"# {a.adv} — Prospecting metrics: Period 1 vs Period 2",
          f"All prospecting campaigns (funnel=1/obj=1). P1 = {a.p1_label}, P2 = {a.p2_label}.", "",
          "| Metric | Period 1 | Period 2 | Δ % |", "|---|---:|---:|---:|"]
    for label, s1, s2, ds, _ in table:
        md.append(f"| {label} | {s1} | {s2} | {ds} |")
    open(a.md, "w").write("\n".join(md) + "\n")
    print(f"wrote {a.md}")

    roas = next(t for t in table if t[0] == "ROAS")
    vr = next(t for t in table if t[0] == "Visit rate")
    aov = next(t for t in table if t[0] == "AOV")
    print(f"FINDING: prospecting P1→P2 — spend {table[0][3]}, impressions {table[1][3]}, but visits "
          f"{table[3][3]}, visit rate {vr[3]}, ROAS {roas[3]} ({roas[1]}→{roas[2]}). AOV {aov[3]} (flat) ⇒ "
          f"the revenue loss is a conversion-COUNT / audience-quality problem, not smaller baskets.")


if __name__ == "__main__":
    main()
