"""Module 06c render — score-tier distribution as a P1-vs-P2 TABLE (module-04 look).

Same table styling as 04 (Tier | Period 1 | Period 2 | Δ%), but rows are the MNTN score tiers and
values are each tier's share of prospecting impressions. Δ% = relative change (n/a where P1 share is 0),
colored by whether the shift is good (more High/Peak = green, more Unscored/MaxReach = red).
For this client P1 (Jan–May '25) predates score logging, so it reads 100% "No score data".

Reads  outputs/<adv>/06_prospecting_score_buckets_monthly.csv
Writes outputs/<adv>/06c_prospecting_score_threshold_table.png
       outputs/<adv>/06c_prospecting_score_threshold_table.md
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

# (col, label, good_direction)  good: +1 up-is-good, -1 down-is-good (bad if up), 0 neutral
TIERS = [
    ("hi",        "High Intent (8001–10000)",   1),
    ("pp",        "Peak Perf (6666–8000)",      1),
    ("mi",        "Mid Intent (3333–6665)",     0),
    ("maxreach",  "MaxReach (1–3332)",         -1),
    ("unscored",  "Unscored (≤0)",             -1),
    ("notlogged", "No score data (pre-2025-06)", 0),
]


def agg(rows, lo, hi):
    tot = 0
    by = {t: 0 for t, _, _ in TIERS}
    for r in rows:
        if lo <= r["mo"] <= hi:
            tot += int(r["total"])
            for t, _, _ in TIERS:
                by[t] += int(r[t])
    return {t: (100 * by[t] / tot if tot else 0.0) for t, _, _ in TIERS}, tot


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default="outputs/kindred_35094/06_prospecting_score_buckets_monthly.csv")
    ap.add_argument("--out", default="outputs/kindred_35094/06c_prospecting_score_threshold_table.png")
    ap.add_argument("--md",  default="outputs/kindred_35094/06c_prospecting_score_threshold_table.md")
    ap.add_argument("--adv", default="Kindred Bravely (35094)")
    ap.add_argument("--p1", nargs=2, default=["2025-01", "2025-05"])
    ap.add_argument("--p2", nargs=2, default=["2026-01", "2026-05"])
    ap.add_argument("--p1-label", default="Jan–May '25")
    ap.add_argument("--p2-label", default="Jan–May '26")
    a = ap.parse_args()

    rows = list(csv.DictReader(open(a.csv)))
    d1, _ = agg(rows, a.p1[0], a.p1[1])
    d2, _ = agg(rows, a.p2[0], a.p2[1])

    table = []
    for t, label, good in TIERS:
        v1, v2 = d1[t], d2[t]
        d = 100 * (v2 - v1) / v1 if v1 else None
        ds = "n/a" if d is None else f"{d:+.1f}%"
        color = GRAY if (good == 0 or d is None) else (GREEN if d * good > 0 else RED)
        table.append((label, f"{v1:.1f}%", f"{v2:.1f}%", ds, color))

    # ---- PNG (module-04 enlarged style) ----
    n = len(table)
    fig, ax = plt.subplots(figsize=(12, 0.46 * n + 1.25))
    ax.axis("off")
    xL, xP1, xP2, xD = 0.02, 0.62, 0.83, 0.995
    yh = n + 0.28
    ax.text(xL, yh, "Score tier", ha="left", va="center", fontsize=17, fontweight="bold", color=NAVY)
    ax.text(xP1, yh, f"Period 1\n{a.p1_label}", ha="right", va="center", fontsize=15.5, fontweight="bold", color=NAVY)
    ax.text(xP2, yh, f"Period 2\n{a.p2_label}", ha="right", va="center", fontsize=15.5, fontweight="bold", color=NAVY)
    ax.text(xD, yh, "Δ %", ha="right", va="center", fontsize=17, fontweight="bold", color=NAVY)
    ax.plot([0, 1], [n - 0.40, n - 0.40], color=NAVY, lw=1.8)
    for i, (label, s1, s2, ds, color) in enumerate(table):
        y = n - 1 - i
        if i % 2 == 0:
            ax.axhspan(y - 0.5, y + 0.5, xmin=0, xmax=1, color="#000000", alpha=0.03, zorder=0)
        ax.text(xL, y, label, ha="left", va="center", fontsize=17, color="#222")
        ax.text(xP1, y, s1, ha="right", va="center", fontsize=17, color="#222")
        ax.text(xP2, y, s2, ha="right", va="center", fontsize=17, color="#222")
        ax.text(xD, y, ds, ha="right", va="center", fontsize=17.5, color=color, fontweight="bold")
    ax.set_xlim(0, 1)
    ax.set_ylim(-0.55, n + 0.95)
    ax.set_title(f"{a.adv} — Prospecting score-tier distribution: Period 1 vs Period 2",
                 fontsize=18, fontweight="bold", loc="left", color="#222", pad=14)
    plt.tight_layout()
    plt.savefig(a.out, dpi=200, bbox_inches="tight")
    print(f"wrote {a.out}")

    md = [f"# {a.adv} — Prospecting score-tier distribution: Period 1 vs Period 2",
          f"% of prospecting impressions per score tier. P1={a.p1_label}, P2={a.p2_label}. RTC-excluded.", "",
          "| Score tier | Period 1 | Period 2 | Δ % |", "|---|---:|---:|---:|"]
    for label, s1, s2, ds, _ in table:
        md.append(f"| {label} | {s1} | {s2} | {ds} |")
    open(a.md, "w").write("\n".join(md) + "\n")
    print(f"wrote {a.md}")
    print(f"FINDING: P1 High Intent {d1['hi']:.1f}% -> P2 {d2['hi']:.1f}%; unscored {d1['unscored']:.1f}% -> "
          f"{d2['unscored']:.1f}%. (P1 no-score-data {d1['notlogged']:.0f}% for this client.)")


if __name__ == "__main__":
    main()
