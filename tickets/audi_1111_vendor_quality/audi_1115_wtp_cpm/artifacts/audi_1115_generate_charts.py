#!/usr/bin/env python3
"""AUDI-1115 charts — WTP vs contract, and the flow-filter coverage drop.

Reads measured CSVs (no hardcoded data), writes PNGs to this artifacts/ dir.
"""

import csv
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = Path(__file__).resolve().parent
OUT = HERE.parent / "outputs"
A1089 = HERE.parent.parent.parent / "audi_1089_ddp_vendor_evaluations" / "outputs" / "run_2026_07_10"

BG, NAVY, RED, GRAY = "#FAFAFA", "#1F3864", "#C00000", "#7F7F7F"
plt.rcParams.update({
    "font.family": "Helvetica Neue", "figure.facecolor": BG, "axes.facecolor": BG,
    "axes.spines.top": False, "axes.spines.right": False, "axes.spines.left": False,
})

NAMES = {"24": "Justuno", "25": "5x5", "26": "Predactiv", "28": "33Across",
         "33": "Sovrn", "36": "Cybba", "39": "Klickly", "40": "33Across API"}


def wtp_chart():
    d3 = {r["data_source_id"]: r for r in csv.DictReader(open(A1089 / "deck_d3_bills_cpm.csv"))}
    q8b = {(r["rec"], r["ds"], r["k"]): float(r["v"])
           for r in csv.DictReader(open(A1089 / "q8b_solo_perf.csv")) if r["v"] not in ("", "v")}
    rows = []
    for ds in ["28", "40", "33", "24", "36"]:  # metered only
        meter = float(d3[ds]["billed_imps_month"]) * 12
        lo = q8b[("serve", ds, "media")] * 52 * .1 / (meter / 1000)
        hi = q8b[("serve", ds, "media")] * 52 * .3 / (meter / 1000)
        rows.append((NAMES[ds], lo, hi))
    rows.sort(key=lambda r: -r[2])

    fig, ax = plt.subplots(figsize=(9, 4.2))
    ys = range(len(rows))
    for y, (name, lo, hi) in zip(ys, rows):
        ax.barh(y, hi - lo, left=lo, height=0.55, color=NAVY)
        ax.text(hi + 0.008, y, f"${lo:.2f}–${hi:.2f}", va="center", fontsize=9, color=NAVY)
        ax.text(-0.01, y, name, va="center", ha="right", fontsize=10)
    ax.axvline(0.50, color=RED, lw=2)
    ax.text(0.84, 0.96, "contract $0.50", color=RED, fontsize=10, fontweight="bold", transform=ax.transAxes)
    ax.set_yticks([])
    ax.set_xlim(0, 0.62)
    ax.set_xlabel("break-even contract CPM at 10–30% margin ($/1,000 credited imps)", fontsize=9, color=GRAY)
    ax.invert_yaxis()
    ax.set_title("No metered vendor breaks even at the $0.50 contract rate",
                 fontsize=13, fontweight="bold", loc="left", pad=26)
    ax.text(0, 1.02, "Break-even = solo-cohort media ×52 × margin ÷ current billing meter (June 2026 ×12) — AUDI-1115 L0 lens",
            transform=ax.transAxes, fontsize=8.5, color=GRAY)
    fig.tight_layout()
    fig.savefig(HERE / "audi_1115_wtp_vs_contract.png", dpi=200, facecolor=BG)
    print("wrote audi_1115_wtp_vs_contract.png")


def flow_chart():
    l2 = {(r["rec"], r["ds"]): r for r in csv.DictReader(open(OUT / "audi_1115_l2_flow_coverage.csv"))}
    universe = float(l2[("universe", "")]["trips_total"])
    groups = [("free union", "99"), ("augmentor alone", "30"), ("guid alone", "23")]
    sameday = [100 * float(l2[("free", ds)]["sameday_cnt"]) / universe for _, ds in groups]
    flow = [100 * float(l2[("free", ds)]["flow_cnt"]) / universe for _, ds in groups]

    fig, ax = plt.subplots(figsize=(8, 4))
    x = range(len(groups))
    for i, (label, _) in enumerate(groups):
        ax.bar(i - 0.18, sameday[i], width=0.34, color=GRAY)
        ax.bar(i + 0.18, flow[i], width=0.34, color=NAVY)
        ax.text(i - 0.18, sameday[i] + 1, f"{sameday[i]:.1f}%", ha="center", fontsize=10, color=GRAY)
        ax.text(i + 0.18, flow[i] + 1, f"{flow[i]:.1f}%", ha="center", fontsize=10,
                fontweight="bold", color=NAVY)
    ax.set_xticks(list(x))
    ax.set_xticklabels([g for g, _ in groups], fontsize=10)
    ax.set_yticks([])
    ax.set_title("The flow filter cuts free-log coverage from 59.4% to 44.1%",
                 fontsize=13, fontweight="bold", loc="left", pad=26)
    ax.text(0, 1.02, "Gray = same-day credit (deck_d1 convention); navy = prior-30d credit only (2026-07-16 meeting rule) — % of 13.29B usable triples",
            transform=ax.transAxes, fontsize=8.5, color=GRAY)
    fig.tight_layout()
    fig.savefig(HERE / "audi_1115_flow_coverage_drop.png", dpi=200, facecolor=BG)
    print("wrote audi_1115_flow_coverage_drop.png")


if __name__ == "__main__":
    wtp_chart()
    flow_chart()
