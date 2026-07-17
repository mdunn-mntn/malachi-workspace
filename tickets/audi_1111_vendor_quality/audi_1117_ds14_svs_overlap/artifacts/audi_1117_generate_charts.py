#!/usr/bin/env python3
"""AUDI-1117 chart — DS14 gate overlap: per-source biddable share + pool expansion split."""

import csv
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = Path(__file__).resolve().parent
OUT = HERE.parent / "outputs"

BG, NAVY, RED, GRAY = "#FAFAFA", "#1F3864", "#C00000", "#7F7F7F"
plt.rcParams.update({
    "font.family": "Helvetica Neue", "figure.facecolor": BG, "axes.facecolor": BG,
    "axes.spines.top": False, "axes.spines.right": False, "axes.spines.left": False,
})

NAMES = {"23": "guid_log", "24": "Justuno", "25": "5x5", "26": "Predactiv", "28": "33Across",
         "30": "augmentor_log", "33": "Sovrn", "36": "Cybba", "39": "Klickly", "40": "33Across API"}


def main():
    rows = list(csv.DictReader(open(OUT / "audi_1117_ds14_overlap_sizing.csv")))
    src = sorted(((NAMES[r["key"]], float(r["pct_in_gate"]), r["key"] in ("23", "30"))
                  for r in rows if r["rec"] == "source"), key=lambda x: -x[1])
    pool = {r["key"]: float(r["ips_total"]) for r in rows if r["rec"] == "pool"}

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4.6), width_ratios=[1.15, 1])

    for y, (name, pct, free) in enumerate(src):
        color = RED if free else NAVY
        ax1.barh(y, pct, height=0.55, color=color)
        ax1.text(pct + 1, y, f"{pct:.0f}%", va="center", fontsize=9.5, color=color)
        ax1.text(-2, y, name, va="center", ha="right", fontsize=9.5)
    ax1.set_yticks([])
    ax1.set_xlim(0, 100)
    ax1.invert_yaxis()
    ax1.set_title("Half of what 33Across sends is not biddable on arrival",
                  fontsize=12.5, fontweight="bold", loc="left", pad=28)
    ax1.text(0, 1.03, "% of each source's 30d delivered IPs inside the DS14 gate (aug 1d | guid 4d, ref 2026-07-01)",
             transform=ax1.transAxes, fontsize=8.5, color=GRAY)
    ax1.set_xticks([])

    bars = [("biddable pool\ntoday", pool["gate_pool_today"] / 1e6, GRAY),
            ("expansion:\nfree-stale IPs\n(widen free windows — $0)", pool["expansion_free_stale"] / 1e6, RED),
            ("expansion:\nvendor-only IPs\n(requires paying vendors)", pool["expansion_vendor_only"] / 1e6, NAVY)]
    for x, (label, v, color) in enumerate(bars):
        ax2.bar(x, v, width=0.6, color=color)
        ax2.text(x, v + 3, f"{v:.0f}M", ha="center", fontsize=11, fontweight="bold", color=color)
        ax2.text(x, -14, label, ha="center", fontsize=8.5, va="top")
    ax2.set_xticks([])
    ax2.set_yticks([])
    ax2.set_ylim(0, 135)
    ax2.set_title("Half the possible pool growth is already free",
                  fontsize=12.5, fontweight="bold", loc="left", pad=28)
    ax2.text(0, 1.03, "svs 30d IPs outside today's gate: 97M seen by free logs vs 96M vendor-only (IPv4)",
             transform=ax2.transAxes, fontsize=8.5, color=GRAY)

    fig.tight_layout()
    fig.savefig(HERE / "audi_1117_ds14_pool.png", dpi=200, facecolor=BG)
    print("wrote audi_1117_ds14_pool.png")


if __name__ == "__main__":
    main()
