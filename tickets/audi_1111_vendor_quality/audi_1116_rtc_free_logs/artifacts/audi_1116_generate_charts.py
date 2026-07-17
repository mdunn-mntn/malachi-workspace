#!/usr/bin/env python3
"""AUDI-1116 chart — per-source ingest latency (the RTC staleness finding)."""

import csv
import statistics
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

NAMES = {23: "guid_log", 24: "Justuno", 25: "5x5", 26: "Predactiv", 28: "33Across",
         30: "augmentor_log", 33: "Sovrn", 36: "Cybba", 39: "Klickly", 40: "33Across API"}


def main():
    by_ds = {}
    for r in csv.DictReader(open(OUT / "audi_1116_hourly_arrival.csv")):
        if r["ingest_lag_med_min"]:
            by_ds.setdefault(int(r["ds"]), []).append(float(r["ingest_lag_med_min"]))
    rows = sorted(((NAMES[ds], statistics.median(v), max(v), ds in (23, 30))
                   for ds, v in by_ds.items()), key=lambda r: -r[1])

    fig, ax = plt.subplots(figsize=(9, 5))
    for y, (name, med, mx, free) in enumerate(rows):
        color = RED if free else NAVY
        ax.barh(y, max(med, 4) / 60, height=0.55, color=color)
        label = "0 min — streaming" if free else (
            f"{med/60:.1f} h" + (f" (buckets to {mx/60:.1f} h)" if mx > med * 1.5 else ""))
        ax.text(max(med, 4) / 60 + 0.08, y, label, va="center", fontsize=9.5,
                color=color, fontweight="bold" if free else "normal")
        ax.text(-0.06, y, name, va="center", ha="right", fontsize=10)
    ax.set_yticks([])
    ax.set_xlabel("median ingest lag (hours) — ULID mint time minus event time, dt=2026-07-01",
                  fontsize=9, color=GRAY)
    ax.invert_yaxis()
    ax.set_title("Free logs are the only real-time sources — vendors arrive hours late",
                 fontsize=13, fontweight="bold", loc="left", pad=26)
    ax.text(0, 1.02, "Consequence: 99.99% of RTC-fired impressions land on free-covered IPs; vendor-only = 0.01% (AUDI-1116)",
            transform=ax.transAxes, fontsize=8.5, color=GRAY)
    fig.tight_layout()
    fig.savefig(HERE / "audi_1116_ingest_latency.png", dpi=200, facecolor=BG)
    print("wrote audi_1116_ingest_latency.png")


if __name__ == "__main__":
    main()
