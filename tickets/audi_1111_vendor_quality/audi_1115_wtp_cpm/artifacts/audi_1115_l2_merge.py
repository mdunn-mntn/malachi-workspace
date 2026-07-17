#!/usr/bin/env python3
"""AUDI-1115 L2 shard merge.

Sums the 4 IP-hash-shard histograms (pm, sf, ff, n) produced by
queries/audi_1115_l2_flow_shard.sql and emits outputs/audi_1115_l2_flow_coverage.csv
with the exact schema the single-query variant would have produced
(rec, ds, trips_total, pct_universe, sameday_cnt, flow_cnt, strict_cnt, pct_flow),
then runs the deck_d1 anchors:
  - per paid vendor: sameday_cnt must equal deck_d1 trips_standalone (<0.1% drift)
  - universe must match deck_d1 (~13,286,674,041)
Exits non-zero if any shard file is missing or an anchor fails.
"""

import csv
import sys
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
OUT = HERE.parent / "outputs"
D1 = HERE.parent.parent.parent / "audi_1089_ddp_vendor_evaluations" / "outputs" / "run_2026_07_10" / "deck_d1_universe_coverage.csv"

NSHARDS = 4
PAID = [(24, 1), (25, 2), (26, 3), (28, 4), (33, 6), (36, 7), (39, 8), (40, 9)]
FREE = [(23, 1), (30, 32), (99, 33)]


def load_hist():
    hist = defaultdict(int)
    for k in range(NSHARDS):
        p = OUT / f"audi_1115_l2_shard{k}.csv"
        if not p.exists():
            sys.exit(f"missing shard file: {p}")
        rows = list(csv.DictReader(open(p)))
        if not rows or "pm" not in rows[0]:
            sys.exit(f"shard file {p} is not a histogram (failed run?)")
        for r in rows:
            hist[(int(r["pm"]), int(r["sf"]), int(r["ff"]))] += int(r["n"])
        print(f"shard {k}: {len(rows)} cells, {sum(int(r['n']) for r in rows):,} triples")
    return hist


def build_table(hist):
    universe = sum(hist.values())

    def tot(pred):
        return sum(n for (pm, sf, ff), n in hist.items() if pred(pm, sf, ff))

    rows = []
    for ds, bit in PAID:
        t = tot(lambda pm, sf, ff: (pm >> bit) & 1)
        rows.append({
            "rec": "vendor", "ds": ds, "trips_total": t,
            "pct_universe": round(100 * t / universe, 2),
            "sameday_cnt": tot(lambda pm, sf, ff: ((pm >> bit) & 1) and sf == 0),
            "flow_cnt": tot(lambda pm, sf, ff: ((pm >> bit) & 1) and ff == 0),
            "strict_cnt": tot(lambda pm, sf, ff: ((pm >> bit) & 1) and ff == 0 and sf == 0),
        })
        rows[-1]["pct_flow"] = round(100 * rows[-1]["flow_cnt"] / universe, 2)
    for ds, fbits in FREE:
        rows.append({
            "rec": "free", "ds": ds, "trips_total": "", "pct_universe": "",
            "sameday_cnt": tot(lambda pm, sf, ff: sf & fbits),
            "flow_cnt": tot(lambda pm, sf, ff: ff & fbits),
            "strict_cnt": tot(lambda pm, sf, ff: (sf & fbits) and (ff & fbits)),
            "pct_flow": round(100 * tot(lambda pm, sf, ff: ff & fbits) / universe, 2),
        })
    rows.append({"rec": "universe", "ds": "", "trips_total": universe,
                 "pct_universe": 100.0, "sameday_cnt": "", "flow_cnt": "",
                 "strict_cnt": "", "pct_flow": ""})
    return rows, universe


def anchors(rows, universe):
    d1 = {r["ds"]: r for r in csv.DictReader(open(D1)) if r["rec"] == "source"}
    ok = True
    print("\nANCHORS vs deck_d1:")
    for r in rows:
        if r["rec"] != "vendor":
            continue
        a, b = r["sameday_cnt"], int(d1[str(r["ds"])]["trips_standalone"])
        drift = 100 * abs(a - b) / b if b else 0.0
        flag = "EXACT" if a == b else f"drift {drift:.4f}%"
        if drift > 0.1:
            ok = False
            flag += "  <-- FAIL"
        print(f"  ds{r['ds']}: sameday {a:>15,} vs d1 standalone {b:>15,}  {flag}")
    d1u = 13286674041
    drift = 100 * abs(universe - d1u) / d1u
    print(f"  universe: {universe:,} vs d1 {d1u:,}  drift {drift:.5f}%")
    if drift > 0.1:
        ok = False
    return ok


def main():
    hist = load_hist()
    rows, universe = build_table(hist)
    out = OUT / "audi_1115_l2_flow_coverage.csv"
    cols = ["rec", "ds", "trips_total", "pct_universe", "sameday_cnt",
            "flow_cnt", "strict_cnt", "pct_flow"]
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)
    print(f"\nwrote {out}")
    for r in rows:
        print("  " + ",".join(str(r[c]) for c in cols))
    if not anchors(rows, universe):
        sys.exit("ANCHOR FAILURE — do not fill the WTP table from this output")
    print("anchors: PASS")


if __name__ == "__main__":
    main()
