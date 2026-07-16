#!/usr/bin/env python3
"""AUDI-1089 deck support: one big-picture coverage table, every column citing its query.

Reads the canonical run CSVs (outputs/run_2026_07_10/) and emits
artifacts/audi_1089_deck_coverage.md — per source: standalone visit-days
(ip x domain x date not held by the free logs) + % of universe, strictly-unique
visit-days, standalone pairs, won impressions on touched / standalone IPs, reach.

Deterministic: rerun = identical file. Asserts the package's mask anchors before
writing (q3c solo masks == q8a fresh_day splits; q3c single-bit == vendor sole rows;
q8a solo_pairs == q3 netnew_vs_free_pairs).
"""
import csv
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
RUN = os.path.join(HERE, "..", "outputs", "run_2026_07_10")

# BITSQ bit order (house convention): ds 23,24,25,26,28,30,33,36,39,40 = bits 0..9
BITSQ = [23, 24, 25, 26, 28, 30, 33, 36, 39, 40]
BIT = {ds: i for i, ds in enumerate(BITSQ)}
FREE_MASK = (1 << BIT[23]) | (1 << BIT[30])  # 33
FREEC = 99  # combined free_logs pseudo-vendor

NAME = {23: "guid_log (free, MNTN pixel)", 30: "augmentor (free, bid-time)",
        24: "Justuno", 25: "5x5", 26: "Predactiv", 28: "33Across", 33: "Sovrn",
        36: "Cybba", 39: "Klickly", 40: "33Across API",
        FREEC: "free_logs UNION (guid+aug)"}
BILLING = {23: "free", 30: "free", FREEC: "free",
           24: "metered", 28: "metered", 33: "metered", 36: "metered", 40: "metered",
           25: "flat", 26: "flat", 39: "flat"}


def read_csv(name):
    with open(os.path.join(RUN, name)) as fh:
        return list(csv.DictReader(fh))


def other_free(ds):
    """Mask of free bits that count as 'someone else' for ds's standalone cohort."""
    if ds == 23:
        return 1 << BIT[30]
    if ds == 30:
        return 1 << BIT[23]
    return FREE_MASK


def fnum(v):
    return float(v) if v not in ("", None) else 0.0


# ---------------------------------------------------------------- load inputs
q2 = {int(r["data_source_id"]): r for r in read_csv("q2_window_reach.csv")}
q1_rows30 = {}  # true 30d raw row totals (q2c's rows_raw is a ONE-DAY sample)
for r in read_csv("q1_scale_by_day.csv"):
    ds = int(r["data_source_id"])
    q1_rows30[ds] = q1_rows30.get(ds, 0.0) + fnum(r["n_rows"])
q3 = {int(r["ds"]): r for r in read_csv("q3_usable_uniqueness.csv")}
q6 = {int(r["data_source_id"]): r for r in read_csv("q6_value_tiers.csv")}
q7d = read_csv("q7d_platform_week.csv")[0]

q3b_masks, q3c_masks, q3c_vendor = {}, {}, {}
for r in read_csv("q3b_credit_reassignment.csv"):
    if r["rec"] == "mask":
        q3b_masks[int(r["k1"])] = (fnum(r["n_pairs"]), fnum(r["n_ips"]))
for r in read_csv("q3c_visit_grain_uniqueness.csv"):
    if r["rec"] == "mask":
        q3c_masks[int(r["k1"])] = fnum(r["n"])
    elif r["rec"] == "vendor":
        q3c_vendor.setdefault(int(r["k1"]), {})[r["k2"]] = fnum(r["n"])

q8a = {}
for r in read_csv("q8a_solo_stock.csv"):
    q8a.setdefault(int(r["ds"]), {}).setdefault(r["rec"], {})[r["k"]] = fnum(r["v"])
q8b = {}
for r in read_csv("q8b_solo_perf.csv"):
    q8b.setdefault(int(r["ds"]), {}).setdefault(r["rec"], {})[r["k"]] = fnum(r["v"])
q15 = {}
for r in read_csv("q15_free_union_perf.csv"):
    q15.setdefault(r["rec"], {}).setdefault(r["k1"], {})[r["k2"]] = fnum(r["v"])
q15b = {}
for r in read_csv("q15b_free_union_stock.csv"):
    q15b.setdefault(r["rec"], {})[r["k1"]] = fnum(r["v"])

bills = {}  # June 2026 metered usage x 12
for r in read_csv("q0_roster_cost.csv"):
    if r["reporting_month"] == "2026-06-01" and r["usage_dollars"]:
        bills[int(r["data_source_id"])] = fnum(r["usage_dollars"]) * 12

# ---------------------------------------------------------------- mask algebra
TRIP_UNIVERSE = sum(q3c_masks.values())          # usable visit-day triples, 30d, all sources
PAIR_UNIVERSE = sum(v[0] for v in q3b_masks.values())  # usable (ip,domain) pairs, 30d


def trips_standalone(ds):
    """Visit-day triples held by ds and by NEITHER free log (free logs: vs the other one).
    For FREEC: union triples no PAID vendor holds."""
    if ds == FREEC:
        return sum(n for m, n in q3c_masks.items() if (m & FREE_MASK) and not (m & ~FREE_MASK))
    b, of = 1 << BIT[ds], other_free(ds)
    return sum(n for m, n in q3c_masks.items() if (m & b) and not (m & of))


def trips_sole(ds):
    """Visit-day triples held by ds and NO other source at all (single-bit mask)."""
    if ds == FREEC:
        return trips_standalone(FREEC)  # identical by definition for the union
    return q3c_masks.get(1 << BIT[ds], 0.0)


def pairs_standalone(ds):
    if ds == FREEC:
        return q15b["stock"]["sole_pairs"]
    return q8a[ds]["stock"]["solo_pairs"]


# ---------------------------------------------------------------- anchors
def check(label, a, b, tol=0.001):
    if b == 0 and a == 0:
        return
    rel = abs(a - b) / max(abs(b), 1.0)
    assert rel <= tol, f"ANCHOR FAIL {label}: {a:,.0f} vs {b:,.0f} ({rel:.2%})"


for ds in BITSQ:
    fd = q8a[ds]["fresh_day"]
    key = "refresh_of_free_pair" if "refresh_of_free_pair" in fd else "refresh_of_paid_pair"
    check(f"q3c-solo-masks==q8a-fresh_day ds{ds}",
          trips_standalone(ds), fd["solo_new_pair"] + fd[key])
    v = q3c_vendor[ds]
    check(f"q3c-single-bit==vendor-sole ds{ds}",
          trips_sole(ds), v["sole_new_pair"] + v["sole_refresh"])
    if ds not in (23, 30):
        check(f"q8a-solo-pairs==q3-netnew ds{ds}",
              pairs_standalone(ds), fnum(q3[ds]["netnew_vs_free_pairs"]), tol=0.002)
check("q15b-union-sole-trips==q15b-fresh_day", trips_standalone(FREEC),
      q15b["fresh_day"]["sole_new_pair"] + q15b["fresh_day"]["refresh_of_paid_pair"])

PLAT_IMPS = fnum(q7d["imps_week"])
PLAT_IPS = fnum(q7d["ips_served_week"])

# ---------------------------------------------------------------- rows
rows = []
for ds in BITSQ + [FREEC]:
    if ds == FREEC:
        raw_rows = q1_rows30[23] + q1_rows30[30]
        ips30 = None  # q15b's union reach is parse-gated — not comparable to raw q2 (see note)
        imps_t = q15["serve"]["touched"]["imps"]
        ips_srv_t = q15["serve"]["touched"]["ips_served"]
        imps_solo = q15["serve"]["sole"]["imps"]
    else:
        raw_rows = q1_rows30[ds]
        ips30 = fnum(q2[ds]["ips_30d"])
        imps_t = fnum(q6[ds]["imps_touched"])
        ips_srv_t = fnum(q6[ds]["ips_touched"])
        imps_solo = q8b[ds]["serve"]["imps"]
    ts, tso = trips_standalone(ds), trips_sole(ds)
    rows.append({
        "ds": ds, "name": NAME[ds], "billing": BILLING[ds],
        "bill": bills.get(ds), "raw_rows": raw_rows, "ips30": ips30,
        "pairs_standalone": pairs_standalone(ds),
        "trips_standalone": ts, "trips_standalone_pct": 100 * ts / TRIP_UNIVERSE,
        "trips_sole": tso, "trips_sole_pct": 100 * tso / TRIP_UNIVERSE,
        "imps_touched": imps_t, "imps_touched_pct": 100 * imps_t / PLAT_IMPS,
        "ips_served_touched": ips_srv_t,
        "ips_served_pct": 100 * ips_srv_t / PLAT_IPS,
        "imps_standalone": imps_solo,
    })
rows.sort(key=lambda r: r["trips_standalone"], reverse=True)

# what each free row's standalone is measured against — annotated in-cell because the
# comparison set differs from the paid rows (a component can exceed the union otherwise)
VS = {23: " *(vs aug only)*", 30: " *(vs guid only)*", FREEC: " *(vs all 8 paid)*"}
AUG_COHELD = trips_standalone(30) - trips_sole(30)  # aug-not-guid triples paid also holds
MASK_BOTH_FREE = q3c_masks.get(FREE_MASK, 0.0)      # guid AND aug, no paid


# ---------------------------------------------------------------- render
def n0(v):
    return f"{v:,.0f}"


def money(r):
    if r["billing"] == "free":
        return "$0 (internal)"
    if r["billing"] == "flat":
        return "flat (pending)"
    return f"${r['bill']:,.0f}"


L = []
L.append("# AUDI-1089 deck support — big-picture coverage, one table, every number cited")
L.append("")
L.append("Windows: delivery/uniqueness = 30d svs, `dt 2026-06-02..07-01`; serving = 37d "
         "membership union x valuation week `2026-07-02..08` (CIL); bills = June 2026 x 12. "
         "Grains: visit-day / pair / universe columns are **usable** rows (domain consumable "
         "by DS13 or DS19); raw rows and raw IPs are ungated (marked raw); serving cohorts "
         "(touched / standalone won-imps) use RAW 37d membership with NO usable gate — per "
         "the q6/q8b/q15 headers. Full defs: `../runbook/queries/VALIDATION_GUIDE.md`.")
L.append("")
L.append("## Universe anchors (the denominators)")
L.append("")
L.append("| Anchor | Value | Query |")
L.append("|---|---:|---|")
L.append(f"| Usable visit-day universe, 30d (distinct ip x domain x date, all 10 sources) "
         f"| {n0(TRIP_UNIVERSE)} | `q3c` mask histogram, summed |")
L.append(f"| Usable (ip, domain) pair universe, 30d | {n0(PAIR_UNIVERSE)} "
         f"| `q3b` mask histogram, summed |")
L.append(f"| Platform valuation week: won impressions | {n0(PLAT_IMPS)} | `q7d` |")
L.append(f"| Platform valuation week: distinct served IPs | {n0(PLAT_IPS)} | `q7d` |")
FREE_COV_TRIPS = sum(n for m, n in q3c_masks.items() if m & FREE_MASK)
FREE_COV_PAIRS = sum(v[0] for m, v in q3b_masks.items() if m & FREE_MASK)
L.append(f"| Free-logs coverage of the visit-day universe (any free bit) "
         f"| {n0(FREE_COV_TRIPS)} ({100 * FREE_COV_TRIPS / TRIP_UNIVERSE:.1f}%) "
         f"| `q3c` masks |")
L.append(f"| Free-logs coverage of the pair universe (any free bit) "
         f"| {n0(FREE_COV_PAIRS)} ({100 * FREE_COV_PAIRS / PAIR_UNIVERSE:.1f}%) "
         f"| `q3b` masks |")
L.append("")
L.append("## Coverage by source (ranked by standalone visit-days)")
L.append("")
L.append("**standalone** = held by this source and by NEITHER free log — the source's "
         "addition over the free logs, i.e. the renewal counterfactual \"this feed as our "
         "only paid feed\" (free logs themselves: vs the other free log; the union row: vs "
         "all 8 paid feeds). "
         "**strictly unique** = held by NO other source at all (free or paid). "
         "**touched** = won impressions (valuation week) on IPs the source delivered in the "
         "37d window — co-delivered IPs count for every holder, so the column is NOT additive.")
L.append("")
L.append("| Source | Bill $/yr | Raw rows 30d | Standalone visit-days (ip x dom x date) "
         "| % of universe | Strictly-unique visit-days | % of universe "
         "| Won imps, touched IPs (wk) | % of platform | Won imps, standalone IPs (wk) |")
L.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
for r in rows:
    L.append(f"| {r['name']} | {money(r)} | {n0(r['raw_rows'])} "
             f"| {n0(r['trips_standalone'])}{VS.get(r['ds'], '')} "
             f"| {r['trips_standalone_pct']:.1f}% "
             f"| {n0(r['trips_sole'])} | {r['trips_sole_pct']:.1f}% "
             f"| {n0(r['imps_touched'])} | {r['imps_touched_pct']:.1f}% "
             f"| {n0(r['imps_standalone'])} |")
L.append("")
L.append("## Same sources — reach and pair-grain detail")
L.append("")
L.append("| Source | Unique IPs 30d (raw) | Standalone (ip, domain) pairs "
         "| % of pair universe | Distinct served IPs, touched (wk) | % of platform served IPs |")
L.append("|---|---:|---:|---:|---:|---:|")
for r in rows:
    ips30_cell = n0(r["ips30"]) if r["ips30"] is not None else "— (see notes)"
    L.append(f"| {r['name']} | {ips30_cell} "
             f"| {n0(r['pairs_standalone'])}{VS.get(r['ds'], '')} "
             f"| {100 * r['pairs_standalone'] / PAIR_UNIVERSE:.1f}% "
             f"| {n0(r['ips_served_touched'])} | {r['ips_served_pct']:.1f}% |")
L.append("")
L.append("## Which query backs which column")
L.append("")
L.append("All files in `../runbook/queries/` (headers carry the exact run command; "
         "`MANIFEST.md` = run order; `VALIDATION_GUIDE.md` = glossary + independent checks).")
L.append("")
L.append("| Column | Query file | Field / derivation |")
L.append("|---|---|---|")
L.append("| Bill $/yr | `q0_roster_cost.sql` | June 2026 `usage_dollars` x 12 (meter check "
         "imps x $0.50 CPM = usage, exact). Flat-fee vendors: amounts pending finance. "
         "Free internal logs: not in the vendor roster/meter at all — $0 |")
L.append("| Raw rows 30d | `q1_scale_by_day.sql` | `n_rows` summed over the 30 days "
         "(union row: guid + aug summed — rows are events, summing is valid at row grain). "
         "NOT q2c's `rows_raw`, which is a one-day sample |")
L.append("| Standalone visit-days + % | `q3c_visit_grain_uniqueness.sql` | mask histogram: "
         "sum of masks with the vendor's bit set and free bits clear, / universe. "
         "Cross-anchored to `q8a` `fresh_day` splits (<0.01%; live wcv/pc snapshot drift "
         "between run days) |")
L.append("| Strictly-unique visit-days + % | `q3c_visit_grain_uniqueness.sql` | single-bit "
         "mask (== vendor `sole_new_pair` + `sole_refresh` rows, exact) |")
L.append("| Won imps, touched IPs | `q6_value_tiers.sql` | `imps_touched`; union row: "
         "`q15_free_union_perf.sql` serve/touched `imps` |")
L.append("| % of platform | `q7d_platform_week.sql` | denominator `imps_week` |")
L.append("| Won imps, standalone IPs | `q8b_solo_perf.sql` | serve `imps` (solo cohort); "
         "union row: `q15_free_union_perf.sql` serve/sole `imps` |")
L.append("| Unique IPs 30d (raw) | `q2_window_reach.sql` | `ips_30d` "
         "(APPROX_COUNT_DISTINCT, ~1%); union row not shown — see notes |")
L.append("| Standalone (ip, domain) pairs + % | `q8a_solo_stock.sql` | stock `solo_pairs` "
         "(== `q3` `netnew_vs_free_pairs` to <0.01%; live-snapshot drift); union row: "
         "`q15b` stock `sole_pairs` |")
L.append("| Distinct served IPs, touched | `q6_value_tiers.sql` | `ips_touched`; union row: "
         "`q15` serve/touched `ips_served`; denominator `q7d` `ips_served_week` |")
L.append("")
L.append("## Reading notes (the traps a validator will hit)")
L.append("")
L.append("- **Columns overlap — never sum a column across sources.** The same visit-day or "
         "IP is typically held by several sources (the average served household is held by "
         "~6.7 of 10 sources; ~7.5 for HI households). Only the strictly-unique column is "
         "disjoint across sources; "
         "standalone slices of two paid vendors can overlap each other (each is a separate "
         "vs-free counterfactual), and touched columns overlap heavily by construction.")
L.append(f"- **Why augmentor's standalone ({100 * trips_standalone(30) / TRIP_UNIVERSE:.1f}%) "
         f"exceeds the union's ({100 * trips_standalone(FREEC) / TRIP_UNIVERSE:.1f}%)**: "
         f"different comparison sets. The free-log rows are measured vs the OTHER free log "
         f"only (paid ignored); the union row vs the paid roster. Of augmentor's "
         f"{n0(trips_standalone(30))} not-in-guid visit-days, {n0(AUG_COHELD)} are ALSO held "
         f"by a paid vendor and drop out of the union's vs-paid count: "
         f"{n0(trips_standalone(FREEC))} = aug-only {n0(trips_sole(30))} + guid-only "
         f"{n0(trips_sole(23))} + guid-and-aug-no-paid {n0(MASK_BOTH_FREE)} (exact, q3c masks).")
L.append("- **Standalone != strictly-unique for paid vendors**: standalone ignores the other "
         "7 paid feeds (the renewal counterfactual: free logs stay either way); strictly-"
         "unique is the hardest \"only this source has it\" cut. Dropping ONE vendor while "
         "keeping the rest loses only its strictly-unique slice; dropping ALL paid loses "
         "universe minus free coverage (see anchors), NOT the sum of standalone columns.")
L.append("- **The union row is not the sum of guid + aug rows** for any distinct-count "
         "column (visit-days, pairs) — it is measured directly on the union (`q15*`).")
L.append("- **The union row's raw unique-IP cell is blank on purpose**: `q15b` measures the "
         "union's reach only on rows with a parseable domain (186.9M), which is NOT "
         "comparable to the ungated per-source `q2` column — the true raw union is at least "
         "guid's 195.0M and at most guid+aug's 300.7M. Don't quote 186.9M as raw union reach.")
L.append("- Touched won-imps look near-identical across vendors (~200-395M) because big "
         "vendors all touch most served IPs — coverage of served IPs saturates. The spread "
         "that matters for money is in the standalone columns.")
L.append("- Generated by `audi_1089_deck_coverage.py` from `outputs/run_2026_07_10/`; "
         "rerun reproduces this file byte-for-byte. In-script asserts re-verify the "
         "mask anchors on every run.")
L.append("")

out = os.path.join(HERE, "audi_1089_deck_coverage.md")
with open(out, "w") as fh:
    fh.write("\n".join(L))
print(f"anchors OK; wrote {out}")
print(f"universe trips={TRIP_UNIVERSE:,.0f} pairs={PAIR_UNIVERSE:,.0f}")
for r in rows:
    print(f"  ds{r['ds']:>2} {r['name']:<28} standalone_trips={r['trips_standalone']:>14,.0f} "
          f"({r['trips_standalone_pct']:5.1f}%) imps_touched={r['imps_touched']:>12,.0f}")
