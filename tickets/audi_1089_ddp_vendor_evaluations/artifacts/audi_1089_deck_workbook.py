#!/usr/bin/env python3
"""AUDI-1089 deck workbook: the user's 6-block deck sheet as an .xlsx, every cell
filled from the canonical run CSVs (outputs/run_2026_07_10/).

Blocks and their supporting queries (deck_d1..d6, see runbook/queries/MANIFEST.md):
  1 coverage + touched won imps + CPM   <- q3c masks (== deck_d1), q6/q15/q7d
                                           (== deck_d2), deck_d3
  2 CPM / bill per year                 <- deck_d3 (Profit column left for the
                                           user's sheet formula — margins internal)
  3 bill if free_logs preempted         <- deck_d3 bill x (1 - free-cohold share
                                           from q3c masks == deck_d1)
  4 scenario ladder                     <- q3c masks (triples) + deck_d4 (HI/PP)
  5 tier coverage, ALL member IPs       <- deck_d5
  6 tier coverage, IPs that got bid on  <- deck_d6

Cells whose scan hasn't landed yet print PENDING; rerun after the deck_d4/d5/d6
CSVs land and they fill in. Deterministic: rerun = identical file.
"""
import csv
import os

HERE = os.path.dirname(os.path.abspath(__file__))
RUN = os.path.join(HERE, "..", "outputs", "run_2026_07_10")
OUT = os.path.join(HERE, "..", "outputs", "audi_1089_deck_sheet.xlsx")

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

BITSQ = [23, 24, 25, 26, 28, 30, 33, 36, 39, 40]
BIT = {ds: i for i, ds in enumerate(BITSQ)}
FREE_MASK = 33
PAID_MASK = 990

NAME = {23: "guid_log (free)", 30: "augmentor (free)", 24: "Justuno", 25: "5x5",
        26: "Predactiv", 28: "33Across", 33: "Sovrn", 36: "Cybba", 39: "Klickly",
        40: "33Across API", 99: "free_logs UNION (guid+aug)"}
METERED = {24, 28, 33, 36, 40}
FLAT = {25, 26, 39}


def read_csv(name):
    p = os.path.join(RUN, name)
    if not os.path.exists(p) or os.path.getsize(p) == 0:
        return None
    with open(p) as fh:
        rows = list(csv.DictReader(fh))
    return rows or None


# ---------------------------------------------------------------- inputs
mh = {}
for r in read_csv("q3c_visit_grain_uniqueness.csv"):
    if r["rec"] == "mask":
        mh[int(r["k1"])] = int(float(r["n"]))
UNIVERSE = sum(mh.values())

q6 = {int(r["data_source_id"]): r for r in read_csv("q6_value_tiers.csv")}
q7d = read_csv("q7d_platform_week.csv")[0]
PLAT_IMPS = float(q7d["imps_week"])
q15_touched_imps = next(float(r["v"]) for r in read_csv("q15_free_union_perf.csv")
                        if r["rec"] == "serve" and r["k1"] == "touched" and r["k2"] == "imps")

d3 = {int(r["data_source_id"]): r for r in read_csv("deck_d3_bills_cpm.csv")
      if r["data_source_id"] and int(r["data_source_id"]) != 27}  # DS27 = context row

d4 = read_csv("deck_d4_scenario_ladder.csv")
d4 = {r["scenario"]: r for r in d4} if d4 else None
d5 = read_csv("deck_d5_tier_free_coverage_all_ips.csv")
d5 = {r["tier"]: r for r in d5} if d5 else None
d6 = read_csv("deck_d6_tier_free_coverage_bid_ips.csv")
d6 = {r["tier"]: r for r in d6} if d6 else None

PENDING = "PENDING (scan running)"


# ---------------------------------------------------------------- mask algebra
def msum(pred):
    return sum(n for m, n in mh.items() if pred(m))


def src_stats(ds):
    b = 1 << BIT[ds]
    fb = {23: 1 << BIT[30], 30: 1 << BIT[23]}.get(ds, FREE_MASK)
    total = msum(lambda m: m & b)
    cohold = msum(lambda m: (m & b) and (m & fb))
    return total, cohold


stats = {ds: src_stats(ds) for ds in BITSQ}
order = sorted(BITSQ, key=lambda d: -stats[d][0])  # rank by total desc (== deck_d1)
FU_TOTAL = msum(lambda m: m & FREE_MASK)

# post-preemption bills (block 3): bill x (1 - free-cohold share), metered only
bill_after = {}
for ds in METERED:
    total, cohold = stats[ds]
    bill_after[ds] = float(d3[ds]["bill_annualized"]) * (1 - cohold / total)
POST_TOTAL = sum(bill_after.values())
assert abs(POST_TOTAL - 538726) < 100, f"preemption tripwire: {POST_TOTAL:,.0f} vs known ~538,726"

SCEN = [  # (sheet label, kept-vendors text, keepmask, deck_d4 scenario key)
    ("Today (all 8)", "Justuno + 5x5 + Predactiv + 33Across + Sovrn + Cybba + Klickly + 33A API", 1023, "today_all_8_paid"),
    ("Drop Sovrn + Cybba", "Justuno + 5x5 + Predactiv + 33Across + Klickly + 33A API", 831, "drop_sovrn_cybba"),
    ("+ drop Klickly", "Justuno + 5x5 + Predactiv + 33Across + 33A API", 575, "plus_drop_klickly"),
    ("+ drop Justuno (knee k=4)", "5x5 + Predactiv + 33Across + 33A API", 573, "plus_drop_justuno_k4"),
    ("33Across combined only", "33Across + 33A API", 561, "33across_combined_only"),
    ("Flat-fee only (5x5 + Predactiv)", "5x5 + Predactiv", 45, "flat_fee_only_5x5_predactiv"),
    ("Free logs only (guid + augmentor)", "(none)", 33, "free_logs_only"),
    ("Free: augmentor DS30 only", "(none)", 32, "augmentor_ds30_only"),
    ("Free: guid_log DS23 only", "(none)", 1, "guid_log_ds23_only"),
]

TIER_ROWS = [("1_all_ips", "Free Logs ONLY (all IPs)"), ("2_hi_10000", "HI10000"),
             ("3_pp_8000", "PP8000"), ("4_high_graduated", "High-Graduated"),
             ("5_mid", "Mid"), ("6_max_reach", "MAX REACH"), ("7_unscored", "Unscored")]


# ---------------------------------------------------------------- xlsx helpers
wb = Workbook()
ws = wb.active
ws.title = "deck"

HDR_FILL = PatternFill("solid", fgColor="203864")
HDR_FONT = Font(bold=True, color="FFFFFF", size=10)
WRAP = Alignment(wrap_text=True, vertical="top")
N0 = "#,##0"
P1 = "0.0"
P2 = "0.00"
MONEY = "$#,##0"

row = 1


def header(cells):
    global row
    for c, text in enumerate(cells, 1):
        cell = ws.cell(row=row, column=c, value=text)
        cell.fill, cell.font, cell.alignment = HDR_FILL, HDR_FONT, WRAP
    ws.row_dimensions[row].height = 30
    row += 1


def emit(cells, fmts=None):
    global row
    for c, v in enumerate(cells, 1):
        cell = ws.cell(row=row, column=c, value=v)
        cell.alignment = Alignment(wrap_text=True, vertical="top")
        if fmts and fmts.get(c) and isinstance(v, (int, float)):
            cell.number_format = fmts[c]
    row += 1


def gap(n=2):
    global row
    row += n


def cpm_label(ds):
    if ds in METERED:
        return 0.50
    if ds in FLAT:
        return "flat (pending)"
    return "$0 (internal)"


def bill_label(ds):
    if ds in METERED:
        return float(d3[ds]["bill_annualized"])
    if ds in FLAT:
        return "flat (pending)"
    return 0


# ---------------------------------------------------------------- block 1
header(["Vendor", "Total IP x Domain x Date triples (usable, 30d)", "% of total universe",
        "Cumulative % of universe (union, top-N by total)",
        "Touched won imps (valuation wk)", "% of platform won imps (NOT a win rate)", "CPM"])
fmts1 = {2: N0, 3: P2, 4: P2, 5: N0, 6: P1}
km = 0
for ds in order:
    total, _ = stats[ds]
    km |= 1 << BIT[ds]
    cum = msum(lambda m: m & km)
    imps = float(q6[ds]["imps_touched"])
    emit([NAME[ds], total, 100 * total / UNIVERSE, 100 * cum / UNIVERSE,
          imps, 100 * imps / PLAT_IMPS, cpm_label(ds)], fmts1)
emit([NAME[99], FU_TOTAL, 100 * FU_TOTAL / UNIVERSE, "—",
      q15_touched_imps, 100 * q15_touched_imps / PLAT_IMPS, "$0 (internal)"], fmts1)
gap()

# ---------------------------------------------------------------- block 2
header(["Vendor", "CPM", "Profit (CPM x margin range) — your sheet formula", "Bill / yr cost"])
fmts2 = {4: MONEY}
b2_order = sorted(METERED, key=lambda d: -float(d3[d]["bill_annualized"])) + sorted(FLAT) + [23, 30, 99]
for ds in b2_order:
    emit([NAME[ds], cpm_label(ds), "", bill_label(ds)], fmts2)
gap()

# ---------------------------------------------------------------- block 3
header(["Vendor", "Bill/yr if free_logs preempted from billing",
        "Profit (CPM x margin range) — your sheet formula",
        "free co-hold % (share of vendor's visit-days a free log also holds)"])
fmts3 = {2: MONEY, 4: P1}
for ds in b2_order:
    if ds in METERED:
        total, cohold = stats[ds]
        emit([NAME[ds], bill_after[ds], "", 100 * cohold / total], fmts3)
    elif ds in FLAT:
        emit([NAME[ds], "flat (pending) — preemption does not change flat fees", "", ""], fmts3)
    else:
        emit([NAME[ds], 0, "", ""], fmts3)
emit(["TOTAL metered (today $812,397)", POST_TOTAL, "",
      f"savings ${812397 - POST_TOTAL:,.0f}/yr = the AUDI-1093 preemption"], fmts3)
gap()

# ---------------------------------------------------------------- block 4
header(["Scenario", "Paid vendors kept", "Total triples kept", "Coverage (% of today)",
        "HI triples kept", "HI-IP coverage %", "PP triples kept", "PP-IP coverage %"])
fmts4 = {3: N0, 4: P2, 5: N0, 6: P2, 7: N0, 8: P2}
for label, kept_txt, keepmask, key in SCEN:
    kept = msum(lambda m: m & keepmask)
    if d4 and key in d4:
        r4 = d4[key]
        hi_t, hi_p = float(r4["hi_trips_kept"]), float(r4["hi_ip_coverage_pct"])
        pp_t, pp_p = float(r4["pp_trips_kept"]), float(r4["pp_ip_coverage_pct"])
    else:
        hi_t = hi_p = pp_t = pp_p = PENDING
    emit([label, kept_txt, kept, 100 * kept / UNIVERSE, hi_t, hi_p, pp_t, pp_p], fmts4)
gap()

# ---------------------------------------------------------------- blocks 5 & 6
for title, data in [
    ("Of ALL member IPs, served or not (audience-size coverage)", d5),
    ("Of member IPs that actually got bid on (won impressions)", d6),
]:
    header([title, "free-covered IPs", "vendor-only IPs", "% free covered"])
    fmts56 = {2: N0, 3: N0, 4: P2}
    for key, label in TIER_ROWS:
        if data and key in data:
            r56 = data[key]
            emit([label, float(r56["free_covered_ips"]), float(r56["vendor_only_ips"]),
                  float(r56["pct_free_covered"])], fmts56)
        else:
            emit([label, PENDING, PENDING, PENDING], fmts56)
    gap()

for c, w in enumerate([34, 22, 13, 16, 18, 16, 18, 16], 1):
    ws.column_dimensions[get_column_letter(c)].width = w

# ---------------------------------------------------------------- queries sheet
qs = wb.create_sheet("queries")
qrow = [["Block", "Supporting query (runbook/queries/)", "What it computes", "Status"],
        ["1 cols B-D", "deck_d1_universe_coverage.sql", "triple holder-mask histogram -> totals, % of universe, cumulative union % (numbers here derive from the identical measured q3c histogram)", "measured (q3c run_2026_07_10)"],
        ["1 cols E-F", "deck_d2_touched_won_bids.sql", "won imps on touched IPs + % of platform won imps (RETITLED col F: share of platform, NOT a win rate)", "measured (q6/q15/q7d)"],
        ["1 col G, 2", "deck_d3_bills_cpm.sql", "registry roster, contract/implied CPM, June 2026 meter bill x 12", "run 2026-07-16"],
        ["3", "deck_d3 x deck_d1", "bill_after = bill x (1 - free-cohold share); sheet formula, inputs in this workbook", "computed"],
        ["4", "deck_d4_scenario_ladder.sql", "9 keep-set scenarios: triples kept, % of today, HI/PP triples + IP-grain coverage", "triples measured; HI/PP: scan running"],
        ["5", "deck_d5_tier_free_coverage_all_ips.sql", "ALL member IPs by score tier: free-covered vs vendor-only", "scan running"],
        ["6", "deck_d6_tier_free_coverage_bid_ips.sql", "same split, only IPs with won impressions", "scan running"]]
for i, rr in enumerate(qrow, 1):
    for c, v in enumerate(rr, 1):
        cell = qs.cell(row=i, column=c, value=v)
        cell.alignment = WRAP
        if i == 1:
            cell.fill, cell.font = HDR_FILL, HDR_FONT
for c, w in enumerate([12, 38, 60, 28], 1):
    qs.column_dimensions[get_column_letter(c)].width = w

# ---------------------------------------------------------------- notes sheet
ns = wb.create_sheet("notes")
notes = [
    "Windows: triples/uniqueness 30d svs (dt 2026-06-02..07-01); serving membership 37d (..07-08); CIL valuation week 2026-07-02..08; bills June 2026 x 12.",
    "Universe = 13,286,670,656 distinct usable (ip x domain x date) triples across all 10 sources. 'Usable' = domain consumable by DS13 or DS19.",
    "Block 1 rows are ranked by total triples desc (matches deck_d1's rank order); cumulative % = deduplicated UNION coverage of the top-N rows, NOT a running sum of column C.",
    "Free-log rows' numbers are their own totals; the free_logs UNION row is measured on the union (never a sum of the two rows). A component can exceed the union on 'standalone'-type cuts — see artifacts/audi_1089_deck_coverage.md for that decomposition.",
    "Column 'Touched won imps' is NOT additive across rows (an IP delivered by several sources counts for each). % col = share of the platform's 398,301,655 won imps (val wk) — it is NOT 'of touched bids, % that won'.",
    "Blocks 2/3 Profit columns are intentionally blank: margin parameters are internal-only; apply your own sheet formula.",
    f"Block 3: preemption applies to METERED vendors only (flat fees don't meter). bill_after = bill x (1 - free co-hold share of the vendor's visit-days), the AUDI-1093 fix. Metered total falls $812,397 -> ${POST_TOTAL:,.0f} (-${812397 - POST_TOTAL:,.0f}/yr).",
    "Block 4: 'HI/PP triples kept' counts signal volume on HI/PP IPs (drops roughly with column C); 'HI/PP-IP coverage %' counts audience MEMBERS still covered (stays 99%+ in every paid-drop scenario). Different grains on purpose — audience size is the IP-grain number.",
    "Blocks 5 vs 6 are the same split over two populations: ALL member IPs (audience-size lens) vs only IPs that got won impressions (delivery-reality lens). Coverage % can differ sharply between them — that gap is the point.",
    "Block 5/6 '% free covered' denominator = member IPs of that row (free-covered + vendor-only). Unscored in block 5 includes IPs never served in the valuation week; in block 6 every row was served.",
    "DS27 LaunchLabs appears in deck_d3's CSV as a registry context row (disabled, never metered) — deliberately excluded from this workbook.",
    "Rebuild: python3 artifacts/audi_1089_deck_workbook.py (rerun after the deck_d4/d5/d6 scans land to replace PENDING cells).",
]
ns.cell(row=1, column=1, value="Reading notes").font = Font(bold=True, size=12)
for i, t in enumerate(notes, 3):
    cell = ns.cell(row=i, column=1, value="• " + t)
    cell.alignment = WRAP
ns.column_dimensions["A"].width = 120

wb.save(OUT)
pend = 0
for sheet in wb.worksheets:
    for r in sheet.iter_rows():
        for c in r:
            if c.value == PENDING:
                pend += 1
print(f"wrote {OUT}")
print(f"pending cells: {pend} (deck_d4 {'LANDED' if d4 else 'running'}, "
      f"d5 {'LANDED' if d5 else 'running'}, d6 {'LANDED' if d6 else 'running'})")
print(f"post-preemption metered total ${POST_TOTAL:,.0f} (savings ${812397 - POST_TOTAL:,.0f})")
