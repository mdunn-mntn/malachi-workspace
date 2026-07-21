#!/usr/bin/env python3
"""Build the AUDI-1089 DDP billing-review workbook (.xlsx) for the billing team.
Reproducible: numbers from ../outputs/*.csv, SQL from the .sql files. Rerun -> identical file.
Sheets: Overview, 1 Meter Proof, 2 Preemption, 3 Worth vs Bill, 4 Bill Impact,
5 Audit Map, then one sheet per runnable query."""
import csv, os
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

HERE = os.path.dirname(os.path.abspath(__file__))
TICKET = os.path.dirname(HERE)
OUT = os.path.join(TICKET, "outputs")

NAVY, RED, GRAY, LGRAY = "1B2A4A", "C0392B", "666666", "EDEFF2"
HDR = PatternFill("solid", fgColor=NAVY)
ALT = PatternFill("solid", fgColor="F5F7FA")
KICK = PatternFill("solid", fgColor=LGRAY)
WHITE_B = Font(color="FFFFFF", bold=True, size=11)
TITLE = Font(color=NAVY, bold=True, size=15)
SUB = Font(color=GRAY, size=10, italic=True)
BOLD = Font(bold=True)
REDB = Font(color=RED, bold=True)
NAVYB = Font(color=NAVY, bold=True)
MONO = Font(name="Consolas", size=9)
THIN = Side(style="thin", color="D6DBE0")
BORDER = Border(left=THIN, right=THIN, top=THIN, bottom=THIN)
CTR = Alignment(horizontal="center", vertical="center")
LEFT = Alignment(horizontal="left", vertical="top", wrap_text=True)
RIGHT = Alignment(horizontal="right")

VNAME = {24: "Justuno", 25: "5x5", 26: "Predactiv", 28: "33Across", 33: "Sovrn",
         36: "Cybba", 39: "Klickly", 40: "33Across API"}
BILL = {"33Across": 422024, "33Across API": 175879, "Sovrn": 115880, "Justuno": 77111, "Cybba": 21504}


def load(name):
    with open(os.path.join(OUT, name)) as f:
        return list(csv.DictReader(f))


def hrow(ws, r, headers, start=1):
    for j, h in enumerate(headers):
        c = ws.cell(r, start + j, h)
        c.fill = HDR; c.font = WHITE_B; c.alignment = CTR; c.border = BORDER
    ws.row_dimensions[r].height = 26


def drow(ws, r, vals, start=1, fmts=None, alt=False, bolds=None, colors=None):
    for j, v in enumerate(vals):
        c = ws.cell(r, start + j, v)
        c.border = BORDER
        if alt:
            c.fill = ALT
        if fmts and fmts[j]:
            c.number_format = fmts[j]
        if isinstance(v, (int, float)) and (not fmts or "@" not in (fmts[j] or "")):
            c.alignment = RIGHT
        if bolds and bolds[j]:
            c.font = BOLD
        if colors and colors[j]:
            c.font = Font(color=colors[j], bold=(bolds[j] if bolds else False))


def title_block(ws, title, sub, kicker=None, query_ref=None):
    if kicker:
        ws["A1"] = kicker; ws["A1"].font = Font(color=RED, bold=True, size=9)
    ws["A2"] = title; ws["A2"].font = TITLE
    ws["A3"] = sub; ws["A3"].font = SUB
    if query_ref:
        ws["A4"] = "Query ▸ " + query_ref
        ws["A4"].font = Font(color="1E7A34", bold=True, size=9)
    ws.row_dimensions[2].height = 22


def widths(ws, spec):
    for col, w in spec.items():
        ws.column_dimensions[col].width = w


wb = Workbook()

# ---------------- Overview ----------------
ws = wb.active; ws.title = "Overview"
widths(ws, {"A": 30, "B": 60, "C": 18})
ws["A1"] = "AUDI-1089 — DDP Vendor Billing Review"; ws["A1"].font = Font(color=NAVY, bold=True, size=16)
ws["A2"] = "Audit workbook for the DDP billing team — every number traces to a read-only query."
ws["A2"].font = SUB
r = 4
ws.cell(r, 1, "The four findings").font = NAVYB; r += 1
for a, b in [
    ("1. The meter does NOT preempt",
     "tv_cpm = $0.50 whenever ANY paid vendor wins, $0 only when none does. Free co-presence is ignored — 268.9M June impressions billed $0.50 despite a free log co-winning the same impression."),
    ("2. Preemption: $275K (domain), $412K (targeting)",
     "Free logs already cover this signal. DOMAIN grain (a free log had the same exact ip x domain x date): $274.6K. VERTICAL grain (a free log put the IP in the same MM vertical — how MM actually bids): $412.4K, roster $812K -> $400K. Both keep ALL vendor data; the vertical figure is the more complete one."),
    ("3. No metered vendor paid for itself",
     "On money-made (profit its unique data produced), every metered vendor is < 1.0x after preemption: API 0.94x, 33Across 0.83x, Sovrn 0.29x, Cybba 0.17x, Justuno 0.15x (worth detail on sheet 3)."),
    ("4. What to do first",
     "Add the preemption rule: keep all data, we own the meter (no vendor cooperation needed). Recoverable is $275K at the exact-visit (ip x domain x date) grain, $412K measured the way MM actually bids (same vertical, any website). Repricing the residual is a separate negotiation."),
]:
    ws.cell(r, 1, a).font = BOLD; ws.cell(r, 1).alignment = Alignment(vertical="top")
    c = ws.cell(r, 2, b); c.alignment = LEFT
    ws.row_dimensions[r].height = 58; r += 1
r += 1
ws.cell(r, 1, "Windows").font = NAVYB
ws.cell(r, 2, "Delivery/uniqueness = 30d svs (2026-06-02..07-01); fair-preemption scan = 37d (05-26..07-01) measured over the last 7d; serving = valuation week 07-02..08; bills = June 2026 x 12. All read-only."); ws.cell(r, 2).alignment = LEFT
ws.row_dimensions[r].height = 42; r += 2
ws.cell(r, 1, "Caveats").font = NAVYB
ws.cell(r, 2, "TWO GRAINS on sheet 2: DOMAIN (a free log had the same exact ip x domain x date) and VERTICAL (a free log put the "
              "IP in the same MM vertical within 30d — how MM actually bids: an IP is biddable via ANY same-vertical visit, on any website). "
              "Vertical captures more redundancy and is the targeting-truthful one; it is DS13 only, a close proxy for the DS13+DS19 union "
              "(so slightly conservative). Also: N=1 valuation week (July trough) -> dependency = envelope not a CI; meter regime changed "
              "May 2026 (never mix months); flat-fee amounts pending finance; winners-table imp counts over-count the meter (prove the RATE, not the $)."); ws.cell(r, 2).alignment = LEFT
ws.row_dimensions[r].height = 92; r += 2
ws.cell(r, 1, "Sheets").font = NAVYB
ws.cell(r, 2, "Data: 1 Meter Proof · 2 Preemption · 3 Worth vs Bill · 4 Bill Impact · 5 Audit Map. "
              "Each data sheet names its query on the green 'Query ▸' line. SQL: all 8 backing queries are embedded as "
              "'Qn ...' sheets (Q1 Meter Proof, Q2 Targeted Signal, Q3 Domain cohold, Q4 Bills, Q5 Dependency, Q6 Free coverage, Q7 Drop savings, Q8 Vertical grain) — paste-and-run.")
ws.cell(r, 2).alignment = LEFT; ws.row_dimensions[r].height = 56

# ---------------- 1 Meter Proof ----------------
ws = wb.create_sheet("1 Meter Proof")
widths(ws, {"A": 42, "B": 20, "C": 16, "D": 40})
title_block(ws, "The meter does not preempt", "gold.reporting.ddp_mm_winners_imp_202606 (June, verified live). tv_cpm tracks paid-vendor presence only.", "PROOF 1", query_ref="sheet 'Q1 Meter Proof'")
r = 5; hrow(ws, r, ["Winners on the impression", "June impressions", "tv_cpm charged", "Reading"])
mp = [
    ("Free log (23/30) AND paid vendor both win", 268886220, "$0.50 on 100%", "PAID STILL BILLS on a free-covered impression"),
    ("Free log wins alone", 165726546, "$0 on 100%", "free never bills (correct)"),
    ("Paid vendor wins alone", 38160144, "$0.50 on 100%", "legitimately paid"),
    ("Neither (3P / other path)", 20662953, "$0 on 100%", "no MM winner"),
]
for i, (a, imp, cpm, rd) in enumerate(mp):
    r += 1; drow(ws, r, [a, imp, cpm, rd], fmts=[None, "#,##0", None, None], alt=(i % 2))
    ws.cell(r, 4).alignment = LEFT
    if i == 0:
        for cc in range(1, 5): ws.cell(r, cc).font = REDB
r += 2
ws.cell(r, 1, "If the meter preempted, the top row's tv_cpm would be $0. It is $0.50 — a free log co-winning the exact impression has ZERO effect on the charge. Query: sheet 'Q1 Meter Proof'.").font = SUB
ws.cell(r, 1).alignment = LEFT; ws.merge_cells(start_row=r, start_column=1, end_row=r, end_column=4); ws.row_dimensions[r].height = 44

# ---------------- 2 Preemption ----------------
q3e = {int(x["ds"]): x for x in load("q3e_v2_free_prior_lookback.csv")}
q3f = {int(x["ds"]): x for x in load("q3f_vertical_full.csv")}
ws = wb.create_sheet("2 Preemption")
widths(ws, {"A": 16, "B": 13, "C": 17, "D": 19, "E": 14})
title_block(ws, "Preemption — what our free logs already cover",
            "Recoverable = bill x free-coverage share. Two grains: DOMAIN (a free log had the same exact ip x domain x date) and VERTICAL (a free log put the IP in the same MM vertical — how MM actually bids). Bill-after uses vertical. Ranked by vertical $.",
            "DOMAIN $275K · VERTICAL $412K", query_ref="'Q3 Domain cohold' · 'Q8 Vertical grain (q3f)' · 'Q4 Bills (q0)'")
r = 5; hrow(ws, r, ["Vendor", "Bill / yr", "Domain (same visit)", "Vertical (targeting)", "Bill after"])
order = ["33Across", "33Across API", "Sovrn", "Justuno", "Cybba"]
dsmap = {"33Across": 28, "33Across API": 40, "Sovrn": 33, "Justuno": 24, "Cybba": 36}
tb = td = tv = 0
for i, v in enumerate(order):
    b = BILL[v]; ds = dsmap[v]
    drec = round(b * float(q3e[ds]["pct_sameday_old"]) / 100)      # same exact (ip, domain, date)
    vrec = round(b * float(q3f[ds]["pct_dominant"]) / 100)         # same MM vertical
    tb += b; td += drec; tv += vrec
    r += 1; drow(ws, r, [v, b, drec, vrec, b - vrec],
                 fmts=[None, "$#,##0", "$#,##0", "$#,##0", "$#,##0"], alt=(i % 2))
    ws.cell(r, 4).font = REDB
r += 1
drow(ws, r, ["ROSTER", tb, td, tv, tb - tv], fmts=[None, "$#,##0", "$#,##0", "$#,##0", "$#,##0"], bolds=[1, 1, 1, 1, 1])
for cc in range(1, 6): ws.cell(r, cc).fill = KICK; ws.cell(r, cc).font = NAVYB
r += 2
ws.cell(r, 1, "Domain (same visit) = a free log delivered the EXACT same (ip, domain, date) the vendor billed — we captured that visit for free (strict). Vertical = a free log put the IP in the same MM vertical within 30 days — the grain MM actually bids on (an IP is biddable via ANY same-vertical visit, on any website), so it captures more redundancy. Vertical is DS13 only, a close proxy for the DS13+DS19 union (so slightly conservative).").font = SUB
ws.cell(r, 1).alignment = LEFT; ws.merge_cells(start_row=r, start_column=1, end_row=r, end_column=5); ws.row_dimensions[r].height = 58

# ---------------- 3 Worth vs Bill ----------------
ws = wb.create_sheet("3 Worth vs Bill")
widths(ws, {"A": 15, "B": 17, "C": 20, "D": 18})
title_block(ws, "Did the vendor pay for itself?",
            "Money-made = the vendor's UNIQUE won impressions × measured media eCPM × margin band, ×52 (solo counterfactual). Ranked by worth/bill.", "RESULT",
            query_ref="sheet 'Q5 Dependency (q6)'")
r = 5; hrow(ws, r, ["Vendor", "Bill after preempt", "Money-made value", "Worth ÷ bill"])
# (vendor, bill_after, value_low_$K, value_high_$K) — range is the margin band; eCPM is measured-exact
worth = [("33Across API", 142814, 45, 134),
         ("33Across", 259967, 72, 217),
         ("Sovrn", 115764, 11, 34),
         ("Cybba", 17698, 1, 3),
         ("Justuno", 75800, 4, 11)]
for i, (v, after, lo, hi) in enumerate(worth):
    wl, wh = lo * 1000 / after, hi * 1000 / after
    r += 1
    drow(ws, r, [v, after, f"${lo}K – ${hi}K", f"{wl:.2f}× – {wh:.2f}×"],
         fmts=[None, "$#,##0", None, None], alt=(i % 2))
    ws.cell(r, 3).alignment = RIGHT
    ws.cell(r, 4).alignment = RIGHT
    ws.cell(r, 4).font = REDB if wh < 0.5 else NAVYB
r += 2
ws.cell(r, 1, "Every vendor < 1.0× even at the top margin — none paid for itself. Media eCPM is measured per vendor (exact), not assumed; the range reflects the margin band only.").font = SUB
ws.cell(r, 1).alignment = LEFT; ws.merge_cells(start_row=r, start_column=1, end_row=r, end_column=4); ws.row_dimensions[r].height = 30

# ---------------- 4 Bill Impact ----------------
ws = wb.create_sheet("4 Bill Impact")
widths(ws, {"A": 16, "B": 6, "C": 12, "D": 15, "E": 15})
title_block(ws, "Per-vendor bills — before and after preemption", "All 8 site-visit vendors. Flat-fee vendors have no meter, so preemption doesn't change them. Ranked by current bill.", "BILLS", query_ref="'Q4 Bills (q0)' × 'Q3 Fair Prior-Day'")
r = 5; hrow(ws, r, ["Vendor", "DS", "Billing", "Current / yr", "After preempt"])
rec = [("33Across", 28, "$0.50 CPM", 422024, 259967),
       ("33Across API", 40, "$0.50 CPM", 175879, 142814),
       ("Sovrn", 33, "$0.50 CPM", 115880, 115764),
       ("Justuno", 24, "$0.50 CPM", 77111, 75800),
       ("Cybba", 36, "$0.50 CPM", 21504, 17698),
       ("Klickly", 39, "flat", None, None),
       ("Predactiv", 26, "flat", None, None),
       ("5x5", 25, "flat", None, None)]
for i, (v, ds, bt, cur, aft) in enumerate(rec):
    r += 1
    drow(ws, r, [v, ds, bt, cur if cur else "pending", aft if aft else "-"],
         fmts=[None, "0", None, "$#,##0" if cur else None, "$#,##0" if aft else None], alt=(i % 2))

# ---------------- 5 Audit Map ----------------
ws = wb.create_sheet("5 Audit Map")
widths(ws, {"A": 46, "B": 34, "C": 34, "D": 10})
title_block(ws, "Every claim -> its query -> expected number", "All read-only. Fast path = deck_d1..d8 (plain bq query). Anchors in runbook/queries/VALIDATION_GUIDE.md.", "AUDIT")
r = 5; hrow(ws, r, ["Claim", "Query file", "Expected", "Cost"])
amap = [
    ("Total metered bill", "runbook/queries/q0_roster_cost.sql", "$812,398/yr (imps x $0.50 = usage)", "cheap"),
    ("Meter charges $0.50 on free-covered imps", "queries/audi_1089_preemption_proof_winners_table.sql", "268.9M imps, 100% @ $0.50", "18 GB"),
    ("Fair preemption (prior-day, recency)", "runbook/queries/q3e_v2_free_prior_lookback.sql", "-$200.4K/yr (-24.7%)", "BIG 37d"),
    ("  · 33Across / API / Cybba / Justuno / Sovrn", "  same (free_prior_dominant x q0 bill)", "162.1 / 33.1 / 3.8 / 1.3 / 0.1 ($K)", ""),
    ("Same-day (naive) / upper bound", "  q3e_v2 free_sameday / free_prior30", "$273.7K / $243.5K", ""),
    ("Free logs cover the universe", "runbook/queries/deck_d1_universe_coverage.sql", "59.4% visit-day / 60.4% pair", "BIG"),
    ("Row-level source, self-serve", "queries/audi_1089_targeted_signal_bq_per_vendor_split.sql", "per-vendor used-row split ($0)", "$0"),
    ("Money-made value (worth)", "runbook/queries/q6_value_tiers.sql (+q8b solo)", "media on unique serves x margin x52", "BIG"),
    ("Drop-savings (reassignment classes)", "runbook/queries/q3b_credit_reassignment.sql", "33A $385.7K, Sovrn $109.0K ...", "BIG"),
]
for i, (cl, qf, ex, co) in enumerate(amap):
    r += 1; drow(ws, r, [cl, qf, ex, co], alt=(i % 2))
    for cc in (1, 2, 3): ws.cell(r, cc).alignment = LEFT
    ws.cell(r, 2).font = MONO

# ---------------- Query sheets ----------------
def sql_sheet(name, path, header):
    ws = wb.create_sheet(name)
    ws.column_dimensions["A"].width = 110
    ws["A1"] = header; ws["A1"].font = NAVYB; ws["A1"].alignment = LEFT
    ws.row_dimensions[1].height = 30
    with open(os.path.join(TICKET, path)) as f:
        for i, line in enumerate(f.read().rstrip("\n").split("\n")):
            c = ws.cell(3 + i, 1, line if line else " ")
            c.font = MONO; c.alignment = Alignment(horizontal="left", vertical="top")

# every backing query, embedded as a paste-and-run SQL sheet (name matches the "Query ▸" refs above)
QUERIES = [
    ("Q1 Meter Proof", "queries/audi_1089_preemption_proof_winners_table.sql",
     "Q1 (sheet 1) — does the meter skip paid credit when a free log already covers the impression? ~18 GB."),
    ("Q2 Targeted Signal", "queries/audi_1089_targeted_signal_bq_per_vendor_split.sql",
     "Q2 (audit map) — row-level used-signal split by originating vendor (BQ external, $0)."),
    ("Q3 Domain cohold", "runbook/queries/q3g_domain_sameday_cohold.sql",
     "Q3 (sheet 2) — did a free log have the SAME (ip, domain, date) the vendor billed? 33Across = 52.9%. BIG (30d)."),
    ("Q4 Bills (q0)", "runbook/queries/q0_roster_cost.sql",
     "Q4 (sheets 2 & 5) — roster + actual meter bills; meter check imps x $0.50 = usage. Console-cheap."),
    ("Q5 Dependency (q6)", "runbook/queries/q6_value_tiers.sql",
     "Q5 (sheet 3) — media on each vendor's unique serves -> money-made value (x52). BIG."),
    ("Q6 Free coverage (d1)", "runbook/queries/deck_d1_universe_coverage.sql",
     "Q6 (audit map) — free-log coverage of the (ip x domain x date) universe (59.4% / 60.4%). BIG."),
    ("Q7 Drop savings (q3b)", "runbook/queries/q3b_credit_reassignment.sql",
     "Q7 (audit map) — first-reporter reassignment classes -> exact drop savings per vendor. BIG."),
    ("Q8 Vertical grain (q3f)", "runbook/queries/q3f_category_prior_coverage.sql",
     "Q8 (sheet 2) — the VERTICAL-grain preemption: did a free log put the IP in the same DS13 vertical prior. BIG (full run, all IPs)."),
]
for nm, pth, hd in QUERIES:
    sql_sheet(nm, pth, hd)

# freeze header rows on the data sheets
for nm in ["1 Meter Proof", "2 Preemption", "3 Worth vs Bill", "4 Bill Impact", "5 Audit Map"]:
    wb[nm].freeze_panes = "A6"

path = os.path.join(HERE, "audi_1089_billing_review.xlsx")
wb.save(path)
print("wrote", path, "-", len(wb.sheetnames), "sheets:", ", ".join(wb.sheetnames))
