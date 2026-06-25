"""INCR-75 — build the deliverable Excel workbook from the scorer CSVs.

Sheets:
  1. Funnel Waterfall      — start -> remaining per hard filter + tier/power split
  2. All Advertisers       — every advertiser, per-filter flags, failed_at_filter, tier (audit)
  3. Final Eligible (tiered)— eligible only, row-colored by tier, all user-required columns
  4. Method & Caveats      — definitions, targets, pitfalls
  5. Spend -> MDE curve     — achievable MDE vs monthly spend at INCR-75 eligible-cohort medians

Style cloned from ti_1053 build_deliverable.py (navy header, tier fills, freeze panes).
Reads ../outputs/incr_75_{all_flagged,final_tiered,funnel_counts}.csv.
"""
import csv
import math
import statistics
import sys
from pathlib import Path

import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "outputs"
BER = ROOT.parent / "ber_2250_incrementality_overhaul"  # reused TI-884 engine lives here
sys.path.insert(0, str(BER / "ti_884_power_sample_size_analysis" / "artifacts"))
from ti_884_mde_calculator import mde_binomial  # noqa: E402

TEST_MONTHS = 56 / 30.4

# ---- palette / styles ----
NAVY = PatternFill("solid", fgColor="1F3A5F")
TOP = PatternFill("solid", fgColor="E8F0E3")     # green
MID = PatternFill("solid", fgColor="FBF3E2")     # amber
LOW = PatternFill("solid", fgColor="F0F0F0")     # gray
GREEN = PatternFill("solid", fgColor="D6EAD0")
RED = PatternFill("solid", fgColor="F6D9D5")
AMBER = PatternFill("solid", fgColor="FBF0D0")
TIER_FILL = {"Top": TOP, "Mid": MID, "Low": LOW, "EXCLUDED": LOW}
WHITE = Font(color="FFFFFF", bold=True, size=11)
WRAP = Alignment(wrap_text=True, vertical="top")
CTR = Alignment(horizontal="center", vertical="top")
THIN = Border(*[Side(style="thin", color="D9D9D9")] * 4)

USD = '$#,##0'
PCT2 = '0.00%'
PP = '0.0" pp"'
NUM1 = '0.0'


def read_csv(name):
    return list(csv.DictReader(open(OUT / name)))


def fnum(v):
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def header_row(ws, headers, r):
    for j, h in enumerate(headers, 1):
        c = ws.cell(r, j, h)
        c.fill = NAVY; c.font = WHITE; c.alignment = WRAP; c.border = THIN


def title_block(ws, ncol, title, subtitle):
    ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=ncol)
    ws.cell(1, 1, title).font = Font(bold=True, size=13, color="1F3A5F")
    ws.merge_cells(start_row=2, start_column=1, end_row=2, end_column=ncol)
    s = ws.cell(2, 1, subtitle)
    s.font = Font(italic=True, size=9, color="555555"); s.alignment = WRAP
    ws.row_dimensions[2].height = 46


# column spec for the metric sheets: (csv_key, header, fmt, width)
# fmt: int | usd | pct (raw fraction) | pctx (already *100 -> /100) | pp | num | text
FINAL_COLS = [
    ("final_tier", "Tier", "text", 7),
    ("value_score", "Value score", "num", 10),
    ("advertiser_id", "AID", "int", 8),
    ("advertiser_name", "Advertiser", "text", 26),
    ("vertical_buckets", "Vertical", "text", 20),
    ("avg_monthly_spend", "Avg monthly spend", "usd", 15),
    ("ivr", "IVR", "pct", 8),
    ("cvr", "CVR", "pct", 9),
    ("mde_ivr_at_normal_pct", "IVR MDE @ normal spend", "pctx", 13),
    ("can_hit_ivr_5pct_8w", "Hit 5% IVR ≤8wk?", "text", 10),
    ("can_hit_ivr_10pct_8w", "Hit 10% IVR ≤8wk?", "text", 10),
    ("budget_for_mde_ivr_5pct", "Budget for 5% IVR MDE (test)", "usd", 15),
    ("budget_for_mde_ivr_10pct", "Budget for 10% IVR MDE (test)", "usd", 15),
    ("req_monthly_spend_ivr_10pct", "Req. monthly spend (10% IVR)", "usd", 15),
    ("extra_spend_ivr_10pct_abs", "Extra $ to hit 10% IVR", "usd", 14),
    ("extra_spend_ivr_10pct_pct", "Extra % over normal", "pctx", 11),
    ("ivr_ask_band", "Extra-ask band", "text", 12),
    ("close_to_ivr_min", "Close to IVR min?", "text", 9),
    ("can_hit_cvr_15pct_8w", "Hit 15% CVR ≤8wk?", "text", 10),
    ("budget_for_mde_cvr_15pct", "Budget for 15% CVR MDE (test)", "usd", 15),
    ("req_monthly_spend_cvr_15pct", "Req. monthly spend (15% CVR)", "usd", 15),
    ("close_to_cvr_min", "Close to CVR min?", "text", 9),
    ("prior_lift_pp", "Prior lift", "pp", 10),
    ("prior_lift_source", "Prior-lift source", "text", 13),
    ("mde_ivr_direct_56d_pct", "IVR MDE (direct 56d)", "pctx", 12),
    ("cpm", "CPM", "usd", 9),
    ("imps_per_ip", "Imps/IP", "num", 8),
    ("distinct_ips_30d", "Reach IPs (30d)", "int", 13),
]


def write_value_cell(ws, r, j, key, fmt, row):
    v = row.get(key, "")
    c = ws.cell(r, j); c.border = THIN; c.alignment = CTR if fmt in ("int", "num", "pp", "pctx") else WRAP
    if fmt == "text":
        c.value = v; c.alignment = WRAP if key in ("advertiser_name", "vertical_buckets", "prior_lift_source") else CTR
        return c
    num = fnum(v)
    if num is None:
        c.value = "—"; c.alignment = CTR; return c
    if fmt == "int":
        c.value = int(num)
    elif fmt == "usd":
        c.value = round(num); c.number_format = USD
    elif fmt == "pct":              # raw fraction
        c.value = num; c.number_format = PCT2
    elif fmt == "pctx":            # already *100 -> back to fraction for % format
        c.value = num / 100.0; c.number_format = PCT2
    elif fmt == "pp":
        c.value = num; c.number_format = PP
    elif fmt == "num":
        c.value = round(num, 1); c.number_format = NUM1
    return c


# ---------------- Sheet 1: Funnel waterfall ----------------
def sheet_funnel(wb, funnel, tiers, n_prior):
    ws = wb.create_sheet("1. Funnel Waterfall")
    title_block(ws, 6,
                "INCR-75 — Eligible-Advertiser Funnel for Incrementality Lift Tests",
                "Hard filters only (per request, spend/IVR-position/power are scored not cut). "
                "Starting universe = advertisers that delivered in the trailing 30 days. "
                f"{tiers['Top']+tiers['Mid']+tiers['Low']:,} eligible; {n_prior} have prior demonstrated lift.")
    r = 4
    header_row(ws, ["Step", "Filter", "Type", "Threshold", "Removed", "Remaining", "% of start"], r)
    for s in funnel:
        r += 1
        vals = [s["step"] if s["step"] != 99 else "→", s["filter"], s["type"], s["threshold"],
                int(s["removed"]), int(s["remaining"]), s["pct_of_start"]]
        for j, v in enumerate(vals, 1):
            c = ws.cell(r, j, v); c.border = THIN
            c.alignment = CTR if j in (1, 3, 5, 6, 7) else WRAP
            if j == 5 and int(s["removed"]) > 0:
                c.fill = RED
            if s["filter"].startswith("FINAL"):
                c.font = Font(bold=True, color="1F3A5F")
    # tier / power split block
    r += 2
    ws.cell(r, 1, "Eligible set — value tiers").font = Font(bold=True, size=11, color="1F3A5F"); r += 1
    header_row(ws, ["Tier", "Count", "Definition"], r)
    defns = {
        "Top": "Clears 5% IVR MDE at normal spend + mid-spend + movable IVR + low saturation (run first)",
        "Mid": "Clears 10% IVR MDE at normal spend, or 5% with an easy/stretch budget bump",
        "Low": "Eligible but needs a large budget bump to power, or saturated / spend far from sweet spot",
    }
    for t in ("Top", "Mid", "Low"):
        r += 1
        for j, v in enumerate([t, tiers[t], defns[t]], 1):
            c = ws.cell(r, j, v); c.border = THIN; c.fill = TIER_FILL[t]
            c.alignment = CTR if j in (1, 2) else WRAP
    widths = [7, 42, 8, 40, 11, 12, 11]
    for j, wd in enumerate(widths, 1):
        ws.column_dimensions[get_column_letter(j)].width = wd
    ws.freeze_panes = "A5"


# ---------------- Sheet 2: All advertisers (audit) ----------------
def sheet_all(wb, rows):
    ws = wb.create_sheet("2. All Advertisers")
    cols = [
        ("advertiser_id", "AID", "int", 8), ("advertiser_name", "Advertiser", "text", 26),
        ("vertical_buckets", "Vertical", "text", 20), ("active", "Active", "text", 7),
        ("is_b2b", "B2B?", "text", 6), ("avg_monthly_spend", "Avg monthly spend", "usd", 15),
        ("ivr", "IVR", "pct", 8), ("cvr", "CVR", "pct", 9),
        ("visiting_ips_30d", "Visiting IPs (30d)", "int", 12),
        ("distinct_ips_30d", "Reach IPs (30d)", "int", 12),
        ("pass_f1_clean_active", "F1 clean/active", "bool", 9),
        ("pass_f2_not_b2b", "F2 not-B2B", "bool", 9),
        ("pass_f3_measurable_ivr", "F3 measurable IVR", "bool", 9),
        ("failed_at_filter", "Disposition", "text", 16),
        ("final_tier", "Tier", "text", 9), ("value_score", "Value score", "num", 9),
    ]
    title_block(ws, len(cols),
                "INCR-75 — All Advertisers (audit trail)",
                "Every advertiser that delivered in the trailing 30d, with each hard-filter pass/fail and final "
                "disposition. PASSED rows are tiered; others show where they dropped out. Sorted PASSED-first by value score.")
    r = 4
    header_row(ws, [h for _, h, _, _ in cols], r)
    for row in rows:
        r += 1
        passed = row["failed_at_filter"] == "PASSED"
        base = TIER_FILL.get(row["final_tier"], LOW) if passed else LOW
        for j, (key, _h, fmt, _w) in enumerate(cols, 1):
            if fmt == "bool":
                val = row.get(key, "").upper() == "TRUE"
                c = ws.cell(r, j, "Y" if val else "N"); c.alignment = CTR; c.border = THIN
                c.fill = GREEN if val else RED
            else:
                c = write_value_cell(ws, r, j, key, fmt, row)
                c.fill = base
                if key == "failed_at_filter" and not passed:
                    c.fill = LOW
    for j, (_k, _h, _f, wd) in enumerate(cols, 1):
        ws.column_dimensions[get_column_letter(j)].width = wd
    ws.freeze_panes = "C5"
    ws.auto_filter.ref = f"A4:{get_column_letter(len(cols))}{r}"


# ---------------- Sheet 3: Final eligible (tiered) ----------------
def sheet_final(wb, rows):
    ws = wb.create_sheet("3. Final Eligible (tiered)")
    title_block(ws, len(FINAL_COLS),
                "INCR-75 — Final Eligible Advertisers (tiered Top → Low)",
                "All must-pass advertisers (not-B2B, active, measurable IVR), ranked by value score. "
                "MDE is RELATIVE (5% MDE on 2% IVR = detect 2.1%). All budgets at NO variance reduction. "
                "8-week test ≈ 1.84 months of spend. CVR columns are informational (need ~$2M+/mo). "
                "Highlights: green=Top, amber=Mid, gray=Low; Yes/No green/red.")
    r = 4
    header_row(ws, [h for _, h, _, _ in FINAL_COLS], r)
    for row in rows:
        r += 1
        fill = TIER_FILL.get(row["final_tier"], LOW)
        for j, (key, _h, fmt, _w) in enumerate(FINAL_COLS, 1):
            c = write_value_cell(ws, r, j, key, fmt, row)
            c.fill = fill
            if key in ("can_hit_ivr_5pct_8w", "can_hit_ivr_10pct_8w", "can_hit_cvr_15pct_8w",
                       "close_to_ivr_min", "close_to_cvr_min"):
                v = (row.get(key) or "").lower()
                if v == "yes":
                    c.fill = GREEN
                elif v == "no":
                    c.fill = RED
                elif v == "no_data":
                    c.fill = LOW
            if key == "ivr_ask_band":
                v = (row.get(key) or "")
                c.fill = {"easy": GREEN, "none": GREEN, "stretch": AMBER, "unreasonable": RED}.get(v, fill)
            if key == "prior_lift_pp" and fnum(row.get(key)) is not None:
                c.font = Font(bold=True, color="1F6B2E")
    for j, (_k, _h, _f, wd) in enumerate(FINAL_COLS, 1):
        ws.column_dimensions[get_column_letter(j)].width = wd
    ws.freeze_panes = "D5"
    ws.auto_filter.ref = f"A4:{get_column_letter(len(FINAL_COLS))}{r}"


# ---------------- Sheet 4: Method & Caveats ----------------
def sheet_method(wb, medians):
    ws = wb.create_sheet("4. Method & Caveats")
    ws.column_dimensions["A"].width = 120
    lines = [
        ("INCR-75 — Method & Caveats", "h"),
        ("Goal", "h2"),
        ("Identify live MNTN advertisers that are the best candidates for incremental-lift studies: measurable-but-"
         "movable IVR, smaller/lesser-known brands, mostly-net-new audience, powerable within 4–8 weeks.", "p"),
        ("Is MDE relative or absolute?", "h2"),
        ("RELATIVE. MDE_rel = MDE_abs / baseline. For a 0.5% IVR advertiser, a 5% MDE means detecting a 0.525% IVR "
         "(a 5% proportional lift, +0.025 percentage points) — NOT 5.5%. Matches how lift is reported at MNTN.", "p"),
        ("Reasonable MDE — IVR vs CVR", "h2"),
        ("IVR: both 5% (credible) and 10% (realistic) computed; eligibility tiers on 10%, Top tier requires 5% at "
         "normal spend. CVR is ~7–10x harder (baseline ~30x lower; MDE_rel ∝ √((1−p)/p) explodes as p→0) — a 5% CVR "
         "MDE needs ~$2–5M/mo, so CVR is INFORMATIONAL only (reported at a looser 15% target), never a gate.", "p"),
        ("What counts as 'enough' spend?", "h2"),
        ("Per-advertiser, not a flat number: enough = running 8 weeks at typical spend accumulates enough treated IPs "
         "to push IVR MDE ≤ target. Encoded in the power columns. Low-spend advertisers fail to power and lack the ROI "
         "to justify a test.", "p"),
        ("Reasonable extra-spend ask", "h2"),
        ("Banded label (never a cut): ≤25% over normal = easy, 25–50% = stretch, >50% = unreasonable.", "p"),
        ("Baseline definition (settled, TI-1019 §7b/§7e)", "h2"),
        ("IVR = distinct visiting∩served IPs / distinct served cost_impression_log.ip (per-IP Bernoulli rate). "
         "NOT graph.visits (event count, 3.36x inflated) or graph.usersreached (device-blended, ~2x). All-funnel grain, "
         "matching the team's MDE-prefill calculator + the gary-ql resolver.", "p"),
        ("Power math", "h2"),
        ("TI-884 Lewis-Rao two-proportion z-test, z=2.80 (α=0.05, power=0.80), 10% holdout, var_reduction=1.0 "
         "(no CUPED/ghost-ad/stratified reduction). Budget-for-MDE is the total test budget to reach the required "
         "distinct treated IPs; required monthly spend = budget / 1.84 (8wk ≈ 1.84 months).", "p"),
        (f"INCR-75 eligible-cohort medians: IVR {medians['ivr']*100:.2f}%, CVR {medians['cvr']*100:.3f}%, "
         f"CPM ${medians['cpm']:.2f}, imps/IP {medians['ipi']:.1f}.", "p"),
        ("Filter funnel", "h2"),
        ("HARD (membership): (1) clean & active; (2) not B2B — exclude the 'B2B Software & Services' vertical bucket; "
         "(3) measurable IVR — ≥100 visiting IPs and IVR>0. SCORED (tier, not cut): mid-spend sweet spot ($25k–$200k/mo), "
         "IVR band position (peak 3–6%, >12% = saturated/hard-to-move), powerability at 5%/10%, brand-size (spend rank + "
         "reach-to-spend), audience saturation (reach-to-spend), and a prior-demonstrated-lift bonus.", "p"),
        ("Pitfalls", "h2"),
        ("• spend_required uses 30d imps/IP and is an OPTIMISTIC floor for large budget gaps (imps/IP grows with window "
         "length); the 'IVR MDE (direct 56d)' column is the no-extrapolation cross-check.\n"
         "• Ghost-bid frequency-cap bias affects the eventual LIFT estimate (conservative), not this POWER screen — "
         "eligibility ≠ guaranteed lift.\n"
         "• CVR MDE reported only when ≥50 converting IPs (else small-p noise).\n"
         "• Managed-service advertisers can log ~$0 spend with real delivery; ratios are SAFE-guarded.\n"
         "• Prior lift: TI-933 = Select clickpass visit-rate pp (significant only); TI-837 = guid total-traffic pp "
         "(all-funnel, includes retargeting — a permissive 'has shown lift' signal, not a prospecting-incrementality "
         "estimate).", "p"),
        ("Sources", "h2"),
        ("Metrics: queries/incr_75_advertiser_metrics.sql (forks TI-1019). Engine: TI-884 ti_884_mde_calculator.py. "
         "Prior lift: TI-933 ti_933_per_advertiser_lift.csv, TI-837 ti_837_lift_30adv_7day_v5. Window: trailing 30d "
         "(rates/CPM/imps-per-IP), trailing 56d (direct power), trailing 12mo (typical-active-month spend).", "p"),
    ]
    r = 1
    for text, kind in lines:
        c = ws.cell(r, 1, text); c.alignment = Alignment(wrap_text=True, vertical="top")
        if kind == "h":
            c.font = Font(bold=True, size=14, color="1F3A5F")
        elif kind == "h2":
            c.font = Font(bold=True, size=11, color="1F3A5F")
        else:
            c.font = Font(size=10, color="333333")
            ws.row_dimensions[r].height = 14 * (1 + text.count("\n") + len(text) // 110)
        r += 1
    ws.sheet_view.showGridLines = False


# ---------------- Sheet 5: Spend -> MDE curve ----------------
def sheet_curve(wb, medians):
    ws = wb.create_sheet("5. Spend → MDE curve")
    title_block(ws, 5,
                "Spend → Achievable MDE (at INCR-75 eligible-cohort medians)",
                f"At median IVR {medians['ivr']*100:.2f}%, CVR {medians['cvr']*100:.3f}%, CPM ${medians['cpm']:.2f}, "
                f"imps/IP {medians['ipi']:.1f}. var_reduction=1.0. Read: a $X/mo advertiser detects ≥ this relative lift "
                "in an 8-week test. Use to size how much budget unlocks a target MDE.")
    r = 4
    header_row(ws, ["Monthly spend", "8-wk test budget", "IVR MDE (rel)", "CVR MDE (rel)", "IVR verdict @5%"], r)
    grid = [10_000, 25_000, 50_000, 75_000, 100_000, 150_000, 200_000, 300_000, 500_000, 1_000_000]
    for ms in grid:
        r += 1
        test_budget = ms * TEST_MONTHS
        imps = test_budget / medians["cpm"] * 1000
        treated = imps / medians["ipi"]
        holdout = treated * (0.10 / 0.90)
        _, mv = mde_binomial(treated, holdout, medians["ivr"], var_reduction=1.0)
        _, mc = mde_binomial(treated, holdout, medians["cvr"], var_reduction=1.0)
        verdict = "well-powered" if mv <= 0.05 else "borderline" if mv <= 0.10 else "underpowered"
        cells = [ms, round(test_budget), mv, mc, verdict]
        for j, v in enumerate(cells, 1):
            c = ws.cell(r, j, v); c.border = THIN; c.alignment = CTR
            if j in (1, 2):
                c.number_format = USD
            if j in (3, 4):
                c.number_format = PCT2
            if j == 5:
                c.fill = {"well-powered": GREEN, "borderline": AMBER, "underpowered": RED}[verdict]
    for j, wd in enumerate([16, 16, 14, 14, 16], 1):
        ws.column_dimensions[get_column_letter(j)].width = wd
    ws.freeze_panes = "A5"


def main():
    all_rows = read_csv("incr_75_all_flagged.csv")
    final_rows = read_csv("incr_75_final_tiered.csv")
    funnel = read_csv("incr_75_funnel_counts.csv")
    tiers = {t: sum(1 for x in final_rows if x["final_tier"] == t) for t in ("Top", "Mid", "Low")}
    n_prior = sum(1 for x in final_rows if x.get("has_prior_lift", "").upper() == "TRUE")

    elig_ivr = [fnum(x["ivr"]) for x in final_rows if fnum(x["ivr"])]
    elig_cvr = [fnum(x["cvr"]) for x in final_rows if fnum(x["cvr"])]
    elig_cpm = [fnum(x["cpm"]) for x in final_rows if fnum(x["cpm"])]
    elig_ipi = [fnum(x["imps_per_ip"]) for x in final_rows if fnum(x["imps_per_ip"])]
    medians = {"ivr": statistics.median(elig_ivr), "cvr": statistics.median(elig_cvr),
               "cpm": statistics.median(elig_cpm), "ipi": statistics.median(elig_ipi)}

    wb = openpyxl.Workbook()
    wb.remove(wb.active)
    sheet_funnel(wb, funnel, tiers, n_prior)
    sheet_all(wb, all_rows)
    sheet_final(wb, final_rows)
    sheet_method(wb, medians)
    sheet_curve(wb, medians)

    path = OUT / "incr_75_eligible_advertisers.xlsx"
    wb.save(path)
    print(f"wrote {path}")
    print(f"  Sheet 2 rows: {len(all_rows)}  Sheet 3 rows: {len(final_rows)}  tiers: {tiers}  prior-lift: {n_prior}")
    print(f"  cohort medians: IVR {medians['ivr']*100:.2f}% CVR {medians['cvr']*100:.3f}% "
          f"CPM ${medians['cpm']:.2f} imps/IP {medians['ipi']:.1f}")


if __name__ == "__main__":
    main()
