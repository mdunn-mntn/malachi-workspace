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
    ("final_tier", "Value tier", "text", 8),
    ("value_score", "Value score (0–100)", "num", 11),
    ("advertiser_id", "Advertiser ID", "int", 11),
    ("advertiser_name", "Advertiser", "text", 26),
    ("vertical_buckets", "Industry (vertical)", "text", 20),
    ("avg_monthly_spend", "Avg monthly spend", "usd", 15),
    ("ivr", "Visit rate (IVR)", "pct", 11),
    ("cvr", "Conversion rate (CVR)", "pct", 11),
    ("mde_ivr_at_normal_pct", "Smallest IVR lift detectable at current spend", "pctx", 15),
    ("can_hit_ivr_5pct_8w", "Can detect 5% IVR lift in ≤8 wks?", "text", 12),
    ("can_hit_ivr_10pct_8w", "Can detect 10% IVR lift in ≤8 wks?", "text", 12),
    ("budget_for_mde_ivr_5pct", "Total test spend to detect 5% IVR lift", "usd", 15),
    ("budget_for_mde_ivr_10pct", "Total test spend to detect 10% IVR lift", "usd", 15),
    ("req_monthly_spend_ivr_10pct", "Monthly spend needed (10% IVR)", "usd", 15),
    ("extra_spend_ivr_10pct_abs", "Extra spend needed (10% IVR)", "usd", 14),
    ("extra_spend_ivr_10pct_pct", "Extra spend, % over current", "pctx", 12),
    ("ivr_ask_band", "Budget-ask feasibility", "text", 13),
    ("close_to_ivr_min", "IVR spend feasible (at/near min)?", "text", 12),
    ("mde_cvr_at_normal_pct", "Smallest CVR lift detectable at current spend", "pctx", 15),
    ("can_hit_cvr_15pct_8w", "Can power a loose 15% CVR test in ≤8 wks?", "text", 12),
    ("budget_for_mde_cvr_15pct", "Total test spend to detect 15% CVR lift", "usd", 15),
    ("req_monthly_spend_cvr_15pct", "Monthly spend needed (15% CVR)", "usd", 15),
    ("close_to_cvr_min", "CVR spend feasible (at/near min)?", "text", 12),
    ("prior_lift_pp", "Prior measured lift", "pp", 11),
    ("prior_lift_source", "Prior-lift source", "text", 14),
    ("mde_ivr_direct_56d_pct", "IVR lift detectable (measured 8-wk reach)", "pctx", 14),
    ("cpm", "CPM (cost/1k imps)", "usd", 11),
    ("imps_per_ip", "Impressions per IP", "num", 10),
    ("distinct_ips_30d", "Unique IPs reached (30d)", "int", 13),
    # ---- CURRENT LIFT (live ghost-bid, MNTN clean leg — added 2026-07-02) ----
    ("in_ghost_table", "In live ghost-bid table?", "text", 10),
    ("current_lift_confirms", "Does current lift confirm the score?", "text", 16),
    ("current_lift_signal", "Current-lift signal", "text", 14),
    ("current_rel_lift", "Current lift (relative)", "relx", 12),
    ("current_abs_lift_pp", "Current lift (abs, bid-grain ITT)", "pp3", 13),
    ("ghost_vis_clean", "Holdout visits (power)", "int", 11),
    ("current_z", "Current-lift z", "num", 9),
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
    elif fmt == "pp3":                 # small percentage-point value (bid-grain ITT)
        c.value = num; c.number_format = '0.000" pp"'
    elif fmt == "relx":               # already in percent-points (18.0 -> "18.0%")
        c.value = round(num, 1); c.number_format = '0.0"%"'
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
        ("advertiser_id", "Advertiser ID", "int", 11), ("advertiser_name", "Advertiser", "text", 26),
        ("vertical_buckets", "Industry (vertical)", "text", 20), ("active", "Active?", "text", 8),
        ("is_b2b", "B2B?", "text", 6), ("avg_monthly_spend", "Avg monthly spend", "usd", 15),
        ("ivr", "Visit rate (IVR)", "pct", 11), ("cvr", "Conversion rate (CVR)", "pct", 11),
        ("visiting_ips_30d", "Visiting IPs (30d)", "int", 12),
        ("distinct_ips_30d", "Unique IPs reached (30d)", "int", 12),
        ("pass_f1_clean_active", "Pass: clean & active", "bool", 10),
        ("pass_f2_not_b2b", "Pass: not B2B", "bool", 9),
        ("pass_f3_measurable_ivr", "Pass: measurable IVR", "bool", 10),
        ("failed_at_filter", "Disposition", "text", 16),
        ("final_tier", "Value tier", "text", 9), ("value_score", "Value score (0–100)", "num", 11),
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
            if key == "current_lift_confirms":
                v = (row.get(key) or "")
                c.fill = {"CONFIRMED": GREEN, "positive": GREEN, "CONTRADICTED": RED,
                          "negative": RED, "unconfirmed(underpowered)": AMBER,
                          "null": LOW, "no_data": LOW}.get(v, fill)
            if key == "current_lift_signal":
                v = (row.get(key) or "")
                c.fill = {"positive_sig": GREEN, "negative_sig": RED}.get(v, fill)
            if key == "in_ghost_table":
                c.fill = GREEN if (row.get(key) == "Y") else LOW
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
         "MDE needs ~$2–5M/mo, so CVR is INFORMATIONAL only, never a gate. The 15% CVR target is a FEASIBILITY CEILING "
         "(a tight CVR MDE is unaffordable for nearly all advertisers), NOT a claim that CVR lifts are ~15% — if "
         "anything CVR's true relative lift is comparable to or smaller than IVR's. Judge CVR on the 'Smallest CVR lift "
         "detectable at current spend' column, not the 15% yes/no.", "p"),
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
         "• Ghost-bid lift artifacts (bid-multiplicity selection; gate to clean ghost_frac) affect the eventual LIFT "
         "estimate, not this POWER screen — eligibility ≠ guaranteed lift. Matt Brorby's population run shows internal "
         "lift is ~0 today, monotonically rising from top-intent (≈0) to mid-intent (highest) — i.e. movability lives "
         "in mid-IVR, which is what this screen rewards.\n"
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


# ---------------- Sheet 6: Column glossary (appendix) ----------------
# (section, column-as-shown, plain definition, how it's computed / notes)
GLOSSARY = [
    ("§", "IDENTITY & RANKING", "", ""),
    ("", "Value tier", "Priority bucket for running a test. Top = run first; Low = eligible but lowest priority.",
     "Top = can detect a 5% IVR lift at current spend + mid-spend + movable IVR + unsaturated. Mid = can detect 10% at current spend (or 5% with a small bump). Low = needs a big budget bump or is saturated / off the spend sweet-spot. (EXCLUDED on the All-Advertisers sheet = failed a hard filter.)"),
    ("", "Value score (0–100)", "Composite ranking score within the eligible set; higher = better candidate.",
     "Power margin (30) + mid-spend fit (20) + smaller-brand/movability (20) + IVR-band position (15) + low audience saturation (15) + prior-lift bonus (+10)."),
    ("", "Advertiser ID", "MNTN advertiser_id.", "From core advertiser dimension."),
    ("", "Advertiser", "Advertiser company name.", "advertisers.company_name."),
    ("", "Industry (vertical)", "Advertiser's industry bucket(s).", "fpa_advertiser_verticals (type=0 industry bucket)."),

    ("§", "SPEND & UNIT ECONOMICS", "", ""),
    ("", "Avg monthly spend", "The advertiser's typical monthly budget — the baseline we test against.",
     "Median monthly media spend across active months (>$1k) over the last 12 months. Robust to on/off & seasonal advertisers."),
    ("", "CPM (cost/1k imps)", "Advertiser-paid cost per 1,000 impressions.",
     "(media + data + platform spend) ÷ impressions × 1000, trailing 30d."),
    ("", "Impressions per IP", "Average ad frequency — impressions delivered per unique IP.",
     "impressions ÷ distinct served IPs (30d). Higher = more frequency, fewer NEW IPs per dollar."),
    ("", "Unique IPs reached (30d)", "Reach: distinct IPs served at least one ad in the last 30 days.",
     "COUNT(DISTINCT ip) from cost_impression_log, 30d."),

    ("§", "RATES (the baseline we measure lift on)", "", ""),
    ("", "Visit rate (IVR)", "Share of served households (IPs) that then visited the site. The headline KPI.",
     "distinct visiting-AND-served IPs ÷ distinct served IPs (30d). A per-IP probability, NOT impressions-based."),
    ("", "Conversion rate (CVR)", "Share of served households (IPs) that converted. ~30× rarer than visits.",
     "distinct converting-AND-served IPs ÷ distinct served IPs (30d)."),

    ("§", "IVR POWER — can we detect a lift? (this is the eligibility driver)", "", ""),
    ("", "Smallest IVR lift detectable at current spend",
     "The minimum lift an 8-week test could prove at the advertiser's current spend. Lower = better powered.",
     "MDE is RELATIVE: 3% means a 3% proportional lift (e.g. 2.0%→2.06% IVR), NOT 3 percentage points. Computed from current 8-wk reach, no variance reduction."),
    ("", "Can detect 5% IVR lift in ≤8 wks?", "Yes = an 8-week test at current spend can prove a 5% lift (the credible bar).",
     "Yes if current 8-wk spend reaches the IPs needed for a 5% relative IVR MDE."),
    ("", "Can detect 10% IVR lift in ≤8 wks?", "Same, for a 10% lift (the realistic bar — easier to clear).",
     "Yes if current 8-wk spend reaches the IPs needed for a 10% relative IVR MDE."),
    ("", "Total test spend to detect 5% / 10% IVR lift",
     "Total dollars over the whole test needed to prove that lift.",
     "TI-884 Lewis-Rao: dollars to reach the required distinct served IPs, at the advertiser's own CPM & impressions/IP, no variance reduction. 5% costs ~4× the 10% figure."),
    ("", "Monthly spend needed (10% IVR)", "The total-test figure expressed as a monthly run-rate.",
     "Total test spend ÷ 1.84 (an 8-week test ≈ 1.84 months)."),
    ("", "Extra spend needed (10% IVR)", "Additional dollars beyond what they'd spend anyway, to reach the 10% bar. $0 if already powered.",
     "max(0, total test spend for 10% − current 8-wk spend)."),
    ("", "Extra spend, % over current", "That extra as a percentage of their current 8-week spend.",
     "extra ÷ (current 8-wk spend)."),
    ("", "Budget-ask feasibility", "How big the budget ask is, in plain terms.",
     "none = already powered · easy = ≤25% more · stretch = 25–50% more · unreasonable = >50% more."),
    ("", "IVR spend feasible (at/near min)?", "Is this advertiser spend-feasible for an IVR test? Yes = already AT/OVER the IVR spend minimum (no ask needed) OR a reasonable (≤50%) bump away. No = would need an unreasonable (>50%) spend increase. Already-spending-plenty advertisers are Yes (not an issue).",
     "Yes if current 8-wk spend ≥ (10% IVR budget) / 1.5. One-sided — spending well over the minimum is never flagged No."),
    ("", "IVR lift detectable (measured 8-wk reach)",
     "Cross-check of the 'at current spend' column using ACTUAL reach, no extrapolation.",
     "MDE from the real distinct IPs reached in the last 56 days. Should roughly match the modeled column; large gaps flag a frequency/reach quirk."),

    ("§", "CVR POWER — informational only (conversions need ~$2M+/mo)", "", ""),
    ("", "Smallest CVR lift detectable at current spend",
     "The honest per-advertiser CVR read — judge CVR on THIS, not the 15% bar. Usually large (10s–100s %) because the CVR base rate is ~30× lower than IVR.",
     "Same Lewis-Rao math as IVR, on the CVR base rate. Lower = better powered. '—' if <50 converting IPs."),
    ("", "Can power a loose 15% CVR test in ≤8 wks?",
     "Yes = even a deliberately LOOSE 15% CVR bar is powered (usually No). 15% is a FEASIBILITY CEILING — a tight CVR MDE is unaffordable for nearly all advertisers — NOT a claim that CVR lifts are ~15%.",
     "Yes if current 8-wk spend reaches the IPs for a 15% relative CVR MDE. 'no_data' if <50 converting IPs."),
    ("", "Total test spend to detect 15% CVR lift", "Total dollars to prove even a loose 15% conversion lift.",
     "Same math as IVR, on the CVR baseline. Far higher because CVR is ~30× rarer."),
    ("", "Monthly spend needed (15% CVR)", "The CVR total-test figure as a monthly run-rate.", "Total ÷ 1.84."),
    ("", "CVR spend feasible (at/near min)?", "Same as the IVR flag, for the loose 15% CVR bar: Yes = already at/over OR a ≤50% bump away; No = needs >50% more; 'no_data' if <50 converting IPs.", "Yes if current 8-wk spend ≥ (15% CVR budget) / 1.5."),

    ("§", "PRIOR EVIDENCE", "", ""),
    ("", "Prior measured lift", "A positive lift this advertiser already showed in a past MNTN test (bonus signal).",
     "In percentage points (pp). Blank if none."),
    ("", "Prior-lift source", "Which past study the prior lift came from.",
     "TI-933 = Select clickpass visit-rate test (significant only). TI-837 = ghost-bid guid total-traffic (all-funnel; permissive 'has shown lift' signal)."),

    ("§", "CURRENT LIFT — live ghost-bid holdout (added 2026-07-02)", "", ""),
    ("", "In live ghost-bid table?", "Whether the advertiser appears in Matt Brorby's live ghost-bid tables (has real holdout-vs-treatment data now).",
     "Y if present in enriched__dev_matthewbrorby.lift__ghost_bid_visits (rolling ~10-day window; logging live since 2026-05-27). 1,182 advertisers present."),
    ("", "Does current lift confirm the score?", "The headline reconciliation: does the ACTUAL measured lift agree with the a-priori score? CONFIRMED = Top/Mid tier with significant positive current lift; CONTRADICTED = significant negative; unconfirmed = not yet powered enough to tell.",
     "CONFIRMED / CONTRADICTED / unconfirmed(underpowered) / no_data. Significance gate: p<0.05 AND ≥20 holdout visits. Green=confirmed, red=contradicted, amber=unconfirmed."),
    ("", "Current-lift signal", "The raw per-advertiser verdict from the ghost-bid holdout.",
     "positive_sig / negative_sig (p<0.05 & ≥20 holdout clean visits) or null/underpowered."),
    ("", "Current lift (relative)", "The measured lift as a % of the holdout baseline — the number to lead with. E.g. +18% = treatment visit rate is 18% above the never-served holdout.",
     "(treat_vr − ghost_vr) / ghost_vr on the debiased clean set. Directional; z is N-inflated so rank by this, not by z."),
    ("", "Current lift (abs, bid-grain ITT)", "The same lift in percentage points. Small because it is bid-grain ITT — measured across ALL bid-eligible IPs, diluted by win-rate (scale by win-rate for a served-user ATT figure).",
     "treat_vr − ghost_vr (percentage points). Earliest-bid-anchored 7d visit window, clean ghost_frac gate."),
    ("", "Holdout visits (power)", "How many holdout (ghost) IPs actually visited — the binding sample-size for detecting lift. Higher = more trustworthy.",
     "Distinct holdout IPs with a visit in the clean set. <20 ⇒ underpowered (signal forced to null)."),
    ("", "Current-lift z / p", "Two-proportion z-test of treatment vs holdout visit rate.",
     "z inflated by the millions of IPs/advertiser — a large z at a tiny magnitude is the bias floor, not proof of a big effect. Use p only as a floor; judge on relative-lift magnitude + direction."),
    ("", "Debias & caveats", "Why these numbers are trustworthy AND their limits.",
     "This is the MNTN bidder leg (clean reference: ghost_frac barely drifts 0.095→0.116 vs Beeswax 0.10→0.47). Debias reproduces the documented negative→positive sign flip (pooled −0.034pp→+0.049pp). Limits: rolling 10-day window (≥30d unavailable), 7d visit window truncates for late-first-bid IPs, bid-grain ITT. Publish as directional per INCR-69 gate."),

    ("§", "ALL-ADVERTISERS SHEET — extra columns", "", ""),
    ("", "Active?", "Whether the advertiser is currently active.", "advertisers.active."),
    ("", "B2B?", "Whether the advertiser is B2B (these are excluded).", "In the 'B2B Software & Services' vertical bucket."),
    ("", "Visiting IPs (30d)", "Distinct IPs that visited the site (the IVR numerator).", "COUNT(DISTINCT ip) from clickpass_log ∩ served, 30d."),
    ("", "Pass: clean & active / not B2B / measurable IVR", "The three hard filters (Y/N).",
     "clean & active = active, named, served. not B2B = not in the B2B bucket. measurable IVR = ≥100 visiting IPs & IVR>0."),
    ("", "Disposition", "Outcome: PASSED (eligible) or the first filter it failed.", "PASSED / F1_clean_active / F2_not_b2b / F3_measurable_ivr."),
]


def sheet_glossary(wb):
    ws = wb.create_sheet("6. Column Glossary")
    title_block(ws, 3,
                "Column Glossary (appendix)",
                "Plain-English definition of every column on the 'All Advertisers' and 'Final Eligible' sheets. "
                "Key reminder: every lift / MDE figure is RELATIVE — a 5% MDE on a 0.5% visit rate means detecting 0.525%, "
                "not 5.5%. All budgets assume NO variance reduction; an 8-week test ≈ 1.84 months of spend.")
    r = 4
    header_row(ws, ["Column", "What it means", "How it's computed / notes"], r)
    for tag, col, definition, computed in GLOSSARY:
        r += 1
        if tag == "§":
            ws.merge_cells(start_row=r, start_column=1, end_row=r, end_column=3)
            c = ws.cell(r, 1, col); c.font = Font(bold=True, color="FFFFFF", size=10.5)
            c.fill = PatternFill("solid", fgColor="33506E"); c.alignment = Alignment(vertical="center")
            ws.row_dimensions[r].height = 18
            continue
        cells = [(col, Font(bold=True, size=10, color="1F3A5F")), (definition, Font(size=10, color="222222")),
                 (computed, Font(size=9, color="555555"))]
        for j, (val, fnt) in enumerate(cells, 1):
            c = ws.cell(r, j, val); c.font = fnt; c.alignment = WRAP; c.border = THIN
        ws.row_dimensions[r].height = max(28, 12 * (1 + max(len(definition) // 48, len(computed) // 58)))
    for j, wd in enumerate([34, 50, 62], 1):
        ws.column_dimensions[get_column_letter(j)].width = wd
    ws.freeze_panes = "A5"
    ws.sheet_view.showGridLines = False


# ---------------- Sheet 7: Current Lift (live ghost-bid) ----------------
CL_COLS = [
    ("rank", "#", 5), ("final_tier", "Tier", 6), ("advertiser_id", "Advertiser ID", 11),
    ("advertiser_name", "Advertiser", 26), ("value_score", "Value score", 10),
    ("ivr", "IVR", 9), ("current_rel_lift", "Current lift (relative)", 12),
    ("current_abs_lift_pp", "Current lift (abs pp)", 12), ("ghost_vis_clean", "Holdout visits (power)", 12),
    ("current_z", "z", 7), ("current_p", "p", 8), ("prior_lift_pp", "Prior lift", 9),
]


def sheet_current_lift(wb, final_rows):
    ws = wb.create_sheet("7. Current Lift (ghost-bid)")
    confirmed = [r for r in final_rows if r.get("current_lift_confirms") == "CONFIRMED"]
    confirmed.sort(key=lambda r: -(fnum(r.get("current_rel_lift")) or -1e9))
    counts = {}
    for t in ("Top", "Mid"):
        sub = [r for r in final_rows if r["final_tier"] == t]
        counts[t] = {k: sum(1 for r in sub if r.get("current_lift_confirms") == k)
                     for k in ("CONFIRMED", "CONTRADICTED", "unconfirmed(underpowered)", "no_data")}
    title_block(ws, len(CL_COLS),
                "INCR-75 — Current Measured Lift (live ghost-bid holdout, MNTN clean leg)",
                "Actual treatment-vs-holdout visit lift from Matt Brorby's live ghost-bid tables "
                "(enriched__dev_matthewbrorby.lift__ghost_bid_visits), debiased per his bias register "
                "(clean ghost_frac gate + single earliest-bid anchor). Window = rolling ~10 days "
                "(2026-06-22..07-01); logging live since 2026-05-27 so ≥30 days is not yet available. "
                "Shortlist below = Top/Mid tier advertisers whose current lift CONFIRMS the a-priori score "
                "(significant positive, ≥20 holdout visits). READ RELATIVE LIFT + DIRECTION, not z (N-inflated); "
                "absolute pp are bid-grain ITT (diluted by win-rate). Publish as directional, not a point estimate.")
    # summary block
    r = 4
    ws.cell(r, 1, "Confirmation vs a-priori tier").font = Font(bold=True, size=11, color="1F3A5F"); r += 1
    header_row(ws, ["Tier", "Confirmed +lift", "Contradicted", "Unconfirmed (underpowered)", "No data"], r)
    for t in ("Top", "Mid"):
        r += 1
        vals = [t, counts[t]["CONFIRMED"], counts[t]["CONTRADICTED"],
                counts[t]["unconfirmed(underpowered)"], counts[t]["no_data"]]
        for j, v in enumerate(vals, 1):
            c = ws.cell(r, j, v); c.border = THIN; c.alignment = CTR; c.fill = TIER_FILL[t]
            if j == 2 and v: c.fill = GREEN
            if j == 3 and v: c.fill = RED
    r += 2
    ws.cell(r, 1, f"Strongest candidates — high score AND confirmed positive current lift "
                  f"({len(confirmed)}), ranked by relative lift").font = Font(bold=True, size=11, color="1F3A5F")
    r += 1
    header_row(ws, [h for _, h, _ in CL_COLS], r)
    for i, row in enumerate(confirmed, 1):
        r += 1
        fill = TIER_FILL.get(row["final_tier"], LOW)
        for j, (key, _h, _w) in enumerate(CL_COLS, 1):
            if key == "rank":
                c = ws.cell(r, j, i); c.alignment = CTR; c.border = THIN; c.fill = fill; continue
            fmt = {"advertiser_id": "int", "advertiser_name": "text", "value_score": "num",
                   "ivr": "pct", "current_rel_lift": "relx", "current_abs_lift_pp": "pp3",
                   "ghost_vis_clean": "int", "current_z": "num", "current_p": "num",
                   "prior_lift_pp": "pp", "final_tier": "text"}[key]
            c = write_value_cell(ws, r, j, key, fmt, row); c.fill = fill
            if key == "current_rel_lift":
                c.font = Font(bold=True, color="1F6B2E")
            if key == "final_tier":
                c.fill = TIER_FILL.get(row["final_tier"], LOW)
    for j, (_k, _h, wd) in enumerate(CL_COLS, 1):
        ws.column_dimensions[get_column_letter(j)].width = wd
    ws.freeze_panes = "A5"
    ws.sheet_view.showGridLines = False


def main():
    all_rows = read_csv("incr_75_all_flagged.csv")
    final_rows = read_csv("incr_75_eligible_with_current_lift.csv")  # tiered + current-lift columns
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
    sheet_glossary(wb)
    sheet_current_lift(wb, final_rows)

    path = OUT / "incr_75_eligible_advertisers.xlsx"
    wb.save(path)
    print(f"wrote {path}")
    print(f"  Sheet 2 rows: {len(all_rows)}  Sheet 3 rows: {len(final_rows)}  tiers: {tiers}  prior-lift: {n_prior}")
    print(f"  cohort medians: IVR {medians['ivr']*100:.2f}% CVR {medians['cvr']*100:.3f}% "
          f"CPM ${medians['cpm']:.2f} imps/IP {medians['ipi']:.1f}")


if __name__ == "__main__":
    main()
