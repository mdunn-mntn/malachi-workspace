#!/usr/bin/env python3
"""TI-1027 — build the 5x5 / data-provider evaluation workbook (.xlsx). Multi-tab:
  1. Summary           — question, bottom line, key numbers
  2. Provider Scorecard — all MM site-visit DDPs rated (value/uniqueness/quality + cost) w/ verdict
  3. 5x5 Deep-Dive      — scale, leverage, uniqueness, IP overlap
  4. Vertical Impact    — verticals most dependent on 5x5-unique domains
  5. Cost Landscape     — all data partners + billing structure
  6. Methodology        — sources, windows, caveats
Data from outputs/*.csv (reproducible)."""
import csv
from pathlib import Path
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

HERE = Path(__file__).resolve().parent
OUT = HERE.parent / "outputs"
XLSX = HERE.parent / "artifacts" / "ti_1027_5x5_data_provider_scorecard.xlsx"

NAVY = "1F3864"; RED = "C00000"; GREEN = "375623"; GREY = "808080"
HDR_FILL = PatternFill("solid", fgColor=NAVY)
HDR_FONT = Font(bold=True, color="FFFFFF", size=11)
TITLE_FONT = Font(bold=True, size=14, color=NAVY)
WRAP = Alignment(wrap_text=True, vertical="top")

def style_header(ws, row, ncols):
    for c in range(1, ncols + 1):
        cell = ws.cell(row=row, column=c)
        cell.fill = HDR_FILL; cell.font = HDR_FONT
        cell.alignment = Alignment(wrap_text=True, vertical="center")

def autowidth(ws, widths):
    for i, w in enumerate(widths, 1):
        ws.column_dimensions[get_column_letter(i)].width = w

def verdict_fill(val):
    v = str(val).upper()
    if "DROP" in v: return PatternFill("solid", fgColor="F4CCCC")
    if "REVIEW" in v: return PatternFill("solid", fgColor="FFF2CC")
    if "KEEP" in v: return PatternFill("solid", fgColor="D9EAD3")
    return None

def load(name):
    with (OUT / name).open() as f:
        return list(csv.DictReader(f))

def main():
    wb = Workbook()

    # ---- Tab 1: Summary ----
    ws = wb.active; ws.title = "Summary"
    ws["A1"] = "5x5 (DS 25) Data Evaluation & Provider Scorecard (TI-1027)"; ws["A1"].font = TITLE_FONT
    ws["A2"] = "Targeting Infrastructure · estimation exercise · 7-day window 2026-06-09→15 (scale: 2026-06-15)"
    ws["A2"].font = Font(italic=True, color=GREY)
    rows = [
        ["", ""],
        ["BOTTOM LINE", "Recommend KEEP (renew) 5x5. Outsized ~3.4x its data scale, #2 most-unique data partner, "
         "B2B-concentrated, flat fee (cost does not scale). Final sign-off pending the flat-fee amount from billing."],
        ["", ""],
        ["Question", "Answer"],
        ["What is 5x5?", "A flat-fee data partner sending IP->website-visit records that feed MNTN Matched "
         "(domain->vertical classification). One of ~8 external + 2 internal sources in site_visit_signal."],
        ["Scale (% of raw data)", "~3.6% of raw site-visit records (93M rows/day, 20.8M IPs/day, 93K domains/day)."],
        ["Outsized or in line?", "OUTSIZED ~3.4x. 68.5% of its domains are unique; 47,069 are MM-usable = ~12% of "
         "the classified-domain universe, from a 3.6%-of-data partner."],
        ["Value = domains, not reach", "73.8% of its IPs we already see via our own bidstream/pixel. It surfaces "
         "DIFFERENT sites for users we already know."],
        ["vs other partners", "#2 unique contributor (Predactiv #1, also flat-fee). The $0.50-CPM per-use vendors "
         "(33Across API/Sovrn/Cybba) are largely redundant -> the real cost-review targets."],
        ["Verticals impacted if dropped", "Overwhelmingly B2B (Hiring, Logistics, Data&Analytics, Sales&Marketing, "
         "IT&Engineering) + premium retail. B2B is MNTN's #1 Q2 growth theme."],
        ["Domain-only complaint?", "CONFIRMED (3.8% of URLs carry a path vs 67-100% elsewhere) but MOOT for MM — the "
         "vertical classifier strips every URL to domain anyway."],
        ["Cost structure", "Flat fee (fixed; marginal cost $0). Peer MM-DDP rate = $0.50 CPM. Absolute $ pending billing."],
        ["Is it worth it?", "MM is worth tens of $M/yr; 5x5 supplies ~12% of its domain signal (B2B-weighted higher) "
         "-> clears a typical DDP flat fee with margin. Keep unless the fee is unusually large (then renegotiate)."],
    ]
    r = 4
    for a, b in rows:
        ws.cell(r, 1, a); ws.cell(r, 2, b)
        ws.cell(r, 1).alignment = WRAP; ws.cell(r, 2).alignment = WRAP
        if a in ("BOTTOM LINE",):
            ws.cell(r, 1).font = Font(bold=True, size=12, color=RED); ws.cell(r, 2).font = Font(bold=True, color=NAVY)
        elif a == "Question":
            ws.cell(r, 1).font = HDR_FONT; ws.cell(r, 2).font = HDR_FONT
            ws.cell(r, 1).fill = HDR_FILL; ws.cell(r, 2).fill = HDR_FILL
        elif a:
            ws.cell(r, 1).font = Font(bold=True, color=NAVY)
        r += 1
    autowidth(ws, [26, 96])

    # ---- Tab 2: Provider Scorecard ----
    ws2 = wb.create_sheet("Provider Scorecard")
    sc = load("ti_1027_vendor_scorecard.csv")
    hdr = ["Rank", "Provider", "DS", "Cost", "CPM", "Rows/day", "IPs/day", "% w/ path",
           "Domains (7d)", "Class. rate %", "Unique %", "Unique MM domains", "Score", "Verdict"]
    ws2.append(hdr); style_header(ws2, 1, len(hdr))
    for i, row in enumerate(sc, 1):
        cost = "internal $0" if row["internal"] == "True" else (row["billing"] or "")
        ws2.append([i, row["partner"], int(row["ds"]), cost,
                    (("$"+row["cpm"]) if row["cpm"] else ""),
                    int(row["rows_day"]), int(row["ips_day"]), float(row["pct_path"]),
                    int(row["total_domains"]), float(row["class_rate"]), float(row["pct_unique"]),
                    int(row["unique_classified"]), float(row["score"]), row["verdict"]])
        rr = ws2.max_row
        for c in (6, 7, 9, 12):
            ws2.cell(rr, c).number_format = "#,##0"
        ws2.cell(rr, 14).alignment = WRAP
        f = verdict_fill(row["verdict"])
        if f: ws2.cell(rr, 14).fill = f
        if row["ds"] == "25":
            ws2.cell(rr, 2).font = Font(bold=True, color=RED)
    autowidth(ws2, [5, 14, 5, 12, 6, 12, 11, 9, 12, 12, 9, 16, 7, 46])
    ws2.freeze_panes = "A2"

    # ---- Tab 3: 5x5 Deep-Dive ----
    ws3 = wb.create_sheet("5x5 Deep-Dive")
    ws3["A1"] = "5x5 (DS 25) — the measurable read"; ws3["A1"].font = TITLE_FONT
    ws3.append([]); ws3.append(["Metric", "Value", "Read"]); style_header(ws3, 3, 3)
    dd = [
        ["Share of raw site-visit records", "3.6%", "~93M rows/day. The leverage denominator."],
        ["Distinct IPs / day", "20.8M", "Mid-pack scale."],
        ["Distinct domains / day", "93K", "Substantial domain breadth."],
        ["% URLs with a page path", "3.8%", "Domain-only feed (vs 67-100% for other partners). Moot for MM."],
        ["Domains unique to 5x5 (7d)", "68.5%", "138,496 of 202,299 — provided by no other partner."],
        ["Unique AND MM-classifiable", "47,069", "~12% of the classified-domain universe. The net MM contribution."],
        ["Leverage ratio", "~3.4x", "12% unique MM signal from 3.6% of raw data -> OUTSIZED."],
        ["IPs already seen internally", "73.8%", "Value is incremental DOMAINS, not reach."],
        ["IPs unique to 5x5", "19.8%", "4.1M/day — modest incremental reach."],
        ["Concentration", "B2B", "20-34% of B2B verticals' fresh domain coverage is 5x5-unique."],
    ]
    for a, b, c in dd:
        ws3.append([a, b, c]); ws3.cell(ws3.max_row, 3).alignment = WRAP
        ws3.cell(ws3.max_row, 1).font = Font(bold=True, color=NAVY)
    autowidth(ws3, [32, 14, 80])

    # ---- Tab 4: Vertical Impact ----
    ws4 = wb.create_sheet("Vertical Impact")
    vt = load("ti_1027_vertical_dependence_7d.csv")
    ws4["A1"] = "Verticals most dependent on 5x5-unique domains (lost if 5x5 dropped)"; ws4["A1"].font = TITLE_FONT
    ws4.append([]); ws4.append(["Bucket", "Vertical", "Classified domains", "5x5-unique", "% dependent on 5x5"])
    style_header(ws4, 3, 5)
    for row in vt:
        ws4.append([int(row["bucket_id"]), row["vertical_name"], int(row["classified_domains"]),
                    int(row["d_5x5_unique"]), float(row["pct_dependent_on_5x5"])])
        rr = ws4.max_row
        ws4.cell(rr, 3).number_format = "#,##0"; ws4.cell(rr, 4).number_format = "#,##0"
        ws4.cell(rr, 5).number_format = "0.0"
        if row["vertical_name"].startswith("B2B"):
            ws4.cell(rr, 2).font = Font(bold=True, color=RED)
    autowidth(ws4, [8, 44, 18, 12, 18]); ws4.freeze_panes = "A4"

    # ---- Tab 5: Cost Landscape ----
    ws5 = wb.create_sheet("Cost Landscape")
    ws5["A1"] = "Data partner cost landscape (tpa.direct_data_partners, is_current=true)"; ws5["A1"].font = TITLE_FONT
    ws5.append([]); ws5.append(["Group", "Partner", "DS", "Billing", "CPM", "Feeds", "Note"])
    style_header(ws5, 3, 7)
    land = [
        ["MM site-visit", "Predactiv", 26, "flat_fee", "", "MNTN Matched", "Best value (#1 unique)"],
        ["MM site-visit", "5x5", 25, "flat_fee", "", "MNTN Matched", "KEEP (#2 unique, B2B)"],
        ["MM site-visit", "Justuno", 24, "fixed_cpm", "$0.50", "MNTN Matched", "Efficient (small, 84% unique)"],
        ["MM site-visit", "33Across", 28, "fixed_cpm", "$0.50", "MNTN Matched", "REVIEW (high CPM volume, 30% unique)"],
        ["MM site-visit", "33Across API", 40, "fixed_cpm", "$0.50", "MNTN Matched", "DROP-CANDIDATE (3% unique)"],
        ["MM site-visit", "Sovrn", 33, "fixed_cpm", "$0.50", "MNTN Matched", "DROP-CANDIDATE (2% unique)"],
        ["MM site-visit", "Cybba", 36, "fixed_cpm", "$0.50", "MNTN Matched", "REVIEW (6% unique, low volume)"],
        ["MM site-visit", "Klickly", 39, "flat_fee", "", "MNTN Matched", "REVIEW (negligible)"],
        ["MM site-visit", "LaunchLabs", 27, "fixed_cpm", "$0.50", "MNTN Matched", "DISABLED"],
        ["Internal", "augmentor_log", 30, "internal", "$0", "MNTN Matched", "Our own bidstream"],
        ["Internal", "guid_log", 23, "internal", "$0", "MNTN Matched", "Our own pixel"],
        ["Interest 3P", "LiveRamp", "11/35", "variable_cpm", "", "Interests", "Dominant 3P spend. Rated by TI-956/999."],
        ["Interest 3P", "ShareThis", 17, "fixed_cpm", "$0.95", "Interests", "Was $1.20. Rated by TI-956/999."],
        ["Interest 3P", "Dstillery", 18, "(unset)", "", "Interests", "Rated by TI-956/999."],
        ["Interest 3P", "OnAudience", 20, "(unset)", "", "Interests", "Dormant."],
        ["CRM", "Experian", 22, "flat_fee", "", "CRM", "Not scored data."],
        ["CRM", "deepsync", 29, "fixed_cpm", "$0.50", "CRM", "Not scored data."],
    ]
    for row in land:
        ws5.append(row); ws5.cell(ws5.max_row, 7).alignment = WRAP
        f = verdict_fill(row[6])
        if f: ws5.cell(ws5.max_row, 7).fill = f
    autowidth(ws5, [14, 14, 8, 14, 7, 14, 44]); ws5.freeze_panes = "A4"

    # ---- Tab 6: Methodology ----
    ws6 = wb.create_sheet("Methodology")
    meth = [
        ["Methodology & Caveats", ""],
        ["Substrate", "gs://mntn-data-archive-prod/signals/site_visit_signal/ (parquet on GCS, keyed by "
         "data_source_id). Queried via BigQuery temporary external-table definitions (read-only)."],
        ["Windows", "Scale: 1 day (2026-06-15). Uniqueness/quality/vertical: 7 days (2026-06-09..15). DDP delivery "
         "is bursty, so multi-day windows are used for the contribution metrics."],
        ["Quality measure", "A domain is 'MM-usable' if it appears in the production domain->vertical table "
         "(website_crawl_verticals, ~1.42M classified domains). 'Unique' = provided by no other data_source_id."],
        ["Cost", "billing_type + per-unit CPM from tpa.direct_data_partners. Absolute flat-fee dollars not yet "
         "available (with billing). Score = 0.55*value + 0.25*non-redundancy + 0.20*signal-quality (value-weighted)."],
        ["Value of MM", "MM touches ~$210-385M/yr of media; drives ~10-36% visit-rate lift (Fangorn). Estimated "
         "value (via retention) is tens of $M/yr. 5x5's attributable slice ~12% of domain signal, B2B-weighted higher."],
        ["Scope", "Scorecard covers MM site-visit DDPs (directly comparable). Interest-segment 3P providers "
         "(LiveRamp/ShareThis/Dstillery) are a different modality, rated by Alex's 9-axis framework (TI-956/TI-999)."],
        ["Not a causal claim", "This is an estimation exercise. A precise causal value would require an add/remove "
         "model ablation (re-run MM with vs without DS25 -> delta-IVR). Proposed as a follow-up only if needed."],
        ["Source files", "tickets/ti_1027_5x5_data_evaluation/ — summary.md, queries/, outputs/*.csv, "
         "artifacts/ (charts, this workbook, scorecard.md)."],
    ]
    for a, b in meth:
        ws6.append([a, b]); ws6.cell(ws6.max_row, 2).alignment = WRAP
        ws6.cell(ws6.max_row, 1).font = TITLE_FONT if b == "" else Font(bold=True, color=NAVY)
    autowidth(ws6, [18, 104])

    wb.save(XLSX)
    print(f"Wrote {XLSX}")

if __name__ == "__main__":
    main()
