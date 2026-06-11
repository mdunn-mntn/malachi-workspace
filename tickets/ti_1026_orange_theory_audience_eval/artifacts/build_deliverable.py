#!/usr/bin/env python3
"""TI-1026 — build the Orange Theory audience-evaluation recommendation workbook (.xlsx),
mirroring the ElevenLabs eval (TI-928) deliverable. Multi-tab:
  1. Recommendations  — headline + prioritized actions
  2. Interest Segments — 11 included + 7 excluded 3P, reach/overlap/recommendation
  3. Keywords          — 379 MNTN Matched keywords, bucket + action
  4. Geo & Exclusions  — geo-fence coverage + demographic-exclusion footprint
  5. Methodology       — sources, dates, caveats

Data from outputs/*.csv (reproducible). Run after the analysis queries land.
"""
import csv
from pathlib import Path
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

HERE = Path(__file__).resolve().parent
OUT = HERE.parent / "outputs"
XLSX = HERE.parent / "artifacts" / "ti_1026_orange_theory_audience_recommendations.xlsx"

NAVY = "1F3864"; RED = "C00000"; GREEN = "375623"; AMBER = "BF8F00"; GREY = "808080"
HDR_FILL = PatternFill("solid", fgColor=NAVY)
HDR_FONT = Font(bold=True, color="FFFFFF", size=11, name="Calibri")
TITLE_FONT = Font(bold=True, size=14, color=NAVY)
WRAP = Alignment(wrap_text=True, vertical="top")
THIN = Border(*[Side(style="thin", color="D9D9D9")] * 4)


def style_header(ws, row, ncols):
    for c in range(1, ncols + 1):
        cell = ws.cell(row=row, column=c)
        cell.fill = HDR_FILL; cell.font = HDR_FONT; cell.alignment = Alignment(wrap_text=True, vertical="center")


def autowidth(ws, widths):
    for i, w in enumerate(widths, 1):
        ws.column_dimensions[get_column_letter(i)].width = w


def action_fill(val):
    v = str(val).upper()
    if v.startswith("DROP"):
        return PatternFill("solid", fgColor="F4CCCC")
    if v.startswith("REVIEW"):
        return PatternFill("solid", fgColor="FFF2CC")
    if v.startswith("KEEP"):
        return PatternFill("solid", fgColor="D9EAD3")
    return None


def load_csv(name):
    with (OUT / name).open() as f:
        return list(csv.DictReader(f))


def main():
    wb = Workbook()

    # ---- Tab 1: Recommendations ----
    ws = wb.active; ws.title = "Recommendations"
    ws["A1"] = "Orange Theory National — Audience Evaluation (TI-1026)"; ws["A1"].font = TITLE_FONT
    ws["A2"] = ("Advertiser 39718 · Audience 34668 'MNTN Matched | New Year's 3P Segments Copy 01' · "
                "prepared by Targeting Infrastructure")
    ws["A2"].font = Font(italic=True, color=GREY)
    rows = [
        ["", ""],
        ["THE FINDING", ""],
        ["1. The 3P 'interest segments' are the problem, not the size.",
         "All 11 bought 3P segments are low-intent: 87% of their users match NO Orange Theory keyword. "
         "Over a week they add only ~12% incremental reach (2.65M of 21.8M IPs) — and that 12% is the slice the "
         "agency already flagged as 8-10x worse on visit rate. 6 of 11 deliver zero users (3 deprecated, 3 no feed); "
         "the 2 that carry reach are a broad fitness-buyer list and a yoga/pilates app-usage list (off-modality for HIIT)."],
        ["2. The keyword (MNTN Matched) layer carries the audience — but ~25% is off-target.",
         "The 379 keywords reach 21.8M IPs/week (14x the 3P layer). ~51 are clearly off-target (e.g. Above Ground "
         "Pools, Antifreeze, Beer Mugs, Motorcycle Lighting, CPUs) and ~43 are over-broad single words (Class, Power, "
         "Experience). These dilute relevance and drag visit rate."],
        ["3. Geo filtering is NOT the bottleneck.",
         "The 946 studio fences (7-mi radius) cover ~half the populated US. With 21.8M MM IPs/week and the fence "
         "retaining roughly half, the in-fence audience is millions — far more than the campaign spends against. "
         "Geo applies equally to MM and 3P, so it does not explain the 3P underperformance."],
        ["", ""],
        ["RECOMMENDATIONS (priority order)", "Expected effect"],
        ["A. Remove all 11 3P interest segments.",
         "Improves visit rate (drops the lowest-intent ~12% slice). 6 already deliver nothing; the rest are broad/off-modality."],
        ["B. Prune the 51 clearly off-target keywords; review the 43 over-broad terms.",
         "Tightens MNTN Matched relevance with little reach loss. See Keywords tab."],
        ["C. To recover/grow reach, relax the demographic exclusions before re-adding any 3P.",
         "Low-income (<$35k) and 65+ bands are duplicated across Oracle/Equifax/Experian/TransUnion. "
         "Consolidate and loosen — pure size add at minimal quality cost. Re-examine the T-Mobile-cellular exclusion. See Geo tab."],
        ["D. Keep the MNTN Matched keyword layer as the core; add on-target HIIT/strength/cardio keywords.",
         "MNTN Matched is the quality reach engine here — invest in it, not in bought 3P."],
        ["E. Keep CRM suppression (existing-member) exclusions as-is.",
         "Good hygiene — leave untouched."],
    ]
    r = 4
    for a, b in rows:
        ws.cell(r, 1, a); ws.cell(r, 2, b)
        ws.cell(r, 1).alignment = WRAP; ws.cell(r, 2).alignment = WRAP
        if a in ("THE FINDING", "RECOMMENDATIONS (priority order)"):
            ws.cell(r, 1).font = Font(bold=True, size=12, color=NAVY)
            ws.cell(r, 2).font = Font(bold=True, size=12, color=NAVY)
        elif a and a[0].isdigit():
            ws.cell(r, 1).font = Font(bold=True, color=NAVY)
        elif a and a[0] in "ABCDE" and a[1] == ".":
            ws.cell(r, 1).font = Font(bold=True)
        r += 1
    autowidth(ws, [60, 70])

    # ---- Tab 2: Interest Segments ----
    ws2 = wb.create_sheet("Interest Segments (3P)")
    seg = load_csv("ti_1026_interest_segments_eval.csv")
    cols = ["role", "data_source_category_id", "provider", "segment", "modality_fit",
            "deprecated", "reach_ips_7d", "pct_matching_otf_keywords", "recommendation", "reason"]
    hdr = ["Role", "LiveRamp Cat ID", "Provider", "Segment", "Modality fit", "Deprecated",
           "Reach (IPs, 7d)", "% matching OTF keywords", "Recommendation", "Reason"]
    ws2.append(hdr); style_header(ws2, 1, len(hdr))
    for row in seg:
        vals = [row[c] for c in cols]
        # format reach as int with commas
        if vals[6].isdigit():
            vals[6] = int(vals[6])
        ws2.append(vals)
        rr = ws2.max_row
        ws2.cell(rr, 4).alignment = WRAP; ws2.cell(rr, 10).alignment = WRAP
        f = action_fill(row["recommendation"])
        if f:
            ws2.cell(rr, 9).fill = f
        if row["deprecated"] == "TRUE":
            ws2.cell(rr, 6).font = Font(bold=True, color=RED)
    ws2.cell(1, 1).comment = None
    autowidth(ws2, [9, 14, 14, 34, 13, 11, 14, 16, 14, 60])
    ws2.freeze_panes = "A2"

    # ---- Tab 3: Keywords ----
    ws3 = wb.create_sheet("Keywords")
    kw = load_csv("ti_1026_keyword_classification.csv")
    ws3.append(["MNTN Matched Cat ID", "Keyword", "Bucket", "Recommended action"])
    style_header(ws3, 1, 4)
    for row in kw:
        ws3.append([row["data_source_category_id"], row["keyword"], row["bucket"], row["recommended_action"]])
        rr = ws3.max_row
        f = action_fill(row["recommended_action"])
        if f:
            ws3.cell(rr, 4).fill = f
    autowidth(ws3, [18, 42, 18, 30]); ws3.freeze_panes = "A2"

    # ---- Tab 4: Geo & Exclusions ----
    ws4 = wb.create_sheet("Geo & Exclusions")
    ws4["A1"] = "Geo-fence coverage (946 studios x 7-mi radius)"; ws4["A1"].font = TITLE_FONT
    ws4.append([]) ; ws4.append(["Measure", "Fenced", "US total", "% fenced"])
    style_header(ws4, 3, 4)
    ws4.append(["Geolocated network blocks (population proxy)", 2203886, 4458870, "49.4%"])
    ws4.append(["IPv4 address capacity", "401M", "1,613M", "24.9%"])
    ws4.append([])
    ws4.append(["Read: the fence covers ~half the populated US. Geo is a real constraint but not the bottleneck; "
                "it applies equally to MNTN Matched and 3P, so it does not explain 3P underperformance."])
    ws4.cell(ws4.max_row, 1).alignment = WRAP
    ws4.append([]); ws4.append([])
    rr = ws4.max_row + 1
    ws4.cell(rr, 1, "Demographic exclusion footprint on the audience (2026-06-08)"); ws4.cell(rr, 1).font = TITLE_FONT
    ws4.append(["Exclusion group", "Cats", "IPs removed from audience", "Note"])
    style_header(ws4, ws4.max_row, 4)
    # placeholder rows — populated from exclusion-bite query
    ws4.append(["LiveRamp income/age bands", 7, "{{LR_EXCL_REMOVED}}", "Overlaps Oracle bands — redundant"])
    ws4.append(["Oracle income/age bands", 13, "{{ORACLE_EXCL_REMOVED}}", "Many duplicate LiveRamp bands"])
    ws4.append(["T-Mobile Cellular (ISP type)", 1, "not in IPDSC (applied at bid time)",
                "Excludes a whole carrier's mobile IPs — re-examine"])
    ws4.append(["CRM suppression (existing members)", 2, "advertiser-specific", "KEEP — good hygiene"])
    autowidth(ws4, [46, 8, 26, 46])

    # ---- Tab 5: Methodology ----
    ws5 = wb.create_sheet("Methodology")
    meth = [
        ["Methodology & Caveats", ""],
        ["Audience", "audience.audiences id 34668 (expression_type_id=2), advertiser 39718. Expression parsed to "
         "include/exclude data-source + category sets and a geo radii_include list."],
        ["Reach / overlap", "ipdsc__v1 (dw-main-bronze.external), distinct IPs, 7-day window 2026-06-04..06-10. "
         "MM=DS19 MNTN Matched (379 cats), 3P=DS35 LiveRamp (11 cats). Single-day 2026-06-10 also shown."],
        ["3P volatility", "Same 11 segments deliver wildly different IP counts day to day (e.g. Stirista 2.1M on 06-08, "
         "0 on 06-06). 7-day window is the fair measure; single-day understates."],
        ["Geo coverage", "geo.maxmind_blocks_ipv4: network blocks within 7 mi (11,265 m) of any of the 946 studios "
         "(ST_DWITHIN). Block-count is a population-density proxy; IP-capacity over-weights rural ranges. "
         "radii_exclude (21 zones) not subtracted."],
        ["Keyword buckets", "Conservative heuristic + curated lists; default = KEEP. The exact keep/drop line is for "
         "Kelly Thurlow / Sales to finalize — this surfaces high-confidence candidates."],
        ["Performance", "Visit rate is audience-level (campaign 319137: 0.25% over 90d). Per-segment delivery visit "
         "rate is not separable from logs; the 87%-non-keyword overlap + agency's 8-10x report are the intent evidence."],
        ["Not a causal claim", "Reach/overlap and keyword relevance are descriptive. They explain WHY 3P underperforms "
         "(low intent, off-modality) but the 8-10x figure is the agency's measured result, not re-derived here."],
    ]
    for a, b in meth:
        ws5.append([a, b]); rr = ws5.max_row
        ws5.cell(rr, 2).alignment = WRAP
        if b == "":
            ws5.cell(rr, 1).font = TITLE_FONT
        else:
            ws5.cell(rr, 1).font = Font(bold=True, color=NAVY)
    autowidth(ws5, [18, 100])

    wb.save(XLSX)
    print(f"Wrote {XLSX}")


if __name__ == "__main__":
    main()
