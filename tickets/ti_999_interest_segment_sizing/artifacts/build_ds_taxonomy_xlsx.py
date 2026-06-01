"""Build the TI-999 DS taxonomy spreadsheet (xlsx) — one row per DS, grouped
by locked taxonomy. Reads the per-DS CSV in outputs/ and the Pass 18 bucket
CSV; writes a formatted xlsx with three sheets.

Run from workspace root:
  python3 tickets/ti_999_interest_segment_sizing/artifacts/build_ds_taxonomy_xlsx.py
"""
from __future__ import annotations

import csv
from pathlib import Path

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

WORKSPACE = Path(__file__).resolve().parents[3]
OUTPUTS = WORKSPACE / "tickets/ti_999_interest_segment_sizing/outputs"
SRC_CSV = OUTPUTS / "ti_999_ds_with_categories_2026_05_29.csv"
PASS_CSV = OUTPUTS / "ti_999_pass21_buckets_2026_05_29.csv"
PASS_LABEL = "Pass 21"
PASS_NOTE = (
    "Four buyer-pickable axes: MM {13,19,38,46} · MNTN Select {9,42} · "
    "3P {17,18,35} (Oracle carved out) · Advertiser CRM {4,8,47}. "
    "RTC dropped as an axis — it's in 99.9% of expressions and belongs with platform plumbing "
    "(geo, DS14 freshness filter, holdout) rather than as a bucket category. "
    "16 no-RTC anomalies on the 'RTC anomalies' sheet."
)
ANOMALIES_CSV = OUTPUTS / "ti_999_pass20_anomalies_2026_05_29.csv"
POLARITY_KPI_CSV = OUTPUTS / "ti_999_pass22c_polarity_aware_buckets_2026_05_29.csv"
GEO_RESTRICTION_CSV = OUTPUTS / "ti_999_pass24_geo_restriction_2026_06_01.csv"
OUT_XLSX = OUTPUTS / "ti_999_ds_taxonomy_2026_05_29.xlsx"

# Locked taxonomy assignments (post-Pass 18, post-Jordan/Sean clarifications).
# Order here controls group sort order in the sheet.
GROUP_ASSIGNMENTS: dict[int, str] = {
    13: "MM", 19: "MM", 38: "MM", 46: "MM",
    9:  "MNTN Select", 42: "MNTN Select",
    2:  "MNTN Pixel", 21: "MNTN Pixel", 34: "MNTN Pixel", 43: "MNTN Pixel",
    17: "3P", 18: "3P", 35: "3P",
    4:  "Advertiser CRM", 8: "Advertiser CRM", 47: "Advertiser CRM",
    14: "Bid mechanics", 16: "Bid mechanics",
    1:  "3P (Oracle deprecated)",  # carved out per Sean Yang
    11: "3P (LiveRamp legacy)",    # deprecated per Sean Yang
}

GROUP_SORT_ORDER = [
    "MM",
    "PP",
    "MNTN Select",
    "MNTN Pixel",
    "3P",
    "3P (Oracle deprecated)",
    "3P (LiveRamp legacy)",
    "Advertiser CRM",
    "Bid mechanics",
    "Dormant / out-of-scope",
]

GROUP_FILL = {
    "MM":                        PatternFill("solid", fgColor="DCEFFC"),  # light blue
    "PP":                        PatternFill("solid", fgColor="BBDEFB"),  # slightly deeper blue
    "MNTN Select":               PatternFill("solid", fgColor="FFF3E0"),  # light orange
    "MNTN Pixel":                PatternFill("solid", fgColor="E1F5E1"),  # light green
    "3P":                        PatternFill("solid", fgColor="F3E5F5"),  # light purple
    "3P (Oracle deprecated)":    PatternFill("solid", fgColor="EDE7F6"),  # paler purple
    "3P (LiveRamp legacy)":      PatternFill("solid", fgColor="EDE7F6"),
    "Advertiser CRM":            PatternFill("solid", fgColor="FFF9C4"),  # light yellow
    "Bid mechanics":             PatternFill("solid", fgColor="ECEFF1"),  # light gray-blue
    "Dormant / out-of-scope":    PatternFill("solid", fgColor="F5F5F5"),  # neutral light gray
}

HEADER_FILL = PatternFill("solid", fgColor="263238")
HEADER_FONT = Font(bold=True, color="FFFFFF", size=11)
TITLE_FONT = Font(bold=True, size=14)


def group_for_ds(ds_id: int) -> str:
    return GROUP_ASSIGNMENTS.get(ds_id, "Dormant / out-of-scope")


def visible_str(raw: str) -> str:
    s = raw.strip().lower()
    if s in ("true", "1", "t", "yes"):
        return "Yes"
    if s in ("false", "0", "f", "no"):
        return "No"
    return raw


def to_int(s: str, default: int = 0) -> int:
    try:
        return int(float(s))
    except (TypeError, ValueError):
        return default


def to_float(s: str, default: float = 0.0) -> float:
    try:
        return float(s)
    except (TypeError, ValueError):
        return default


def load_per_ds_rows() -> list[dict]:
    rows: list[dict] = []
    with SRC_CSV.open() as f:
        reader = csv.DictReader(f)
        for r in reader:
            ds_id = to_int(r["ds"])
            rows.append({
                "ds": ds_id,
                "ds_name": r.get("ds_name", ""),
                "display_name": r.get("display_name", "") or "",
                "visible": visible_str(r.get("visible", "")),
                "group": group_for_ds(ds_id),
                "pos_camps": to_int(r.get("prospecting_pos_camps", "")),
                "pos_spend_M": to_float(r.get("prospecting_pos_spend_M", "")),
                "neg_camps": to_int(r.get("prospecting_neg_camps", "")),
                "category_count": to_int(r.get("category_count", "")),
                "sample_categories": r.get("sample_categories", ""),
            })
    rows.sort(key=lambda r: (GROUP_SORT_ORDER.index(r["group"]) if r["group"] in GROUP_SORT_ORDER else 99, r["ds"]))
    return rows


def write_taxonomy_sheet(wb: Workbook, rows: list[dict]) -> None:
    ws = wb.active
    ws.title = "DS taxonomy"

    title = "TI-999 DS taxonomy — locked 2026-05-29 (Pass 18)"
    ws.cell(row=1, column=1, value=title).font = TITLE_FONT
    ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=10)

    note = (
        "Source: bronze.integrationprod.data_sources (canonical type=1 DSes only, 68 total) + "
        "bronze.tpa.categories. Usage = 30d prospecting window 2026-04-29 to 2026-05-28. "
        "PP shares DSes with MM (DS13 vertical + DS19 keyword → score-tier inside MM 2.0). "
        "Oracle (DS1) and LiveRamp legacy (DS11) are deprecated 3P; called out separately."
    )
    ws.cell(row=2, column=1, value=note).alignment = Alignment(wrap_text=True, vertical="top")
    ws.merge_cells(start_row=2, start_column=1, end_row=2, end_column=10)
    ws.row_dimensions[2].height = 48

    headers = [
        "DS ID", "Group", "Name (canonical)", "Display name", "Visible (buyer pick?)",
        "+camps (30d)", "+spend (30d, $M)", "−camps (30d)", "Categories in tpa.categories",
        "Sample categories",
    ]
    header_row = 4
    for col_idx, h in enumerate(headers, start=1):
        c = ws.cell(row=header_row, column=col_idx, value=h)
        c.fill = HEADER_FILL
        c.font = HEADER_FONT
        c.alignment = Alignment(horizontal="left", vertical="center", wrap_text=True)
    ws.row_dimensions[header_row].height = 28

    for i, r in enumerate(rows, start=header_row + 1):
        fill = GROUP_FILL.get(r["group"], GROUP_FILL["Dormant / out-of-scope"])
        values = [
            r["ds"],
            r["group"],
            r["ds_name"],
            r["display_name"],
            r["visible"],
            r["pos_camps"],
            r["pos_spend_M"],
            r["neg_camps"],
            r["category_count"],
            r["sample_categories"],
        ]
        for col_idx, v in enumerate(values, start=1):
            cell = ws.cell(row=i, column=col_idx, value=v)
            cell.fill = fill
            if col_idx == 7:  # +spend
                cell.number_format = '"$"#,##0.00'
            elif col_idx in (1, 6, 8, 9):
                cell.number_format = "#,##0"
            cell.alignment = Alignment(vertical="top", wrap_text=(col_idx == 10))

    widths = [8, 24, 36, 22, 12, 12, 16, 12, 16, 80]
    for col_idx, w in enumerate(widths, start=1):
        ws.column_dimensions[get_column_letter(col_idx)].width = w

    ws.freeze_panes = "A5"


def write_group_summary_sheet(wb: Workbook, rows: list[dict]) -> None:
    ws = wb.create_sheet("Group summary")

    by_group: dict[str, dict] = {}
    for r in rows:
        g = r["group"]
        if g not in by_group:
            by_group[g] = {"n_ds": 0, "n_active": 0, "n_visible": 0}
        by_group[g]["n_ds"] += 1
        is_active = (r["pos_camps"] > 0) or (r["neg_camps"] > 0)
        if is_active:
            by_group[g]["n_active"] += 1
        if r["visible"] == "Yes":
            by_group[g]["n_visible"] += 1

    ws.cell(row=1, column=1, value="Group summary").font = TITLE_FONT
    ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=4)

    note = (
        "Active (30d) = at least one campaign referenced this DS positively or negatively in 30d prospecting. "
        "Visible to buyers = data_sources.visible=true (buyer-pickable in the UI)."
    )
    ws.cell(row=2, column=1, value=note).alignment = Alignment(wrap_text=True, vertical="top")
    ws.merge_cells(start_row=2, start_column=1, end_row=2, end_column=4)
    ws.row_dimensions[2].height = 28

    headers = ["Group", "# DSes in group", "# active (30d)", "# visible to buyers"]
    for col_idx, h in enumerate(headers, start=1):
        c = ws.cell(row=4, column=col_idx, value=h)
        c.fill = HEADER_FILL
        c.font = HEADER_FONT

    row_i = 5
    for g in GROUP_SORT_ORDER:
        if g not in by_group:
            continue
        d = by_group[g]
        fill = GROUP_FILL.get(g, GROUP_FILL["Dormant / out-of-scope"])
        vals = [g, d["n_ds"], d["n_active"], d["n_visible"]]
        for col_idx, v in enumerate(vals, start=1):
            cell = ws.cell(row=row_i, column=col_idx, value=v)
            cell.fill = fill
            if col_idx in (2, 3, 4):
                cell.number_format = "#,##0"
        row_i += 1

    for col_idx, w in enumerate([28, 18, 18, 22], start=1):
        ws.column_dimensions[get_column_letter(col_idx)].width = w
    ws.freeze_panes = "A5"


def write_pass_sheet(wb: Workbook) -> None:
    if not PASS_CSV.exists():
        return
    ws = wb.create_sheet(f"{PASS_LABEL} buckets")

    ws.cell(row=1, column=1, value=f"{PASS_LABEL} audience-bucket results (30d prospecting, 2026-04-29 to 2026-05-28)").font = TITLE_FONT
    ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=10)

    ws.cell(row=2, column=1, value=PASS_NOTE).alignment = Alignment(wrap_text=True, vertical="top")
    ws.merge_cells(start_row=2, start_column=1, end_row=2, end_column=10)
    ws.row_dimensions[2].height = 32

    headers = ["Bucket", "n_campaigns", "% campaigns", "n_advertisers", "Spend (30d, $M)", "% spend", "Annualized ($M)"]
    for col_idx, h in enumerate(headers, start=1):
        c = ws.cell(row=4, column=col_idx, value=h)
        c.fill = HEADER_FILL
        c.font = HEADER_FONT
        c.alignment = Alignment(horizontal="left", vertical="center", wrap_text=True)
    ws.row_dimensions[4].height = 30

    # Highlight 3P-touching buckets only (the focus of TI-999's curation case)
    fill_3p = PatternFill("solid", fgColor="F3E5F5")

    with PASS_CSV.open() as f:
        reader = csv.DictReader(f)
        row_i = 5
        for r in reader:
            is_3p_bucket = "3P" in r["bucket"]
            fill = fill_3p if is_3p_bucket else None
            for col_idx in range(1, 8):
                cell = ws.cell(row=row_i, column=col_idx)
                if fill:
                    cell.fill = fill
            ws.cell(row=row_i, column=1, value=r["bucket"])
            ws.cell(row=row_i, column=2, value=to_int(r["n_campaigns"])).number_format = "#,##0"
            ws.cell(row=row_i, column=3, value=to_float(r["pct_campaigns"])).number_format = "0.0"
            ws.cell(row=row_i, column=4, value=to_int(r["n_advertisers"])).number_format = "#,##0"
            ws.cell(row=row_i, column=5, value=to_float(r["spend_30d_M"])).number_format = '"$"#,##0.000'
            ws.cell(row=row_i, column=6, value=to_float(r["pct_spend"])).number_format = "0.0"
            ws.cell(row=row_i, column=7, value=to_float(r["spend_annualized_M"])).number_format = '"$"#,##0.0'
            if fill:
                for col_idx in range(1, 8):
                    ws.cell(row=row_i, column=col_idx).fill = fill
            row_i += 1

    for col_idx, w in enumerate([36, 14, 14, 16, 18, 12, 18], start=1):
        ws.column_dimensions[get_column_letter(col_idx)].width = w
    ws.freeze_panes = "A5"


def write_anomalies_sheet(wb: Workbook) -> None:
    if not ANOMALIES_CSV.exists():
        return
    ws = wb.create_sheet("RTC anomalies")

    title = "Pass 20 anomalies — 16 prospecting campaigns with no score_type=rtc (concentrated in 3 advertisers)"
    ws.cell(row=1, column=1, value=title).font = TITLE_FONT
    ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=7)

    note = (
        "99.9% of prospecting expressions have score_type=rtc auto-attached. The 16 below are exceptions, "
        "concentrated in AID 36678 (9 campaigns), AID 37336 (6), AID 42097 (1). "
        "Both 36678 and 37336 are heavy MNTN Select household users (DS9). "
        "MM-without-RTC = buyer added DS19 batch keywords but no RTC flag. "
        "no-RTC no-MM = custom audience targeting via DS2/DS9/DS8 only. "
        "Likely deliberately bypassing RTC default via API or custom tooling — RTC isn't UI-selectable. "
        "AUD team to confirm the opt-out mechanism."
    )
    ws.cell(row=2, column=1, value=note).alignment = Alignment(wrap_text=True, vertical="top")
    ws.merge_cells(start_row=2, start_column=1, end_row=2, end_column=7)
    ws.row_dimensions[2].height = 72

    headers = ["Anomaly class", "Campaign ID", "Audience Segment ID", "Advertiser ID", "Spend 30d ($)", "Has RTC", "Has MM", "DS refs"]
    for col_idx, h in enumerate(headers, start=1):
        c = ws.cell(row=4, column=col_idx, value=h)
        c.fill = HEADER_FILL
        c.font = HEADER_FONT
        c.alignment = Alignment(horizontal="left", vertical="center", wrap_text=True)
    ws.row_dimensions[4].height = 28

    mm_fill = PatternFill("solid", fgColor="FFE0B2")
    nomm_fill = PatternFill("solid", fgColor="F8BBD0")

    with ANOMALIES_CSV.open() as f:
        reader = csv.DictReader(f)
        row_i = 5
        for r in reader:
            fill = mm_fill if r["anomaly_class"] == "ANOMALY_MM_without_RTC" else nomm_fill
            ws.cell(row=row_i, column=1, value=r["anomaly_class"]).fill = fill
            ws.cell(row=row_i, column=2, value=to_int(r["campaign_id"])).fill = fill
            ws.cell(row=row_i, column=2).number_format = "#,##0"
            ws.cell(row=row_i, column=3, value=to_int(r["audience_segment_id"])).fill = fill
            ws.cell(row=row_i, column=3).number_format = "#,##0"
            ws.cell(row=row_i, column=4, value=to_int(r["advertiser_id"])).fill = fill
            ws.cell(row=row_i, column=4).number_format = "#,##0"
            ws.cell(row=row_i, column=5, value=to_float(r["spend_30d_K"]) * 1000).fill = fill
            ws.cell(row=row_i, column=5).number_format = '"$"#,##0'
            ws.cell(row=row_i, column=6, value=r["has_rtc"]).fill = fill
            ws.cell(row=row_i, column=7, value=r["has_mm"]).fill = fill
            ws.cell(row=row_i, column=8, value=r["ds_refs"]).fill = fill
            ws.cell(row=row_i, column=8).alignment = Alignment(wrap_text=True)
            row_i += 1

    for col_idx, w in enumerate([28, 14, 18, 14, 14, 10, 10, 56], start=1):
        ws.column_dimensions[get_column_letter(col_idx)].width = w
    ws.freeze_panes = "A5"


def write_polarity_kpi_sheet(wb: Workbook) -> None:
    if not POLARITY_KPI_CSV.exists():
        return
    ws = wb.create_sheet("Polarity KPIs")

    ws.cell(row=1, column=1, value="Pass 22c — polarity-aware bucket KPIs (ratios)").font = TITLE_FONT
    ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=10)

    note = (
        "Pass 21 buckets BUT 3P and CRM are split by polarity so MM combos are properly disambiguated. "
        "MM + CRM-incl = customer-list-seeded MM scoring (adds non-MM-scored customer IPs as positive layer). "
        "MM + CRM-excl = MM scoring with customer suppression (drilling down on MM-scored audience). "
        "All KPIs are ratios: CVR/IVR/CTR as percentages, CPM and cost-per-conv in dollars. "
        "3P-only (no MM, no CRM, no Select) is the cleanest baseline for measuring 3P quality interventions."
    )
    ws.cell(row=2, column=1, value=note).alignment = Alignment(wrap_text=True, vertical="top")
    ws.merge_cells(start_row=2, start_column=1, end_row=2, end_column=10)
    ws.row_dimensions[2].height = 72

    headers = [
        "Bucket", "n_campaigns", "n_advertisers", "Spend (30d, $M)", "% spend",
        "CVR (ratio)", "IVR (ratio)", "CTR (ratio)", "CPM ($)", "Cost/conv ($)",
    ]
    for col_idx, h in enumerate(headers, start=1):
        c = ws.cell(row=4, column=col_idx, value=h)
        c.fill = HEADER_FILL
        c.font = HEADER_FONT
        c.alignment = Alignment(horizontal="left", vertical="center", wrap_text=True)
    ws.row_dimensions[4].height = 30

    # Minimal coloring — only the rows that matter for the deck story
    fill_baseline = PatternFill("solid", fgColor="F3E5F5")    # 3P-incl alone = clean baseline for 3P quality work
    fill_high_cvr = PatternFill("solid", fgColor="FFF9C4")    # CRM-incl combos = high CVR cohort
    fill_default = None

    with POLARITY_KPI_CSV.open() as f:
        reader = csv.DictReader(f)
        row_i = 5
        for r in reader:
            bucket = r["bucket"]
            if bucket == "3P-incl":
                fill = fill_baseline
            elif "CRM-incl" in bucket and "CRM-excl" not in bucket:
                fill = fill_high_cvr
            else:
                fill = fill_default
            def apply_fill(col, value, fmt=None):
                cell = ws.cell(row=row_i, column=col, value=value)
                if fill:
                    cell.fill = fill
                if fmt:
                    cell.number_format = fmt
                return cell
            apply_fill(1, bucket)
            apply_fill(2, to_int(r["n_campaigns"]), "#,##0")
            apply_fill(3, to_int(r["n_advertisers"]), "#,##0")
            apply_fill(4, to_float(r["spend_30d_M"]), '"$"#,##0.000')
            apply_fill(5, to_float(r["pct_spend"]), "0.0")
            apply_fill(6, to_float(r["cvr"]), "0.000000")
            apply_fill(7, to_float(r["ivr"]), "0.000000")
            apply_fill(8, to_float(r["ctr"]), "0.000000")
            apply_fill(9, to_float(r["cpm_dollars"]), '"$"#,##0.00')
            cpc_val = r.get("cost_per_conv_dollars", "")
            if cpc_val and cpc_val.strip():
                apply_fill(10, to_float(cpc_val), '"$"#,##0.00')
            else:
                apply_fill(10, "")
            row_i += 1

    for col_idx, w in enumerate([44, 14, 14, 16, 12, 12, 12, 12, 12, 16], start=1):
        ws.column_dimensions[get_column_letter(col_idx)].width = w
    ws.freeze_panes = "B5"


def write_geo_restriction_sheet(wb: Workbook) -> None:
    if not GEO_RESTRICTION_CSV.exists():
        return
    ws = wb.create_sheet("Pass 24 — Geo restriction")

    title = "Pass 24 — bucket KPIs split by geo restriction (Alyson + Toph ask, 2026-06-01)"
    ws.cell(row=1, column=1, value=title).font = TITLE_FONT
    ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=11)

    note = (
        "geo_restricted = expression has explicit location_ids[] in the geos clause (buyer-picked specific locations). "
        "geo_broad_or_default = expression has a geos clause but no explicit location_ids[] (likely country-level / wildcard). "
        "Toph's hypothesis: MM+3P+geo-restriction audiences perform poorly. Empirically: this IS the biggest single audience-targeted cohort ($5.62M / 17.6% of prospecting spend), but its cost-per-conv ($65.54) is actually BETTER than MM-only with geo restriction ($93.53). Geo restriction itself is the bigger drag — geo-broad versions consistently outperform geo-restricted across MM, MM+3P, and Geo-only. TI-956 curation lands at maximum leverage within this cohort."
    )
    ws.cell(row=2, column=1, value=note).alignment = Alignment(wrap_text=True, vertical="top")
    ws.merge_cells(start_row=2, start_column=1, end_row=2, end_column=11)
    ws.row_dimensions[2].height = 90

    headers = [
        "Bucket", "Geo status", "n_campaigns", "n_advertisers", "Spend (30d, $M)", "% total spend",
        "CVR (ratio)", "IVR (ratio)", "CTR (ratio)", "CPM ($)", "Cost/conv ($)",
    ]
    for col_idx, h in enumerate(headers, start=1):
        c = ws.cell(row=4, column=col_idx, value=h)
        c.fill = HEADER_FILL
        c.font = HEADER_FONT
        c.alignment = Alignment(horizontal="left", vertical="center", wrap_text=True)
    ws.row_dimensions[4].height = 30

    fill_largest = PatternFill("solid", fgColor="FFF9C4")  # the headline MM+3P+geo-restricted row
    fill_default = None

    with GEO_RESTRICTION_CSV.open() as f:
        reader = csv.DictReader(f)
        row_i = 5
        for r in reader:
            bucket = r["bucket"]
            geo_status = r["geo_status"]
            is_headline = bucket == "MM + 3P-incl" and geo_status == "geo_restricted"
            fill = fill_largest if is_headline else fill_default

            def apply(col, value, fmt=None):
                cell = ws.cell(row=row_i, column=col, value=value)
                if fill:
                    cell.fill = fill
                if fmt:
                    cell.number_format = fmt
                return cell
            apply(1, bucket)
            apply(2, geo_status)
            apply(3, to_int(r["n_campaigns"]), "#,##0")
            apply(4, to_int(r["n_advertisers"]), "#,##0")
            apply(5, to_float(r["spend_30d_M"]), '"$"#,##0.000')
            apply(6, to_float(r["pct_total_spend"]), "0.0")
            apply(7, to_float(r["cvr"]), "0.000000")
            apply(8, to_float(r["ivr"]), "0.000000")
            apply(9, to_float(r["ctr"]), "0.000000")
            apply(10, to_float(r["cpm_dollars"]), '"$"#,##0.00')
            cpc = r.get("cost_per_conv_dollars", "")
            if cpc and cpc.strip():
                apply(11, to_float(cpc), '"$"#,##0.00')
            else:
                apply(11, "")
            row_i += 1

    for col_idx, w in enumerate([34, 22, 14, 16, 18, 14, 14, 14, 14, 12, 16], start=1):
        ws.column_dimensions[get_column_letter(col_idx)].width = w
    ws.freeze_panes = "C5"


def main() -> None:
    rows = load_per_ds_rows()
    wb = Workbook()
    write_taxonomy_sheet(wb, rows)
    write_group_summary_sheet(wb, rows)
    write_pass_sheet(wb)
    write_polarity_kpi_sheet(wb)
    write_geo_restriction_sheet(wb)
    write_anomalies_sheet(wb)
    OUT_XLSX.parent.mkdir(parents=True, exist_ok=True)
    wb.save(OUT_XLSX)
    print(f"Wrote {OUT_XLSX} ({OUT_XLSX.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
