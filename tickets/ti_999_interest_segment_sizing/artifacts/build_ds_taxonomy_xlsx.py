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
PASS_CSV = OUTPUTS / "ti_999_pass20_buckets_2026_05_29.csv"
PASS_LABEL = "Pass 20"
PASS_NOTE = (
    "Five independent axes: MM {13,19,38,46} · RTC (score_type=rtc) · "
    "MNTN Select {9,42} · 3P {17,18,35} (Oracle carved out) · Advertiser CRM {4,8,47}. "
    "RTC kept as its own axis per Sean Yang's revised reading (2026-05-29): "
    "RTC is an independent pipeline from MM, not the real-time variant of MM."
)
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
            by_group[g] = {"n_ds": 0, "n_active": 0, "n_visible": 0, "pos_camps": 0, "pos_spend_M": 0.0, "neg_camps": 0}
        by_group[g]["n_ds"] += 1
        is_active = (r["pos_camps"] > 0) or (r["neg_camps"] > 0)
        if is_active:
            by_group[g]["n_active"] += 1
        if r["visible"] == "Yes":
            by_group[g]["n_visible"] += 1
        by_group[g]["pos_camps"] += r["pos_camps"]
        by_group[g]["pos_spend_M"] += r["pos_spend_M"]
        by_group[g]["neg_camps"] += r["neg_camps"]

    ws.cell(row=1, column=1, value="Group summary").font = TITLE_FONT
    ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=7)

    headers = ["Group", "# DSes", "# active (30d)", "# visible to buyers", "+camps total", "+spend total ($M)", "−camps total"]
    for col_idx, h in enumerate(headers, start=1):
        c = ws.cell(row=3, column=col_idx, value=h)
        c.fill = HEADER_FILL
        c.font = HEADER_FONT

    row_i = 4
    for g in GROUP_SORT_ORDER:
        if g not in by_group:
            continue
        d = by_group[g]
        fill = GROUP_FILL.get(g, GROUP_FILL["Dormant / out-of-scope"])
        vals = [g, d["n_ds"], d["n_active"], d["n_visible"], d["pos_camps"], d["pos_spend_M"], d["neg_camps"]]
        for col_idx, v in enumerate(vals, start=1):
            cell = ws.cell(row=row_i, column=col_idx, value=v)
            cell.fill = fill
            if col_idx == 6:
                cell.number_format = '"$"#,##0.00'
            elif col_idx in (2, 3, 4, 5, 7):
                cell.number_format = "#,##0"
        row_i += 1

    for col_idx, w in enumerate([28, 10, 16, 22, 16, 20, 14], start=1):
        ws.column_dimensions[get_column_letter(col_idx)].width = w
    ws.freeze_panes = "A4"


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

    with PASS_CSV.open() as f:
        reader = csv.DictReader(f)
        row_i = 5
        for r in reader:
            ws.cell(row=row_i, column=1, value=r["bucket"])
            ws.cell(row=row_i, column=2, value=to_int(r["n_campaigns"])).number_format = "#,##0"
            ws.cell(row=row_i, column=3, value=to_float(r["pct_campaigns"])).number_format = "0.0"
            ws.cell(row=row_i, column=4, value=to_int(r["n_advertisers"])).number_format = "#,##0"
            ws.cell(row=row_i, column=5, value=to_float(r["spend_30d_M"])).number_format = '"$"#,##0.000'
            ws.cell(row=row_i, column=6, value=to_float(r["pct_spend"])).number_format = "0.0"
            ws.cell(row=row_i, column=7, value=to_float(r["spend_annualized_M"])).number_format = '"$"#,##0.0'
            row_i += 1

    for col_idx, w in enumerate([36, 14, 14, 16, 18, 12, 18], start=1):
        ws.column_dimensions[get_column_letter(col_idx)].width = w
    ws.freeze_panes = "A5"


def main() -> None:
    rows = load_per_ds_rows()
    wb = Workbook()
    write_taxonomy_sheet(wb, rows)
    write_group_summary_sheet(wb, rows)
    write_pass_sheet(wb)
    OUT_XLSX.parent.mkdir(parents=True, exist_ok=True)
    wb.save(OUT_XLSX)
    print(f"Wrote {OUT_XLSX} ({OUT_XLSX.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
