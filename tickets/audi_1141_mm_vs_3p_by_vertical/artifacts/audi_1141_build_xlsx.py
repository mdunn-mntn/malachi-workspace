#!/usr/bin/env python3
"""AUDI-1141 — build the shareable .xlsx scorecard (openpyxl).
Tabs: Read me · MM vs 3P by vertical · Full scorecard · Overall · Campaign detail (pivotable).
Reproducible: reads outputs/*.csv only. Google-Sheets friendly (autofilter, freeze panes, number fmts)."""
import pandas as pd, numpy as np
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from openpyxl.utils.dataframe import dataframe_to_rows

OUT = "tickets/audi_1141_mm_vs_3p_by_vertical/outputs/"
DEST = "tickets/audi_1141_mm_vs_3p_by_vertical/outputs/audi_1141_mm_vs_3p_scorecard.xlsx"
ADV_MIN_IMPS = 20000
ZIP_KEEP = {"Auto, Travel & Hospitality", "ProServ"}
BO = ["MM (gated)", "MM (no gate)", "Mixed", "3P"]

# ---- styles ----
NAVY = "1a3c5e"; TEAL = "1a6e6a"; HEAD = "1a3c5e"; BAND = "eef3f4"; GREY = "666666"
HFONT = Font(bold=True, color="FFFFFF", size=11, name="Calibri")
TITLE = Font(bold=True, size=15, color=NAVY, name="Calibri")
SUB = Font(italic=True, size=10, color=GREY, name="Calibri")
BOLD = Font(bold=True, name="Calibri")
HFILL = PatternFill("solid", fgColor=HEAD)
BANDF = PatternFill("solid", fgColor=BAND)
CEN = Alignment(horizontal="center", vertical="center")
LFT = Alignment(horizontal="left", vertical="center", wrap_text=True)
THIN = Side(style="thin", color="CCCCCC")
BORD = Border(left=THIN, right=THIN, top=THIN, bottom=THIN)

wb = Workbook()

def style_header(ws, row, ncols):
    for c in range(1, ncols+1):
        cell = ws.cell(row=row, column=c); cell.font = HFONT; cell.fill = HFILL
        cell.alignment = CEN; cell.border = BORD

def write_df(ws, df, start_row, fmts=None, band=True):
    """Write a dataframe with header styling, borders, number formats, banding."""
    fmts = fmts or {}
    for j, col in enumerate(df.columns, 1):
        ws.cell(row=start_row, column=j, value=col)
    style_header(ws, start_row, len(df.columns))
    for i, (_, r) in enumerate(df.iterrows(), 1):
        rr = start_row + i
        for j, col in enumerate(df.columns, 1):
            v = r[col]
            if isinstance(v, (np.integer,)): v = int(v)
            elif isinstance(v, (np.floating,)): v = float(v) if pd.notna(v) else None
            cell = ws.cell(row=rr, column=j, value=v)
            cell.border = BORD
            if col in fmts: cell.number_format = fmts[col]
            if j == 1: cell.font = BOLD
            if band and i % 2 == 0: cell.fill = BANDF
    return start_row + len(df) + 1

def autosize(ws, widths):
    for col, w in widths.items(): ws.column_dimensions[col].width = w

# ============================================================ 1) READ ME
ws = wb.active; ws.title = "Read me"; ws.sheet_view.showGridLines = False
ws["A1"] = "MNTN Matched vs 3P Segments — Prospecting Performance by Vertical"; ws["A1"].font = TITLE
ws["A2"] = "Prepared for Jon Zucker · trailing 6 months · S1 prospecting · AUDI-1141 · 2026-07-20"; ws["A2"].font = SUB
notes = [
 ("", ""),
 ("THE ONE THING", "For the typical advertiser, gated MNTN Matched drives ~4x the visit rate of 3P at ~1/3 the cost per visit — and wins in every vertical. Turn MM's score gate off and it collapses toward 3P."),
 ("", ""),
 ("How campaigns are bucketed", "Every S1 prospecting campaign classified from its live bidder audience expression:"),
 ("  MM", "MNTN scoring only (DS13/19/38/46), no 3P segment."),
 ("  3P", "Bought interest only — ShareThis / Dstillery / LiveRamp (DS17/18/35), no MM."),
 ("  Mixed", "Both. ~72% of campaigns using a 3P segment also carry MM, which does the scoring underneath — so a naive 'MM vs 3P' double-counts MM. Kept separate."),
 ("  MM (gated) vs (no gate)", "MM (gated) = an intent score threshold (HHST) is on; MM (no gate) = MM on but not gating. The gate is what creates MM's value."),
 ("", ""),
 ("Metrics", "VR = visits per 1,000 impressions · CPV = cost per visit · CVR = conversions per visit · ROAS = revenue / spend."),
 ("Weighting", "Headline = ADVERTISER-WEIGHTED MEDIAN (each advertiser = one vote; whale-robust). 'pooled' columns = impression/spend-weighted. Advertisers with <20k imps in a cell are excluded from the median."),
 ("", ""),
 ("READ BEFORE QUOTING", ""),
 ("  1. Pooled 3P is a whale", "If you pool all impressions, 3P's visit rate beats MM — but that's ~39% one non-representative account (WGU). Use the per-advertiser (median) numbers, not pooled totals."),
 ("  2. ROAS is directional", "Prospecting-only, last-touch. Revenue mostly lands in retargeting (excluded), and some verticals have bad conversion pixels (e.g. a ProServ advertiser shows 812x). Visit rate & CPV are the solid metrics."),
 ("  3. Vertical mapping", "37 MNTN internal verticals rolled into the 8 sales verticals — my crosswalk, not RevOps-official. Send the canonical one and it's a one-line swap. B2B, Food & Beverage are judgment calls."),
 ("  4. Zip filter", "Zip-code-targeted campaigns dropped for all verticals EXCEPT Auto and ProServ (per Jon)."),
 ("  5. MM (no gate)", "Small group (74 advertisers) — directionally clear, thinner than the rest."),
 ("", ""),
 ("Tabs", "'MM vs 3P by vertical' = the headline (filter here). 'Full scorecard' = every bucket x vertical, all metrics. 'Overall' = all verticals combined. 'Campaign detail' = raw campaign-level rows for your own pivots."),
 ("Questions", "Ping Malachi for a different cut (channel, specific advertiser, add a vertical, different attribution lens)."),
]
r = 4
for k, v in notes:
    ws.cell(row=r, column=1, value=k).font = BOLD
    c = ws.cell(row=r, column=2, value=v); c.alignment = LFT
    r += 1
autosize(ws, {"A": 26, "B": 105})
ws.row_dimensions[1].height = 20

# ============================================================ load + transform cohort (for detail tab)
df = pd.read_csv(OUT+"audi_1141_campaign_grain.csv").dropna(subset=["vertical_id"])
names = pd.read_csv(OUT+"audi_1141_advertiser_names.csv").drop_duplicates("advertiser_id")
df = df.merge(names, on="advertiser_id", how="left")
df["gated_frac"] = np.where(df.hhst_writes > 0, df.hhst_writes_gated/df.hhst_writes, 0.0)
df["capped"] = df.gated_frac >= 0.5
df = df[~(df.zip_narrow & ~df.sales_vertical.isin(ZIP_KEEP))]
df = df[df.bucket != "Neither"].copy()
df["bucket_detail"] = np.where(df.bucket=="MM", np.where(df.capped,"MM (gated)","MM (no gate)"), df.bucket)

# ============================================================ 2) MM vs 3P BY VERTICAL (headline)
bv = pd.read_csv(OUT+"audi_1141_scorecard_by_vertical.csv")
def cell(v,b,col):
    r = bv[(bv.sales_vertical==v)&(bv.bucket_detail==b)]
    return r[col].iloc[0] if len(r) else np.nan
verts = [v for v in bv.sales_vertical.unique() if v != "Other / Unmapped"]
verts = sorted(verts, key=lambda v: -(cell(v,"MM (gated)","VR_med")/max(cell(v,"3P","VR_med"),0.01)))
head = pd.DataFrame([{
    "Sales vertical": v,
    "MM VR": round(cell(v,"MM (gated)","VR_med"),1), "3P VR": round(cell(v,"3P","VR_med"),1),
    "VR advantage (x)": round(cell(v,"MM (gated)","VR_med")/cell(v,"3P","VR_med"),1) if cell(v,"3P","VR_med") else np.nan,
    "MM CPV": round(cell(v,"MM (gated)","CPV_med"),2), "3P CPV": round(cell(v,"3P","CPV_med"),2),
    "CPV advantage (x)": round(cell(v,"3P","CPV_med")/cell(v,"MM (gated)","CPV_med"),1) if cell(v,"MM (gated)","CPV_med") else np.nan,
    "MM advertisers": int(cell(v,"MM (gated)","n_adv_qual")), "3P advertisers": int(cell(v,"3P","n_adv_qual")),
} for v in verts])
ws2 = wb.create_sheet("MM vs 3P by vertical"); ws2.sheet_view.showGridLines = False
ws2["A1"] = "MM (gated) vs 3P — median advertiser, by vertical"; ws2["A1"].font = TITLE
ws2["A2"] = "Visits per 1,000 imps (higher=better) · $ cost per visit (lower=better) · MM wins both in every vertical"; ws2["A2"].font = SUB
end = write_df(ws2, head, 4, fmts={"MM VR":"0.0","3P VR":"0.0","VR advantage (x)":'0.0"x"',
    "MM CPV":'"$"0.00',"3P CPV":'"$"0.00',"CPV advantage (x)":'0.0"x"'})
ws2.freeze_panes = "A5"; ws2.auto_filter.ref = f"A4:I{4+len(head)}"
autosize(ws2, {"A":28,"B":9,"C":9,"D":16,"E":11,"F":11,"G":17,"H":15,"I":14})

# ============================================================ 3) FULL SCORECARD (vertical x bucket)
full = bv.copy()
full["_o"] = full.bucket_detail.map({b:i for i,b in enumerate(BO)})
full = full.sort_values(["sales_vertical","_o"])
fcols = {"sales_vertical":"Sales vertical","bucket_detail":"Bucket","n_adv":"Advertisers",
 "n_adv_qual":"Adv (qual.)","n_camp":"Campaigns","spend":"Spend","imps":"Impressions",
 "VR_med":"VR (median)","CPV_med":"CPV (median)","ROAS_med":"ROAS (median)*","n_adv_roas":"Adv w/ rev",
 "VR_pooled":"VR (pooled)","CPV_pooled":"CPV (pooled)","ROAS_pooled":"ROAS (pooled)*"}
full = full[list(fcols)].rename(columns=fcols).round(2)
ws3 = wb.create_sheet("Full scorecard"); ws3.sheet_view.showGridLines = False
ws3["A1"] = "Full scorecard — every bucket x vertical"; ws3["A1"].font = TITLE
ws3["A2"] = "*ROAS directional only (prospecting/last-touch; pixel artifacts). Median = whale-robust headline; pooled = impression/spend-weighted."; ws3["A2"].font = SUB
write_df(ws3, full, 4, fmts={"Spend":'"$"#,##0',"Impressions":"#,##0","VR (median)":"0.0",
 "CPV (median)":'"$"0.00',"ROAS (median)*":"0.00","VR (pooled)":"0.0","CPV (pooled)":'"$"0.00',"ROAS (pooled)*":"0.00"})
ws3.freeze_panes = "A5"; ws3.auto_filter.ref = f"A4:N{4+len(full)}"
autosize(ws3, {"A":28,"B":14,"C":11,"D":11,"E":10,"F":13,"G":14,"H":11,"I":12,"J":13,"K":11,"L":11,"M":12,"N":13})

# ============================================================ 4) OVERALL
ov = pd.read_csv(OUT+"audi_1141_scorecard_overall.csv")
ov["_o"] = ov.bucket_detail.map({b:i for i,b in enumerate(BO)}); ov = ov.sort_values("_o")
ov = ov[list(fcols)[1:]].rename(columns=fcols).round(2)  # drop sales_vertical col
ws4 = wb.create_sheet("Overall"); ws4.sheet_view.showGridLines = False
ws4["A1"] = "Overall — all verticals combined"; ws4["A1"].font = TITLE
ws4["A2"] = "The gate story: un-gated MM collapses toward 3P. Use median (advertiser-weighted), not pooled (WGU dominates 3P)."; ws4["A2"].font = SUB
write_df(ws4, ov, 4, fmts={"Spend":'"$"#,##0',"Impressions":"#,##0","VR (median)":"0.0",
 "CPV (median)":'"$"0.00',"ROAS (median)*":"0.00","VR (pooled)":"0.0","CPV (pooled)":'"$"0.00',"ROAS (pooled)*":"0.00"})
ws4.freeze_panes = "A5"
autosize(ws4, {"A":14,"B":11,"C":11,"D":10,"E":13,"F":13,"G":11,"H":12,"I":12,"J":11,"K":11,"L":12,"M":13})

# ============================================================ 5) CAMPAIGN DETAIL (pivotable)
det = df.copy()
det["VR"] = (1000*det.visits/det.imps).round(2)
det["CPV"] = (det.spend/det.visits).replace([np.inf,-np.inf],np.nan).round(2)
det["ROAS"] = (det.revenue/det.spend).replace([np.inf,-np.inf],np.nan).round(2)
det = det[["company_name","advertiser_id","campaign_id","sales_vertical","vertical_name","bucket_detail",
           "capped","zip_narrow","imps","visits","conv","revenue","spend","VR","CPV","ROAS"]]
det = det.rename(columns={"company_name":"Advertiser","advertiser_id":"Adv ID","campaign_id":"Campaign ID",
  "sales_vertical":"Sales vertical","vertical_name":"MNTN vertical","bucket_detail":"Bucket",
  "capped":"HHST gated","zip_narrow":"Zip-narrowed","imps":"Impressions","visits":"Visits","conv":"Conversions",
  "revenue":"Revenue","spend":"Spend"}).sort_values(["Sales vertical","Bucket","Spend"],ascending=[True,True,False])
ws5 = wb.create_sheet("Campaign detail"); ws5.sheet_view.showGridLines = False
for j, col in enumerate(det.columns, 1): ws5.cell(row=1, column=j, value=col)
style_header(ws5, 1, len(det.columns))
for i, (_, r) in enumerate(det.iterrows(), 2):
    for j, col in enumerate(det.columns, 1):
        v = r[col]
        if isinstance(v,(np.integer,)): v=int(v)
        elif isinstance(v,(np.floating,)): v=float(v) if pd.notna(v) else None
        elif isinstance(v,(np.bool_,)): v=bool(v)
        ws5.cell(row=i, column=j, value=v)
for col,fmt in {"Impressions":"#,##0","Visits":"#,##0","Conversions":"#,##0","Revenue":'"$"#,##0',
                "Spend":'"$"#,##0',"CPV":'"$"0.00',"ROAS":"0.00","VR":"0.0"}.items():
    ci = list(det.columns).index(col)+1
    for rr in range(2, len(det)+2): ws5.cell(row=rr, column=ci).number_format = fmt
ws5.freeze_panes = "A2"; ws5.auto_filter.ref = f"A1:{get_column_letter(len(det.columns))}{len(det)+1}"
autosize(ws5, {"A":32,"B":9,"C":11,"D":26,"E":22,"F":13,"G":11,"H":12,"I":12,"J":10,"K":11,"L":12,"M":12,"N":8,"O":9,"P":8})

wb.save(DEST)
print("saved", DEST, "|", len(det), "campaign rows across", det["Sales vertical"].nunique(), "verticals")
