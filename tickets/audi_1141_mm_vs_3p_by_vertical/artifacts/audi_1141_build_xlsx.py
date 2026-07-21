#!/usr/bin/env python3
"""AUDI-1141 shareable .xlsx. Tabs: Read me / MM vs 3P by vertical / Full scorecard / Overall /
Campaign detail / Queries. Rates stored as decimals, formatted as % and $. Content-sized columns."""
import pandas as pd, numpy as np
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

OUT = "tickets/audi_1141_mm_vs_3p_by_vertical/outputs/"
DEST = OUT + "audi_1141_mm_vs_3p_scorecard.xlsx"
SQLFILE = "tickets/audi_1141_mm_vs_3p_by_vertical/queries/audi_1141_cohort_scorecard.sql"
ADV_MIN_IMPS = 20000
BO = ["MM (gated)", "MM (no gate)", "MM restricted", "3P"]

HEAD="1a3c5e"; BAND="eef3f4"; GREY="666666"
HFONT=Font(bold=True,color="FFFFFF",size=11); TITLE=Font(bold=True,size=15,color=HEAD)
SUB=Font(italic=True,size=10,color=GREY); BOLD=Font(bold=True); MONO=Font(name="Consolas",size=9)
HFILL=PatternFill("solid",fgColor=HEAD); BANDF=PatternFill("solid",fgColor=BAND)
CEN=Alignment(horizontal="center",vertical="center",wrap_text=True)
LFT=Alignment(horizontal="left",vertical="top",wrap_text=True)
THIN=Side(style="thin",color="CCCCCC"); BORD=Border(left=THIN,right=THIN,top=THIN,bottom=THIN)
PCT2="0.00%"; PCT3="0.000%"; USD="\"$\"#,##0.00"; USD0="\"$\"#,##0"; NUM2="0.00"; INT="#,##0"

wb=Workbook()

def autosize(ws, df, fmt_lens=None):
    """Width from max(header, formatted values). fmt_lens = {col: display_len} override for numeric cols."""
    fmt_lens = fmt_lens or {}
    for j,col in enumerate(df.columns,1):
        if col in fmt_lens:
            w = max(len(str(col)), fmt_lens[col])
        else:
            vmax = df[col].astype(str).map(len).max() if len(df) else 0
            w = max(len(str(col)), int(vmax))
        ws.column_dimensions[get_column_letter(j)].width = min(max(w+2, 9), 46)

def write_table(ws, df, start_row, fmts, band=True, header_wrap=True):
    for j,col in enumerate(df.columns,1):
        c=ws.cell(row=start_row,column=j,value=col); c.font=HFONT; c.fill=HFILL
        c.alignment=CEN if header_wrap else Alignment(horizontal="center"); c.border=BORD
    for i,(_,r) in enumerate(df.iterrows(),1):
        rr=start_row+i
        for j,col in enumerate(df.columns,1):
            v=r[col]
            if isinstance(v,(np.integer,)): v=int(v)
            elif isinstance(v,(np.floating,)): v=float(v) if pd.notna(v) else None
            elif isinstance(v,(np.bool_,)): v=bool(v)
            c=ws.cell(row=rr,column=j,value=v); c.border=BORD
            if col in fmts: c.number_format=fmts[col]
            if j==1: c.font=BOLD
            if band and i%2==0: c.fill=BANDF
    ws.row_dimensions[start_row].height=30
    return start_row+len(df)+1

# ---------------- load + transform ----------------
df=pd.read_csv(OUT+"audi_1141_campaign_grain.csv").dropna(subset=["vertical_id"])
names=pd.read_csv(OUT+"audi_1141_advertiser_names.csv").drop_duplicates("advertiser_id")
df=df.merge(names,on="advertiser_id",how="left")
df["gated_frac"]=np.where(df.hhst_writes>0, df.hhst_writes_gated/df.hhst_writes, 0.0)
df["capped"]=df.gated_frac>=0.5
df=df[df.bucket!="Neither"].copy()
df["bucket_detail"]=np.where(df.bucket=="MM", np.where(df.capped,"MM (gated)","MM (no gate)"), df.bucket)

ov=pd.read_csv(OUT+"audi_1141_scorecard_overall.csv")
bv=pd.read_csv(OUT+"audi_1141_scorecard_by_vertical.csv")

# ================= 1) READ ME =================
ws=wb.active; ws.title="Read me"; ws.sheet_view.showGridLines=False
ws["A1"]="MNTN Matched vs 3P Segments: Prospecting Performance by Vertical"; ws["A1"].font=TITLE
ws["A2"]="Trailing 6 months. Stage-1 prospecting campaigns. Source AUDI-1141. Generated 2026-07-20."; ws["A2"].font=SUB
rows=[
 ("",""),
 ("Summary","For the typical (median) advertiser, gated MNTN Matched delivers about 6x the visit rate of 3P segments at roughly one quarter the cost per visit, and leads visit rate in every vertical. Removing the intent gate, or narrowing the audience, erodes most of that edge."),
 ("",""),
 ("How campaigns are grouped","Each Stage-1 prospecting campaign is classified from its live bidder audience expression."),
 ("MM (gated)","MNTN scoring (DS13/19/38/46) with an intent score threshold (HHST) active. Any 3P segment present is joined by OR (additive reach), and the geo is broad."),
 ("MM (no gate)","Same as MM but no intent score threshold is set."),
 ("MM restricted","MM whose audience is narrowed by an AND-required 3P clause, or by a sub-DMA geo (zip, city, or radius). Narrowing greatly restricts the eligible audience."),
 ("3P","Bought interest segments only (ShareThis, Dstillery, LiveRamp), no MM signal."),
 ("Why OR vs AND matters","About 85% of campaigns that carry a 3P segment join it with OR, which only adds reach and leaves MM doing the scoring. Only an AND-required 3P actually narrows the audience. Grouping all 3P-carrying campaigns together would misread additive 3P as restrictive."),
 ("",""),
 ("Metrics (all rates are over impressions)",""),
 ("IVR","Visit rate. Visits divided by impressions (visits = views + clicks)."),
 ("CVR","Conversion rate. Conversions divided by impressions."),
 ("CTR","Click rate. Clicks divided by impressions."),
 ("CPV","Cost per visit. Spend divided by visits."),
 ("CPM","Cost per thousand impressions."),
 ("ROAS","Revenue divided by spend."),
 ("Weighting","Headline numbers are advertiser-weighted medians (each advertiser counts once, so one large account cannot set the result). Columns labeled 'pooled' are impression or spend weighted. Advertisers with under 20,000 impressions in a cell are excluded from the median."),
 ("",""),
 ("Read before quoting",""),
 ("1. Pooled 3P is one account","If all impressions are pooled, 3P visit rate looks competitive, but roughly 39% of 3P impressions come from a single large, non-representative account. Use the per-advertiser (median) numbers."),
 ("2. ROAS is directional","Prospecting only, last-touch. Revenue mostly lands in retargeting, which is excluded, and some verticals have unreliable conversion pixels (one account shows over 800x). Visit rate and cost per visit are the solid metrics."),
 ("3. Vertical mapping","The 37 MNTN internal verticals are rolled up into the 8 sales verticals using an interim crosswalk, not an official one. Provide the RevOps crosswalk and it is a one-line swap."),
 ("4. Restricted is expected for local","Auto and ProServ legitimately use local (zip, radius) targeting, so their 'MM restricted' share is high by design. In other verticals it flags narrowing."),
 ("5. MM (no gate) is small","144 advertisers. Directionally clear, thinner than the other groups."),
 ("",""),
 ("Tabs","'MM vs 3P by vertical' is the headline. 'Full scorecard' has every group by vertical. 'Overall' is all verticals combined. 'Campaign detail' is the raw campaign rows for your own pivots. 'Queries' has the SQL used to produce these numbers."),
]
r=4
for k,v in rows:
    ws.cell(row=r,column=1,value=k).font=BOLD
    c=ws.cell(row=r,column=2,value=v); c.alignment=LFT
    r+=1
ws.column_dimensions["A"].width=30; ws.column_dimensions["B"].width=112

# ================= 2) MM vs 3P by vertical =================
def cell(v,b,col):
    x=bv[(bv.sales_vertical==v)&(bv.bucket_detail==b)]
    return x[col].iloc[0] if len(x) else np.nan
verts=[v for v in bv.sales_vertical.unique() if v!="Other / Unmapped"]
verts=sorted(verts,key=lambda v:-(cell(v,"MM (gated)","IVR_med")/max(cell(v,"3P","IVR_med"),1e-9)))
head=pd.DataFrame([{
 "Sales vertical":v,
 "MM (gated) IVR":cell(v,"MM (gated)","IVR_med"), "3P IVR":cell(v,"3P","IVR_med"),
 "IVR advantage":cell(v,"MM (gated)","IVR_med")/cell(v,"3P","IVR_med") if cell(v,"3P","IVR_med") else np.nan,
 "MM (gated) CPV":cell(v,"MM (gated)","CPV_med"), "3P CPV":cell(v,"3P","CPV_med"),
 "MM restricted IVR":cell(v,"MM restricted","IVR_med"),
 "MM advertisers":int(cell(v,"MM (gated)","n_adv_qual")) if pd.notna(cell(v,"MM (gated)","n_adv_qual")) else 0,
 "3P advertisers":int(cell(v,"3P","n_adv_qual")) if pd.notna(cell(v,"3P","n_adv_qual")) else 0,
} for v in verts])
ws2=wb.create_sheet("MM vs 3P by vertical"); ws2.sheet_view.showGridLines=False
ws2["A1"]="MM (gated) vs 3P by vertical, median advertiser"; ws2["A1"].font=TITLE
ws2["A2"]="Visit rate (higher is better) and cost per visit (lower is better). MM leads visit rate in every vertical."; ws2["A2"].font=SUB
end=write_table(ws2,head,4,{"MM (gated) IVR":PCT2,"3P IVR":PCT2,"IVR advantage":"0.0\"x\"",
  "MM (gated) CPV":USD,"3P CPV":USD,"MM restricted IVR":PCT2})
ws2.freeze_panes="A5"; ws2.auto_filter.ref=f"A4:I{4+len(head)}"
autosize(ws2,head,{"MM (gated) IVR":14,"3P IVR":10,"IVR advantage":13,"MM (gated) CPV":14,"3P CPV":10,"MM restricted IVR":16,"MM advertisers":14,"3P advertisers":14})

# ================= 3) Full scorecard =================
fcols={"sales_vertical":"Sales vertical","bucket_detail":"Group","n_adv":"Advertisers","n_adv_qual":"Adv (qualifying)",
 "n_camp":"Campaigns","spend":"Spend","imps":"Impressions","IVR_med":"IVR (median)","CVR_med":"CVR (median)",
 "CTR_med":"CTR (median)","CPV_med":"CPV (median)","ROAS_med":"ROAS (median)","n_adv_roas":"Adv with revenue",
 "IVR_pooled":"IVR (pooled)","CPV_pooled":"CPV (pooled)","CPM_pooled":"CPM (pooled)","ROAS_pooled":"ROAS (pooled)"}
full=bv[list(fcols)].rename(columns=fcols)
ws3=wb.create_sheet("Full scorecard"); ws3.sheet_view.showGridLines=False
ws3["A1"]="Full scorecard: every group by vertical"; ws3["A1"].font=TITLE
ws3["A2"]="Median = advertiser-weighted (whale-robust). Pooled = impression or spend weighted. ROAS directional only."; ws3["A2"].font=SUB
ffmt={"Spend":USD0,"Impressions":INT,"IVR (median)":PCT2,"CVR (median)":PCT3,"CTR (median)":PCT3,
 "CPV (median)":USD,"ROAS (median)":NUM2,"IVR (pooled)":PCT2,"CPV (pooled)":USD,"CPM (pooled)":USD,"ROAS (pooled)":NUM2}
write_table(ws3,full,4,ffmt)
ws3.freeze_panes="C5"; ws3.auto_filter.ref=f"A4:{get_column_letter(len(full.columns))}{4+len(full)}"
autosize(ws3,full,{"Spend":13,"Impressions":13,"IVR (median)":11,"CVR (median)":11,"CTR (median)":11,"CPV (median)":11,
 "ROAS (median)":12,"Adv with revenue":15,"IVR (pooled)":11,"CPV (pooled)":11,"CPM (pooled)":11,"ROAS (pooled)":12})

# ================= 4) Overall =================
oc=[c for c in fcols if c!="sales_vertical"]
overall=ov[oc].rename(columns=fcols)
ws4=wb.create_sheet("Overall"); ws4.sheet_view.showGridLines=False
ws4["A1"]="Overall: all verticals combined"; ws4["A1"].font=TITLE
ws4["A2"]="The intent gate and audience breadth both matter: un-gated and restricted MM fall well below gated MM."; ws4["A2"].font=SUB
write_table(ws4,overall,4,ffmt)
ws4.freeze_panes="A5"
autosize(ws4,overall,{"Spend":13,"Impressions":13,"IVR (median)":11,"CVR (median)":11,"CTR (median)":11,"CPV (median)":11,
 "ROAS (median)":12,"Adv with revenue":15,"IVR (pooled)":11,"CPV (pooled)":11,"CPM (pooled)":11,"ROAS (pooled)":12})

# ================= 5) Campaign detail =================
det=df.copy()
det["IVR"]=det.visits/det.imps; det["CVR"]=det.conv/det.imps; det["CTR"]=det.clicks/det.imps
det["CPV"]=(det.spend/det.visits).replace([np.inf,-np.inf],np.nan)
det["ROAS"]=(det.revenue/det.spend).replace([np.inf,-np.inf],np.nan)
det=det[["company_name","advertiser_id","campaign_id","sales_vertical","vertical_name","bucket_detail",
  "semantics","capped","zip_narrow","city_narrow","radius_narrow","imps","visits","clicks","conv","revenue","spend",
  "IVR","CVR","CTR","CPV","ROAS"]].rename(columns={
  "company_name":"Advertiser","advertiser_id":"Adv ID","campaign_id":"Campaign ID","sales_vertical":"Sales vertical",
  "vertical_name":"MNTN vertical","bucket_detail":"Group","semantics":"3P semantics","capped":"HHST gated",
  "zip_narrow":"Zip","city_narrow":"City","radius_narrow":"Radius","imps":"Impressions","visits":"Visits",
  "clicks":"Clicks","conv":"Conversions","revenue":"Revenue","spend":"Spend"}).sort_values(
  ["Sales vertical","Group","Spend"],ascending=[True,True,False])
ws5=wb.create_sheet("Campaign detail"); ws5.sheet_view.showGridLines=False
write_table(ws5,det,1,{"Impressions":INT,"Visits":INT,"Clicks":INT,"Conversions":INT,"Revenue":USD0,"Spend":USD0,
  "IVR":PCT2,"CVR":PCT3,"CTR":PCT3,"CPV":USD,"ROAS":NUM2},band=False,header_wrap=False)
ws5.freeze_panes="A2"; ws5.auto_filter.ref=f"A1:{get_column_letter(len(det.columns))}{len(det)+1}"
autosize(ws5,det,{"Impressions":12,"Visits":10,"Clicks":9,"Conversions":11,"Revenue":12,"Spend":12,
  "IVR":8,"CVR":8,"CTR":8,"CPV":9,"ROAS":8,"Adv ID":8,"Campaign ID":11})
ws5.row_dimensions[1].height=15

# ================= 6) Queries =================
ws6=wb.create_sheet("Queries"); ws6.sheet_view.showGridLines=False
ws6["A1"]="Queries used (for validation)"; ws6["A1"].font=TITLE
ws6["A2"]="Cohort SQL below (BigQuery). Aggregation to medians/pooled done in audi_1141_aggregate.py; see the ticket folder."; ws6["A2"].font=SUB
with open(SQLFILE) as fh: sql_lines=fh.read().split("\n")
r=4
for ln in sql_lines:
    c=ws6.cell(row=r,column=1,value=ln); c.font=MONO; r+=1
ws6.column_dimensions["A"].width=120

wb.save(DEST)
print("saved",DEST,"|",len(det),"campaign rows |",wb.sheetnames)
