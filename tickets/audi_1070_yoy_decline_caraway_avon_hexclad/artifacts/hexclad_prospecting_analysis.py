"""AUDI-1070 — HexClad PROSPECTING performance, first-touch (industry_standard).
Jan-May 2025 vs 2026, two scopes: stage1_only (obj=1 funnel=1) and all_prospecting (obj=1).
Raw counts + rates + true %-change, monthly and aggregate. Mirror of the Avon analysis."""
import csv
D = "tickets/audi_1070_yoy_decline_caraway_avon_hexclad/"
MONTHS = {1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May"}
# (scope, yr, mo, spend, imps, households, visits, conversions, order_value)
monthly = [
 ("all_prospecting",2025,1,139592,6583176,3323905,143429,3843,1256838),
 ("all_prospecting",2025,2,134707,6503047,4001245,99685,2762,1183189),
 ("all_prospecting",2025,3,99217,4938516,2896532,64585,1279,473901),
 ("all_prospecting",2025,4,83606,4025320,2159029,79526,1456,687647),
 ("all_prospecting",2025,5,185074,8633572,3861225,166365,4056,2035921),
 ("all_prospecting",2026,1,179841,7597402,4283233,74224,1209,464028),
 ("all_prospecting",2026,2,217761,9463529,5706745,128081,1788,688947),
 ("all_prospecting",2026,3,190409,8049671,4277062,53943,730,305562),
 ("all_prospecting",2026,4,182020,8280440,5250612,99735,1702,760178),
 ("all_prospecting",2026,5,198290,9008301,4082985,148993,3360,1531792),
 ("stage1_only",2025,1,122790,5772208,3132279,83435,2140,717220),
 ("stage1_only",2025,2,123465,5958853,3865677,73096,1601,686773),
 ("stage1_only",2025,3,84637,4189043,2815579,46857,893,341663),
 ("stage1_only",2025,4,70889,3389388,2032215,62740,1085,511080),
 ("stage1_only",2025,5,157923,7360607,3754452,127588,3133,1567064),
 ("stage1_only",2026,1,152624,6403349,3972129,74041,1207,463650),
 ("stage1_only",2026,2,184799,8009853,5616514,127769,1778,684831),
 ("stage1_only",2026,3,161632,6820566,3972968,43592,565,234611),
 ("stage1_only",2026,4,153713,6982380,5117226,74025,1158,517081),
 ("stage1_only",2026,5,167021,7579572,3888854,108982,2315,1029403),
]
AGG_HH = {("all_prospecting",2025):11540841,("all_prospecting",2026):14783353,
          ("stage1_only",2025):11388026,("stage1_only",2026):14652354}
data={}
for s,y,m,sp,im,hh,vi,co,ov in monthly:
    data.setdefault((s,y),{})[m]={"spend":sp,"imps":im,"households":hh,"visits":vi,"conversions":co,"order_value":ov}
def agg(s,y):
    t={"spend":0,"imps":0,"visits":0,"conversions":0,"order_value":0}
    for m in range(1,6):
        for k in t: t[k]+=data[(s,y)][m][k]
    t["households"]=AGG_HH[(s,y)]; return t
def rates(d):
    r=dict(d)
    r["roas"]=d["order_value"]/d["spend"] if d["spend"] else 0
    r["visit_rate_pct"]=d["visits"]/d["households"]*100 if d["households"] else 0
    r["conv_rate_pct"]=d["conversions"]/d["visits"]*100 if d["visits"] else 0
    r["cpa"]=d["spend"]/d["conversions"] if d["conversions"] else 0
    r["aov"]=d["order_value"]/d["conversions"] if d["conversions"] else 0
    r["cpm"]=d["spend"]/d["imps"]*1000 if d["imps"] else 0
    return r
MET=[("spend","Spend","$%,.0f"),("imps","Impressions","%,d"),("households","Households","%,d"),
    ("visits","Verified Visits","%,d"),("conversions","Conversions","%,d"),("order_value","Order Value","$%,.0f"),
    ("roas","ROAS","%.2fx"),("visit_rate_pct","Visit Rate","%.3f%%"),("conv_rate_pct","Conv Rate","%.2f%%"),
    ("cpa","CPA","$%.2f"),("aov","AOV","$%.2f"),("cpm","CPM","$%.2f")]
def pct(a,b): return (b/a-1)*100 if a else 0
def fmt(v,f):
    try: return f%v
    except: return str(v)
out=[["scope","period","metric","2025","2026","pct_change"]]
for scope,title in [("stage1_only","STAGE 1 ONLY (obj=1 funnel=1)"),("all_prospecting","ALL PROSPECTING (obj=1)")]:
    a25=rates(agg(scope,2025)); a26=rates(agg(scope,2026))
    print(f"\n{'='*70}\n{title} — AGGREGATE (Jan–May, first-touch)\n{'='*70}")
    print(f"{'Metric':<16}{'2025':>15}{'2026':>15}{'%chg':>10}")
    for k,lab,f in MET:
        c=pct(a25[k],a26[k]); print(f"{lab:<16}{fmt(a25[k],f):>15}{fmt(a26[k],f):>15}{c:>+9.1f}%")
        out.append([scope,"AGG",lab,fmt(a25[k],f),fmt(a26[k],f),f"{c:+.1f}%"])
    print(f"{title} — MONTH-vs-MONTH %chg (ROAS / Conv rate / Visit rate)")
    for k in ["roas","conv_rate_pct","visit_rate_pct"]:
        cells="".join(f"{MONTHS[m]} {pct(rates(data[(scope,2025)][m])[k],rates(data[(scope,2026)][m])[k]):>+5.0f}%  " for m in range(1,6))
        print(f"  {k:<15} {cells}")
        for m in range(1,6):
            c=pct(rates(data[(scope,2025)][m])[k],rates(data[(scope,2026)][m])[k])
            out.append([scope,MONTHS[m],k,"","",f"{c:+.1f}%"])
with open(D+"outputs/hexclad_prospecting_ft_full.csv","w",newline="") as fh:
    csv.writer(fh).writerows(out)
print(f"\nsaved outputs/hexclad_prospecting_ft_full.csv")
