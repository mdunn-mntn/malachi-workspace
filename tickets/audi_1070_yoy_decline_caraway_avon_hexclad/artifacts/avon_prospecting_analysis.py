"""AUDI-1070 — Avon PROSPECTING performance, first-touch (industry_standard) attribution.
Jan-May 2025 vs Jan-May 2026, two scopes: stage1_only (obj=1, the MM CTV prospecting) and
all_prospecting (obj!=4). Raw counts + rates + true %-change, monthly and aggregate.
Reproduces the prospecting UI card exactly (all_prospecting: $56,833/272,218 visits/9.39x)."""
import csv, os

D = "tickets/audi_1070_yoy_decline_caraway_avon_hexclad/"
MONTHS = {1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May"}

# monthly raw: scope -> yr -> mo -> dict
RAW = {}
# (scope, yr, mo, spend, imps, households, visits, conversions, order_value)
monthly = [
 ("all_prospecting",2025,1,6375,466044,292278,34186,1626,79484),
 ("all_prospecting",2025,2,13845,1135839,553731,48626,1292,68287),
 ("all_prospecting",2025,3,14542,1157052,564724,71764,2254,115667),
 ("all_prospecting",2025,4,7308,591722,322095,61008,2709,141126),
 ("all_prospecting",2025,5,14763,1128420,541115,56634,2586,129044),
 ("all_prospecting",2026,1,5460,376132,226100,23341,1266,60442),
 ("all_prospecting",2026,2,6825,497166,266019,29160,1166,59135),
 ("all_prospecting",2026,3,6830,468893,258318,34612,1707,92885),
 ("all_prospecting",2026,4,13779,1019802,436653,47189,2407,125040),
 ("all_prospecting",2026,5,13721,982508,484158,52898,2850,145944),
 ("stage1_only",2025,1,5494,267872,254521,32773,1573,76648),
 ("stage1_only",2025,2,11470,544081,494585,42610,1152,60822),
 ("stage1_only",2025,3,12422,623905,507479,65193,2041,103825),
 ("stage1_only",2025,4,6262,318350,277499,57817,2539,131258),
 ("stage1_only",2025,5,12751,595879,503689,54718,2502,124907),
 ("stage1_only",2026,1,4691,203857,191024,20786,1149,55025),
 ("stage1_only",2026,2,5830,251441,230205,26360,1084,55278),
 ("stage1_only",2026,3,5820,243076,218422,31271,1574,85016),
 ("stage1_only",2026,4,11705,519542,387829,43398,2234,115387),
 ("stage1_only",2026,5,11655,525716,424623,49411,2678,137197),
]
# aggregate households come from a separate HLL merge (can't sum monthly sketches)
AGG_HH = {("all_prospecting",2025):1826270,("all_prospecting",2026):1144625,
          ("stage1_only",2025):1790411,("stage1_only",2026):1109896}

def rates(d):
    r = dict(d)
    r["roas"] = d["order_value"]/d["spend"] if d["spend"] else 0
    r["visit_rate_pct"] = d["visits"]/d["households"]*100 if d["households"] else 0
    r["conv_rate_pct"] = d["conversions"]/d["visits"]*100 if d["visits"] else 0
    r["cpa"] = d["spend"]/d["conversions"] if d["conversions"] else 0
    r["aov"] = d["order_value"]/d["conversions"] if d["conversions"] else 0
    r["cpm"] = d["spend"]/d["imps"]*1000 if d["imps"] else 0
    return r

def mk(row):
    s,y,m,sp,im,hh,vi,co,ov = row
    return s,y,m,{"spend":sp,"imps":im,"households":hh,"visits":vi,"conversions":co,"order_value":ov}

data = {}
for row in monthly:
    s,y,m,d = mk(row)
    data.setdefault((s,y),{})[m]=d

def agg(scope,yr):
    tot={"spend":0,"imps":0,"visits":0,"conversions":0,"order_value":0}
    for m in range(1,6):
        for k in tot: tot[k]+=data[(scope,yr)][m][k]
    tot["households"]=AGG_HH[(scope,yr)]
    return tot

METRICS=[("spend","Spend","$%,.0f"),("imps","Impressions","%,d"),("households","Households","%,d"),
    ("visits","Verified Visits","%,d"),("conversions","Conversions","%,d"),("order_value","Order Value","$%,.0f"),
    ("roas","ROAS","%.2fx"),("visit_rate_pct","Visit Rate","%.2f%%"),("conv_rate_pct","Conv Rate","%.2f%%"),
    ("cpa","CPA","$%.2f"),("aov","AOV","$%.2f"),("cpm","CPM","$%.2f")]

def pct(a,b): return (b/a-1)*100 if a else 0
def fmt(v,f):
    try: return f % v
    except: return str(v)

def table(scope, title, out_rows):
    a25=rates(agg(scope,2025)); a26=rates(agg(scope,2026))
    print(f"\n{'='*74}\n{title}  —  AGGREGATE (Jan–May 2025 vs 2026, first-touch)\n{'='*74}")
    print(f"{'Metric':<16}{'2025':>16}{'2026':>16}{'% change':>14}")
    for key,lab,f in METRICS:
        c=pct(a25[key],a26[key]); print(f"{lab:<16}{fmt(a25[key],f):>16}{fmt(a26[key],f):>16}{c:>+13.1f}%")
        out_rows.append([scope,"AGG",lab,fmt(a25[key],f),fmt(a26[key],f),f"{c:+.1f}%"])
    # monthly % change per metric
    print(f"\n{title}  —  MONTH-vs-MONTH % change (2026 vs 2025, first-touch)")
    hdr="".join(f"{MONTHS[m]:>12}" for m in range(1,6))
    print(f"{'Metric':<16}{hdr}")
    for key,lab,f in METRICS:
        cells=""
        for m in range(1,6):
            r25=rates(data[(scope,2025)][m]); r26=rates(data[(scope,2026)][m])
            c=pct(r25[key],r26[key]); cells+=f"{c:>+11.0f}%"
            out_rows.append([scope,MONTHS[m],lab,fmt(r25[key],f),fmt(r26[key],f),f"{c:+.1f}%"])
        print(f"{lab:<16}{cells}")

out_rows=[["scope","period","metric","val_2025","val_2026","pct_change"]]
table("stage1_only","STAGE 1 ONLY (obj=1, MM CTV prospecting — 259556)", out_rows)
table("all_prospecting","ALL PROSPECTING STAGES (obj!=4)", out_rows)

with open(D+"outputs/avon_prospecting_ft_full.csv","w",newline="") as fh:
    csv.writer(fh).writerows(out_rows)
print(f"\nsaved outputs/avon_prospecting_ft_full.csv ({len(out_rows)-1} rows)")
