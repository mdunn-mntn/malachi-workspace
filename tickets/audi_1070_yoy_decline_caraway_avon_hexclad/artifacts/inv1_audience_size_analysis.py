import csv
names={31921:'Avon',34611:'HexClad',40341:'Caraway'}

# Audience size (UI-reported addressable pool, stage-1)
aud={}
with open('outputs/q_inv1_audsize_monthly.csv') as f:
    for r in csv.DictReader(f):
        aud[(int(r['advertiser_id']), r['mo'])]=float(r['avg_pool'])

# CIL HI supply via model_params
cil={}
with open('outputs/q_inv1_cil_hi_modelparams.csv') as f:
    for r in csv.DictReader(f):
        cil[(int(r['advertiser_id']), r['mo'])]=r

print("="*110)
print("INVESTIGATION 1 — HI/MM AUDIENCE SIZE OVER TIME")
print("="*110)
for aid in [31921,34611,40341]:
    print(f"\n### {names[aid]} ({aid})")
    print(f"{'month':10} {'UI_addr_pool':>14} {'served_IPs':>11} {'HI_IPs(ahs>=8k)':>15} {'HI_share%':>9} {'reach/avail%':>11}")
    months=sorted({k[1] for k in list(aud)+list(cil) if k[0]==aid})
    for m in months:
        ap=aud.get((aid,m))
        c=cil.get((aid,m))
        served=int(c['ips_total']) if c else None
        hi=int(c['ips_ahs8000']) if c else None
        hishare = (hi/served*100) if (hi and served) else None
        reach = (hi/ap*100) if (hi and ap) else None
        print(f"{m[:7]:10} {('%.1fM'%(ap/1e6)) if ap else '—':>14} "
              f"{('%.0fk'%(served/1e3)) if served else '—':>11} "
              f"{('%.0fk'%(hi/1e3)) if hi else '—':>15} "
              f"{('%.1f'%hishare) if hishare else '—':>9} "
              f"{('%.2f'%reach) if reach else '—':>11}")
