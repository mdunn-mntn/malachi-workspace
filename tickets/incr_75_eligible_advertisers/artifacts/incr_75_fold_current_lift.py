import csv, os
BASE="tickets/incr_75_eligible_advertisers/outputs"
def load(fn):
    with open(os.path.join(BASE,fn)) as f: return list(csv.DictReader(f))
lift={r["advertiser_id"]:r for r in load("incr_75_ghost_debiased_crossref.csv")}
elig=load("incr_75_final_tiered.csv")   # 1,287 eligible, ordered by value_score

NEW=["in_ghost_table","ghost_days_note","ghost_ips_clean","treat_ips_clean","ghost_vis_clean",
     "current_abs_lift_pp","current_rel_lift","current_z","current_p","current_lift_signal","current_lift_confirms"]
out=[]
for r in elig:
    a=r["advertiser_id"]; L=lift.get(a)
    d=dict(r)
    if L:
        sig=L["current_lift_signal"]; tier=r.get("final_tier","")
        gx=int(L["ghost_vis_clean"] or 0)
        if tier in ("Top","Mid"):
            confirms = {"positive_sig":"CONFIRMED","negative_sig":"CONTRADICTED"}.get(sig,"unconfirmed(underpowered)")
        else:
            confirms = {"positive_sig":"positive","negative_sig":"negative"}.get(sig,"null")
        rel=L["rel_lift_clean"]
        d.update(in_ghost_table="Y", ghost_days_note="<=10d rolling window",
                 ghost_ips_clean=L["ghost_ips_clean"], treat_ips_clean=L["treat_ips_clean"], ghost_vis_clean=gx,
                 current_abs_lift_pp=(round(float(L["abs_lift_pp_clean"]),4) if L["abs_lift_pp_clean"] else ""),
                 current_rel_lift=(round(float(rel)*100,1) if rel not in (None,"") else ""),
                 current_z=(round(float(L["z_clean"]),2) if L["z_clean"] else ""),
                 current_p=(round(float(L["p_clean"]),4) if L["p_clean"] else ""),
                 current_lift_signal=sig, current_lift_confirms=confirms)
    else:
        d.update(in_ghost_table="N", ghost_days_note="", ghost_ips_clean="", treat_ips_clean="",
                 ghost_vis_clean="", current_abs_lift_pp="", current_rel_lift="", current_z="",
                 current_p="", current_lift_signal="no_data", current_lift_confirms="no_data")
    out.append(d)

cols=list(elig[0].keys())+NEW
outp=os.path.join(BASE,"incr_75_eligible_with_current_lift.csv")
with open(outp,"w",newline="") as f:
    w=csv.DictWriter(f,fieldnames=cols); w.writeheader(); w.writerows(out)

from collections import Counter
for tier in ("Top","Mid"):
    sub=[r for r in out if r["final_tier"]==tier]
    c=Counter(r["current_lift_confirms"] for r in sub)
    print(f"{tier} tier ({len(sub)}): CONFIRMED={c.get('CONFIRMED',0)}  CONTRADICTED={c.get('CONTRADICTED',0)}  "
          f"unconfirmed={c.get('unconfirmed(underpowered)',0)}  no_data={c.get('no_data',0)}")

# strongest test candidates: high value_score AND confirmed positive current lift
conf=[r for r in out if r["current_lift_confirms"]=="CONFIRMED"]
conf.sort(key=lambda r:-(float(r["current_rel_lift"]) if r["current_rel_lift"] not in ("",None) else -1))
print(f"\n=== STRONGEST CANDIDATES: Top/Mid tier + confirmed positive current lift ({len(conf)}) — top 25 by current rel lift ===")
print(f"{'AID':>6} {'name':<22} {'tier':>4} {'val_score':>9} {'ivr%':>6} {'cur_rel':>8} {'cur_pp':>7} {'z':>6} {'prior_pp':>8}")
for r in conf[:25]:
    ivr=f"{float(r['ivr'])*100:.2f}" if r.get('ivr') not in ("",None) else "NA"
    print(f"{r['advertiser_id']:>6} {r['advertiser_name'][:22]:<22} {r['final_tier']:>4} {str(r['value_score'])[:9]:>9} {ivr:>6} "
          f"{str(r['current_rel_lift'])+'%':>8} {str(r['current_abs_lift_pp']):>7} {str(r['current_z']):>6} {str(r['prior_lift_pp'])[:6]:>8}")
print(f"\nWrote: {outp}")
