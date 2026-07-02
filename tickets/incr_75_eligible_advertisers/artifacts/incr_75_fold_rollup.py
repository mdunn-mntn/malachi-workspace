import csv, os, math
BASE="tickets/incr_75_eligible_advertisers/outputs"
def load(fn):
    with open(os.path.join(BASE,fn)) as f: return list(csv.DictReader(f))
def Phi(z): return 0.5*(1+math.erf(z/math.sqrt(2)))
def f(x):
    try: return float(x)
    except: return None

roll={r["advertiser_id"]:r for r in load("incr_75_ghost_rollup_advertiser.csv")}
elig=load("incr_75_final_tiered.csv")
prev={r["advertiser_id"]:r for r in load("incr_75_ghost_debiased_crossref.csv")}  # my hand-rolled

def signal(r):
    vh=int(r["vis_holdout"]); sig=r["significant_95"].lower()=="true"; rel=f(r["rel_itt"])
    powered = vh>=20
    if not (sig and powered): return "null/underpowered"
    return "positive_sig" if rel and rel>0 else "negative_sig"

NEW=["in_ghost_table","current_lift_confirms","current_lift_signal","current_rel_lift","current_abs_lift_pp",
     "holdout_visits","current_z","current_ci_low_pp","current_ci_high_pp","mh_agrees","low_coverage","current_lift_source"]
out=[]
for r in elig:
    a=r["advertiser_id"]; R=roll.get(a); d=dict(r); tier=r.get("final_tier","")
    if R:
        sig=signal(R); rel=f(R["rel_itt"]); vh=int(R["vis_holdout"])
        if tier in ("Top","Mid"):
            conf={"positive_sig":"CONFIRMED","negative_sig":"CONTRADICTED"}.get(sig,"unconfirmed(underpowered)")
        else:
            conf={"positive_sig":"positive","negative_sig":"negative"}.get(sig,"null")
        d.update(in_ghost_table="Y", current_lift_confirms=conf, current_lift_signal=sig,
            current_rel_lift=(round(rel*100,1) if rel is not None else ""),
            current_abs_lift_pp=(round(f(R["abs_itt"])*100,4) if f(R["abs_itt"]) is not None else ""),
            holdout_visits=vh, current_z=(round(f(R["z"]),2) if f(R["z"]) is not None else ""),
            current_ci_low_pp=(round(f(R["abs_ci_low"])*100,4) if f(R["abs_ci_low"]) is not None else ""),
            current_ci_high_pp=(round(f(R["abs_ci_high"])*100,4) if f(R["abs_ci_high"]) is not None else ""),
            mh_agrees=R.get("ivw_mh_agree",""), low_coverage=R.get("low_coverage",""),
            current_lift_source="matt_gold_rollup")
    else:
        d.update(in_ghost_table="N", current_lift_confirms="no_data", current_lift_signal="no_data",
            current_rel_lift="", current_abs_lift_pp="", holdout_visits="", current_z="",
            current_ci_low_pp="", current_ci_high_pp="", mh_agrees="", low_coverage="", current_lift_source="")
    out.append(d)

cols=list(elig[0].keys())+NEW
with open(os.path.join(BASE,"incr_75_eligible_with_current_lift.csv"),"w",newline="") as fo:
    w=csv.DictWriter(fo,fieldnames=cols); w.writeheader(); w.writerows(out)

from collections import Counter
print("=== Matt's gold rollup vs my hand-rolled debias ===")
inr=[r for r in out if r["in_ghost_table"]=="Y"]
print(f"Eligible advertisers with a rollup lift row: {len(inr)}  (of 1,287 eligible)")
for tier in ("Top","Mid"):
    sub=[r for r in out if r["final_tier"]==tier]
    c=Counter(r["current_lift_confirms"] for r in sub)
    print(f"  {tier} ({len(sub)}): CONFIRMED={c.get('CONFIRMED',0)} CONTRADICTED={c.get('CONTRADICTED',0)} "
          f"unconfirmed={c.get('unconfirmed(underpowered)',0)} no_data={c.get('no_data',0)}")
# compare to my earlier signal
both=[(r, prev.get(r['advertiser_id'])) for r in out if r['in_ghost_table']=='Y' and prev.get(r['advertiser_id'])]
agree=sum(1 for r,p in both if r['current_lift_signal']==p.get('current_lift_signal'))
mypos=[ (r,p) for r,p in both if p.get('current_lift_signal')=='positive_sig']
mp_confirm=sum(1 for r,p in mypos if r['current_lift_signal']=='positive_sig')
mp_flip=sum(1 for r,p in mypos if r['current_lift_signal']=='negative_sig')
print(f"\nMy 'positive_sig' set n={len(mypos)} (that also have a rollup row):")
print(f"   Matt's rollup agrees positive: {mp_confirm}   flips negative: {mp_flip}   null: {len(mypos)-mp_confirm-mp_flip}")

# robust confirmed shortlist
conf=[r for r in out if r["current_lift_confirms"]=="CONFIRMED"]
conf.sort(key=lambda r:-(f(r["current_z"]) or -1e9))  # rank by z (Matt's SE-based), most reliable first
print(f"\n=== CONFIRMED (Top/Mid, sig + >=20 holdout visits) — {len(conf)}, top 20 by z (SE-ranked) ===")
print(f"{'AID':>6} {'name':<22} {'tier':>4} {'hv':>5} {'rel%':>7} {'abs_pp':>8} {'z':>6} {'CI_pp':>16}")
for r in conf[:20]:
    ci=f"[{r['current_ci_low_pp']},{r['current_ci_high_pp']}]"
    print(f"{r['advertiser_id']:>6} {r['advertiser_name'][:22]:<22} {r['final_tier']:>4} {r['holdout_visits']:>5} "
          f"{str(r['current_rel_lift']):>7} {str(r['current_abs_lift_pp']):>8} {str(r['current_z']):>6} {ci:>16}")
