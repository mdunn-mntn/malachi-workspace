"""Definitive current-lift fold — entry-anchored, EXCLUDING 2026-06-22 left edge,
7d-from-first-bid visit window (per Matt Brorby, 2026-07-02). Source: silver
enriched.lift__ghost_bid_visits. Supersedes the hand-rolled debias AND the gold
rollup (rollup is all-time/includes 06-22 -> contaminated negative)."""
import csv, os, math
BASE="tickets/incr_75_eligible_advertisers/outputs"
def load(fn):
    with open(os.path.join(BASE,fn)) as f: return list(csv.DictReader(f))
def Phi(z): return 0.5*(1+math.erf(z/math.sqrt(2)))
def stats(vt,nt,vh,nh):
    if nt==0 or nh==0 or vh==0: return (None,)*6
    p1,p2=vt/nt,vh/nh; se=math.sqrt(p1*(1-p1)/nt+p2*(1-p2)/nh)
    rel=(p1-p2)/p2; z=(p1-p2)/se if se>0 else 0; p=2*(1-Phi(abs(z)))
    return (p1-p2),rel,z,p,(p1-p2)-1.96*se,(p1-p2)+1.96*se
gi=lambda r,k:int(r[k])
ex={r["advertiser_id"]:r for r in load("incr_75_ghost_excl0622.csv")}
elig=load("incr_75_final_tiered.csv")

NEW=["in_ghost_table","current_lift_confirms","current_lift_signal","current_rel_lift","current_abs_lift_pp",
     "ghost_vis_clean","current_z","current_p","current_ci_low_pp","current_ci_high_pp","current_lift_source"]
out=[]
for r in elig:
    a=r["advertiser_id"]; d=dict(r); tier=r.get("final_tier",""); R=ex.get(a)
    if R and gi(R,"n_h")>0:
        nt,nh,vt,vh=gi(R,"n_t"),gi(R,"n_h"),gi(R,"v_t"),gi(R,"v_h")
        ab,rel,z,p,lo,hi=stats(vt,nt,vh,nh)
        powered=vh>=20; sig=powered and p is not None and p<0.05
        s="null/underpowered" if not sig else ("positive_sig" if rel>0 else "negative_sig")
        conf=({"positive_sig":"CONFIRMED","negative_sig":"CONTRADICTED"}.get(s,"unconfirmed(underpowered)")
              if tier in ("Top","Mid") else {"positive_sig":"positive","negative_sig":"negative"}.get(s,"null"))
        d.update(in_ghost_table="Y",current_lift_confirms=conf,current_lift_signal=s,
                 current_rel_lift=(round(rel*100,1) if rel is not None else ""),
                 current_abs_lift_pp=(round(ab*100,4) if ab is not None else ""),
                 ghost_vis_clean=vh,current_z=(round(z,2) if z is not None else ""),
                 current_p=(round(p,4) if p is not None else ""),
                 current_ci_low_pp=(round(lo*100,4) if lo is not None else ""),
                 current_ci_high_pp=(round(hi*100,4) if hi is not None else ""),
                 current_lift_source="entry_cohort_excl_0622")
    else:
        d.update(in_ghost_table="N",current_lift_confirms="no_data",current_lift_signal="no_data",
                 current_rel_lift="",current_abs_lift_pp="",ghost_vis_clean="",current_z="",current_p="",
                 current_ci_low_pp="",current_ci_high_pp="",current_lift_source="")
    out.append(d)
cols=list(elig[0].keys())+NEW
with open(os.path.join(BASE,"incr_75_eligible_with_current_lift.csv"),"w",newline="") as fo:
    csv.DictWriter(fo,fieldnames=cols).writeheader() or None
    w=csv.DictWriter(fo,fieldnames=cols); 
with open(os.path.join(BASE,"incr_75_eligible_with_current_lift.csv"),"w",newline="") as fo:
    w=csv.DictWriter(fo,fieldnames=cols); w.writeheader(); w.writerows(out)
from collections import Counter
for t in ("Top","Mid"):
    sub=[r for r in out if r["final_tier"]==t]; c=Counter(r["current_lift_confirms"] for r in sub)
    print(f"{t} ({len(sub)}): CONFIRMED={c.get('CONFIRMED',0)} CONTRADICTED={c.get('CONTRADICTED',0)} "
          f"unconfirmed={c.get('unconfirmed(underpowered)',0)} no_data={c.get('no_data',0)}")
print("wrote incr_75_eligible_with_current_lift.csv (source: entry_cohort_excl_0622)")
