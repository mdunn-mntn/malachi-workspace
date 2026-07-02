import csv, os, math
BASE="tickets/incr_75_eligible_advertisers/outputs"
def load(fn):
    with open(os.path.join(BASE,fn)) as f: return list(csv.DictReader(f))
def Phi(z): return 0.5*(1+math.erf(z/math.sqrt(2)))
def stats(vt,nt,vh,nh):
    if nt==0 or nh==0 or vh==0: return (None,)*4
    p1,p2=vt/nt,vh/nh; se=math.sqrt(p1*(1-p1)/nt+p2*(1-p2)/nh)
    rel=(p1-p2)/p2; z=(p1-p2)/se if se>0 else 0; p=2*(1-Phi(abs(z)))
    return rel,z,p,(p1-p2)
ex={r["advertiser_id"]:r for r in load("incr_75_ghost_excl0622.csv")}
roll={r["advertiser_id"]:r for r in load("incr_75_ghost_rollup_advertiser.csv")}
gi=lambda r,k:int(r[k])

# pooled: exclude-0622 vs rollup(all-time incl 0622)
tt=sum(gi(r,'v_t') for r in ex.values()); tn=sum(gi(r,'n_t') for r in ex.values())
gt=sum(gi(r,'v_h') for r in ex.values()); gn=sum(gi(r,'n_h') for r in ex.values())
rel,z,p,ab=stats(tt,tn,gt,gn)
print(f"POOLED, entry-anchored, EXCLUDING 06-22 (all days 06-23..07-01, Matt pragmatic):")
print(f"   treat_vr={100*tt/tn:.4f}%  ghost_vr={100*gt/gn:.4f}%  abs={ab*100:+.4f}pp  rel={rel*100:+.1f}%  z={z:+.1f}")
# rollup pooled (incl 0622, all-time)
rtt=sum(gi(r,'vis_treatment') for r in roll.values()); rtn=sum(gi(r,'n_treatment') for r in roll.values())
rgt=sum(gi(r,'vis_holdout') for r in roll.values()); rgn=sum(gi(r,'n_holdout') for r in roll.values())
rr=stats(rtt,rtn,rgt,rgn)
print(f"POOLED, Matt's gold ROLLUP (all-time, includes 06-22):")
print(f"   treat_vr={100*rtt/rtn:.4f}%  ghost_vr={100*rgt/rgn:.4f}%  abs={rr[3]*100:+.4f}pp  rel={rr[0]*100:+.1f}%  z={rr[1]:+.1f}")
print(f"   -> ghost_frac rollup={rgn/(rtn+rgn):.4f}  vs exclude-0622={gn/(tn+gn):.4f} (design=0.10)")

# per-advertiser signal on exclude-0622
tier={r["advertiser_id"]:r for r in load("incr_75_final_tiered.csv")}
out=[]
for a,r in ex.items():
    nt,nh,vt,vh=gi(r,'n_t'),gi(r,'n_h'),gi(r,'v_t'),gi(r,'v_h')
    rel,z,p,ab=stats(vt,nt,vh,nh)
    powered=vh>=20; sig=powered and p is not None and p<0.05
    s="null/underpowered" if not sig else ("positive_sig" if rel>0 else "negative_sig")
    t=tier.get(a,{}); out.append(dict(advertiser_id=a,name=t.get("advertiser_name",""),
        final_tier=t.get("final_tier",""),hv=vh,rel=(round(rel*100,1) if rel is not None else None),
        z=(round(z,2) if z is not None else None),sig=s))
from collections import Counter
powered=[r for r in out if r["hv"]>=20]
print(f"\nPer-advertiser (exclude 06-22): {len(out)} advs, {len(powered)} powered (>=20 holdout visits)")
c=Counter(r["sig"] for r in powered); print("  signal:",dict(c))
for tier_ in ("Top","Mid"):
    sub=[r for r in out if r["final_tier"]==tier_]
    cc=Counter(("CONFIRMED" if r["sig"]=="positive_sig" else "CONTRADICTED" if r["sig"]=="negative_sig" else "unconfirmed") for r in sub if r["hv"]>=20)
    print(f"  {tier_}: {dict(cc)} (of {len(sub)}; rest underpowered/no-data)")
