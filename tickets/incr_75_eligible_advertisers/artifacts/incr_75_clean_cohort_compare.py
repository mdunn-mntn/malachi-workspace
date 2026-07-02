import csv, os, math
BASE="tickets/incr_75_eligible_advertisers/outputs"
def load(fn):
    with open(os.path.join(BASE,fn)) as f: return list(csv.DictReader(f))
def Phi(z): return 0.5*(1+math.erf(z/math.sqrt(2)))
def stats(v_t,n_t,v_h,n_h):
    if n_t==0 or n_h==0 or v_h==0: return (None,None,None)
    p1,p2=v_t/n_t,v_h/n_h
    se=math.sqrt(p1*(1-p1)/n_t + p2*(1-p2)/n_h)
    rel=(p1-p2)/p2
    z=(p1-p2)/se if se>0 else 0.0
    p=2*(1-Phi(abs(z)))
    return rel,z,p

rows=load("incr_75_ghost_entry_cohort.csv")
gi=lambda r,k:int(r[k])
clean={}  # advertiser -> 06-23 counts
for r in rows:
    if r["entry_dt"]=="2026-06-23":
        clean[r["advertiser_id"]]=dict(n_t=gi(r,"n_t"),n_h=gi(r,"n_h"),v_t=gi(r,"v_t"),v_h=gi(r,"v_h"))

# my earlier folded result
prev={r["advertiser_id"]:r for r in load("incr_75_ghost_debiased_crossref.csv")}
tier={r["advertiser_id"]:r for r in load("incr_75_final_tiered.csv")}

out=[]
for a,c in clean.items():
    rel,z,p = stats(c["v_t"],c["n_t"],c["v_h"],c["n_h"])
    powered = c["v_h"]>=20
    sig = powered and p is not None and p<0.05
    if not sig: clean_sig="null/underpowered"
    elif rel>0: clean_sig="positive_sig"
    else: clean_sig="negative_sig"
    pv=prev.get(a,{})
    out.append(dict(advertiser_id=a, name=(tier.get(a,{}) or {}).get("advertiser_name",""),
        final_tier=(tier.get(a,{}) or {}).get("final_tier",""),
        n_h=c["n_h"], v_h=c["v_h"], clean_rel=(round(rel*100,1) if rel is not None else None),
        clean_z=(round(z,2) if z is not None else None), clean_sig=clean_sig,
        prev_sig=pv.get("current_lift_signal",""), prev_rel=pv.get("rel_lift_clean","")))

from collections import Counter
print(f"Advertisers with a 06-23 clean cohort: {len(out)}")
powered=[r for r in out if r["v_h"]>=20]
print(f"  ...powered on the single clean day (>=20 holdout visits): {len(powered)}   (vs 619 in my 10-day-pooled version)")
print()
# pooled clean-day
tt=sum(c['v_t'] for c in clean.values()); tn=sum(c['n_t'] for c in clean.values())
gt=sum(c['v_h'] for c in clean.values()); gn=sum(c['n_h'] for c in clean.values())
rel,z,p=stats(tt,tn,gt,gn)
print(f"POOLED clean-day (06-23): treat_vr={100*tt/tn:.4f}%  ghost_vr={100*gt/gn:.4f}%  rel_lift={rel*100:+.1f}%  z={z:+.1f}")
print()
cc=Counter(r["clean_sig"] for r in powered)
print("Clean-day signal (powered advertisers only):", dict(cc))
print()
# how my 'positive_sig' advertisers fare under the clean method
mine_pos=[r for r in out if r["prev_sig"]=="positive_sig"]
mp_pow=[r for r in mine_pos if r["v_h"]>=20]
agree=[r for r in mp_pow if r["clean_sig"]=="positive_sig"]
flip=[r for r in mp_pow if r["clean_sig"]=="negative_sig"]
nulld=[r for r in mp_pow if r["clean_sig"]=="null/underpowered"]
under=[r for r in mine_pos if r["v_h"]<20]
print(f"Of my {len(mine_pos)} 'positive_sig' advertisers:")
print(f"   still powered on clean day: {len(mp_pow)}   |  now underpowered (thin clean cohort): {len(under)}")
print(f"      -> clean-day CONFIRMS positive: {len(agree)}")
print(f"      -> clean-day FLIPS to negative: {len(flip)}")
print(f"      -> clean-day now NULL:          {len(nulld)}")
print()
# Top-tier confirmed shortlist under clean method
tops=[r for r in out if r["final_tier"] in ("Top","Mid") and r["v_h"]>=20]
tops.sort(key=lambda r:-(r["clean_rel"] if r["clean_rel"] is not None else -1e9))
print("=== Top/Mid tier, powered on clean day — by clean relative lift (top 20) ===")
print(f"{'AID':>6} {'name':<22} {'tier':>4} {'v_h':>5} {'clean_rel':>9} {'clean_z':>8} {'my_prev_rel':>11}")
for r in tops[:20]:
    print(f"{r['advertiser_id']:>6} {r['name'][:22]:<22} {r['final_tier']:>4} {r['v_h']:>5} "
          f"{str(r['clean_rel'])+'%':>9} {str(r['clean_z']):>8} {str(r['prev_rel'])+'%':>11}")
# write
cols=list(out[0].keys())
with open(os.path.join(BASE,"incr_75_clean_cohort_compare.csv"),"w",newline="") as f:
    w=csv.DictWriter(f,fieldnames=cols); w.writeheader(); w.writerows(out)
