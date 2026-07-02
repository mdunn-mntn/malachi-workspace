import csv, os, math
BASE="tickets/incr_75_eligible_advertisers/outputs"
def load(fn):
    with open(os.path.join(BASE,fn)) as f: return list(csv.DictReader(f))
def Phi(z): return 0.5*(1+math.erf(z/math.sqrt(2)))
def ztest(x1,n1,x2,n2):
    # arm1=treat, arm2=ghost; returns (abs_pp, rel, z, p, lo, hi) on absolute diff p1-p2
    if n1==0 or n2==0: return (None,)*6
    p1,p2=x1/n1,x2/n2
    diff=p1-p2
    pp=(x1+x2)/(n1+n2)
    se_pool=math.sqrt(pp*(1-pp)*(1/n1+1/n2)) if pp>0 else 0
    z=diff/se_pool if se_pool>0 else 0.0
    p=2*(1-Phi(abs(z)))
    se_u=math.sqrt(p1*(1-p1)/n1 + p2*(1-p2)/n2)
    lo,hi=diff-1.96*se_u, diff+1.96*se_u
    rel=diff/p2 if p2>0 else None
    return diff,rel,z,p,lo,hi

# pivot arm-level
arm=load("incr_75_ghost_debiased_arm.csv")
adv={}
for r in arm:
    a=r["advertiser_id"]; d=adv.setdefault(a,{})
    d[r["arm"]]=r
tier={r["advertiser_id"]:r for r in load("incr_75_final_tiered.csv")}
allf={r["advertiser_id"]:r for r in load("incr_75_all_flagged.csv")}
gi=lambda r,k:int(r[k]) if r.get(k) not in (None,"") else 0

rows=[]
agg={"raw":[0,0,0,0],"clean":[0,0,0,0]}  # tx,tn,gx,gn
for a,d in adv.items():
    g=d.get("ghost"); s=d.get("submitted")
    if not g or not s: continue
    tn_r,tx_r=gi(s,"ips_raw"),gi(s,"vis_raw"); gn_r,gx_r=gi(g,"ips_raw"),gi(g,"vis_raw")
    tn_c,tx_c=gi(s,"ips_clean"),gi(s,"vis_clean"); gn_c,gx_c=gi(g,"ips_clean"),gi(g,"vis_clean")
    agg["raw"][0]+=tx_r; agg["raw"][1]+=tn_r; agg["raw"][2]+=gx_r; agg["raw"][3]+=gn_r
    agg["clean"][0]+=tx_c; agg["clean"][1]+=tn_c; agg["clean"][2]+=gx_c; agg["clean"][3]+=gn_c
    raw=ztest(tx_r,tn_r,gx_r,gn_r)
    cln=ztest(tx_c,tn_c,gx_c,gn_c)
    t=tier.get(a); af=allf.get(a); meta=t or af or {}
    # publish gate: significant only if p<.05 AND >=20 holdout clean visits
    sig = cln[3] is not None and cln[3]<0.05 and gx_c>=20
    if not sig: signal="null/underpowered"
    elif cln[0]>0: signal="positive_sig"
    else: signal="negative_sig"
    rows.append(dict(
        advertiser_id=a, advertiser_name=meta.get("advertiser_name",""),
        final_tier=(t or {}).get("final_tier",""), in_eligible=a in tier,
        value_score=meta.get("value_score",""), ivr=meta.get("ivr",""),
        prior_lift_pp=meta.get("prior_lift_pp",""), avg_monthly_spend=meta.get("avg_monthly_spend",""),
        ghost_ips_clean=gn_c, treat_ips_clean=tn_c, ghost_vis_clean=gx_c, treat_vis_clean=tx_c,
        ghost_vr_clean=(gx_c/gn_c if gn_c else None), treat_vr_clean=(tx_c/tn_c if tn_c else None),
        abs_lift_pp_clean=(cln[0]*100 if cln[0] is not None else None),
        rel_lift_clean=(cln[1] if cln[1] is not None else None),
        z_clean=cln[2], p_clean=cln[3],
        ci_lo_pp=(cln[4]*100 if cln[4] is not None else None), ci_hi_pp=(cln[5]*100 if cln[5] is not None else None),
        abs_lift_pp_raw=(raw[0]*100 if raw[0] is not None else None),
        current_lift_signal=signal,
    ))

# --- pooled headline: raw vs debiased ---
def pooled(v):
    tx,tn,gx,gn=v
    d=ztest(tx,tn,gx,gn)
    return d,(tx/tn if tn else 0),(gx/gn if gn else 0)
pr=pooled(agg["raw"]); pc=pooled(agg["clean"])
print("=== POOLED (all advertisers) — debias effect ===")
print(f"  RAW (MAX window, all bids):     treat_vr={pr[1]*100:.4f}%  ghost_vr={pr[2]*100:.4f}%  abs_lift={pr[0][0]*100:+.4f}pp  z={pr[0][2]:+.2f}  p={pr[0][3]:.3g}")
print(f"  DEBIASED (earliest anchor, gf-clean bids<=10): treat_vr={pc[1]*100:.4f}%  ghost_vr={pc[2]*100:.4f}%  abs_lift={pc[0][0]*100:+.4f}pp  z={pc[0][2]:+.2f}  p={pc[0][3]:.3g}")
print()
from collections import Counter
sigc=Counter(r["current_lift_signal"] for r in rows)
sigc_elig=Counter(r["current_lift_signal"] for r in rows if r["in_eligible"])
print(f"=== Per-advertiser signal (n={len(rows)} with both arms) ===")
for k in ["positive_sig","negative_sig","null/underpowered"]:
    print(f"  {k:>20}: {sigc.get(k,0):>4}  (of which in INCR-75 eligible: {sigc_elig.get(k,0)})")
print()
# powered subset
powered=[r for r in rows if r["ghost_vis_clean"]>=20]
print(f"Advertisers with >=20 holdout (clean) visits — i.e. minimally powered: {len(powered)}")
print()
# write full crossref
cols=list(rows[0].keys())
outp=os.path.join(BASE,"incr_75_ghost_debiased_crossref.csv")
with open(outp,"w",newline="") as f:
    w=csv.DictWriter(f,fieldnames=cols); w.writeheader(); w.writerows(rows)

# Top-tier debiased table
top=[r for r in rows if r["final_tier"]=="Top"]
top.sort(key=lambda r:-(r["ghost_vis_clean"]))
print("=== Top-tier INCR-75 advertisers — DEBIASED current lift (publish-gated) ===")
print(f"{'AID':>6} {'name':<20} {'g_vis':>6} {'t_vr%':>7} {'g_vr%':>7} {'lift_pp':>8} {'rel':>7} {'z':>6} {'p':>7} {'signal':>16}  {'prior_pp':>8}")
for r in top[:30]:
    f2=lambda x,d=4: (f"{x:.{d}f}" if x is not None else "NA")
    tvr = f2(r['treat_vr_clean']*100) if r['treat_vr_clean'] is not None else "NA"
    gvr = f2(r['ghost_vr_clean']*100) if r['ghost_vr_clean'] is not None else "NA"
    rel = (f"{r['rel_lift_clean']*100:+.0f}%") if r['rel_lift_clean'] is not None else "NA"
    prior = str(r['prior_lift_pp'])[:6]
    print(f"{r['advertiser_id']:>6} {r['advertiser_name'][:20]:<20} {r['ghost_vis_clean']:>6} "
          f"{tvr:>7} {gvr:>7} {f2(r['abs_lift_pp_clean']):>8} {rel:>7} "
          f"{f2(r['z_clean'],2):>6} {f2(r['p_clean'],4):>7} {r['current_lift_signal']:>16}  {prior:>8}")
print(f"\nWrote: {outp}")
