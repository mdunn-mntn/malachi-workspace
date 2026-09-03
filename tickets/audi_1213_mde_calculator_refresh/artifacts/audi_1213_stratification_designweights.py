import csv, math
from collections import defaultdict
D="/private/tmp/claude-501/-Users-malachi-Developer-work-mntn-workspace/fffdd8e6-e849-4e2e-abbe-6a3a7c4f267d/scratchpad/"
def load(fn):
    out=[]
    for r in csv.DictReader(open(D+fn)):
        if not r.get("advertiser_id") or r.get("se") in (None,"") : continue
        for k in ("n_treatment","n_holdout","vis_treatment","vis_holdout"): r[k]=int(r[k])
        for k in ("se","abs_itt"): r[k]=float(r[k])
        out.append(r)
    return out
def dist(l,v):
    v=sorted(v); q=lambda p:v[min(len(v)-1,int(p*(len(v)-1)+0.5))]
    print("%-46s n=%4d p10=%.3f p25=%.3f MED=%.3f p75=%.3f p90=%.3f mean=%.3f pct<=0.85=%.0f%%"
      %(l,len(v),q(.1),q(.25),q(.5),q(.75),q(.9),sum(v)/len(v),100*sum(1 for x in v if x<=0.85)/len(v)))
def analyse(groups,name,minvh):
    rd=[];rat=[];theo=[];shift=[]
    nu=0;nk=0
    for g,ss in groups.items():
        ss=[s for s in ss if s["vis_holdout"]>=minvh]
        if len(ss)<2: continue
        nt=sum(s["n_treatment"] for s in ss);nh=sum(s["n_holdout"] for s in ss)
        vt=sum(s["vis_treatment"] for s in ss);vh=sum(s["vis_holdout"] for s in ss)
        pt,ph=vt/nt,vh/nh
        var_naive=pt*(1-pt)/nt+ph*(1-ph)/nh
        wd=[1/(pt*(1-pt)/s["n_treatment"]+ph*(1-ph)/s["n_holdout"]) for s in ss]
        sw=sum(wd)
        var_dw=sum(w*w*s["se"]**2 for w,s in zip(wd,ss))/(sw*sw)
        th_dw=sum(w*s["abs_itt"] for w,s in zip(wd,ss))/sw
        th_nv=pt-ph
        varp=sum((s["n_treatment"]/nt)*(s["vis_treatment"]/s["n_treatment"]-pt)**2 for s in ss)
        theo.append(math.sqrt(max(0.0,1-varp/(pt*(1-pt)))) if pt>0 else 1.0)
        rd.append(math.sqrt(var_dw/var_naive))
        if abs(th_nv)>0: shift.append(abs(th_dw-th_nv)/abs(th_nv))
        nu+=1;nk+=len(ss)
    print("\n  [%s, >=%d holdout visits/stratum]  %d units, %d strata"%(name,minvh,nu,nk))
    dist("   design-weighted IVW SE / naive SE",rd)
    dist("   theoretical sqrt(1-Var_n(p)/p(1-p))",theo)
    dist("   |estimand shift| design-wt vs naive (rel)",shift)

ov=load("overall_rows.csv")
byadv=defaultdict(list)
for r in ov: byadv[r["advertiser_id"]].append(r)
print("A. campaign-group strata within advertiser")
for mv in (0,10,100): analyse(byadv,"campaign within advertiser",mv)

R=load("strata_rows.csv")
bands=defaultdict(list)
for r in R:
    if r["stratum_type"]=="score_band" and r["se"]>0: bands[r["campaign_group_id"]].append(r)
print("\nB. score-band strata within campaign group")
for mv in (0,10,100): analyse(bands,"score band within campaign",mv)
