import csv, math
from collections import defaultdict
D="/private/tmp/claude-501/-Users-malachi-Developer-work-mntn-workspace/fffdd8e6-e849-4e2e-abbe-6a3a7c4f267d/scratchpad/"
def load(fn):
    out=[]
    for r in csv.DictReader(open(D+fn)):
        if not r.get("advertiser_id") or r.get("se") in (None,"") or r.get("abs_itt") in (None,""): continue
        for k in ("n_treatment","n_holdout","vis_treatment","vis_holdout"): r[k]=int(r[k])
        for k in ("se","abs_itt"): r[k]=float(r[k])
        out.append(r)
    return out
def se_binom(vt,nt,vh,nh):
    pt,ph=vt/nt,vh/nh
    return math.sqrt(pt*(1-pt)/nt+ph*(1-ph)/nh)
def pool(ss):
    nt=sum(s["n_treatment"] for s in ss);nh=sum(s["n_holdout"] for s in ss)
    vt=sum(s["vis_treatment"] for s in ss);vh=sum(s["vis_holdout"] for s in ss)
    w=[1/s["se"]**2 for s in ss];y=[s["abs_itt"] for s in ss];k=len(ss)
    sw=sum(w);swy=sum(a*b for a,b in zip(w,y))
    Q=sum(a*b*b for a,b in zip(w,y))-swy*swy/sw
    den=sw-sum(a*a for a in w)/sw
    tau2=max(0.0,(Q-(k-1))/den) if den>0 else 0.0
    se_cw=math.sqrt(sum((s["n_treatment"]/nt)**2*s["se"]**2 for s in ss))
    return dict(k=k,nt=nt,nh=nh,vt=vt,vh=vh,se_naive=se_binom(vt,nt,vh,nh),
        se_fe=1/math.sqrt(sw),se_re=1/math.sqrt(sum(1/(s["se"]**2+tau2) for s in ss)),
        se_cw=se_cw,Q=Q,tau2=tau2,theta_naive=vt/nt-vh/nh,theta_fe=swy/sw,
        theta_cw=sum(s["n_treatment"]/nt*s["abs_itt"] for s in ss))
def dist(label,v):
    v=sorted(v)
    q=lambda p:v[min(len(v)-1,int(p*(len(v)-1)+0.5))]
    gm=math.exp(sum(math.log(x) for x in v)/len(v))
    print("%-38s n=%4d p10=%.3f p25=%.3f MED=%.3f p75=%.3f p90=%.3f mean=%.3f geo=%.3f pct<=0.85=%.0f%%"
      %(label,len(v),q(.1),q(.25),q(.5),q(.75),q(.9),sum(v)/len(v),gm,100*sum(1 for x in v if x<=0.85)/len(v)))

ov=load("overall_rows.csv")
byadv=defaultdict(list)
for r in ov: byadv[r["advertiser_id"]].append(r)
print("A. STRATUM = CAMPAIGN GROUP within ADVERTISER (clean-gated, partner 8, read 2026-09-03)")
for mv,lab in ((0,"no min"),(10,">=10 hv/stratum"),(100,">=100 hv/stratum")):
    fe=[];cw=[];re_=[];na=0;nk=0
    for a,ss in byadv.items():
        ss=[s for s in ss if s["vis_holdout"]>=mv]
        if len(ss)<2: continue
        p=pool(ss);na+=1;nk+=p["k"]
        fe.append(p["se_fe"]/p["se_naive"]);cw.append(p["se_cw"]/p["se_naive"]);re_.append(p["se_re"]/p["se_naive"])
    print("\n  [%s]  %d advertisers, %d strata"%(lab,na,nk))
    dist("   IVW fixed-effect / naive",fe)
    dist("   count-weighted strat / naive",cw)
    dist("   IVW random-effects / naive",re_)

R=load("strata_rows.csv")
bands=defaultdict(list)
for r in R:
    if r["stratum_type"]=="score_band" and r["se"]>0: bands[r["campaign_group_id"]].append(r)
print("\nB. STRATUM = SCORE BAND within CAMPAIGN GROUP")
for mv,lab in ((0,"no min"),(10,">=10 hv/stratum"),(100,">=100 hv/stratum")):
    fe=[];cw=[];re_=[];nc=0;nk=0
    for cg,bs in bands.items():
        bs=[b for b in bs if b["vis_holdout"]>=mv]
        if len(bs)<2: continue
        p=pool(bs);nc+=1;nk+=p["k"]
        fe.append(p["se_fe"]/p["se_naive"]);cw.append(p["se_cw"]/p["se_naive"]);re_.append(p["se_re"]/p["se_naive"])
    print("\n  [%s]  %d campaign groups, %d strata"%(lab,nc,nk))
    dist("   IVW fixed-effect / naive",fe)
    dist("   count-weighted strat / naive",cw)
    dist("   IVW random-effects / naive",re_)

print("\nC. DEGENERATE-STRATUM ARTIFACT: advertisers with IVW/naive < 0.3, campaign strata")
n=0
for a,ss in byadv.items():
    if len(ss)<2: continue
    p=pool(ss)
    if p["se_fe"]/p["se_naive"]<0.30 and n<6:
        n+=1
        print("  adv %s  k=%d  ratio=%.3f"%(a,p["k"],p["se_fe"]/p["se_naive"]))
        for s in sorted(ss,key=lambda x:x["se"]):
            print("    cg %-8s n_t=%9d v_t=%7d n_h=%8d v_h=%6d  p_t=%.5f p_h=%.5f  se=%.3e  IVW wt=%5.1f%%"%(
                s["campaign_group_id"],s["n_treatment"],s["vis_treatment"],s["n_holdout"],s["vis_holdout"],
                s["vis_treatment"]/s["n_treatment"],s["vis_holdout"]/s["n_holdout"],s["se"],
                100*(1/s["se"]**2)/sum(1/x["se"]**2 for x in ss)))
