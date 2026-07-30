#!/usr/bin/env python3
"""AUDI-1172 Ask 2: pool advertiser-level lift into 3 product-mix groups two ways.
IVW (precision-weighted, headline-consistent) + equal-weight advertiser (mean & robust median),
for visits and conversions. Reads the rollup group query JSON; writes a 3-row pooled CSV.
Observational comparison (advertisers self-select into Select) — not causal."""
import json, csv, math, statistics, pathlib

TDIR = pathlib.Path("/Users/malachi/Developer/work/mntn/workspace/tickets/audi_1172_select_vs_nonselect_incrementality")
rows = json.loads((TDIR / "outputs/audi_1172_aid_group_lift.json").read_text())
GROUPS = ["Both", "Select-only", "PTV-only"]

def f(x):
    return float(x) if x not in (None, "", "NaN") else None

def pool(recs):
    n_adv = len(recs)
    # --- visits ---
    # IVW pooled abs_itt / group holdout rate
    wsum = sum(1.0/f(r["se"])**2 for r in recs)
    ivw_abs = sum(f(r["abs_itt"])/f(r["se"])**2 for r in recs) / wsum
    ivw_se = math.sqrt(1.0/wsum)
    tot_vis_h = sum(f(r["vis_h"]) for r in recs); tot_n_h = sum(f(r["n_h"]) for r in recs)
    base = tot_vis_h/tot_n_h
    ivw_rel = ivw_abs/base
    ivw_ci = ((ivw_abs-1.96*ivw_se)/base, (ivw_abs+1.96*ivw_se)/base)
    ivw_sig = abs(ivw_abs/ivw_se) >= 1.96
    # equal-weight per-advertiser relative visit lift (advs with a real holdout rate)
    per = [f(r["abs_itt"])/(f(r["holdout_vr"])) for r in recs if f(r["holdout_vr"]) and f(r["holdout_vr"])>0]
    ew_mean = statistics.mean(per); ew_med = statistics.median(per)
    # --- conversions (thinner: only advs with holdout conversions and a conv SE) ---
    crecs = [r for r in recs if f(r["conv_holdout_vr"]) and f(r["conv_holdout_vr"])>0
             and f(r["conv_se"]) and f(r["conv_se"])>0 and f(r["conv_abs_itt"]) is not None]
    n_conv = len(crecs)
    if n_conv >= 1:
        cw = sum(1.0/f(r["conv_se"])**2 for r in crecs)
        civw_abs = sum(f(r["conv_abs_itt"])/f(r["conv_se"])**2 for r in crecs)/cw
        ctot_h = sum(f(r["conv_h"]) for r in crecs); ctot_n = sum(f(r["n_h"]) for r in crecs)
        cbase = ctot_h/ctot_n
        civw_rel = civw_abs/cbase
        civw_sig = abs(civw_abs/math.sqrt(1.0/cw)) >= 1.96
        cper = [f(r["conv_abs_itt"])/f(r["conv_holdout_vr"]) for r in crecs]
        cew_mean = statistics.mean(cper); cew_med = statistics.median(cper)
    else:
        civw_rel = civw_sig = cew_mean = cew_med = None
    return dict(n_adv=n_adv,
                vis_ivw=ivw_rel, vis_ivw_lo=ivw_ci[0], vis_ivw_hi=ivw_ci[1], vis_sig=ivw_sig,
                vis_ew_mean=ew_mean, vis_ew_med=ew_med,
                n_conv=n_conv, conv_ivw=civw_rel, conv_sig=civw_sig,
                conv_ew_mean=cew_mean, conv_ew_med=cew_med)

out = []
for g in GROUPS:
    recs = [r for r in rows if r["grp"] == g]
    out.append(dict(group=g, **pool(recs)))

with open(TDIR / "outputs/audi_1172_aid_group_pooled.csv", "w", newline="") as fh:
    w = csv.DictWriter(fh, fieldnames=list(out[0].keys())); w.writeheader(); w.writerows(out)

pct = lambda x: "  n/a " if x is None else f"{x*100:+6.1f}%"
print(f"{'group':<12}{'#adv':>5}  {'VIS ivw':>8}{'ivw CI':>16}{'sig':>5}{'ew mean':>9}{'ew med':>8}"
      f"  {'#conv':>5}{'CONV ivw':>9}{'ew mean':>9}")
for d in out:
    ci = f"[{d['vis_ivw_lo']*100:+.1f},{d['vis_ivw_hi']*100:+.1f}]"
    print(f"{d['group']:<12}{d['n_adv']:>5}  {pct(d['vis_ivw'])}{ci:>16}{('Y' if d['vis_sig'] else '-'):>5}"
          f"{pct(d['vis_ew_mean'])}{pct(d['vis_ew_med'])}  {d['n_conv']:>5}{pct(d['conv_ivw'])}{pct(d['conv_ew_mean'])}")
