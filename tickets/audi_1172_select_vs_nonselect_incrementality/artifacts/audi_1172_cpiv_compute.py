#!/usr/bin/env python3
"""AUDI-1172 CPIV reconciliation compute. Reads the obj=1-matched reconcile JSON,
derives pooled CPIV on two bases (Matt's pipeline visit basis; Reporting Verified-Visit
basis via the measured pipeline->VV factor k), plus the spend cross-checks Matt asked
about (eCPM, cost/household, implied ip_compliance). Writes a pooled CSV."""
import json, csv, pathlib

TDIR = pathlib.Path("/Users/malachi/Developer/work/mntn/workspace/tickets/audi_1172_select_vs_nonselect_incrementality")
rows = json.loads((TDIR / "outputs/audi_1172_cpiv_reconcile.json").read_text())
conv = {c["product"]: c for c in json.loads((TDIR / "outputs/audi_1172_cpia_conv.json").read_text())}

out = []
for r in rows:
    p = r["product"]
    spend = float(r["spend_total"])
    incr = float(r["incr_visits"])
    vis_pipe = float(r["vis_treatment_pipeline"])
    vv7 = float(r["vv_7d_daybucket"])
    n_t = float(r["n_treatment"])
    hh = float(r["households_reached"])
    imps = float(r["impressions"])

    k = vv7 / vis_pipe                       # pipeline -> Reporting 7d Verified-Visit factor
    incr_vv = incr * k                       # incremental visits expressed on the VV basis
    cpiv_pipe = spend / incr                 # cost per incremental (pipeline) visit
    cpiv_vv = spend / incr_vv                # cost per incremental Verified Visit
    ecpm = spend / imps * 1000              # effective CPM
    cost_per_hh = spend / hh                 # cost per household reached (Matt's compliance path anchor)
    ip_comp = hh / n_t                       # implied ip_compliance (households / bid IPs)
    freq = imps / hh                         # impressions per household

    incr_conv = float(conv[p]["incr_conv"])  # conv_abs_itt * n_treatment, pooled
    cpia_pipe = spend / incr_conv            # cost per incremental conversion (pipeline basis)

    out.append(dict(product=p, n_treatment=int(n_t), incr_visits=round(incr),
                    spend=round(spend), cpiv_pipeline=round(cpiv_pipe, 2),
                    k_pipe_to_vv=round(k, 3), incr_vv=round(incr_vv),
                    cpiv_vv=round(cpiv_vv, 2), incr_conv=round(incr_conv),
                    cpia_pipeline=round(cpia_pipe, 2), ecpm=round(ecpm, 2),
                    cost_per_household=round(cost_per_hh, 3),
                    ip_compliance=round(ip_comp, 3), freq=round(freq, 2)))

out.sort(key=lambda d: d["cpiv_pipeline"])   # rank ascending (cheapest first) for a cost metric
with open(TDIR / "outputs/audi_1172_cpiv_pooled.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(out[0].keys()))
    w.writeheader(); w.writerows(out)

print(f"{'product':<11}{'spend':>11}{'incrV':>9}{'CPIV_pipe':>10}{'k':>7}{'incrVV':>9}{'CPIV_VV':>9}{'eCPM':>7}{'$/hh':>7}{'ipComp':>7}")
for d in out:
    print(f"{d['product']:<11}{d['spend']:>11,}{d['incr_visits']:>9,}{d['cpiv_pipeline']:>10}"
          f"{d['k_pipe_to_vv']:>7}{d['incr_vv']:>9,}{d['cpiv_vv']:>9}{d['ecpm']:>7}"
          f"{d['cost_per_household']:>7}{d['ip_compliance']:>7}")
sel = next(d for d in out if d["product"] == "Select")
non = next(d for d in out if d["product"] == "non_Select")
print(f"\nCPIV gap (non-Select / Select): pipeline {non['cpiv_pipeline']/sel['cpiv_pipeline']:.1f}x"
      f"  |  VV-basis {non['cpiv_vv']/sel['cpiv_vv']:.1f}x")
print(f"CPIA (pipeline): Select ${sel['cpia_pipeline']:,.2f}  non-Select ${non['cpia_pipeline']:,.2f}"
      f"  |  gap {non['cpia_pipeline']/sel['cpia_pipeline']:.1f}x")
