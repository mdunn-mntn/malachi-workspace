#!/usr/bin/env python3
"""AUDI-1172 CPIV/CPIA on the CLIENT-FACING Verified-Visit basis (what Kirsa sees in Reporting).
Method (Matt's call): incremental = Reporting_metric x rel_lift/(1+rel_lift), rel_lift = volume-weighted
(raw-count) pooled lift from the ghost-bid pipeline. Reporting VV = clicks+views+competing_views;
Reporting conv = click_conversions+view_conversions+competing_view_conversions (AUDI-1070 authoritative).
Contrast with the pipeline basis (spend / Matt's pipeline incremental_visits) which UNDERCOUNTS visits."""
import json, csv, pathlib

TDIR = pathlib.Path("/Users/malachi/Developer/work/mntn/workspace/tickets/audi_1172_select_vs_nonselect_incrementality")
rows = json.loads((TDIR / "outputs/audi_1172_cpiv_vv_correct.json").read_text())

out = []
for r in rows:
    p = r["product"]
    spend = float(r["spend"])
    vv = float(r["vv_reported"])
    conv = float(r["conv_reported"])
    rl_v = float(r["rel_lift_raw"])
    rl_c = float(r["conv_rel_lift_raw"])

    incr_vv = vv * rl_v / (1 + rl_v)          # incremental Verified Visits
    incr_cv = conv * rl_c / (1 + rl_c)        # incremental Reporting conversions
    cpiv = spend / incr_vv
    cpia = spend / incr_cv
    out.append(dict(product=p, spend=round(spend), vv_reported=round(vv),
                    rel_lift_raw=round(rl_v, 3), incr_vv=round(incr_vv), cpiv_vv=round(cpiv, 2),
                    conv_reported=round(conv), conv_rel_lift=round(rl_c, 3),
                    incr_conv=round(incr_cv), cpia_vv=round(cpia, 2)))

out.sort(key=lambda d: d["cpiv_vv"])
with open(TDIR / "outputs/audi_1172_cpiv_vv_pooled.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(out[0].keys())); w.writeheader(); w.writerows(out)

print(f"{'product':<11}{'spend':>11}{'VV_rep':>10}{'relLift':>8}{'incrVV':>9}{'CPIV_VV':>9}"
      f"{'conv_rep':>9}{'incrConv':>9}{'CPIA_VV':>9}")
for d in out:
    print(f"{d['product']:<11}{d['spend']:>11,}{d['vv_reported']:>10,}{d['rel_lift_raw']:>8}"
          f"{d['incr_vv']:>9,}{d['cpiv_vv']:>9}{d['conv_reported']:>9,}{d['incr_conv']:>9,}{d['cpia_vv']:>9}")
sel = next(d for d in out if d["product"] == "Select")
non = next(d for d in out if d["product"] == "non_Select")
print(f"\nCLIENT (Verified-Visit) basis:  CPIV {non['cpiv_vv']/sel['cpiv_vv']:.1f}x cheaper Select"
      f"  |  CPIA {non['cpia_vv']/sel['cpia_vv']:.1f}x cheaper Select")
print("Pipeline basis (for contrast): CPIV 5.1x, CPIA 9.8x — inflated by Matt's pipeline undercounting non-Select visits")
