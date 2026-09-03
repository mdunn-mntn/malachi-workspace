"""Attribute a saved BigQuery plan's slot-ms to its source tables by tracing stage inputs."""
import json
import re
import sys
from collections import defaultdict

VIEW_TABLES = {"external.impression__v1", "external.bidder_win_notifications__v1", "external.vastimpression__v1"}
CIL_TABLES = {"sqlmesh__logdata.logdata__cost_impression_log__2498930125"}
BOS_TABLES = {"external.camperbid_prod__bos__campaign_summary_hourly"}


def classify(tables):
    kinds = set()
    for t in tables:
        if t in VIEW_TABLES:
            kinds.add("view_bwn_impressions")
        elif t in CIL_TABLES:
            kinds.add("cil")
        elif t in BOS_TABLES:
            kinds.add("bos_target")
        elif t.startswith("integrationprod."):
            kinds.add("dims_margins")
        else:
            kinds.add("other")
    return kinds


plan = json.load(open(sys.argv[1]))["statistics"]["query"]["queryPlan"]
by_id = {s["id"]: s for s in plan}
sources = {}


def resolve(stage_id):
    if stage_id in sources:
        return sources[stage_id]
    tables = set()
    for step in by_id[stage_id].get("steps", []):
        for sub in step.get("substeps", []):
            m = re.match(r"FROM (\S+)", sub)
            if not m:
                continue
            ref = m.group(1)
            if ref.startswith("__stage"):
                tables |= resolve(str(int(re.match(r"__stage([0-9A-F]+)_", ref).group(1), 16)))
            elif ref.startswith("dw-main-"):
                tables.add(".".join(ref.split(".")[1:]))
    sources[stage_id] = tables
    return tables


total = sum(int(s["slotMs"]) for s in plan)
bucket = defaultdict(int)
for s in plan:
    kinds = classify(resolve(s["id"]))
    if "view_bwn_impressions" in kinds and "cil" not in kinds:
        key = "spend_pacing view (bwn/impression logs + dims)"
    elif "cil" in kinds and "view_bwn_impressions" not in kinds:
        key = "cost_impression_log 4-day half"
    elif "cil" in kinds and "view_bwn_impressions" in kinds:
        key = "union of both halves onward"
    elif kinds:
        key = "dims/margins only (view side)"
    else:
        key = "no table lineage"
    bucket[key] += int(s["slotMs"])
print(f"stages={len(plan)} total_slot_h={total/3.6e6:.2f}")
for k, v in sorted(bucket.items(), key=lambda kv: -kv[1]):
    print(f"{v/3.6e6:7.2f} slot-h  {100*v/total:5.1f}%  {k}")
top = sorted(plan, key=lambda s: -int(s["slotMs"]))[:8]
print("top stages:")
for s in top:
    print(f"  {s['name']:<28} {int(s['slotMs'])/3.6e6:6.2f} slot-h  read={int(s['recordsRead']):>13,}  sources={sorted(resolve(s['id']))}")
