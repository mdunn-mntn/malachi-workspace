#!/usr/bin/env python3
"""Map each Spark stage in an event log to its SQL operators and print a per-stage skew table.

Usage: python3 audi_1276_stage_map.py <airflow-ti include dir> <eventlog.zstd> [...]
Writes <log>.stagemap.txt next to each log and one CSV row per stage to stdout (--csv).
"""
import argparse
import csv
import json
import os
import re
import sys
from collections import defaultdict
from statistics import median

DAG_BY_APP = {
    "Populate conv_log_ip_advertiser_id.ConvLogIpAdvertiserId": "conv_log_ip_advertiser_id",
    "Populate guid_log_ip_advertiser_id.GuidLogIpAdvertiserId": "guid_log_ip_advertiser_id",
    "Populate guid_log_ip_guid_advertiser_id.GuidLogIpGuidAdvertiserId": "guid_log_ip_guid_advertiser_id",
    "Populate ipdsc_42_monitor.IPDSC42Monitor": "ipdsc_42_monitor",
}
PROP_KEYS = (
    "spark.sql.adaptive.enabled",
    "spark.sql.adaptive.skewJoin.enabled",
    "spark.sql.adaptive.skewJoin.skewedPartitionFactor",
    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes",
    "spark.sql.autoBroadcastJoinThreshold",
    "spark.sql.shuffle.partitions",
    "spark.sql.requireAllClusterKeysForDistribution",
    "spark.dynamicAllocation.initialExecutors",
    "spark.dynamicAllocation.maxExecutors",
    "spark.executor.instances",
)
JOIN_RE = re.compile(r"(BroadcastHashJoin|SortMergeJoin|ShuffledHashJoin|BroadcastNestedLoopJoin)")
CODEGEN_RE = re.compile(r"WholeStageCodegen \((\d+)\)")
ID_SUFFIX_RE = re.compile(r"#\d+L?")


def clean(s: str) -> str:
    return ID_SUFFIX_RE.sub("", s)


class Run:
    def __init__(self):
        self.app_id = None
        self.app_name = None
        self.start_ts = None
        self.end_ts = None
        self.props = {}
        self.stages = {}
        self.tasks = defaultdict(list)
        self.jobs = {}
        self.exec_plans = {}
        self.exec_desc = {}
        self.exec_first_plan = {}


def parse(path: str, read_events) -> Run:
    run = Run()
    for e in read_events(path):
        ev = e.get("Event", "")
        if ev == "SparkListenerApplicationStart":
            run.app_id = e.get("App ID")
            run.app_name = e.get("App Name")
            run.start_ts = e.get("Timestamp")
        elif ev == "SparkListenerApplicationEnd":
            run.end_ts = e.get("Timestamp")
        elif ev == "SparkListenerEnvironmentUpdate":
            run.props = e.get("Spark Properties", {}) or {}
        elif ev == "SparkListenerJobStart":
            props = e.get("Properties", {}) or {}
            run.jobs[e["Job ID"]] = {
                "exec_id": props.get("spark.sql.execution.id"),
                "stage_ids": e.get("Stage IDs", []),
                "start": e.get("Submission Time"),
            }
        elif ev == "SparkListenerJobEnd":
            job = run.jobs.get(e["Job ID"])
            if job is not None:
                job["end"] = e.get("Completion Time")
        elif ev == "SparkListenerStageCompleted":
            si = e["Stage Info"]
            scopes = []
            for rdd in si.get("RDD Info", []):
                sc = rdd.get("Scope")
                if sc:
                    try:
                        scopes.append(json.loads(sc).get("name", ""))
                    except json.JSONDecodeError:
                        scopes.append(sc)
            run.stages[si["Stage ID"]] = {
                "name": si.get("Stage Name", ""),
                "attempt": si.get("Stage Attempt ID", 0),
                "num_tasks": si.get("Number of Tasks", 0),
                "parents": si.get("Parent IDs", []),
                "scopes": list(dict.fromkeys(scopes)),
                "submit": si.get("Submission Time"),
                "complete": si.get("Completion Time"),
            }
        elif ev == "SparkListenerTaskEnd":
            ti = e.get("Task Info", {}) or {}
            tm = e.get("Task Metrics", {}) or {}
            sr = tm.get("Shuffle Read Metrics", {}) or {}
            sw = tm.get("Shuffle Write Metrics", {}) or {}
            inp = tm.get("Input Metrics", {}) or {}
            run.tasks[e["Stage ID"]].append({
                "dur_ms": (ti.get("Finish Time", 0) or 0) - (ti.get("Launch Time", 0) or 0),
                "read": (inp.get("Bytes Read", 0) or 0)
                + (sr.get("Remote Bytes Read", 0) or 0)
                + (sr.get("Local Bytes Read", 0) or 0),
                "shuffle_read": (sr.get("Remote Bytes Read", 0) or 0) + (sr.get("Local Bytes Read", 0) or 0),
                "shuffle_write": sw.get("Shuffle Bytes Written", 0) or 0,
                "fetch_wait_ms": sr.get("Fetch Wait Time", 0) or 0,
                "run_ms": tm.get("Executor Run Time", 0) or 0,
                "cpu_ms": (tm.get("Executor CPU Time", 0) or 0) // 1_000_000,
                "exec": ti.get("Executor ID"),
                "failed": bool(ti.get("Failed")),
            })
        elif ev.endswith("SparkListenerSQLExecutionStart"):
            eid = e.get("executionId")
            run.exec_desc[eid] = e.get("description", "")
            run.exec_first_plan[eid] = e.get("sparkPlanInfo")
            run.exec_plans.setdefault(eid, e.get("sparkPlanInfo"))
        elif ev.endswith("SparkListenerSQLAdaptiveExecutionUpdate"):
            run.exec_plans[e.get("executionId")] = e.get("sparkPlanInfo")
    return run


def codegen_blocks(plan: dict) -> dict:
    """{codegen number: [operator strings]} for one sparkPlanInfo tree."""
    blocks = {}

    def walk(node, current):
        name = node.get("nodeName", "")
        m = CODEGEN_RE.fullmatch(name)
        if m:
            current = int(m.group(1))
            blocks.setdefault(current, [])
        elif current is not None and name != "InputAdapter":
            blocks[current].append(clean(node.get("simpleString", name)))
        if name == "InputAdapter":
            current = None
        for c in node.get("children", []):
            walk(c, current)

    walk(plan, None)
    return blocks


def plan_nodes(plan: dict) -> list:
    out = []

    def walk(node, depth):
        out.append((depth, node.get("nodeName", ""), clean(node.get("simpleString", ""))))
        for c in node.get("children", []):
            walk(c, depth + 1)

    walk(plan, 0)
    return out


def stage_rows(run: Run) -> list:
    exec_of_stage = {}
    job_of_stage = {}
    for jid, job in run.jobs.items():
        for sid in job["stage_ids"]:
            exec_of_stage[sid] = job["exec_id"]
            job_of_stage[sid] = jid
    blocks_by_exec = {eid: codegen_blocks(p) for eid, p in run.exec_plans.items() if p}
    rows = []
    for sid, st in sorted(run.stages.items()):
        tasks = [t for t in run.tasks.get(sid, []) if not t["failed"]]
        if not tasks:
            continue
        durs = [t["dur_ms"] for t in tasks]
        reads = [t["read"] for t in tasks]
        eid = exec_of_stage.get(sid)
        blocks = blocks_by_exec.get(int(eid), {}) if eid is not None else {}
        ops = []
        for sc in st["scopes"]:
            m = CODEGEN_RE.fullmatch(sc)
            if m and int(m.group(1)) in blocks:
                ops.extend(blocks[int(m.group(1))])
            elif not m:
                ops.append(sc)
        med_d = median(durs)
        med_r = median(reads)
        rows.append({
            "stage": sid,
            "job": job_of_stage.get(sid),
            "exec_id": eid,
            "name": st["name"],
            "num_tasks": len(tasks),
            "max_task_s": round(max(durs) / 1000, 1),
            "median_task_s": round(med_d / 1000, 1),
            "skew_x": round(max(durs) / med_d, 1) if med_d > 0 and len(durs) >= 4 else None,
            "data_skew_x": round(max(reads) / med_r, 1) if med_r > 0 and len(reads) >= 4 else None,
            "hot_partition_share": round(max(reads) / sum(reads), 3) if sum(reads) else None,
            "read_gb": round(sum(reads) / 1e9, 2),
            "shuffle_read_gb": round(sum(t["shuffle_read"] for t in tasks) / 1e9, 2),
            "shuffle_write_gb": round(sum(t["shuffle_write"] for t in tasks) / 1e9, 2),
            "fetch_wait_pct": round(100 * sum(t["fetch_wait_ms"] for t in tasks) / max(1, sum(t["run_ms"] for t in tasks)), 1),
            "stage_wall_s": round(((st["complete"] or 0) - (st["submit"] or 0)) / 1000, 1),
            "scopes": " | ".join(st["scopes"]),
            "operators": " || ".join(ops),
        })
    return rows


def write_report(run: Run, rows: list, out_path: str) -> None:
    with open(out_path, "w") as f:
        f.write(f"app_id={run.app_id} app_name={run.app_name} wall_s={((run.end_ts or 0) - (run.start_ts or 0)) / 1000:.0f}\n")
        for k in PROP_KEYS:
            f.write(f"  {k}={run.props.get(k, '<unset>')}\n")
        f.write("\n== stages (ranked by max task) ==\n")
        for r in sorted(rows, key=lambda r: -r["max_task_s"]):
            f.write(json.dumps(r) + "\n")
        for eid in sorted(run.exec_plans, key=lambda x: int(x)):
            f.write(f"\n== execution {eid}: {run.exec_desc.get(eid, '')[:200]} ==\n")
            jobs = [f"job {j} stages {job['stage_ids']}" for j, job in run.jobs.items() if str(job["exec_id"]) == str(eid)]
            f.write("  " + "; ".join(jobs) + "\n")
            for depth, name, simple in plan_nodes(run.exec_plans[eid]):
                f.write("  " * depth + (simple or name)[:400] + "\n")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("include_dir")
    ap.add_argument("logs", nargs="+")
    ap.add_argument("--csv", help="append one row per stage to this CSV")
    args = ap.parse_args()
    sys.path.insert(0, args.include_dir)
    from spark_optimizer.eventlog import _read_events

    all_rows = []
    for log in args.logs:
        run = parse(log, _read_events)
        rows = stage_rows(run)
        write_report(run, rows, log.replace(".zstd", "") + ".stagemap.txt")
        dag = DAG_BY_APP.get(run.app_name, run.app_name)
        wall_s = ((run.end_ts or 0) - (run.start_ts or 0)) / 1000
        joins = []
        for eid, plan in run.exec_plans.items():
            for _, name, simple in plan_nodes(plan):
                if JOIN_RE.match(name):
                    joins.append(f"e{eid}:{simple[:120]}")
        for r in rows:
            r.update({
                "dag": dag,
                "app_id": run.app_id,
                "run_wall_min": round(wall_s / 60, 1),
                "skewjoin_setting": run.props.get("spark.sql.adaptive.skewJoin.enabled", "unset (default true)"),
                "autoBroadcastJoinThreshold": run.props.get("spark.sql.autoBroadcastJoinThreshold"),
                "shuffle_partitions": run.props.get("spark.sql.shuffle.partitions"),
                "runtime_joins": " ; ".join(dict.fromkeys(joins)),
            })
        all_rows.extend(rows)
        top = sorted(rows, key=lambda r: -r["max_task_s"])[:3]
        print(f"{dag} {run.app_id} wall={wall_s / 60:.1f}min stages={len(rows)}")
        for r in top:
            print(f"  stage {r['stage']} max={r['max_task_s']}s skew={r['skew_x']}x data={r['data_skew_x']}x share={r['hot_partition_share']} tasks={r['num_tasks']} ops={r['operators'][:160]}")
    if args.csv:
        fields = list(all_rows[0].keys())
        new = not os.path.exists(args.csv)
        with open(args.csv, "a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            if new:
                w.writeheader()
            w.writerows(all_rows)


if __name__ == "__main__":
    main()
