"""Print AQE coalesce evidence from one Spark event log: env knobs, AQEShuffleRead nodes, spilling stages.

Usage: zstd -dc app-<id>.zstd | python3 audi_1274_aqe_probe.py [--stage 33 --stage 34]
"""

import argparse
import json
import sys
from collections import defaultdict

ENV_KEYS = (
    "spark.sql.adaptive.enabled",
    "spark.sql.adaptive.coalescePartitions.enabled",
    "spark.sql.adaptive.coalescePartitions.parallelismFirst",
    "spark.sql.adaptive.coalescePartitions.minPartitionSize",
    "spark.sql.adaptive.coalescePartitions.minPartitionNum",
    "spark.sql.adaptive.coalescePartitions.initialPartitionNum",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes",
    "spark.sql.shuffle.partitions",
    "spark.default.parallelism",
    "spark.executor.cores",
    "spark.executor.memory",
    "spark.dynamicAllocation.initialExecutors",
    "spark.app.name",
)


def num(v):
    try:
        return int(str(v).replace(",", ""))
    except ValueError:
        return None


def walk(node, out):
    out.append(node)
    for c in node.get("children", []):
        walk(c, out)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--stage", type=int, action="append", default=[])
    args = ap.parse_args()

    props, spark_version = {}, None
    acc = {}
    acc_series = defaultdict(list)
    stages = {}
    stage_metrics = defaultdict(lambda: defaultdict(int))
    final_plans = {}
    sql_desc = {}
    for line in sys.stdin:
        try:
            e = json.loads(line)
        except json.JSONDecodeError:
            continue
        ev = e.get("Event", "")
        if ev == "SparkListenerLogStart":
            spark_version = e.get("Spark Version")
        elif ev == "SparkListenerEnvironmentUpdate":
            props = e.get("Spark Properties", {}) or {}
        elif ev == "SparkListenerStageCompleted":
            si = e.get("Stage Info", {}) or {}
            sid = si.get("Stage ID")
            stages[sid] = (si.get("Number of Tasks"), (si.get("Stage Name") or "")[:60])
            for a in si.get("Accumulables", []):
                if a.get("ID") is not None and a.get("Value") is not None:
                    acc[a["ID"]] = num(a["Value"])
        elif ev == "SparkListenerTaskEnd":
            sid = e.get("Stage ID")
            tm = e.get("Task Metrics") or {}
            sm = stage_metrics[sid]
            sm["tasks"] += 1
            sm["shuffle_read"] += (tm.get("Shuffle Read Metrics") or {}).get("Remote Bytes Read", 0) + (
                tm.get("Shuffle Read Metrics") or {}
            ).get("Local Bytes Read", 0)
            sm["shuffle_write"] += (tm.get("Shuffle Write Metrics") or {}).get("Shuffle Bytes Written", 0)
            sm["mem_spill"] += tm.get("Memory Bytes Spilled", 0)
            sm["disk_spill"] += tm.get("Disk Bytes Spilled", 0)
            sm["input"] += (tm.get("Input Metrics") or {}).get("Bytes Read", 0)
        elif ev.endswith("SQLExecutionStart"):
            sql_desc[e.get("executionId")] = (e.get("description") or "")[:80]
        elif ev.endswith("SQLAdaptiveExecutionUpdate"):
            final_plans[e.get("executionId")] = e.get("sparkPlanInfo")
        elif ev.endswith("DriverAccumUpdates"):
            for pair in e.get("accumUpdates", []):
                if isinstance(pair, list) and len(pair) == 2:
                    acc[pair[0]] = num(pair[1])
                    acc_series[pair[0]].append(num(pair[1]) or 0)

    gib = 1024**3
    print(f"spark_version={spark_version} app={props.get('spark.app.name')}")
    for k in ENV_KEYS:
        print(f"  {k} = {props.get(k, '<unset>')}")

    print("\nAQEShuffleRead nodes in final adaptive plans (per SQL execution):")
    for eid, plan in sorted(final_plans.items(), key=lambda kv: kv[0]):
        nodes = []
        walk(plan, nodes)
        reads = [n for n in nodes if "AQEShuffleRead" in n.get("nodeName", "") or "CustomShuffleReader" in n.get("nodeName", "")]
        if not reads:
            continue
        print(f"  exec {eid} {sql_desc.get(eid, '')!r}")
        for n in reads:
            m = {}
            for x in n.get("metrics", []):
                name, aid = x.get("name"), x.get("accumulatorId")
                series = acc_series.get(aid, [])
                if name == "partition data size" and series:
                    m["partitions_seen"] = len(series)
                    m["avg_MiB"] = round(sum(series) / len(series) / 2**20, 1)
                    m["max_MiB"] = round(max(series) / 2**20, 1)
                    m["total_GiB"] = round(sum(series) / 2**30, 2)
                elif acc.get(aid) is not None:
                    m[name] = acc[aid]
            print(f"    {n.get('nodeName')} {n.get('simpleString', '')[:40]!r} {m}")

    wanted = set(args.stage)
    print("\nStages (task count from StageCompleted; bytes summed over TaskEnd):")
    rows = []
    for sid, (ntasks, name) in stages.items():
        sm = stage_metrics.get(sid, {})
        rows.append((sm.get("disk_spill", 0), sid, ntasks, name, sm))
    rows.sort(reverse=True)
    for disk, sid, ntasks, name, sm in rows[:8] + [r for r in rows if r[1] in wanted and r not in rows[:8]]:
        print(
            f"  stage {sid:>3} tasks={ntasks:>5} shuffle_read={sm.get('shuffle_read', 0)/gib:8.1f}GiB "
            f"shuffle_write={sm.get('shuffle_write', 0)/gib:8.1f}GiB mem_spill={sm.get('mem_spill', 0)/gib:8.1f}GiB "
            f"disk_spill={disk/gib:6.1f}GiB  {name}"
        )


if __name__ == "__main__":
    main()
