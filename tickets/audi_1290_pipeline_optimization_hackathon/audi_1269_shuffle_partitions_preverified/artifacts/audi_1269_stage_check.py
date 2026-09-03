"""Print what AUDI-1269 needs from one Spark event log before shuffle.partitions is raised.

    PYTHONPATH=/Users/malachi/Developer/work/mntn/workspace \
    python3 artifacts/audi_1269_stage_check.py <eventlog> <target_partitions>

Per log: the configured partition count and AQE knobs, every stage that shuffles more
than 10 GiB or spills more than 1 GiB (summed over all attempts of the stage), whether
each reducer's shuffle read still has the configured partition count (the knob is live,
read from the stage's ShuffledRowRDD rather than its task count, which a fetch-failure
retry shrinks and a union with a scan inflates), the projected per-task size at the
target, and the projected shuffle block size per map stage that feeds it.
"""

from __future__ import annotations

import sys
from collections import defaultdict

from airflow_optimizer.eventlog import _read_events

GIB = 1024**3
MIB = 1024**2
PROPS = (
    "spark.sql.shuffle.partitions",
    "spark.sql.adaptive.enabled",
    "spark.sql.adaptive.coalescePartitions.enabled",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes",
    "spark.sql.adaptive.coalescePartitions.initialPartitionNum",
    "spark.driver.memory",
    "spark.executor.memory",
    "spark.executor.cores",
)
METRICS = ("input", "write", "read", "disk", "mem", "run_ms", "wait_ms")


def scan(path: str) -> tuple[str, dict, dict, dict]:
    app_name, props = "", {}
    stages: dict[int, dict] = defaultdict(lambda: {"attempts": {}, "reducer_parts": None, "fetch_failed": 0})
    totals: dict[int, dict] = defaultdict(lambda: dict.fromkeys(METRICS, 0))
    for e in _read_events(path):
        kind = e.get("Event")
        if kind == "SparkListenerApplicationStart":
            app_name = e.get("App Name", "")
        elif kind == "SparkListenerEnvironmentUpdate":
            props = e.get("Spark Properties", {})
        elif kind == "SparkListenerStageSubmitted":
            info = e["Stage Info"]
            st = stages[info["Stage ID"]]
            st["attempts"][info["Stage Attempt ID"]] = info["Number of Tasks"]
            shuffled = [r["Number of Partitions"] for r in info["RDD Info"] if r["Name"] == "ShuffledRowRDD"]
            if shuffled:
                st["reducer_parts"] = max(shuffled)
        elif kind == "SparkListenerTaskEnd":
            sid = e.get("Stage ID")
            if (e.get("Task End Reason") or {}).get("Reason") == "FetchFailed":
                stages[sid]["fetch_failed"] += 1
            tm = e.get("Task Metrics")
            if not tm:
                continue
            srm = tm.get("Shuffle Read Metrics") or {}
            t = totals[sid]
            t["input"] += (tm.get("Input Metrics") or {}).get("Bytes Read", 0)
            t["write"] += (tm.get("Shuffle Write Metrics") or {}).get("Shuffle Bytes Written", 0)
            t["read"] += srm.get("Remote Bytes Read", 0) + srm.get("Local Bytes Read", 0)
            t["disk"] += tm.get("Disk Bytes Spilled", 0)
            t["mem"] += tm.get("Memory Bytes Spilled", 0)
            t["run_ms"] += tm.get("Executor Run Time", 0)
            t["wait_ms"] += srm.get("Fetch Wait Time", 0)
    return app_name, props, stages, totals


def main(path: str, target: int) -> None:
    app_name, props, stages, totals = scan(path)
    configured = int(props.get("spark.sql.shuffle.partitions") or 200)
    print(f"{app_name}  ({path.rstrip('/').rsplit('/', 1)[-1]})")
    for key in PROPS:
        print(f"  {key} = {props.get(key)}")
    print(f"  target = {target} ({target / configured:.1f}x)")
    feeders: dict[int, int] = {}
    for sid in sorted(stages):
        st, t = stages[sid], totals[sid]
        wide = t["write"] > 10 * GIB or t["read"] > 10 * GIB
        if not wide and t["disk"] < GIB:
            continue
        attempts = st["attempts"]
        planned = attempts.get(0, next(iter(attempts.values()), 0))
        tasks = f"{planned:>6}" + (f" (+{len(attempts) - 1} retry, {st['fetch_failed']} fetch failures)" if len(attempts) > 1 else "")
        print(
            f"  stage {sid:>3} tasks {tasks} in {t['input'] / GIB:8.1f} GiB  shuffle write {t['write'] / GIB:8.1f} "
            f"read {t['read'] / GIB:8.1f}  spill disk {t['disk'] / GIB:7.1f} mem {t['mem'] / GIB:8.0f}  "
            f"fetch wait {t['wait_ms'] / max(1, t['run_ms']):.0%}"
        )
        if t["write"] > 10 * GIB:
            feeders[sid] = planned
            print(f"        map stage: blocks ~{t['write'] / (planned * target) / 1024:.1f} KiB at target ({planned} map tasks x {target})")
        if t["read"] > 10 * GIB:
            parts = st["reducer_parts"]
            if parts == configured:
                live = "LIVE"
            elif parts is None:
                live = "no ShuffledRowRDD in stage (not a shuffle reducer)"
            elif parts < configured:
                live = f"COALESCED to {parts} (config {configured})"
            else:
                live = f"SPLIT to {parts} (config {configured})"
            if parts and planned != parts:
                live += f", {planned} tasks ({'union with a scan' if planned > parts else 'partial retry'})"
            per_task = t["read"] / target / MIB
            expansion = (t["mem"] + t["read"]) / t["read"]
            print(f"        reducer {live}; at target: {per_task:.0f} MiB compressed/task, ~{per_task * expansion:.0f} MiB in memory")


if __name__ == "__main__":
    main(sys.argv[1], int(sys.argv[2]))
