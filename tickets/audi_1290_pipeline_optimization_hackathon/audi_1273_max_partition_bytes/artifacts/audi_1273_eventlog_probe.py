"""Print per-stage input/spill metrics, the input-split confs, and the scan/join plan nodes of Spark event logs.

Usage (from the workspace root, airflow-ti checkout on sys.path):
  python3 artifacts/audi_1273_eventlog_probe.py --repo /path/to/airflow-ti outputs/eventlogs/app-*.zstd
"""
import argparse
import json
import re
import subprocess
import sys

CONF_KEYS = (
    "spark.sql.files.maxPartitionBytes",
    "spark.sql.files.openCostInBytes",
    "spark.sql.shuffle.partitions",
    "spark.executor.memory",
    "spark.executor.cores",
    "spark.dynamicAllocation.initialExecutors",
    "spark.dynamicAllocation.maxExecutors",
)
PLAN_PATTERNS = ("Location:", "JDBCRelation", "SortMergeJoin", "BroadcastHashJoin", "BroadcastExchange", "Generate explode")


def gib(n):
    return f"{n / 2**30:.1f}"


def plan_lines(path):
    seen = set()
    proc = subprocess.Popen(["zstd", "-dc", path], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    for raw in proc.stdout:
        if b"SQLExecutionStart" not in raw and b"AdaptiveSparkPlan" not in raw:
            continue
        try:
            event = json.loads(raw)
        except ValueError:
            continue
        text = event.get("physicalPlanDescription", "")
        for line in text.splitlines():
            if any(p in line for p in PLAN_PATTERNS):
                key = re.sub(r"#\d+L?", "", line.strip())[:220]
                if key not in seen:
                    seen.add(key)
                    yield key
    proc.stdout.close()
    proc.wait()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", required=True, help="airflow-ti checkout holding include/spark_optimizer")
    ap.add_argument("logs", nargs="+")
    args = ap.parse_args()
    sys.path.insert(0, args.repo)
    from include.spark_optimizer.eventlog import parse_eventlog

    for path in args.logs:
        run = parse_eventlog(path)
        print(f"\n=== {run.app_name} {run.app_id} dur={run.duration_ms / 60000:.1f}min stages={len(run.stages)}")
        for k in CONF_KEYS:
            print(f"  conf {k} = {run.spark_props.get(k)}")
        print("  stage  tasks  input_GiB  shuf_read_GiB  shuf_write_GiB  mem_spill_GiB  disk_spill_GiB  name")
        for s in run.stages:
            if s.input_bytes or s.mem_spill or s.shuffle_write_bytes:
                print(f"  {s.stage_id:>5}  {s.num_tasks:>5}  {gib(s.input_bytes):>9}  {gib(s.shuffle_read_bytes):>13}"
                      f"  {gib(s.shuffle_write_bytes):>14}  {gib(s.mem_spill):>13}  {gib(s.disk_spill):>14}  {s.name[:50]}")
        print("  plan:")
        for line in plan_lines(path):
            print("   ", line)


if __name__ == "__main__":
    main()
