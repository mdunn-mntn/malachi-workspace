"""Parse downloaded Spark event logs and print every spilling or shuffle-heavy stage with the run's effective config.

Usage (from the airflow-ti worktree root):
    PYTHONDONTWRITEBYTECODE=1 python3 <this file> <eventlogs_dir> <out_dir>
Writes <out_dir>/audi_1270_stage_metrics.csv and <out_dir>/audi_1270_spark_props.csv.
"""

import csv
import os
import sys

from include.spark_optimizer.eventlog import parse_eventlog

GIB = 1024**3
PROP_KEYS = [
    "spark.sql.shuffle.partitions",
    "spark.sql.adaptive.enabled",
    "spark.sql.adaptive.coalescePartitions.enabled",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes",
    "spark.sql.adaptive.coalescePartitions.minPartitionSize",
    "spark.sql.adaptive.coalescePartitions.initialPartitionNum",
    "spark.sql.files.maxPartitionBytes",
    "spark.executor.memory",
    "spark.executor.memoryOverhead",
    "spark.executor.cores",
    "spark.driver.memory",
    "spark.driver.memoryOverhead",
    "spark.dynamicAllocation.maxExecutors",
    "spark.memory.fraction",
]
STAGE_FLOOR = GIB


def gib(n):
    return round(n / GIB, 1)


def stage_rows(run):
    for st in sorted(run.stages, key=lambda s: s.stage_id):
        if max(st.disk_spill, st.mem_spill, st.shuffle_read_bytes, st.shuffle_write_bytes) < STAGE_FLOOR:
            continue
        yield {
            "app_id": run.app_id,
            "app_name": run.app_name,
            "stage_id": st.stage_id,
            "stage_name": st.name[:60],
            "num_tasks": st.num_tasks,
            "input_gib": gib(st.input_bytes),
            "shuffle_read_gib": gib(st.shuffle_read_bytes),
            "shuffle_write_gib": gib(st.shuffle_write_bytes),
            "mem_spill_gib": gib(st.mem_spill),
            "disk_spill_gib": gib(st.disk_spill),
            "peak_exec_mem_gib": gib(st.peak_exec_mem),
            "run_time_min": round(st.run_time_ms / 60000, 1),
            "skew_ratio": round(st.skew_ratio, 1),
        }


def main(logs_dir, out_dir):
    metrics, props = [], []
    for name in sorted(os.listdir(logs_dir)):
        if not name.endswith(".zstd"):
            continue
        run = parse_eventlog(os.path.join(logs_dir, name))
        props.append({"app_id": run.app_id, "app_name": run.app_name, "file": name,
                      "duration_min": round((run.duration_ms or 0) / 60000, 1),
                      "executors": len(run.executors),
                      **{k: run.spark_props.get(k, "") for k in PROP_KEYS}})
        metrics.extend(stage_rows(run))
        print(f"{name} {run.app_name} stages={len(run.stages)} flagged={sum(1 for m in metrics if m['app_id'] == run.app_id)}")
    for fname, rows in (("audi_1270_stage_metrics.csv", metrics), ("audi_1270_spark_props.csv", props)):
        with open(os.path.join(out_dir, fname), "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
