#!/usr/bin/env python3
"""Estimate the time each flagged stage would save once its partitions are balanced, from the prod event logs in this folder.

Usage: python3 audi_1276_savings_estimate.py <airflow-ti include dir> <eventlog.zstd> [...]
Writes audi_1276_savings_estimate.csv next to this file and prints one row per flagged stage.
"""
import csv
import os
import sys
from statistics import median

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from audi_1276_stage_map import DAG_BY_APP, parse  # noqa: E402

FLAGGED = {
    "conv_log_ip_advertiser_id": [13],
    "guid_log_ip_advertiser_id": [9],
    "guid_log_ip_guid_advertiser_id": [9],
    "ipdsc_42_monitor": [18, 22, 26],
}


def stage_row(run, dag: str, sid: int) -> dict:
    st = run.stages[sid]
    tasks = [t for t in run.tasks.get(sid, []) if not t["failed"]]
    durs = [t["dur_ms"] / 1000 for t in tasks]
    executors = len({t["exec"] for t in tasks})
    cores = int(run.props.get("spark.executor.cores", 4))
    slots = executors * cores
    stage_wall = ((st["complete"] or 0) - (st["submit"] or 0)) / 1000
    balanced_wall = sum(durs) / slots if slots else None
    return {
        "dag": dag,
        "app_id": run.app_id,
        "run_wall_min": round(((run.end_ts or 0) - (run.start_ts or 0)) / 60000, 1),
        "stage": sid,
        "num_tasks": len(tasks),
        "executors_in_stage": executors,
        "executor_cores": cores,
        "slots": slots,
        "stage_wall_s": round(stage_wall, 1),
        "sum_task_s": round(sum(durs), 1),
        "max_task_s": round(max(durs), 1),
        "median_task_s": round(median(durs), 1),
        "balanced_wall_s": round(balanced_wall, 1),
        "saving_wall_s": round(max(0.0, stage_wall - balanced_wall), 1),
        "fetch_wait_s": round(sum(t["fetch_wait_ms"] for t in tasks) / 1000, 1),
        "submit_offset_s": round(((st["submit"] or 0) - (run.start_ts or 0)) / 1000, 1),
        "complete_offset_s": round(((st["complete"] or 0) - (run.start_ts or 0)) / 1000, 1),
    }


def main() -> None:
    sys.path.insert(0, sys.argv[1])
    from spark_optimizer.eventlog import _read_events

    rows = []
    for log in sys.argv[2:]:
        run = parse(log, _read_events)
        dag = DAG_BY_APP.get(run.app_name, run.app_name)
        for sid in FLAGGED.get(dag, []):
            if sid in run.stages and run.tasks.get(sid):
                rows.append(stage_row(run, dag, sid))
    rows.sort(key=lambda r: (r["dag"], r["app_id"], r["stage"]))
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "audi_1276_savings_estimate.csv")
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    for r in rows:
        print(
            f"{r['dag']:32} {r['app_id']} stage {r['stage']:>2} tasks={r['num_tasks']:>3} slots={r['slots']:>3} "
            f"wall={r['stage_wall_s']:>6} sum={r['sum_task_s']:>7} max={r['max_task_s']:>6} med={r['median_task_s']:>5} "
            f"balanced={r['balanced_wall_s']:>6} saving={r['saving_wall_s']:>6} fetch_wait={r['fetch_wait_s']:>7} "
            f"window=[{r['submit_offset_s']}, {r['complete_offset_s']}]"
        )


if __name__ == "__main__":
    main()
