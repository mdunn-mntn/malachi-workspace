"""Per-run idle-trim and fetch-wait profile from Spark event logs, one CSV row per log.

Usage: python3 audi_1271_run_profile.py <out_csv> <eventlog> [<eventlog> ...]
"""

from __future__ import annotations

import collections
import csv
import sys
from datetime import datetime, timezone

sys.path.insert(0, "/Users/malachi/Developer/work/mntn/workspace")
from airflow_optimizer.eventlog import _read_events  # noqa: E402

MAP_STAGE_MIN_TASKS = 1000
REDUCE_STAGE_MIN_TASKS = 100
TARGET_EXECUTORS = 200
DYN_KEYS = (
    "spark.dynamicAllocation.initialExecutors",
    "spark.dynamicAllocation.minExecutors",
    "spark.dynamicAllocation.maxExecutors",
    "spark.dynamicAllocation.executorIdleTimeout",
    "spark.executor.cores",
)

FIELDS = [
    "log", "app_name", "app_start_utc", "duration_min", "executor_cores",
    "dyn_initial", "dyn_min", "dyn_max", "dyn_idle_timeout",
    "initial_fleet_reached", "initial_fleet_reached_at_s", "first_removal_at_s", "trim_gap_s",
    "registered_after_trim", "registered_at_60s", "registered_at_90s",
    "first_task_launch_s", "prologue_s", "map_stage", "map_stage_submit_s", "registered_at_map_submit",
    "peak_registered", "map_write_gib", "map_executors_with_output", "map_executors_holding_90pct",
    "map_hottest_share_pct", "reduce_stage", "reduce_fetch_wait_share_pct", "reduce_fetch_wait_core_h",
    "reduce_fetch_wait_exec_h", "stage11_fetch_wait_share_pct", "stage11_fetch_wait_exec_h",
    "prologue_registered_exec_h", "prologue_gross_200_exec_h", "prologue_incremental_200_exec_h",
    "total_registered_exec_h", "busy_exec_h",
]


def ts_fmt(ms: float) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def registered_at(t: float, adds: dict[str, float], rems: dict[str, float]) -> int:
    return sum(1 for eid, a in adds.items() if a <= t and rems.get(eid, float("inf")) > t)


def executor_seconds_between(t_a: float, t_b: float, adds: dict, rems: dict) -> float:
    total = 0.0
    for eid, a in adds.items():
        r = rems.get(eid, float("inf"))
        lo, hi = max(a, t_a), min(r, t_b)
        if hi > lo:
            total += (hi - lo) / 1000
    return total


def busy_seconds(tasks: list[tuple[float, float, str]]) -> float:
    by_exec: dict[str, list[tuple[float, float]]] = collections.defaultdict(list)
    for launch, finish, eid in tasks:
        by_exec[eid].append((launch, finish))
    total = 0.0
    for iv in by_exec.values():
        iv.sort()
        cur_s, cur_e = iv[0]
        for s, e in iv[1:]:
            if s > cur_e:
                total += (cur_e - cur_s) / 1000
                cur_s, cur_e = s, e
            else:
                cur_e = max(cur_e, e)
        total += (cur_e - cur_s) / 1000
    return total


def profile(path: str) -> dict:
    adds: dict[str, float] = {}
    rems: dict[str, float] = {}
    tasks: list[tuple[float, float, str]] = []
    stage_submit: dict[int, float] = {}
    stage_tasks: dict[int, int] = {}
    stage_end: dict[int, float] = {}
    fetch_wait: collections.Counter = collections.Counter()
    run_time: collections.Counter = collections.Counter()
    read_bytes: collections.Counter = collections.Counter()
    write_by_exec: dict[int, collections.Counter] = collections.defaultdict(collections.Counter)
    props: dict[str, str] = {}
    app_name = ""
    app_start = app_end = None

    for e in _read_events(path):
        ev = e.get("Event", "")
        if ev == "SparkListenerApplicationStart":
            app_start = e["Timestamp"]
            app_name = e.get("App Name", "")
        elif ev == "SparkListenerApplicationEnd":
            app_end = e["Timestamp"]
        elif ev == "SparkListenerEnvironmentUpdate":
            sp = e.get("Spark Properties") or {}
            props = {k: sp.get(k, "") for k in DYN_KEYS}
        elif ev == "SparkListenerExecutorAdded":
            adds[str(e["Executor ID"])] = e["Timestamp"]
        elif ev == "SparkListenerExecutorRemoved":
            rems[str(e["Executor ID"])] = e["Timestamp"]
        elif ev == "SparkListenerStageSubmitted":
            si = e["Stage Info"]
            stage_submit[si["Stage ID"]] = si.get("Submission Time")
            stage_tasks[si["Stage ID"]] = si.get("Number of Tasks", 0)
        elif ev == "SparkListenerStageCompleted":
            si = e["Stage Info"]
            stage_end[si["Stage ID"]] = si.get("Completion Time")
            stage_tasks.setdefault(si["Stage ID"], si.get("Number of Tasks", 0))
        elif ev == "SparkListenerTaskEnd":
            sid, info = e["Stage ID"], e["Task Info"]
            metrics = e.get("Task Metrics") or {}
            srm = metrics.get("Shuffle Read Metrics") or {}
            swm = metrics.get("Shuffle Write Metrics") or {}
            tasks.append((info["Launch Time"], info["Finish Time"], str(info["Executor ID"])))
            fetch_wait[sid] += srm.get("Fetch Wait Time", 0)
            run_time[sid] += metrics.get("Executor Run Time", 0)
            read_bytes[sid] += srm.get("Remote Bytes Read", 0) + srm.get("Local Bytes Read", 0)
            write_by_exec[sid][str(info["Executor ID"])] += swm.get("Shuffle Bytes Written", 0)

    t0 = app_start or min(adds.values())
    t1 = app_end or max([f for _, f, _ in tasks] + list(rems.values()))
    cores = int(props.get("spark.executor.cores") or 1)
    initial = int(props.get("spark.dynamicAllocation.initialExecutors") or 0)

    add_times = sorted(adds.values())
    reached_at = add_times[initial - 1] if initial and len(add_times) >= initial else None
    first_rem = min(rems.values()) if rems else None
    trim_gap = (first_rem - reached_at) / 1000 if reached_at and first_rem else None
    registered_after_trim = registered_at(first_rem + 1000, adds, rems) if first_rem else None

    map_stages = sorted(s for s, n in stage_tasks.items() if n >= MAP_STAGE_MIN_TASKS and s in stage_submit)
    map_stage = map_stages[0] if map_stages else None
    map_submit = stage_submit.get(map_stage) if map_stage is not None else None
    first_task = min(l for l, _, _ in tasks) if tasks else None
    prologue_end = map_submit if map_submit else first_task
    prologue_s = (prologue_end - t0) / 1000 if prologue_end else None

    by = write_by_exec.get(map_stage, collections.Counter()) if map_stage is not None else collections.Counter()
    total_w = sum(by.values())
    top = by.most_common()
    cum = n90 = 0
    for i, (_, v) in enumerate(top, 1):
        cum += v
        if cum >= 0.9 * total_w:
            n90 = i
            break

    reduce_stage = None
    if map_stage is not None and map_stage in stage_end:
        candidates = [s for s in sorted(stage_submit) if s > map_stage and read_bytes[s] > 0
                      and stage_tasks.get(s, 0) >= REDUCE_STAGE_MIN_TASKS]
        reduce_stage = candidates[0] if candidates else None

    def wait_share(sid: int | None) -> float | None:
        return 100 * fetch_wait[sid] / run_time[sid] if sid is not None and run_time[sid] else None

    def wait_exec_h(sid: int | None) -> float | None:
        return fetch_wait[sid] / 1000 / 3600 / cores if sid is not None else None

    prologue_exec_s = executor_seconds_between(t0, prologue_end, adds, rems) if prologue_end else 0.0
    gross_200 = TARGET_EXECUTORS * prologue_s / 3600 if prologue_s else None
    total_exec_s = executor_seconds_between(t0, t1, adds, rems)

    return {
        "log": path.rsplit("/", 1)[-1],
        "app_name": app_name,
        "app_start_utc": ts_fmt(t0),
        "duration_min": round((t1 - t0) / 60000, 1),
        "executor_cores": cores,
        "dyn_initial": props.get("spark.dynamicAllocation.initialExecutors", ""),
        "dyn_min": props.get("spark.dynamicAllocation.minExecutors", ""),
        "dyn_max": props.get("spark.dynamicAllocation.maxExecutors", ""),
        "dyn_idle_timeout": props.get("spark.dynamicAllocation.executorIdleTimeout", "") or "(default)",
        "initial_fleet_reached": int(reached_at is not None),
        "initial_fleet_reached_at_s": round((reached_at - t0) / 1000, 1) if reached_at else None,
        "first_removal_at_s": round((first_rem - t0) / 1000, 1) if first_rem else None,
        "trim_gap_s": round(trim_gap, 1) if trim_gap is not None else None,
        "registered_after_trim": registered_after_trim,
        "registered_at_60s": registered_at(t0 + 60_000, adds, rems),
        "registered_at_90s": registered_at(t0 + 90_000, adds, rems),
        "first_task_launch_s": round((first_task - t0) / 1000, 1) if first_task else None,
        "prologue_s": round(prologue_s, 1) if prologue_s else None,
        "map_stage": map_stage,
        "map_stage_submit_s": round((map_submit - t0) / 1000, 1) if map_submit else None,
        "registered_at_map_submit": registered_at(map_submit, adds, rems) if map_submit else None,
        "peak_registered": max(registered_at(t, adds, rems) for t in add_times) if add_times else 0,
        "map_write_gib": round(total_w / 1024**3, 2),
        "map_executors_with_output": len(by),
        "map_executors_holding_90pct": n90,
        "map_hottest_share_pct": round(100 * top[0][1] / total_w, 1) if total_w else None,
        "reduce_stage": reduce_stage,
        "reduce_fetch_wait_share_pct": round(wait_share(reduce_stage), 1) if wait_share(reduce_stage) is not None else None,
        "reduce_fetch_wait_core_h": round(fetch_wait[reduce_stage] / 3.6e6, 3) if reduce_stage is not None else None,
        "reduce_fetch_wait_exec_h": round(wait_exec_h(reduce_stage), 3) if reduce_stage is not None else None,
        "stage11_fetch_wait_share_pct": round(wait_share(11), 1) if wait_share(11) is not None else None,
        "stage11_fetch_wait_exec_h": round(wait_exec_h(11), 3) if run_time[11] else None,
        "prologue_registered_exec_h": round(prologue_exec_s / 3600, 3),
        "prologue_gross_200_exec_h": round(gross_200, 3) if gross_200 is not None else None,
        "prologue_incremental_200_exec_h": round(gross_200 - prologue_exec_s / 3600, 3) if gross_200 is not None else None,
        "total_registered_exec_h": round(total_exec_s / 3600, 2),
        "busy_exec_h": round(busy_seconds(tasks) / 3600, 2) if tasks else 0.0,
    }


def main(out_csv: str, paths: list[str]) -> None:
    with open(out_csv, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for p in paths:
            row = profile(p)
            w.writerow(row)
            print(f"{row['log']}: prologue {row['prologue_s']}s, at +60s {row['registered_at_60s']}, "
                  f"at map submit {row['registered_at_map_submit']}, trim gap {row['trim_gap_s']}s, "
                  f"reduce s{row['reduce_stage']} wait {row['reduce_fetch_wait_share_pct']}%")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2:])
