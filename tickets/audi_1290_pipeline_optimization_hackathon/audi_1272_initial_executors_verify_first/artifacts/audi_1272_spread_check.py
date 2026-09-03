"""Map-output spread, fleet timeline and idle-cost numbers per fetch-wait stage, one CSV row per log x stage.

Usage: PYTHONPATH=<workspace> python3 audi_1272_spread_check.py --out-csv <csv> [--min-wait-pct 20] [--min-run-s 300] <eventlog> [<eventlog> ...]
"""

from __future__ import annotations

import argparse
import collections
import csv
import sys
from datetime import datetime, timezone

from airflow_optimizer.eventlog import _read_events

PROP_KEYS = {
    "spark.app.name": "prop_app_name",
    "spark.dynamicAllocation.initialExecutors": "dyn_initial",
    "spark.dynamicAllocation.minExecutors": "dyn_min",
    "spark.dynamicAllocation.maxExecutors": "dyn_max",
    "spark.dynamicAllocation.executorIdleTimeout": "dyn_idle_timeout",
    "spark.dynamicAllocation.cachedExecutorIdleTimeout": "dyn_cached_idle_timeout",
    "spark.dynamicAllocation.executorAllocationRatio": "dyn_alloc_ratio",
    "spark.executor.instances": "executor_instances",
    "spark.executor.cores": "executor_cores",
}
FIELDS = [
    "log", "app_name", "app_start_utc", "duration_min", *PROP_KEYS.values(), "start_count",
    "first_task_s", "registered_at_60s", "registered_at_90s", "first_removal_s", "first_removal_reason",
    "registered_after_first_removal", "peak_registered", "total_registered_exec_h",
    "stage", "stage_tasks", "stage_submit_s", "stage_live_at_submit", "fetch_wait_pct", "fetch_wait_exec_h",
    "blocks", "block_bytes", "read_gib", "feeding_stage", "feeding_source", "feeding_parents_with_output",
    "map_tasks", "map_submit_s", "map_live_at_submit", "map_live_at_first_task", "map_peak_live", "map_output_gib",
    "executors_with_output", "executors_holding_90pct", "hottest_share_pct", "removal_before_map",
    "target_initial", "extra_executors", "extra_idle_window_s", "extra_idle_exec_h", "cost_over_wait_ratio",
    "spread_class",
]
SERVERLESS_DEFAULT_CORES = 4


def ts_fmt(ms: float) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def registered_at(t: float, adds: dict[str, float], rems: dict[str, float]) -> int:
    return sum(1 for eid, a in adds.items() if a <= t and rems.get(eid, float("inf")) > t)


def peak_between(t_a: float, t_b: float, adds: dict, rems: dict) -> int:
    points = [t_a, t_b] + [t for t in list(adds.values()) + list(rems.values()) if t_a <= t <= t_b]
    return max(registered_at(t, adds, rems) for t in points)


def executor_seconds(t_a: float, t_b: float, adds: dict, rems: dict) -> float:
    total = 0.0
    for eid, a in adds.items():
        lo, hi = max(a, t_a), min(rems.get(eid, float("inf")), t_b)
        if hi > lo:
            total += (hi - lo) / 1000
    return total


def concentration(by: collections.Counter) -> tuple[float, int, int, float | None]:
    total = sum(by.values())
    top = by.most_common()
    cum = n90 = 0
    for i, (_, v) in enumerate(top, 1):
        cum += v
        if cum >= 0.9 * total:
            n90 = i
            break
    hottest = round(100 * top[0][1] / total, 2) if total else None
    return total, len(by), n90, hottest


class Log:
    """Everything the check needs from one event log."""

    def __init__(self, path: str) -> None:
        self.path = path
        self.props: dict[str, str] = {}
        self.app_name = ""
        self.t0 = self.t1 = None
        self.adds: dict[str, float] = {}
        self.rems: dict[str, float] = {}
        self.rem_reason: dict[str, str] = {}
        self.submit: dict[int, float] = {}
        self.complete: dict[int, float] = {}
        self.parents: dict[int, list[int]] = {}
        self.top_rdd: dict[int, int] = {}
        self.n_tasks: dict[int, int] = {}
        self.first_launch: dict[int, float] = {}
        self.last_finish: dict[int, float] = {}
        self.done_tasks: collections.Counter = collections.Counter()
        self.fetch_wait: collections.Counter = collections.Counter()
        self.run_time: collections.Counter = collections.Counter()
        self.blocks: collections.Counter = collections.Counter()
        self.read_bytes: collections.Counter = collections.Counter()
        self.write_by_exec: dict[int, collections.Counter] = collections.defaultdict(collections.Counter)
        for e in _read_events(path):
            self._take(e)
        if self.t0 is None:
            self.t0 = min(self.adds.values())
        if self.t1 is None:
            self.t1 = max(list(self.last_finish.values()) + list(self.rems.values()) + [self.t0])

    def _take(self, e: dict) -> None:
        ev = e.get("Event", "")
        if ev == "SparkListenerApplicationStart":
            self.t0, self.app_name = e["Timestamp"], e.get("App Name", "")
        elif ev == "SparkListenerApplicationEnd":
            self.t1 = e["Timestamp"]
        elif ev == "SparkListenerEnvironmentUpdate":
            sp = e.get("Spark Properties") or {}
            self.props = {k: sp.get(k, "") for k in PROP_KEYS}
        elif ev == "SparkListenerExecutorAdded":
            self.adds[str(e["Executor ID"])] = e["Timestamp"]
        elif ev == "SparkListenerExecutorRemoved":
            self.rems[str(e["Executor ID"])] = e["Timestamp"]
            self.rem_reason[str(e["Executor ID"])] = e.get("Removed Reason", "")
        elif ev == "SparkListenerJobStart":
            for si in e.get("Stage Infos") or []:
                rdds = [r.get("RDD ID", -1) for r in si.get("RDD Info") or []]
                self.top_rdd[si["Stage ID"]] = max(rdds) if rdds else -1
                self.parents.setdefault(si["Stage ID"], list(si.get("Parent IDs") or []))
        elif ev == "SparkListenerStageSubmitted":
            si = e["Stage Info"]
            sid = si["Stage ID"]
            self.submit[sid] = si.get("Submission Time")
            self.n_tasks[sid] = si.get("Number of Tasks", 0)
            self.parents[sid] = list(si.get("Parent IDs") or [])
        elif ev == "SparkListenerStageCompleted":
            si = e["Stage Info"]
            self.complete[si["Stage ID"]] = si.get("Completion Time")
            self.n_tasks.setdefault(si["Stage ID"], si.get("Number of Tasks", 0))
        elif ev == "SparkListenerTaskEnd":
            sid, info = e["Stage ID"], e["Task Info"]
            m = e.get("Task Metrics") or {}
            srm = m.get("Shuffle Read Metrics") or {}
            swm = m.get("Shuffle Write Metrics") or {}
            self.done_tasks[sid] += 1
            self.first_launch[sid] = min(self.first_launch.get(sid, float("inf")), info["Launch Time"])
            self.last_finish[sid] = max(self.last_finish.get(sid, 0), info["Finish Time"])
            self.fetch_wait[sid] += srm.get("Fetch Wait Time", 0)
            self.run_time[sid] += m.get("Executor Run Time", 0)
            self.blocks[sid] += srm.get("Remote Blocks Fetched", 0) + srm.get("Local Blocks Fetched", 0)
            self.read_bytes[sid] += srm.get("Remote Bytes Read", 0) + srm.get("Local Bytes Read", 0)
            self.write_by_exec[sid][str(info["Executor ID"])] += swm.get("Shuffle Bytes Written", 0)

    def rel(self, t: float | None) -> float | None:
        return round((t - self.t0) / 1000, 1) if t is not None else None

    def cores(self) -> int:
        return int(self.props.get("spark.executor.cores") or SERVERLESS_DEFAULT_CORES)

    def start_count(self) -> int:
        p = self.props
        for k in ("spark.dynamicAllocation.initialExecutors", "spark.executor.instances", "spark.dynamicAllocation.minExecutors"):
            if p.get(k):
                return int(p[k])
        return 2

    def cap(self) -> int:
        return int(self.props.get("spark.dynamicAllocation.maxExecutors") or 1000)

    def wait_pct(self, sid: int) -> float:
        return 100 * self.fetch_wait[sid] / self.run_time[sid] if self.run_time[sid] else 0.0

    def output_of(self, sid: int) -> int:
        return sum(self.write_by_exec[sid].values())

    def executed_twin(self, skipped: int) -> int | None:
        top_rdd = self.top_rdd.get(skipped)
        twins = [s for s, r in self.top_rdd.items() if r == top_rdd and s != skipped and self.output_of(s) > 0]
        return max(twins, key=self.output_of) if twins else None

    def feeding_stage(self, sid: int) -> tuple[int | None, str, int]:
        resolved = []
        for p in self.parents.get(sid, []):
            if self.output_of(p) > 0:
                resolved.append((p, "parent"))
            elif (twin := self.executed_twin(p)) is not None:
                resolved.append((twin, "parent_twin"))
        if resolved:
            best = max(resolved, key=lambda ps: self.output_of(ps[0]))
            return best[0], best[1], len(resolved)
        start = self.first_launch.get(sid)
        earlier = [s for s in self.write_by_exec if s in self.last_finish and start and self.last_finish[s] <= start
                   and self.output_of(s) > 0]
        if not earlier:
            return None, "none", 0
        sized = [s for s in earlier if self.output_of(s) >= 0.5 * self.read_bytes[sid]] or earlier
        return max(sized, key=lambda s: self.last_finish[s]), "time", len(earlier)


def target_initial(peak: int, cap: int) -> int:
    return cap if peak >= 0.8 * cap else peak


def spread_class(row: dict) -> str:
    if row["feeding_stage"] is None:
        return "no_feeding_stage"
    uneven = row["hottest_share_pct"] is not None and row["hottest_share_pct"] >= 10
    if uneven or row["executors_holding_90pct"] < 0.5 * row["executors_with_output"]:
        return "concentrated"
    if row["executors_with_output"] >= 0.8 * row["peak_registered"]:
        return "spread"
    return "server_count"


def rows_for(log: Log, min_wait_pct: float, min_run_s: float) -> list[dict]:
    adds, rems = log.adds, log.rems
    first_rem_id = min(rems, key=rems.get) if rems else None
    first_rem = rems[first_rem_id] if first_rem_id else None
    first_task = min(log.first_launch.values()) if log.first_launch else None
    peak = peak_between(log.t0, log.t1, adds, rems)
    base = {
        "log": log.path.rsplit("/", 1)[-1],
        "app_name": log.app_name,
        "app_start_utc": ts_fmt(log.t0),
        "duration_min": round((log.t1 - log.t0) / 60000, 1),
        **{col: log.props.get(k, "") for k, col in PROP_KEYS.items()},
        "start_count": log.start_count(),
        "first_task_s": log.rel(first_task),
        "registered_at_60s": registered_at(log.t0 + 60_000, adds, rems),
        "registered_at_90s": registered_at(log.t0 + 90_000, adds, rems),
        "first_removal_s": log.rel(first_rem),
        "first_removal_reason": log.rem_reason.get(first_rem_id, "") if first_rem_id else "",
        "registered_after_first_removal": registered_at(first_rem + 1000, adds, rems) if first_rem else None,
        "peak_registered": peak,
        "total_registered_exec_h": round(executor_seconds(log.t0, log.t1, adds, rems) / 3600, 2),
    }
    flagged = [s for s in sorted(log.done_tasks)
               if log.run_time[s] >= min_run_s * 1000 and log.blocks[s] and log.wait_pct(s) >= min_wait_pct]
    out = []
    for s in flagged:
        m, source, n_parents = log.feeding_stage(s)
        row = dict(base)
        row.update({
            "stage": s,
            "stage_tasks": log.n_tasks.get(s, log.done_tasks[s]),
            "stage_submit_s": log.rel(log.submit.get(s)),
            "stage_live_at_submit": registered_at(log.submit[s], adds, rems) if s in log.submit else None,
            "fetch_wait_pct": round(log.wait_pct(s), 1),
            "fetch_wait_exec_h": round(log.fetch_wait[s] / 3.6e6 / log.cores(), 3),
            "blocks": log.blocks[s],
            "block_bytes": round(log.read_bytes[s] / log.blocks[s]),
            "read_gib": round(log.read_bytes[s] / 1024**3, 2),
            "feeding_stage": m,
            "feeding_source": source,
            "feeding_parents_with_output": n_parents,
        })
        if m is not None:
            total, n_exec, n90, hottest = concentration(log.write_by_exec[m])
            m_submit = log.submit.get(m, log.first_launch.get(m))
            m_end = log.complete.get(m, log.last_finish.get(m))
            row.update({
                "map_tasks": log.n_tasks.get(m, log.done_tasks[m]),
                "map_submit_s": log.rel(m_submit),
                "map_live_at_submit": registered_at(m_submit, adds, rems),
                "map_live_at_first_task": registered_at(log.first_launch[m], adds, rems),
                "map_peak_live": peak_between(m_submit, m_end, adds, rems),
                "map_output_gib": round(total / 1024**3, 2),
                "executors_with_output": n_exec,
                "executors_holding_90pct": n90,
                "hottest_share_pct": hottest,
                "removal_before_map": int(first_rem is not None and first_rem < m_submit),
            })
            target = target_initial(peak, log.cap())
            extra = max(0, target - log.start_count())
            window = min(row["map_submit_s"], row["first_removal_s"] if first_rem else float("inf"))
            idle_h = extra * window / 3600
            row.update({
                "target_initial": target,
                "extra_executors": extra,
                "extra_idle_window_s": round(window, 1),
                "extra_idle_exec_h": round(idle_h, 3),
                "cost_over_wait_ratio": round(idle_h / row["fetch_wait_exec_h"], 1) if row["fetch_wait_exec_h"] else None,
            })
        row["spread_class"] = spread_class(row)
        out.append(row)
    return out


def print_log(log: Log, rows: list[dict]) -> None:
    b = rows[0] if rows else None
    print(f"\n{log.path.rsplit('/', 1)[-1]}  {log.app_name}  start {ts_fmt(log.t0)}  {round((log.t1 - log.t0) / 60000, 1)} min")
    print(f"  env: initial={log.props.get('spark.dynamicAllocation.initialExecutors') or '-'} min={log.props.get('spark.dynamicAllocation.minExecutors') or '-'} "
          f"max={log.props.get('spark.dynamicAllocation.maxExecutors') or '-'} instances={log.props.get('spark.executor.instances') or '-'} "
          f"cores={log.props.get('spark.executor.cores') or '-'} idle={log.props.get('spark.dynamicAllocation.executorIdleTimeout') or '(default 60s)'} "
          f"start_count={log.start_count()}")
    if b:
        removal = (f"{b['first_removal_s']}s ({b['first_removal_reason'][:40]}) -> {b['registered_after_first_removal']}"
                   if b["first_removal_s"] is not None else "none")
        print(f"  fleet: +60s {b['registered_at_60s']}, +90s {b['registered_at_90s']}, first task {b['first_task_s']}s, "
              f"first removal {removal}, peak {b['peak_registered']}, registered {b['total_registered_exec_h']} exec-h")
    else:
        print("  no stage over the fetch-wait floor")
    for r in rows:
        print(f"  stage {r['stage']}: {r['stage_tasks']} tasks, {r['fetch_wait_pct']}% wait = {r['fetch_wait_exec_h']} exec-h, "
              f"{r['blocks']:,} blocks @ {r['block_bytes']} B, {r['stage_live_at_submit']} live at submit")
        if r["feeding_stage"] is None:
            print("      no feeding stage found")
            continue
        print(f"      fed by stage {r['feeding_stage']} ({r['feeding_source']}, {r['feeding_parents_with_output']} parents with output): "
              f"{r['map_tasks']} tasks -> {r['map_output_gib']} GiB on {r['executors_with_output']} executors, 90% on {r['executors_holding_90pct']}, "
              f"hottest {r['hottest_share_pct']}%; live at map submit {r['map_live_at_submit']} (first task {r['map_live_at_first_task']}, peak during map {r['map_peak_live']}), "
              f"map submit at {r['map_submit_s']}s, removal before map {r['removal_before_map']}")
        print(f"      target {r['target_initial']} (+{r['extra_executors']}): idle window {r['extra_idle_window_s']}s -> {r['extra_idle_exec_h']} exec-h, "
              f"cost/wait {r['cost_over_wait_ratio']}x, class {r['spread_class']}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-csv", required=True)
    ap.add_argument("--min-wait-pct", type=float, default=20)
    ap.add_argument("--min-run-s", type=float, default=300)
    ap.add_argument("logs", nargs="+")
    a = ap.parse_args()
    with open(a.out_csv, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for p in a.logs:
            log = Log(p)
            rows = rows_for(log, a.min_wait_pct, a.min_run_s)
            print_log(log, rows)
            for r in rows:
                w.writerow({k: r.get(k) for k in FIELDS})


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        raise SystemExit(2)
    main()
