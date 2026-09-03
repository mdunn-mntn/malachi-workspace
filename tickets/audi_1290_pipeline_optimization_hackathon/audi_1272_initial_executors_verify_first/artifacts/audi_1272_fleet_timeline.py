"""Registered-executor count at fixed offsets plus every stage's submit, first-task and end times, per event log.

Usage: PYTHONPATH=<workspace> python3 audi_1272_fleet_timeline.py [--until-s 300] [--step-s 10] <eventlog> [<eventlog> ...]
"""

from __future__ import annotations

import argparse
import sys

from airflow_optimizer.eventlog import _read_events


def timeline(path: str, until_s: int, step_s: int) -> None:
    t0 = None
    adds: dict[str, float] = {}
    rems: dict[str, float] = {}
    submit: dict[int, float] = {}
    end: dict[int, float] = {}
    n_tasks: dict[int, int] = {}
    first_launch: dict[int, float] = {}
    shuffle_write: dict[int, int] = {}
    for e in _read_events(path):
        ev = e.get("Event", "")
        if ev == "SparkListenerApplicationStart":
            t0 = e["Timestamp"]
        elif ev == "SparkListenerExecutorAdded":
            adds[str(e["Executor ID"])] = e["Timestamp"]
        elif ev == "SparkListenerExecutorRemoved":
            rems[str(e["Executor ID"])] = e["Timestamp"]
        elif ev == "SparkListenerStageSubmitted":
            si = e["Stage Info"]
            submit[si["Stage ID"]] = si.get("Submission Time")
            n_tasks[si["Stage ID"]] = si.get("Number of Tasks", 0)
        elif ev == "SparkListenerStageCompleted":
            end[e["Stage Info"]["Stage ID"]] = e["Stage Info"].get("Completion Time")
        elif ev == "SparkListenerTaskEnd":
            sid, info = e["Stage ID"], e["Task Info"]
            first_launch[sid] = min(first_launch.get(sid, float("inf")), info["Launch Time"])
            swm = (e.get("Task Metrics") or {}).get("Shuffle Write Metrics") or {}
            shuffle_write[sid] = shuffle_write.get(sid, 0) + swm.get("Shuffle Bytes Written", 0)

    def live(t: float) -> int:
        return sum(1 for eid, a in adds.items() if a <= t and rems.get(eid, float("inf")) > t)

    def rel(t: float | None) -> str:
        return f"{(t - t0) / 1000:.1f}" if t else "-"

    print(f"\n{path.rsplit('/', 1)[-1]}")
    ticks = [(s, live(t0 + s * 1000)) for s in range(0, until_s + 1, step_s)]
    print("  live executors: " + " ".join(f"{s}s={n}" for s, n in ticks))
    first_add = sorted(adds.values())
    print(f"  executor adds: first {rel(first_add[0]) if first_add else '-'}s, 10th {rel(first_add[9]) if len(first_add) > 9 else '-'}s, "
          f"last {rel(first_add[-1]) if first_add else '-'}s, total {len(adds)}, removals {len(rems)}")
    for sid in sorted(submit):
        if submit[sid] is None or (submit[sid] - t0) / 1000 > until_s:
            continue
        print(f"  stage {sid}: {n_tasks[sid]} tasks, submit {rel(submit[sid])}s (live {live(submit[sid])}), "
              f"first task {rel(first_launch.get(sid))}s (live {live(first_launch[sid]) if sid in first_launch else '-'}), "
              f"end {rel(end.get(sid))}s (live {live(end[sid]) if end.get(sid) else '-'}), "
              f"shuffle write {shuffle_write.get(sid, 0) / 1024**3:.2f} GiB")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--until-s", type=int, default=300)
    ap.add_argument("--step-s", type=int, default=10)
    ap.add_argument("logs", nargs="+")
    a = ap.parse_args(sys.argv[1:])
    for p in a.logs:
        timeline(p, a.until_s, a.step_s)
