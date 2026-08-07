"""Executor-utilization timeline from a Spark event log: quantify reserved-vs-busy executors.

Built for the aud-int-int-map validation run (AUDI-1194): Ryan saw 1 active executor with ~240
reserved for ~1h. This walks the raw events and measures it: registered executors vs executors
actually running >=1 task, sampled on a fixed grid, plus a per-stage table and executor-removal
reasons. Emits a markdown report and a CSV timeline.

Usage: python3 audi_1194_executor_timeline.py <eventlog_path_or_rolling_dir> <out_prefix>
"""

from __future__ import annotations

import csv
import sys
from bisect import bisect_right
from datetime import datetime, timezone

sys.path.insert(0, "/Users/malachi/Developer/work/mntn/workspace")
from airflow_optimizer.eventlog import _read_events  # noqa: E402

SAMPLE_S = 10


def ts_fmt(ms: float) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%H:%M:%S")


def main(path: str, out_prefix: str) -> None:
    events = _read_events(path)
    exec_add: dict[str, float] = {}
    exec_rem: dict[str, float] = {}
    rem_reason: dict[str, str] = {}
    tasks: list[tuple[float, float, str, int]] = []  # (launch, finish, exec_id, stage_id)
    stages: dict[int, dict] = {}
    app_start = app_end = None

    for e in events:
        ev = e.get("Event", "")
        if ev == "SparkListenerApplicationStart":
            app_start = e.get("Timestamp")
        elif ev == "SparkListenerApplicationEnd":
            app_end = e.get("Timestamp")
        elif ev == "SparkListenerExecutorAdded":
            exec_add[str(e.get("Executor ID"))] = e.get("Timestamp")
        elif ev == "SparkListenerExecutorRemoved":
            eid = str(e.get("Executor ID"))
            exec_rem[eid] = e.get("Timestamp")
            rem_reason[eid] = e.get("Removed Reason") or ""
        elif ev == "SparkListenerTaskEnd":
            ti = e.get("Task Info", {}) or {}
            if ti.get("Launch Time") and ti.get("Finish Time"):
                tasks.append((ti["Launch Time"], ti["Finish Time"], str(ti.get("Executor ID")),
                              e.get("Stage ID", -1)))
        elif ev == "SparkListenerStageSubmitted":
            si = e.get("Stage Info", {}) or {}
            s = stages.setdefault(si.get("Stage ID"), {})
            s["name"] = (si.get("Stage Name") or "")[:80]
            s["submitted"] = si.get("Submission Time")
            s["num_tasks"] = si.get("Number of Tasks", 0)
        elif ev == "SparkListenerStageCompleted":
            si = e.get("Stage Info", {}) or {}
            s = stages.setdefault(si.get("Stage ID"), {})
            s["name"] = (si.get("Stage Name") or "")[:80]
            s.setdefault("submitted", si.get("Submission Time"))
            s["completed"] = si.get("Completion Time")
            s["num_tasks"] = si.get("Number of Tasks", 0)

    t0 = app_start or min(exec_add.values())
    t1 = app_end or max([f for _, f, _, _ in tasks] + list(exec_rem.values()))

    # sampling grid: registered vs busy executors, running tasks
    task_starts = sorted(t for t, _, _, _ in tasks)
    grid = []
    t = t0
    while t <= t1:
        registered = sum(1 for eid, a in exec_add.items()
                         if a <= t and exec_rem.get(eid, float("inf")) > t)
        running = [(le, fe, ex) for le, fe, ex, _ in tasks if le <= t < fe]
        busy = len({ex for _, _, ex in running})
        grid.append((t, registered, busy, len(running)))
        t += SAMPLE_S * 1000

    total_exec_s = sum((min(exec_rem.get(eid, t1), t1) - a) / 1000 for eid, a in exec_add.items())
    busy_map: dict[str, list[tuple[float, float]]] = {}
    for le, fe, ex, _ in tasks:
        busy_map.setdefault(ex, []).append((le, fe))
    busy_exec_s = 0.0
    for ex, iv in busy_map.items():
        iv.sort()
        cur_s, cur_e = iv[0]
        for s_, e_ in iv[1:]:
            if s_ > cur_e:
                busy_exec_s += (cur_e - cur_s) / 1000
                cur_s, cur_e = s_, e_
            else:
                cur_e = max(cur_e, e_)
        busy_exec_s += (cur_e - cur_s) / 1000

    # tail windows: contiguous grid segments with busy <= 2
    tail_windows = []
    seg = None
    for t, reg, busy, _ in grid:
        if busy <= 2 and reg >= 10:
            seg = (t, reg) if seg is None else seg
            seg_end = t
        else:
            if seg and (seg_end - seg[0]) >= 5 * 60 * 1000:
                tail_windows.append((seg[0], seg_end))
            seg = None
    if seg and (seg_end - seg[0]) >= 5 * 60 * 1000:
        tail_windows.append((seg[0], seg_end))

    with open(f"{out_prefix}_timeline.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["utc_time", "registered_executors", "busy_executors", "running_tasks"])
        for t, reg, busy, ntask in grid:
            w.writerow([ts_fmt(t), reg, busy, ntask])

    lines = ["# Executor utilization: registered vs busy", ""]
    dur_s = (t1 - t0) / 1000
    lines.append(f"- App window (UTC): {ts_fmt(t0)} -> {ts_fmt(t1)}  ({dur_s/60:.1f} min)")
    lines.append(f"- Executors ever registered: {len(exec_add)}; removed during run: {len(exec_rem)}")
    lines.append(f"- Total executor-hours registered: {total_exec_s/3600:.1f}")
    lines.append(f"- Executor-hours actually running >=1 task: {busy_exec_s/3600:.1f} "
                 f"({100*busy_exec_s/max(total_exec_s,1):.1f}% utilization)")
    lines.append(f"- Idle-reserved executor-hours: {(total_exec_s-busy_exec_s)/3600:.1f}")
    peak = max(g[1] for g in grid)
    lines.append(f"- Peak registered executors: {peak}")
    lines.append("")
    lines.append("## Low-parallelism windows (busy executors <= 2 for >= 5 min)")
    for s_, e_ in tail_windows:
        regs = [g[1] for g in grid if s_ <= g[0] <= e_]
        lines.append(f"- {ts_fmt(s_)} -> {ts_fmt(e_)} ({(e_-s_)/60000:.0f} min), registered "
                     f"executors {min(regs)}-{max(regs)}")
    lines.append("")
    lines.append("## Stages by duration (top 12)")
    lines.append("| stage | tasks | start | end | dur_min | name |")
    lines.append("|---|---|---|---|---|---|")
    sl = [(sid, s) for sid, s in stages.items() if s.get("submitted") and s.get("completed")]
    sl.sort(key=lambda x: x[1]["completed"] - x[1]["submitted"], reverse=True)
    for sid, s in sl[:12]:
        lines.append(f"| {sid} | {s['num_tasks']} | {ts_fmt(s['submitted'])} | "
                     f"{ts_fmt(s['completed'])} | {(s['completed']-s['submitted'])/60000:.1f} | "
                     f"{s['name']} |")
    lines.append("")
    lines.append("## Executor removal reasons")
    reasons: dict[str, int] = {}
    for r in rem_reason.values():
        reasons[r[:80] or "(none)"] = reasons.get(r[:80] or "(none)", 0) + 1
    for r, n in sorted(reasons.items(), key=lambda x: -x[1]):
        lines.append(f"- {n}x {r}")
    never_removed = len(exec_add) - len(exec_rem)
    lines.append(f"- {never_removed} executors never removed before app end")
    lines.append("")
    lines.append("## Task activity in the final hour")
    final_hour = [x for x in tasks if x[1] >= t1 - 3600_000]
    lines.append(f"- Tasks finishing in the last 60 min: {len(final_hour)}")
    by_stage: dict[int, int] = {}
    for _, _, _, sid in final_hour:
        by_stage[sid] = by_stage.get(sid, 0) + 1
    for sid, n in sorted(by_stage.items(), key=lambda x: -x[1])[:8]:
        s = stages.get(sid, {})
        lines.append(f"  - stage {sid} ({s.get('num_tasks','?')} tasks, {s.get('name','?')}): {n}")
    lines.append("")
    lines.append(f"- Idle tail (last task finish -> app end): "
                 f"{(t1 - max(f for _, f, _, _ in tasks))/60000:.1f} min" if tasks else "")
    # first sample index where busy drops to <=2 and never exceeds 2 again
    grid_busy = [g[2] for g in grid]
    last_high = max((i for i, b in enumerate(grid_busy) if b > 2), default=-1)
    if 0 <= last_high < len(grid) - 1:
        lines.append(f"- After {ts_fmt(grid[last_high+1][0])}, busy executors never exceed 2 "
                     f"(registered at that moment: {grid[last_high+1][1]})")

    with open(f"{out_prefix}_report.md", "w") as f:
        f.write("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
