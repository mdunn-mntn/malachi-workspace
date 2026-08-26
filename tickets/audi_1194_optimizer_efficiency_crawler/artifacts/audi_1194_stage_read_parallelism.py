"""Why one stage stalls on shuffle fetch and the next, reading the same output, does not.

Reads a Spark event log and reports, per stage: when it ran, how many executors were live at
its start, how many shuffle blocks its tasks fetched locally vs remotely, and what share of
task time went to fetch wait. Written for site_network_hourly stage 9 vs stage 15.
"""

from __future__ import annotations

import json
import subprocess
import sys
from collections.abc import Iterator
from dataclasses import dataclass, field


@dataclass
class Stage:
    """One stage attempt, with the shuffle-read detail the optimizer's parser drops."""

    stage_id: int
    submit_ts: int = 0
    complete_ts: int = 0
    num_tasks: int = 0
    run_ms: int = 0
    fetch_wait_ms: int = 0
    remote_blocks: int = 0
    local_blocks: int = 0
    remote_bytes: int = 0
    local_bytes: int = 0
    shuffle_read_bytes: int = 0
    parents: list = field(default_factory=list)

    @property
    def wait_share(self) -> float:
        """Share of task time spent waiting on shuffle blocks to arrive."""
        return self.fetch_wait_ms / self.run_ms if self.run_ms else 0.0


def _lines(path: str) -> Iterator[str]:
    if path.endswith(".zstd"):
        out = subprocess.run(["zstd", "-dc", path], capture_output=True, text=True)
        if out.returncode:
            raise ValueError(f"zstd failed on {path}")
        yield from out.stdout.splitlines()
    else:
        with open(path) as fh:
            yield from fh


def parse(path: str) -> tuple[dict, list]:
    """(stages by id, executor add/remove events) for one event log."""
    stages: dict[int, Stage] = {}
    execs: list = []
    for line in _lines(path):
        line = line.strip()
        if not line:
            continue
        try:
            e = json.loads(line)
        except json.JSONDecodeError:
            continue
        ev = e.get("Event", "")
        if ev == "SparkListenerExecutorAdded":
            execs.append((e.get("Timestamp", 0), 1))
        elif ev == "SparkListenerExecutorRemoved":
            execs.append((e.get("Timestamp", 0), -1))
        elif ev == "SparkListenerStageSubmitted":
            info = e.get("Stage Info", {})
            sid = info.get("Stage ID", -1)
            s = stages.setdefault(sid, Stage(stage_id=sid))
            s.submit_ts = info.get("Submission Time") or e.get("Timestamp") or s.submit_ts
            s.num_tasks = info.get("Number of Tasks", s.num_tasks)
            s.parents = info.get("Parent IDs", s.parents)
        elif ev == "SparkListenerStageCompleted":
            info = e.get("Stage Info", {})
            sid = info.get("Stage ID", -1)
            s = stages.setdefault(sid, Stage(stage_id=sid))
            s.complete_ts = info.get("Completion Time") or s.complete_ts
        elif ev == "SparkListenerTaskEnd":
            sid = e.get("Stage ID", -1)
            s = stages.setdefault(sid, Stage(stage_id=sid))
            m = e.get("Task Metrics") or {}
            s.run_ms += m.get("Executor Run Time", 0)
            sr = m.get("Shuffle Read Metrics") or {}
            s.fetch_wait_ms += sr.get("Fetch Wait Time", 0)
            s.remote_blocks += sr.get("Remote Blocks Fetched", 0)
            s.local_blocks += sr.get("Local Blocks Fetched", 0)
            s.remote_bytes += sr.get("Remote Bytes Read", 0)
            s.local_bytes += sr.get("Local Bytes Read", 0)
            s.shuffle_read_bytes += sr.get("Remote Bytes Read", 0) + sr.get("Local Bytes Read", 0)
    return stages, sorted(execs)


def live_at(execs: list, ts: int) -> int:
    """Executors registered and not yet removed at `ts`."""
    n = 0
    for t, delta in execs:
        if t > ts:
            break
        n += delta
    return n


def report(path: str) -> list[dict]:
    """One row per shuffle-reading stage, in run order."""
    stages, execs = parse(path)
    rows = []
    for s in sorted(stages.values(), key=lambda s: s.submit_ts):
        if not s.shuffle_read_bytes:
            continue
        rows.append({
            "stage": s.stage_id,
            "live_execs": live_at(execs, s.submit_ts),
            "tasks": s.num_tasks,
            "wait_share": round(s.wait_share, 3),
            "remote_blocks": s.remote_blocks,
            "local_blocks": s.local_blocks,
            "remote_gib": round(s.remote_bytes / 1024**3, 1),
            "local_gib": round(s.local_bytes / 1024**3, 1),
            "run_h": round(s.run_ms / 3_600_000, 1),
        })
    return rows


if __name__ == "__main__":
    for p in sys.argv[1:]:
        print(f"== {p}")
        print(f"{'stage':>6}{'live_ex':>9}{'tasks':>7}{'wait':>7}{'remoteB':>10}{'localB':>9}"
              f"{'rem GiB':>9}{'loc GiB':>9}{'run h':>7}")
        for r in report(p):
            print(f"{r['stage']:>6}{r['live_execs']:>9}{r['tasks']:>7}{r['wait_share']:>7.0%}"
                  f"{r['remote_blocks']:>10,}{r['local_blocks']:>9,}{r['remote_gib']:>9.1f}"
                  f"{r['local_gib']:>9.1f}{r['run_h']:>7.1f}")


def concurrency(path: str) -> dict:
    """Peak and mean concurrently-running tasks against the executor slots held."""
    events, execs, cores = [], [], 4
    for line in _lines(path):
        line = line.strip()
        if not line:
            continue
        try:
            e = json.loads(line)
        except json.JSONDecodeError:
            continue
        ev = e.get("Event", "")
        if ev == "SparkListenerTaskEnd":
            info = e.get("Task Info", {})
            # Only ENDED tasks are counted: a TaskStart whose end never lands (killed at stage
            # end, speculative) would stay "running" forever and inflate the peak 50x.
            start, end = info.get("Launch Time") or 0, info.get("Finish Time") or 0
            if start and end > start:
                events.append((start, 1))
                events.append((end, -1))
        elif ev == "SparkListenerExecutorAdded":
            execs.append((e.get("Timestamp", 0), 1))
        elif ev == "SparkListenerExecutorRemoved":
            execs.append((e.get("Timestamp", 0), -1))
        elif ev == "SparkListenerEnvironmentUpdate":
            cores = int((e.get("Spark Properties") or {}).get("spark.executor.cores", 4) or 4)
    events.sort(key=lambda x: (x[0], -x[1]))
    execs.sort()
    peak = cur = 0
    area = 0
    prev = events[0][0] if events else 0
    for ts, delta in events:
        area += cur * (ts - prev)
        prev = ts
        cur += delta
        peak = max(peak, cur)
    span = (events[-1][0] - events[0][0]) if len(events) > 1 else 0
    peak_execs = 0
    cur_e = 0
    for _ts, delta in execs:
        cur_e += delta
        peak_execs = max(peak_execs, cur_e)
    return {"peak_tasks": peak, "mean_tasks": area / span if span else 0,
            "task_hours": area / 3_600_000,
            "peak_execs": peak_execs, "cores": cores,
            "peak_slots": peak_execs * cores, "span_h": span / 3_600_000}
