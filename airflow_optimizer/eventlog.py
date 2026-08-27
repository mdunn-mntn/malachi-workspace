"""Comprehensive Spark event-log parser - the full 7-surface capture.

The plan text is one slice; the highest-value optimization signal (per-stage shuffle+spill,
per-task SKEW, per-executor failed-tasks+GC, config, SQL per-node metrics) lives in the event
log. This turns a Spark event log (`.zstd` rolling dir, `.zstd` file, or plain JSON) into a
structured `SparkRun` covering every surface, so detectors can reason over real metrics.

Field names are taken from a real Spark 4.0 event log (verified, not guessed).
"""

from __future__ import annotations

import glob
import json
import os
import re
import subprocess
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from statistics import median


@dataclass
class StageMetrics:
    """Aggregated per-stage metrics rolled up from TaskEnd events."""

    stage_id: int
    name: str = ""
    num_tasks: int = 0
    succeeded: int = 0
    failed: int = 0
    fetch_failed: int = 0
    input_bytes: int = 0
    output_bytes: int = 0
    shuffle_read_bytes: int = 0
    shuffle_write_bytes: int = 0
    mem_spill: int = 0
    disk_spill: int = 0
    gc_time_ms: int = 0
    run_time_ms: int = 0
    fetch_wait_ms: int = 0
    peak_exec_mem: int = 0
    failure_reason: str | None = None
    task_durs: list = field(default_factory=list)  # per-task wall time, for skew
    task_read_bytes: list = field(default_factory=list)  # per-task input+shuffle read, for data skew

    @property
    def skew_ratio(self) -> float:
        """max task time / median task time (>~5 with enough tasks = real skew)."""
        if len(self.task_durs) < 4:
            return 1.0
        med = median(self.task_durs)
        if med <= 0:  # a zero median manufactures astronomical ratios on trivial stages
            return 1.0
        return max(self.task_durs) / med

    @property
    def data_skew_ratio(self) -> float:
        """max task read bytes / median - separates true data skew from a slow-node straggler."""
        if len(self.task_read_bytes) < 4:
            return 1.0
        med = median(self.task_read_bytes)
        if med <= 0:  # median task read 0 bytes -> data-skew assessment is meaningless
            return 1.0
        return max(self.task_read_bytes) / med


@dataclass
class ExecutorInfo:
    """Per-executor rollup (failed tasks, GC, shuffle, removal reason)."""

    exec_id: str
    cores: int = 0  # from ExecutorAdded; spark.executor.cores is absent from many logs
    added_ts: int | None = None
    removed_ts: int | None = None
    removed_reason: str | None = None
    failed_tasks: int = 0
    completed_tasks: int = 0
    gc_time_ms: int = 0
    run_time_ms: int = 0


@dataclass
class SqlExec:
    """One SQL execution: description, plan text, and per-node metric values."""

    exec_id: int
    description: str = ""
    plan_text: str = ""
    node_metrics: list = field(default_factory=list)  # [{node, metrics:{name:value}}]


@dataclass
class SparkRun:
    """The whole run across all 7 surfaces."""

    app_id: str | None = None
    app_name: str | None = None
    duration_ms: int | None = None
    app_start_ts: int | None = None
    app_end_ts: int | None = None
    last_event_ts: int | None = None
    spark_props: dict = field(default_factory=dict)
    stages: list = field(default_factory=list)
    executors: list = field(default_factory=list)
    sql: list = field(default_factory=list)
    jobs: int = 0
    # storage surface (needs spark.eventLog.logBlockUpdates.enabled=true, else zeros)
    cached_rdd_bytes: int = 0
    rdd_cached_blocks: int = 0
    rdd_evictions: int = 0


_CLOCK_KEYS = ("Timestamp", "Completion Time", "Finish Time", "Submission Time")


def _later(current: int | None, event: dict) -> int | None:
    """The latest wall-clock stamp seen so far, used when an app writes no ApplicationEnd."""
    for k in _CLOCK_KEYS:
        v = event.get(k)
        if isinstance(v, int) and (current is None or v > current):
            current = v
    return current


def _part_order(path: str) -> tuple:
    """Sort key for v2 rolling parts: numeric part index (events_10 after events_2)."""
    m = re.search(r"events_(\d+)_", os.path.basename(path))
    return (int(m.group(1)) if m else 1 << 30, path)


def _read_events(path: str) -> Iterator[dict]:
    """Yield event dicts from a plain-JSON, single `.zstd`, or v2 rolling-dir event log.

    Streams line-by-line (a 98MB .zstd expands to ~1.8GB; materializing it OOMs the task).
    Raises ValueError on an undecodable part or an ambiguous directory - a corrupt log must
    surface as an error upstream, never parse to an empty "clean" run.
    """
    if os.path.isdir(path):
        parts = sorted(glob.glob(os.path.join(path, "events_*")), key=_part_order)
        if not parts:
            cand = [c for c in sorted(glob.glob(os.path.join(path, "*")))
                    if "appstatus" not in c and not c.endswith(".crc")]
            if len(cand) != 1:
                raise ValueError(
                    f"{path}: directory holds {len(cand)} files and is not a v2 rolling log - "
                    "pass each event log individually"
                )
            parts = cand
    else:
        parts = [path]
    for part in parts:
        with open(part, "rb") as f:
            magic = f.read(4)
        lines = _zstd_lines(part) if magic == b"\x28\xb5\x2f\xfd" else _plain_lines(part)
        for line in lines:
            if not line.strip():
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue  # tolerate a truncated/malformed final line (in-progress or crashed logs)


def _plain_lines(part: str) -> Iterator[str]:
    with open(part, encoding="utf-8", errors="replace") as f:
        yield from f


def _zstd_lines(part: str) -> Iterator[str]:
    """Stream decompressed lines; real Spark logs are MULTI-FRAME zstd, and a one-shot
    single-frame decompress silently returns only the first ~58 bytes."""
    try:
        import io

        import zstandard

        with open(part, "rb") as f:
            reader = zstandard.ZstdDecompressor().stream_reader(f, read_across_frames=True)
            yield from io.TextIOWrapper(reader, encoding="utf-8", errors="replace")
        return
    except ImportError:
        pass
    except Exception as e:
        raise ValueError(f"{part}: zstd decode failed ({e})") from e
    proc = subprocess.Popen(["zstd", "-dc", part], stdout=subprocess.PIPE,
                            stderr=subprocess.DEVNULL)
    try:
        yield from (ln.decode("utf-8", "replace") for ln in proc.stdout)
    finally:
        proc.stdout.close()
        if proc.wait() != 0:
            raise ValueError(f"{part}: zstd CLI decode failed (rc={proc.returncode})")


def _plan_node_metrics(spark_plan_info: dict, acc_values: dict) -> list:
    """Walk sparkPlanInfo, joining each node's metric accumulatorIds to their values."""
    out = []

    def walk(node: dict) -> None:
        metrics = {}
        for m in node.get("metrics", []):
            v = acc_values.get(m.get("accumulatorId"))
            if v is not None:
                metrics[m.get("name", "?")] = v
        if metrics:
            out.append({"node": node.get("nodeName", "?"), "metrics": metrics})
        for c in node.get("children", []):
            walk(c)

    walk(spark_plan_info)
    return out


def parse_eventlog(path: str) -> SparkRun:
    """Parse a Spark event log into a structured `SparkRun` (all 7 surfaces)."""
    run = SparkRun()
    stages: dict[tuple, StageMetrics] = {}  # (stage_id, attempt) - retries must not merge
    execs: dict[str, ExecutorInfo] = {}
    acc_values: dict[int, int] = {}  # accumulatorId -> value, for SQL node metrics
    plan_infos: dict[int, dict] = {}
    block_cached: dict[str, bool] = {}  # rdd block -> currently cached (storage surface)
    block_bytes: dict[str, int] = {}
    app_start = app_end = None

    def stage(sid: int, attempt: int = 0) -> StageMetrics:
        return stages.setdefault((sid, attempt), StageMetrics(stage_id=sid))

    def execu(eid: str) -> ExecutorInfo:
        return execs.setdefault(eid, ExecutorInfo(exec_id=eid))

    last_ts = None
    for e in _read_events(path):
        ev = e.get("Event", "")
        last_ts = _later(last_ts, e)
        if ev == "SparkListenerApplicationStart":
            app_start = e.get("Timestamp")
            run.app_id = e.get("App ID") or e.get("appId")
            run.app_name = e.get("App Name")
        elif ev == "SparkListenerApplicationEnd":
            app_end = e.get("Timestamp")
        elif ev == "SparkListenerEnvironmentUpdate":
            run.spark_props = e.get("Spark Properties", {}) or {}
        elif ev == "SparkListenerExecutorAdded":
            x = execu(str(e.get("Executor ID")))
            x.added_ts = e.get("Timestamp")
            x.cores = (e.get("Executor Info") or {}).get("Total Cores", 0) or 0
        elif ev == "SparkListenerExecutorRemoved":
            x = execu(str(e.get("Executor ID")))
            x.removed_ts = e.get("Timestamp")
            x.removed_reason = (e.get("Removed Reason") or "")[:120]
        elif ev == "SparkListenerJobStart":
            run.jobs += 1
        elif ev == "SparkListenerStageCompleted":
            si = e.get("Stage Info", {}) or {}
            st = stage(si.get("Stage ID"), si.get("Stage Attempt ID", 0))
            st.name = si.get("Stage Name", st.name)
            st.num_tasks = max(st.num_tasks, si.get("Number of Tasks", 0))
            if si.get("Failure Reason"):
                st.failure_reason = str(si["Failure Reason"])[:200]
            for a in si.get("Accumulables", []):
                if a.get("ID") is not None and a.get("Value") is not None:
                    acc_values[a["ID"]] = _num(a["Value"])
        elif ev == "SparkListenerTaskEnd":
            _task_end(e, stage, execu)
        elif ev.endswith("SQLExecutionStart"):
            eid = e.get("executionId")
            run.sql.append(
                SqlExec(exec_id=eid, description=e.get("description", "")[:200],
                        # keep the TAIL: physical plan + stats come last; a head-cap loses them
                        plan_text=(e.get("physicalPlanDescription") or "")[-8000:])
            )
            if e.get("sparkPlanInfo"):
                plan_infos[eid] = e["sparkPlanInfo"]
        elif ev.endswith("DriverAccumUpdates"):
            for pair in e.get("accumUpdates", []):
                if isinstance(pair, list) and len(pair) == 2:
                    acc_values[pair[0]] = _num(pair[1])
        elif ev == "SparkListenerBlockUpdated":
            run.rdd_evictions += _block_updated(e, block_cached, block_bytes)

    for s in run.sql:
        if s.exec_id in plan_infos:
            s.node_metrics = _plan_node_metrics(plan_infos[s.exec_id], acc_values)

    run.cached_rdd_bytes = sum(block_bytes.values())
    run.rdd_cached_blocks = sum(1 for v in block_cached.values() if v)
    run.app_start_ts, run.app_end_ts = app_start, app_end
    run.last_event_ts = last_ts
    if app_start and app_end:
        run.duration_ms = app_end - app_start
    run.stages = _finalize_stages(stages)
    run.executors = list(execs.values())
    return run


def _finalize_stages(stages: dict) -> list:
    """Keep each stage's LAST attempt (retries must not double-count) and backfill
    num_tasks from observed task ends when lifecycle events were dropped under load."""
    last: dict[int, tuple] = {}
    for (sid, att), st in stages.items():
        if sid not in last or att > last[sid][0]:
            last[sid] = (att, st)
    final = [st for _, st in last.values()]
    for st in final:
        st.num_tasks = max(st.num_tasks, st.succeeded + st.failed + st.fetch_failed)
    return sorted(final, key=lambda s: s.stage_id)


def _num(v: object) -> int:
    try:
        return int(float(str(v).replace(",", "").split()[0]))
    except (ValueError, IndexError):
        return 0


def _block_updated(e: dict, block_cached: dict, block_bytes: dict) -> int:
    """Track a cached RDD block's state; return 1 if it was just evicted, else 0."""
    bi = e.get("Block Updated Info", {}) or {}
    bid = bi.get("Block ID", "")
    if not bid.startswith("rdd_"):
        return 0
    sl = bi.get("Storage Level", {}) or {}
    cached = bool(sl.get("Use Memory") or sl.get("Use Disk"))
    evicted = 1 if (block_cached.get(bid) and not cached) else 0
    block_cached[bid] = cached
    block_bytes[bid] = (bi.get("Memory Size", 0) + bi.get("Disk Size", 0)) if cached else 0
    return evicted


def _task_end(e: dict, stage: Callable, execu: Callable) -> None:
    st = stage(e.get("Stage ID"), e.get("Stage Attempt ID", 0))
    ti = e.get("Task Info", {}) or {}
    reason = (e.get("Task End Reason", {}) or {}).get("Reason", "")
    x = execu(str(ti.get("Executor ID")))
    launch = ti.get("Launch Time") or 0
    if launch and (x.added_ts is None or launch < x.added_ts):
        x.added_ts = launch
    succeeded = reason == "Success"
    if succeeded:
        st.succeeded += 1
        x.completed_tasks += 1
    elif reason == "FetchFailed":
        st.fetch_failed += 1
    else:
        st.failed += 1
        x.failed_tasks += 1
    tm = e.get("Task Metrics") or {}
    if not tm:
        return
    st.run_time_ms += tm.get("Executor Run Time", 0)
    st.gc_time_ms += tm.get("JVM GC Time", 0)
    st.mem_spill += tm.get("Memory Bytes Spilled", 0)
    st.disk_spill += tm.get("Disk Bytes Spilled", 0)
    st.peak_exec_mem = max(st.peak_exec_mem, tm.get("Peak Execution Memory", 0))
    srm = tm.get("Shuffle Read Metrics") or {}
    task_read = srm.get("Remote Bytes Read", 0) + srm.get("Local Bytes Read", 0)
    st.shuffle_read_bytes += task_read
    st.fetch_wait_ms += srm.get("Fetch Wait Time", 0)
    st.shuffle_write_bytes += (tm.get("Shuffle Write Metrics") or {}).get("Shuffle Bytes Written", 0)
    task_input = (tm.get("Input Metrics") or {}).get("Bytes Read", 0)
    st.input_bytes += task_input
    st.output_bytes += (tm.get("Output Metrics") or {}).get("Bytes Written", 0)
    x.gc_time_ms += tm.get("JVM GC Time", 0)
    x.run_time_ms += tm.get("Executor Run Time", 0)
    if succeeded:  # a failed task's wall time is a failure to route, not skew evidence
        fin, lau = ti.get("Finish Time", 0), ti.get("Launch Time", 0)
        st.task_durs.append((fin - lau) if (fin and lau) else tm.get("Executor Run Time", 0))
        st.task_read_bytes.append(task_read + task_input)


if __name__ == "__main__":
    import sys

    r = parse_eventlog(sys.argv[1])
    print(f"app={r.app_name} dur={r.duration_ms}ms jobs={r.jobs} stages={len(r.stages)} "
          f"execs={len(r.executors)} sql={len(r.sql)}")
    print(f"shuffle.partitions={r.spark_props.get('spark.sql.shuffle.partitions')}")
    for s in r.stages:
        if s.shuffle_write_bytes or s.mem_spill or s.skew_ratio > 3:
            print(f"  stage {s.stage_id} tasks={s.num_tasks} shW={s.shuffle_write_bytes/1e6:.0f}MB "
                  f"spill={s.mem_spill/1e6:.0f}MB skew={s.skew_ratio:.1f}x")
    for s in r.sql:
        print(f"  sql[{s.exec_id}] nodes_with_metrics={len(s.node_metrics)} :: {s.description[:60]}")
