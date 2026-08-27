"""Spark query-plan optimization detectors (use-case #2: efficiency, not failures).

The failure taxonomy answers "why did it break"; this answers "why is it slow / what
is the low-hanging fix", from the SAME artifacts a debugger already sees - the Spark
physical-plan text + the `== Optimizer Statistics ==` block Databricks emits (and, when
available, per-node metrics). Deterministic-first, same shape as `signatures.py`: each
detector returns an evidence-backed finding with an impact tier and a concrete fix, so an
LLM only writes the summary, never invents the diagnosis.

Runs on SUCCEEDED jobs too - a slow-but-green job is exactly the optimization target.
Input here is the plan TEXT (cheapest key-free source); per-node timing/spill metrics
(Spark REST / event log) sharpen the impact ranking and are layered in later.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

_UNIT = {"B": 1, "KIB": 1024, "MIB": 1024**2, "GIB": 1024**3, "TIB": 1024**4}
BROADCAST_MAX_BYTES = 100 * 1024**2  # below this a join side is broadcast-eligible
LARGE_SHUFFLE_BYTES = 50 * 1024**3  # a shuffle this wide is worth partition-sizing
TARGET_PARTITION_BYTES = 256 * 1024**2  # Spark rule-of-thumb per shuffle partition
SKEW_MIN_TASK_MS = 60_000  # skew/straggler noise floor: slowest task must be material


def _to_bytes(num: str, unit: str) -> int:
    return int(float(num) * _UNIT.get(unit.upper(), 1))


@dataclass
class PlanScan:
    """One leaf scan node: table + estimated cardinality/size when present."""

    table: str
    rows: int | None = None
    size_bytes: int | None = None


@dataclass
class ParsedPlan:
    """Structured view of a Spark physical-plan text."""

    scans: list = field(default_factory=list)
    missing_stats: list = field(default_factory=list)
    partial_stats: list = field(default_factory=list)
    join_types: list = field(default_factory=list)
    shuffle_bytes: list = field(default_factory=list)
    has_running_window: bool = False
    write_target: str | None = None


@dataclass
class OptFinding:
    """One optimization opportunity: what, how big, and the concrete fix."""

    key: str
    title: str
    impact: str  # high | medium | low
    evidence: str
    fix: str
    rec_type: str = "code"  # code (query/PR) | infra (cores/memory) | failure (RCA)
    cost_h: float = 0.0  # executor-hours at stake, 0 when the detector cannot derive them


def parse_plan_text(text: str) -> ParsedPlan:
    """Extract scans, stats-state, joins, shuffles, windows, and the write target."""
    p = ParsedPlan()

    # `== Optimizer Statistics ==` block: `missing = a, b` / `partial = ...`
    for line in text.splitlines():
        m = re.match(r"\s*(missing|partial)\s*=\s*(.+)$", line)
        if m:
            tables = [t.strip() for t in re.split(r"[,\s]+", m.group(2)) if t.strip()]
            (p.missing_stats if m.group(1) == "missing" else p.partial_stats).extend(tables)

    # Scan leaves: `Scan parquet <table>` optionally followed by a rowcount column.
    for m in re.finditer(r"Scan \w+ ([\w.]+)\s*\|?[^\n|]*\|?\s*([\d,]{4,})?", text):
        rows = int(m.group(2).replace(",", "")) if m.group(2) else None
        p.scans.append(PlanScan(table=m.group(1), rows=rows))

    # Join strategy nodes.
    p.join_types = re.findall(r"(BroadcastHashJoin|SortMergeJoin|ShuffledHashJoin)", text)

    # Runtime shuffle sizes from `Statistics(sizeInBytes=182.2 GiB, ...)` on Shuffle stages.
    for m in re.finditer(
        r"(?:ShuffleQueryStage|Exchange)[^\n]*?sizeInBytes=([\d.]+)\s*(B|KiB|MiB|GiB|TiB)", text
    ):
        p.shuffle_bytes.append(_to_bytes(m.group(1), m.group(2)))

    p.has_running_window = "RunningWindowFunction" in text or "WindowGroupLimit" in text

    w = re.search(r"InsertIntoHadoopFsRelationCommand[^\n]*?(gs://[\w\-/]+)", text)
    if w:
        p.write_target = w.group(1)
    return p


def analyze_plan(text: str) -> list[OptFinding]:
    """Run the deterministic optimization detectors over a plan text (impact-ranked)."""
    p = parse_plan_text(text)
    out: list[OptFinding] = []

    # Missing stats drive join-strategy and shuffle-size mis-planning, so they lead.
    for tbl in p.missing_stats:
        rows = next((s.rows for s in p.scans if s.table.endswith(tbl) and s.rows), None)
        row_note = f" (~{rows:,} rows scanned)" if rows else ""
        out.append(
            OptFinding(
                "missing_statistics",
                f"Missing table statistics on {tbl}{row_note}",
                "high",
                f"Optimizer reports `missing = {tbl}`; without stats it cannot size the scan, "
                "so it defaults to SortMergeJoin + full sorts instead of a broadcast/right-sized shuffle.",
                f"ANALYZE TABLE {tbl} COMPUTE STATISTICS FOR ALL COLUMNS (then re-check the plan).",
            )
        )

    # 2. A SortMergeJoin with a broadcast-eligible side wastes a shuffle+sort.
    small_sides = [b for b in p.shuffle_bytes if b < BROADCAST_MAX_BYTES]
    if "SortMergeJoin" in p.join_types and small_sides:
        out.append(
            OptFinding(
                "broadcast_candidate",
                "SortMergeJoin with a broadcast-eligible side",
                "medium",
                f"A join side is ~{min(small_sides) // 1024**2} MiB, under the broadcast threshold, "
                "yet the plan sort-merge-joins it.",
                "Broadcast the small side (raise autoBroadcastJoinThreshold or a broadcast() hint) "
                "to drop the shuffle+sort.",
            )
        )

    # 3. Wide shuffle - size partitions to ~256 MiB so it doesn't spill (INC-009/INC-005 shape).
    for b in p.shuffle_bytes:
        if b >= LARGE_SHUFFLE_BYTES:
            want = max(1, round(b / TARGET_PARTITION_BYTES))
            out.append(
                OptFinding(
                    "shuffle_partition_sizing",
                    f"Wide shuffle (~{b // 1024**3} GiB) - size shuffle partitions",
                    "high" if b >= 5 * LARGE_SHUFFLE_BYTES else "medium",
                    f"A ~{b // 1024**3} GiB shuffle at the default partition count makes multi-GiB "
                    "partitions that spill to disk.",
                    f"Set spark.sql.shuffle.partitions ~{want} (~256 MiB each), or enable AQE "
                    "coalesce; cache the reused upstream so it is not recomputed per action.",
                )
            )

    # 4. RunningWindowFunction over a large sorted shuffle - the sort dominates.
    if p.has_running_window and any(b >= LARGE_SHUFFLE_BYTES for b in p.shuffle_bytes):
        out.append(
            OptFinding(
                "window_full_sort",
                "Window function forces a full sort of a large shuffle",
                "medium",
                "A RunningWindow/Window sorts the entire wide shuffle; the Sort is the dominant stage.",
                "Confirm the PARTITION BY has enough distinct keys to parallelize; keep WindowGroupLimit; "
                "pre-aggregate before the window if possible.",
            )
        )

    # 5. Same table scanned more than once - uncached recompute (the INC-005 signature).
    seen: dict[str, int] = {}
    for s in p.scans:
        seen[s.table] = seen.get(s.table, 0) + 1
    for tbl, n in seen.items():
        if n > 1:
            out.append(
                OptFinding(
                    "repeated_scan",
                    f"{tbl} scanned {n}x - likely uncached recompute",
                    "high",
                    f"The plan scans {tbl} {n} times; an uncached shared lineage is recomputed per action.",
                    f"cache()/persist() the {tbl} lineage before the branching actions.",
                )
            )

    # A stage can appear in both the metrics table and the plan text; collapse duplicates.
    deduped, seen_keys = [], set()
    for f in out:
        sig = (f.key, f.title)
        if sig not in seen_keys:
            seen_keys.add(sig)
            deduped.append(f)

    rank = {"high": 0, "medium": 1, "low": 2}
    deduped.sort(key=lambda f: rank.get(f.impact, 3))
    return deduped


def _gb(b: int) -> float:
    return b / 1024**3


HIGH_MIN_SHARE = 0.10
HIGH_MIN_HOURS = 10.0


def _gated(tier: str, cost_h: float, run_h: float) -> str:
    """Demote a high tier that clears neither floor; an underivable cost (0) never demotes."""
    if tier != "high" or not run_h or not cost_h:
        return tier
    big = cost_h >= HIGH_MIN_HOURS or cost_h / run_h >= HIGH_MIN_SHARE
    return tier if big else "medium"


def _cores(run: object, props: dict) -> int:
    """Slots per executor, 0 when neither the event log nor the property reports one."""
    reported = max((getattr(e, "cores", 0) or 0) for e in run.executors) if run.executors else 0
    return reported or int(props.get("spark.executor.cores", "0") or 0)


def _cost_note(cost_h: float, run_h: float) -> str:
    """The finding's absolute cost, against the run's own when that is known."""
    if not cost_h:
        return "executor-hours unknown (no per-executor core count in the log)"
    if run_h:
        return f"{cost_h:.1f} of the run's {run_h:.1f} executor-hours"
    return f"{cost_h:.1f} executor-hours"


def analyze_run(run: object) -> list[OptFinding]:
    """Detectors over a parsed SparkRun (event log) - the metrics the plan text can't give.

    Emits the three recommendation types: `code` (query/PR), `infra` (cores/memory), and
    `failure` (a real fault to route). Each carries the real numbers as evidence.
    """
    out: list[OptFinding] = []
    props = getattr(run, "spark_props", {}) or {}
    parts = props.get("spark.sql.shuffle.partitions")
    execs = [e for e in run.executors if getattr(e, "added_ts", None) is not None]
    cores = _cores(run, props)
    # A killed app writes no ApplicationEnd but still held its executors to the last event.
    ended = getattr(run, "app_end_ts", None)
    app_end = ended or getattr(run, "last_event_ts", None)
    reg_ms = sum(max((e.removed_ts or app_end) - e.added_ts, 0) for e in execs) if app_end else 0
    run_h = reg_ms / 3_600_000

    for s in run.stages:
        # Uniform data behind a slow task means a straggler, not skew; salting won't fix it.
        if (s.num_tasks >= 8 and s.skew_ratio >= 5
                and max(s.task_durs, default=0) >= SKEW_MIN_TASK_MS):
            if s.data_skew_ratio >= 2:
                out.append(OptFinding(
                    "skew", f"Stage {s.stage_id} skewed {s.skew_ratio:.1f}x (max vs median task)",
                    "high" if s.skew_ratio >= 10 else "medium",
                    f"{s.num_tasks} tasks, slowest is {s.skew_ratio:.1f}x the median and reads "
                    f"{s.data_skew_ratio:.1f}x the median data - one partition holds most of the data.",
                    "Salt the skewed join/group key or enable AQE skew join; a plain repartition will "
                    "not fix a value-skewed key.", rec_type="code"))
            else:
                spec_off = props.get("spark.speculation", "false") != "true"
                out.append(OptFinding(
                    "straggler",
                    f"Stage {s.stage_id} straggler: slowest task {s.skew_ratio:.1f}x the median on "
                    "uniform data",
                    "high" if s.skew_ratio >= 10 else "medium",
                    f"{s.num_tasks} tasks, slowest is {s.skew_ratio:.1f}x the median wall time but "
                    f"reads only {s.data_skew_ratio:.1f}x the median data - a slow executor/node or "
                    "IO stall, not data skew."
                    + (" spark.speculation is OFF, so nothing re-ran it." if spec_off else ""),
                    "Enable spark.speculation=true (with spark.speculation.quantile ~0.9) so a "
                    "straggling task is re-launched on an idle executor instead of pinning the stage.",
                    rec_type="infra"))
        # Disk and in-memory spill count the same records, so summing overstates ~6.5x.
        if s.disk_spill >= 2 * 1024**3 or s.mem_spill >= 32 * 1024**3:
            out.append(OptFinding(
                "disk_spill",
                f"Stage {s.stage_id} spilled {_gb(s.disk_spill):.1f} GiB to disk "
                f"({_gb(s.mem_spill):.0f} GiB in-memory at spill time)",
                "high" if (s.disk_spill >= 64 * 1024**3 or s.mem_spill >= 512 * 1024**3)
                else "medium",
                f"Disk spill {_gb(s.disk_spill):.1f} GiB / in-memory {_gb(s.mem_spill):.1f} GiB "
                f"over {s.num_tasks} tasks - per-task data exceeds execution memory.",
                "Raise spark.sql.shuffle.partitions (smaller partitions) first; if it persists, raise "
                "executor memory.", rec_type="code"))
        # Only "raise partitions" is safe advice: AQE already coalesces too-small ones.
        parts_n = int(parts) if str(parts or "").isdigit() else 200
        per_part = s.shuffle_write_bytes / parts_n
        if s.shuffle_write_bytes >= 50 * 1024**3 and per_part >= 512 * 1024**2:
            want = max(parts_n + 1, round(s.shuffle_write_bytes / (256 * 1024**2)))
            aqe = props.get("spark.sql.adaptive.enabled") == "true"
            out.append(OptFinding(
                "shuffle_partition_sizing",
                f"Stage {s.stage_id} wide shuffle ({_gb(s.shuffle_write_bytes):.0f} GiB, "
                f"~{per_part / 1024**2:.0f} MiB/partition)",
                "high", f"{_gb(s.shuffle_write_bytes):.0f} GiB shuffle write over "
                f"shuffle.partitions={parts or 'default'} = ~{per_part / 1024**2:.0f} MiB per "
                "partition - oversized reducers spill.",
                f"Raise spark.sql.shuffle.partitions to ~{want} (~256 MiB each)."
                + (" AQE coalesce is already on; it only merges small partitions and cannot "
                   "split oversized ones." if aqe else ""),
                rec_type="code"))
        # FETCH-WAIT dominance - tasks stall waiting on shuffle fetch, not computing.
        if s.run_time_ms >= 300_000 and s.fetch_wait_ms / s.run_time_ms >= 0.3:
            ratio = s.fetch_wait_ms / s.run_time_ms
            wait_h = (min(s.fetch_wait_ms / cores / 3_600_000, run_h or float('inf'))
                      if cores else 0.0)
            out.append(OptFinding(
                "shuffle_fetch_wait",
                f"Stage {s.stage_id} spends {100 * ratio:.0f}% of task time waiting on "
                "shuffle fetch",
                _gated("high" if ratio >= 0.5 else "medium", wait_h, run_h),
                f"{s.fetch_wait_ms / 1000:.0f}s of {s.run_time_ms / 1000:.0f}s task time is "
                f"shuffle-fetch wait over {s.num_tasks} tasks, {_cost_note(wait_h, run_h)} - the "
                "shuffle IO path is the bottleneck, not compute.",
                "Check which executors hold the map output before changing partition counts. If "
                "it is concentrated (the map stage ran while the fleet was still scaling up), "
                "raise dynamicAllocation.initialExecutors so the map stage spreads its output; "
                "raising spark.sql.shuffle.partitions then makes it WORSE by multiplying block "
                "count. Raise partitions only when the blocks themselves are large.",
                rec_type="code", cost_h=wait_h))

    # GC PRESSURE across the run - an infra (memory) signal.
    run_ms = sum(s.run_time_ms for s in run.stages)
    gc_ms = sum(s.gc_time_ms for s in run.stages)
    if run_ms and gc_ms / run_ms >= 0.1:
        gc_h = min(gc_ms / cores / 3_600_000, run_h or float('inf')) if cores else 0.0
        out.append(OptFinding(
            "gc_pressure", f"GC is {100 * gc_ms / run_ms:.0f}% of task time",
            _gated("high" if gc_ms / run_ms >= 0.2 else "medium", gc_h, run_h),
            f"{gc_ms / 1000:.0f}s GC of {run_ms / 1000:.0f}s task time, {_cost_note(gc_h, run_h)} - "
            "executors are memory-starved.",
            "Raise executor memory / use memory-optimized workers; secondarily cut per-task data via "
            "more partitions.", rec_type="infra", cost_h=gc_h))

    # "decommission"/"lost" are normal serverless scale-down strings, not preemption.
    preempted = [e for e in run.executors
                 if e.removed_reason and any(t in e.removed_reason.lower()
                                             for t in ("preempt", "spot"))]
    total_failed = sum(e.failed_tasks for e in preempted)
    if preempted and total_failed:
        out.append(OptFinding(
            "spot_preemption_cost",
            f"{total_failed} task failures from {len(preempted)} reclaimed executors",
            "high", f"Executors removed ({preempted[0].removed_reason}) forced {total_failed} task "
            "re-runs - spot churn is costing wall-clock.",
            "Raise first_on_demand / add on-demand fallback for this job, or checkpoint before the "
            "long shuffle.", rec_type="infra"))

    # shuffleTracking exempts executors a live job's shuffle references, so a tail pins all.
    if execs and app_end and len(execs) >= 8:
        busy_ms = sum(e.run_time_ms for e in execs)
        # A metrics-less TaskEnd adds no run time, and a FetchFailed one bumps no executor counter.
        ran = (busy_ms or sum(s.succeeded + s.failed + s.fetch_failed for s in run.stages)
               or sum(e.completed_tasks + e.failed_tasks for e in execs))
        # Until the app ends, a still-writing first log part is indistinguishable from a no-op.
        if not ran and not ended:
            return _ranked(out)
        if not ran and run_h >= 2:
            out.append(OptFinding(
                "idle_reserved_executors",
                f"{len(execs)} executors held {run_h:.1f} executor-hours with ZERO tasks run",
                "high",
                f"The app registered {len(execs)} executors for {run_h:.1f} executor-hours and "
                "never ran a task - the whole allocation was billed for nothing.",
                "Check why the driver allocated executors it never used (eager allocation before "
                "a driver-side step, or a no-op run); lower minExecutors/initialExecutors.",
                rec_type="infra", cost_h=run_h))
        elif busy_ms and cores and run_h >= 20 and busy_ms / (reg_ms * cores) < 0.4:
            util = busy_ms / (reg_ms * cores)
            idle_h = (reg_ms * cores - busy_ms) / 3_600_000 / cores
            removed = sum(1 for e in execs if e.removed_ts)
            tracking = props.get("spark.dynamicAllocation.shuffleTracking.enabled") == "true"
            hold = (" shuffleTracking pins executors whose shuffle blocks a live job still "
                    "references, so the fleet is held until the tail task finishes."
                    if tracking else "")
            out.append(OptFinding(
                "idle_reserved_executors",
                f"Executors {100 * util:.0f}% utilized: ~{idle_h:.0f} idle executor-hours held",
                "high" if util < 0.25 else "medium",
                f"{len(execs)} executors held {run_h:.0f} executor-hours but task slots "
                f"were busy only {100 * util:.0f}% of that; {removed} were released before app "
                f"end.{hold}",
                "Fix the tail that keeps the final job alive (speculation for stragglers, skew "
                "fixes) - releasing executors mid-query is not achievable via "
                "shuffleTracking.timeout, which only applies after the referencing job ends.",
                rec_type="infra", cost_h=idle_h))

    # CACHE eviction - a persisted RDD got evicted (memory pressure) so it recomputes.
    if getattr(run, "rdd_evictions", 0) > 0:
        out.append(OptFinding(
            "cache_ineffective",
            f"Cached data evicted {run.rdd_evictions}x (cache under memory pressure)",
            "medium",
            f"{run.rdd_evictions} cached RDD blocks were dropped ({_gb(run.cached_rdd_bytes):.1f} GiB "
            "still cached) - an evicted cache is recomputed on the next read.",
            "Raise executor/storage memory, use MEMORY_AND_DISK, or cache a narrower projection so the "
            "working set fits.", rec_type="infra"))

    # FETCH-FAILED instability -> a real fault to route (failure), not just slow.
    fetch = sum(s.fetch_failed for s in run.stages)
    if fetch:
        out.append(OptFinding(
            "shuffle_fetch_instability", f"{fetch} FetchFailed tasks (shuffle instability)",
            "high", f"{fetch} FetchFailed re-runs - lost executors/nodes or >2 GiB shuffle blocks.",
            "Route as infra instability; reduce shuffle block size (more partitions) and check "
            "node health / preemption.", rec_type="failure"))

    return _ranked(out)


def _ranked(out: list) -> list:
    """Findings worst-first."""
    rank = {"high": 0, "medium": 1, "low": 2}
    out.sort(key=lambda f: rank.get(f.impact, 3))
    return out


if __name__ == "__main__":
    import sys

    with open(sys.argv[1], encoding="utf-8", errors="replace") as f:
        findings = analyze_plan(f.read())
    if not findings:
        print("No optimization findings.")
    for fnd in findings:
        print(f"[{fnd.impact.upper()}] {fnd.title}\n  {fnd.evidence}\n  FIX: {fnd.fix}\n")
