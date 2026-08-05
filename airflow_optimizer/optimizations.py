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

    # 1. Missing/partial table statistics - the optimizer itself flags this and it drives
    #    join-strategy and shuffle-size mis-planning. Highest-leverage, cheapest fix.
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


def analyze_run(run: object) -> list[OptFinding]:
    """Detectors over a parsed SparkRun (event log) - the metrics the plan text can't give.

    Emits the three recommendation types: `code` (query/PR), `infra` (cores/memory), and
    `failure` (a real fault to route). Each carries the real numbers as evidence.
    """
    out: list[OptFinding] = []
    props = getattr(run, "spark_props", {}) or {}
    parts = props.get("spark.sql.shuffle.partitions")

    for s in run.stages:
        spill = s.mem_spill + s.disk_spill
        # SKEW - one task runs far longer than the median (invisible in the plan text).
        if s.num_tasks >= 8 and s.skew_ratio >= 5:
            out.append(OptFinding(
                "skew", f"Stage {s.stage_id} skewed {s.skew_ratio:.1f}x (max vs median task)",
                "high" if s.skew_ratio >= 10 else "medium",
                f"{s.num_tasks} tasks, slowest is {s.skew_ratio:.1f}x the median - one partition holds "
                "most of the data.",
                "Salt the skewed join/group key or enable AQE skew join; a plain repartition will not "
                "fix a value-skewed key.", rec_type="code"))
        # SPILL - shuffle/agg spilled to disk; size partitions up (code) or memory (infra).
        if spill >= 1024**3:
            out.append(OptFinding(
                "disk_spill", f"Stage {s.stage_id} spilled {_gb(spill):.1f} GiB",
                "high" if spill >= 20 * 1024**3 else "medium",
                f"Memory+disk spill {_gb(spill):.1f} GiB over {s.num_tasks} tasks - partitions exceed "
                "executor memory.",
                "Raise spark.sql.shuffle.partitions (smaller partitions) first; if it persists, raise "
                "executor memory.", rec_type="code"))
        # WIDE SHUFFLE at the default partition count.
        if s.shuffle_write_bytes >= 50 * 1024**3:
            want = max(1, round(s.shuffle_write_bytes / (256 * 1024**2)))
            out.append(OptFinding(
                "shuffle_partition_sizing",
                f"Stage {s.stage_id} wide shuffle ({_gb(s.shuffle_write_bytes):.0f} GiB)",
                "high", f"{_gb(s.shuffle_write_bytes):.0f} GiB shuffle write at "
                f"shuffle.partitions={parts or 'default'}.",
                f"Set spark.sql.shuffle.partitions ~{want} (~256 MiB each) or enable AQE coalesce.",
                rec_type="code"))

    # GC PRESSURE across the run - an infra (memory) signal.
    run_ms = sum(s.run_time_ms for s in run.stages)
    gc_ms = sum(s.gc_time_ms for s in run.stages)
    if run_ms and gc_ms / run_ms >= 0.1:
        out.append(OptFinding(
            "gc_pressure", f"GC is {100 * gc_ms / run_ms:.0f}% of task time",
            "high" if gc_ms / run_ms >= 0.2 else "medium",
            f"{gc_ms / 1000:.0f}s GC of {run_ms / 1000:.0f}s task time - executors are memory-starved.",
            "Raise executor memory / use memory-optimized workers; secondarily cut per-task data via "
            "more partitions.", rec_type="infra"))

    # SPOT PREEMPTION - failed tasks from reclaimed executors (an infra config choice).
    preempted = [e for e in run.executors
                 if e.removed_reason and any(t in e.removed_reason.lower()
                                             for t in ("preempt", "spot", "lost", "decommission"))]
    total_failed = sum(e.failed_tasks for e in run.executors)
    if preempted and total_failed:
        out.append(OptFinding(
            "spot_preemption_cost",
            f"{total_failed} task failures from {len(preempted)} reclaimed executors",
            "high", f"Executors removed ({preempted[0].removed_reason}) forced {total_failed} task "
            "re-runs - spot churn is costing wall-clock.",
            "Raise first_on_demand / add on-demand fallback for this job, or checkpoint before the "
            "long shuffle.", rec_type="infra"))

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
