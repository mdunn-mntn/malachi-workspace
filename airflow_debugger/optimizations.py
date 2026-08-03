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


if __name__ == "__main__":
    import sys

    with open(sys.argv[1], encoding="utf-8", errors="replace") as f:
        findings = analyze_plan(f.read())
    if not findings:
        print("No optimization findings.")
    for fnd in findings:
        print(f"[{fnd.impact.upper()}] {fnd.title}\n  {fnd.evidence}\n  FIX: {fnd.fix}\n")
