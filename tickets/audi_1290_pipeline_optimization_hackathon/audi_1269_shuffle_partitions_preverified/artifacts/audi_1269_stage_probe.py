"""Explain why a reducer stage's task count differs from spark.sql.shuffle.partitions.

    PYTHONPATH=/Users/malachi/Developer/work/mntn/workspace \
    python3 artifacts/audi_1269_stage_probe.py <eventlog> <stage_id> [<stage_id> ...]

Prints each named stage's parents, call site and RDD chain, the per-task split of
input vs shuffle-read bytes, and every AQE plan line that coalesces or skew-splits a shuffle.
"""

from __future__ import annotations

import json
import re
import statistics
import sys

from airflow_optimizer.eventlog import _read_events

MIB = 1024**2
AQE_PATTERN = re.compile(r"AQEShuffleRead|CustomShuffleReader|coalesced|skewed|Skew", re.IGNORECASE)


def main(path: str, stage_ids: set[int]) -> None:
    submitted: dict[int, dict] = {}
    tasks: dict[int, list[tuple[int, int]]] = {sid: [] for sid in stage_ids}
    plans: dict[int, str] = {}
    for e in _read_events(path):
        kind = e.get("Event")
        if kind == "SparkListenerStageSubmitted":
            info = e["Stage Info"]
            if info["Stage ID"] in stage_ids:
                submitted[info["Stage ID"]] = info
        elif kind == "SparkListenerTaskEnd":
            sid = e.get("Stage ID")
            if sid in stage_ids and "Task Metrics" in e:
                m = e["Task Metrics"]
                read = m["Shuffle Read Metrics"]["Remote Bytes Read"] + m["Shuffle Read Metrics"]["Local Bytes Read"]
                tasks[sid].append((m["Input Metrics"]["Bytes Read"], read))
        elif kind == "org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate":
            plans[e["executionId"]] = e.get("physicalPlanDescription", "")
        elif kind == "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart":
            plans.setdefault(e["executionId"], e.get("physicalPlanDescription", ""))
    for sid in sorted(stage_ids):
        info = submitted.get(sid)
        if info is None:
            print(f"stage {sid}: never submitted")
            continue
        print(f"stage {sid}: {info['Stage Name']}  tasks {info['Number of Tasks']}  parents {info['Parent IDs']}")
        print(f"  call site: {info.get('Details', '').splitlines()[0] if info.get('Details') else ''}")
        for rdd in info["RDD Info"][:12]:
            print(f"  rdd {rdd['RDD ID']:>6} {rdd['Name']:<28} parts {rdd['Number of Partitions']:>7} scope {json.loads(rdd.get('Scope', '{}') or '{}').get('name', '')}")
        rows = tasks[sid]
        if rows:
            with_input = sum(1 for i, _ in rows if i > 0)
            with_read = sum(1 for _, r in rows if r > 0)
            reads = sorted(r for _, r in rows)
            print(
                f"  tasks seen {len(rows)}: {with_input} read input, {with_read} read shuffle; "
                f"shuffle read per task min/median/p99/max = "
                f"{reads[0] / MIB:.0f}/{statistics.median(reads) / MIB:.0f}/{reads[int(len(reads) * 0.99)] / MIB:.0f}/{reads[-1] / MIB:.0f} MiB"
            )
    for exec_id, plan in sorted(plans.items()):
        hits = [line.strip() for line in plan.splitlines() if AQE_PATTERN.search(line)]
        if hits:
            print(f"sql exec {exec_id}: {len(hits)} AQE shuffle-read lines")
            for line in hits[:40]:
                print(f"    {line[:200]}")


if __name__ == "__main__":
    main(sys.argv[1], {int(s) for s in sys.argv[2:]})
