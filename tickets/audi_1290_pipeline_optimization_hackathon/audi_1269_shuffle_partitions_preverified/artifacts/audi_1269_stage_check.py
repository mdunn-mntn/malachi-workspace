"""Print what AUDI-1269 needs from one Spark event log before shuffle.partitions is raised.

    PYTHONPATH=/Users/malachi/Developer/work/mntn/workspace \
    python3 artifacts/audi_1269_stage_check.py <eventlog> <target_partitions>

Per log: the configured partition count and AQE knobs, every stage that shuffles more
than 10 GiB or spills more than 1 GiB, whether each reducer runs exactly the configured
count (the knob is live) and the projected per-task and per-block sizes at the target.
"""

from __future__ import annotations

import dataclasses
import sys

from airflow_optimizer.eventlog import parse_eventlog

GIB = 1024**3
MIB = 1024**2
PROPS = (
    "spark.sql.shuffle.partitions",
    "spark.sql.adaptive.enabled",
    "spark.sql.adaptive.coalescePartitions.enabled",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes",
    "spark.sql.adaptive.coalescePartitions.initialPartitionNum",
    "spark.driver.memory",
    "spark.executor.memory",
    "spark.executor.cores",
)


def main(path: str, target: int) -> None:
    run = parse_eventlog(path)
    props = run.spark_props
    configured = int(props.get("spark.sql.shuffle.partitions") or 200)
    print(f"{run.app_name}  ({path.rsplit('/', 1)[-1]})")
    for key in PROPS:
        print(f"  {key} = {props.get(key)}")
    print(f"  target = {target} ({target / configured:.1f}x)")
    maps = {}
    for s in sorted(run.stages, key=lambda s: s.stage_id):
        d = dataclasses.asdict(s)
        wide = d["shuffle_write_bytes"] > 10 * GIB or d["shuffle_read_bytes"] > 10 * GIB
        if not wide and d["disk_spill"] < GIB:
            continue
        print(
            f"  stage {s.stage_id:>3} tasks {s.num_tasks:>6} "
            f"in {d['input_bytes'] / GIB:8.1f} GiB  shuffle write {d['shuffle_write_bytes'] / GIB:8.1f} "
            f"read {d['shuffle_read_bytes'] / GIB:8.1f}  spill disk {d['disk_spill'] / GIB:7.1f} "
            f"mem {d['mem_spill'] / GIB:8.0f}  fetch wait {d['fetch_wait_ms'] / max(1, d['run_time_ms']):.0%}"
        )
        if d["shuffle_write_bytes"] > 10 * GIB:
            maps[s.stage_id] = (s.num_tasks, d["shuffle_write_bytes"])
        if d["shuffle_read_bytes"] > 10 * GIB:
            live = "LIVE" if s.num_tasks == configured else f"COALESCED (config {configured})"
            feeder = max(maps.values(), key=lambda m: m[1]) if maps else (0, d["shuffle_read_bytes"])
            per_task = d["shuffle_read_bytes"] / target / MIB
            block = d["shuffle_read_bytes"] / (feeder[0] * target) / 1024 if feeder[0] else 0
            expansion = (d["mem_spill"] + d["shuffle_read_bytes"]) / d["shuffle_read_bytes"]
            print(
                f"        reducer {live}; at target: {per_task:.0f} MiB compressed/task, "
                f"~{per_task * expansion:.0f} MiB in memory, blocks ~{block:.0f} KiB "
                f"({feeder[0]} map tasks x {target})"
            )


if __name__ == "__main__":
    main(sys.argv[1], int(sys.argv[2]))
