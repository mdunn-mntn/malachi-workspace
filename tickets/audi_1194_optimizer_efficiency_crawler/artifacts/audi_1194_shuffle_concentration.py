"""Why a stage stalls on shuffle fetch: map-side output concentration.

Shuffle blocks are served by the executor that wrote them, so a reduce stage is
rate-limited by the hottest map-side executor, not by block size or block count.
This prints, per stage, how many executors hold the shuffle output and what share
the largest one holds - the discriminator between a stalling fetch and a clean one.

    python3 artifacts/audi_1194_shuffle_concentration.py <eventlog> [<eventlog> ...]
"""

from __future__ import annotations

import collections
import sys

from airflow_optimizer.eventlog import _read_events


def profile(path: str) -> None:
    """Print the fetch-wait stages and the concentration of the map output feeding them."""
    span: dict[int, list[int | None]] = collections.defaultdict(lambda: [None, None])
    fetch_wait: collections.Counter = collections.Counter()
    run_time: collections.Counter = collections.Counter()
    blocks: collections.Counter = collections.Counter()
    read_bytes: collections.Counter = collections.Counter()
    write_by_exec: dict[int, collections.Counter] = collections.defaultdict(collections.Counter)
    tasks: collections.Counter = collections.Counter()
    adds: list[int] = []
    t0 = None

    for e in _read_events(path):
        ev = e.get("Event", "")
        if ev == "SparkListenerApplicationStart":
            t0 = e["Timestamp"]
        elif ev == "SparkListenerExecutorAdded":
            adds.append(e["Timestamp"])
        elif ev == "SparkListenerTaskEnd":
            sid, info = e["Stage ID"], e["Task Info"]
            metrics = e.get("Task Metrics") or {}
            srm = metrics.get("Shuffle Read Metrics") or {}
            swm = metrics.get("Shuffle Write Metrics") or {}
            s = span[sid]
            s[0] = info["Launch Time"] if s[0] is None else min(s[0], info["Launch Time"])
            s[1] = info["Finish Time"] if s[1] is None else max(s[1], info["Finish Time"])
            tasks[sid] += 1
            fetch_wait[sid] += srm.get("Fetch Wait Time", 0)
            run_time[sid] += metrics.get("Executor Run Time", 0)
            blocks[sid] += srm.get("Remote Blocks Fetched", 0) + srm.get("Local Blocks Fetched", 0)
            read_bytes[sid] += srm.get("Remote Bytes Read", 0) + srm.get("Local Bytes Read", 0)
            write_by_exec[sid][info["Executor ID"]] += swm.get("Shuffle Bytes Written", 0)

    adds.sort()
    print(f"\n{path.rsplit('/', 1)[-1]}  ({len(adds)} executors registered)")
    for sid in sorted(tasks):
        if run_time[sid] < 300_000 or not blocks[sid]:
            continue
        ratio = fetch_wait[sid] / run_time[sid]
        start = span[sid][0]
        live = sum(1 for a in adds if a <= start) if start else 0
        print(f"  stage {sid}: {tasks[sid]} tasks, {100 * ratio:.0f}% fetch wait, "
              f"{blocks[sid]:,} blocks @ {read_bytes[sid] / blocks[sid]:.0f} B, "
              f"{live} executors live at start")
        _map_side(sid, span, write_by_exec, tasks, adds, t0)


def _map_side(sid: int, span: dict, write_by_exec: dict, tasks: collections.Counter,
              adds: list[int], t0: int | None) -> None:
    """Report the concentration of whichever map stage finished just before `sid` started."""
    start = span[sid][0]
    upstream = [s for s in tasks if span[s][1] and start and span[s][1] <= start and tasks[s] > 1000]
    if not upstream:
        return
    m = max(upstream, key=lambda s: span[s][1])
    by = write_by_exec[m]
    total = sum(by.values())
    if not total:
        return
    top = by.most_common()
    cum = n90 = 0
    for i, (_, v) in enumerate(top, 1):
        cum += v
        if cum >= 0.9 * total:
            n90 = i
            break
    live_at_map_start = sum(1 for a in adds if a <= span[m][0])
    print(f"      fed by stage {m}: {tasks[m]} tasks -> {total / 1024**3:.1f} GiB over "
          f"{len(by)} executors, 90% on {n90}, hottest holds {100 * top[0][1] / total:.1f}%; "
          f"{live_at_map_start} executors live when it started")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        raise SystemExit(2)
    for arg in sys.argv[1:]:
        profile(arg)
