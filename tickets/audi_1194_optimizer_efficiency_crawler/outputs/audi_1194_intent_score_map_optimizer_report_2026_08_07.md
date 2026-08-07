app: Populate intent_score_map.IntentScoreMap | stages: 5 | executors: 240 | duration_min: 105.7

Optimization: 7 findings. Top [high]: Stage 2 spilled 16565.4 GiB

## INFRA / compute
- [high] Stage 6 straggler: slowest task 13.4x the median on uniform data
    why: 4915 tasks, slowest is 13.4x the median wall time but reads only 1.0x the median data - a slow executor/node or IO stall, not data skew. spark.speculation is OFF, so nothing re-ran it.
    fix: Enable spark.speculation=true (with spark.speculation.quantile ~0.9) so a straggling task is re-launched on an idle executor instead of pinning the stage.
- [medium] Executors 32% utilized: ~270 idle executor-hours held
    why: 240 executors held 396 executor-hours but task slots were busy only 32% of that; 0 were released before app end. shuffleTracking has no timeout, so executors holding shuffle blocks are never released.
    fix: Set spark.dynamicAllocation.shuffleTracking.timeout (e.g. 300s) so idle executors are reaped, and fix the tail (straggler/skew) that keeps the stage alive.

## CODE / query-PR
- [high] Stage 2 spilled 16565.4 GiB
    why: Memory+disk spill 16565.4 GiB over 14000 tasks - partitions exceed executor memory.
    fix: Raise spark.sql.shuffle.partitions (smaller partitions) first; if it persists, raise executor memory.
- [high] Stage 2 wide shuffle (3176 GiB)
    why: 3176 GiB shuffle write at shuffle.partitions=4915.
    fix: Set spark.sql.shuffle.partitions ~12705 (~256 MiB each) or enable AQE coalesce.
- [high] Stage 3 spilled 22191.4 GiB
    why: Memory+disk spill 22191.4 GiB over 40000 tasks - partitions exceed executor memory.
    fix: Raise spark.sql.shuffle.partitions (smaller partitions) first; if it persists, raise executor memory.
- [high] Stage 3 wide shuffle (7364 GiB)
    why: 7364 GiB shuffle write at shuffle.partitions=4915.
    fix: Set spark.sql.shuffle.partitions ~29456 (~256 MiB each) or enable AQE coalesce.
- [high] Stage 6 spilled 52186.5 GiB
    why: Memory+disk spill 52186.5 GiB over 4915 tasks - partitions exceed executor memory.
    fix: Raise spark.sql.shuffle.partitions (smaller partitions) first; if it persists, raise executor memory.
