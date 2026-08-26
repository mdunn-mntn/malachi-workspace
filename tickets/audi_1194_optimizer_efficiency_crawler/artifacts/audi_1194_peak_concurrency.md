# Peak concurrency on site_network_hourly: what the event log can and cannot say

Measured 2026-08-26 on `app-20260825065124173-0803.zstd` (one 57-minute run,
`spark.executor.cores=4`, `maxExecutors=500`, `initialExecutors=50`).

## Settled: the job pins its ceiling and then uses 2-3% of it

| | |
|---|---|
| distinct executors that ran a task | **497** |
| highest executor id issued | 544 |
| `maxExecutors` | 500 |
| mean concurrent tasks | 45 |
| slots at 497 x 4 cores | 1,988 |
| mean slot utilisation | **2.3%** |
| executor-hours held | **356.6** |
| task work performed | 41.6 executor-hours |

`maxExecutors=500` binds. The job scales to the ceiling and then leaves 97% of the slots idle,
which is the same 86%-of-cost picture `idle_reserved_executors` reports. Lowering the ceiling is
not the first lever; the tail that holds the fleet is.

## Settled: peak TASK concurrency is not measurable from this log

Task intervals taken as `Launch Time` to `Finish Time` put up to **8 concurrent tasks on every
one of the 497 four-core executors**, without exception. A 4-core executor cannot run 8 tasks, so
the interval overstates slot occupancy by roughly 2x. The likely cause is that `Finish Time` is
when the driver recorded completion, which trails the moment the slot was released. Any peak
built on those intervals is therefore an upper bound, not a measurement, and the earlier
"peak 2,130 tasks against 1,052 slots" figure is an artifact of exactly this.

## Found while measuring: the cost figure was understated 29%

`ExecutorAdded` is not a census in this log. It appears for **359** executors while **497** ran
tasks, and a running Added-minus-Removed count goes as low as **-46**. The missing ids are
clustered high (333-544), so this is not a truncated file head. Every executor with no `Added`
event was scoring zero executor-hours.

Fixed in `eventlog.py`: an executor first seen in a `TaskEnd` gets its `added_ts` seeded from
that task's launch time, a floor rather than a guess. On this log the run's cost moves
**276.1 to 356.6 executor-hours (+29%)**, and the two findings now rank the right way round:

```
[high  ]  346.2h  Executors 3% utilized: ~346 idle executor-hours held
[medium]    0.6h  Stage 9 spends 68% of task time waiting on shuffle fetch
```

That is the IMP-084 gate doing its job on real data: the 68% ratio is true and worth 0.6 of the
run's 356.6 executor-hours.
