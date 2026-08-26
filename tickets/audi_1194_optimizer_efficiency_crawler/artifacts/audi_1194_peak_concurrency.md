# Peak concurrency on site_network_hourly, and what a Spark event log will and will not give up

Measured 2026-08-26 on `app-20260825065124173-0803.zstd` (57-minute run, `spark.executor.cores=4`,
`maxExecutors=500`, `initialExecutors=50`). Every figure below was re-derived by an independent
parser before being recorded here; the corrections that pass forced are called out.

## The job saturates its ceiling at peak and holds it idle the rest of the time

| | |
|---|---|
| distinct executors that ran a task | **497** |
| highest executor id issued | 544 |
| `maxExecutors` | 500 |
| slots held at peak (497 x 4) | **1,988** |
| **peak concurrent tasks** | **1,988** |
| mean concurrent tasks | **43.9** |
| mean slot utilisation | **2.2%** |
| executor-hours held | 356.6 |
| task work performed | 41.6 executor-hours |

At its peak instant every slot the job holds is busy, so the 500 ceiling binds and lowering it
would lengthen the peak. The waste is not the ceiling, it is that mean occupancy is 2.2% of it:
the fleet is acquired for a burst and then held. That is the same finding
`idle_reserved_executors` reports at 346 idle executor-hours, and it is the lever.

## Measuring the peak needs a slot-handoff tolerance

Taking a task's occupancy as `Launch Time` to `Finish Time` from `SparkListenerTaskEnd` puts
**every one of the 497 executors above its 4-slot ceiling at some instant**. Per-executor peaks
are 5 on 137 executors, 6 on 309, 7 on 43, and 8 on 8; the mode is 6 and the max is 8.

This is a sub-100ms handoff artifact, not an inflated interval. At a peak-8 instant, four tasks
finish and four replacements launch inside a 1-3ms window. The interval itself is sound: summed
`Launch`-to-`Finish` is 1.7% above summed Executor Run + Deserialize + Result Serialization, a
median per-task ratio of 1.009. Time-weighted, only **0.12%** of busy executor-time sits above
concurrency 4, mean busy concurrency is 3.41, and no executor exceeds 3.93.

**Shrinking each interval's end by 100ms collapses all 497 executors to a peak of exactly 4**, and
the fleet peak to exactly 1,988 = 497 x 4. So the raw instantaneous maximum is unusable and the
same intervals with a small end-tolerance recover the ceiling exactly. An earlier draft of this
document said peak concurrency could not be measured from the event log. That was wrong; it can,
with the tolerance.

## `ExecutorAdded` is not a census, and the cost figure was 29% short

The log carries `SparkListenerExecutorAdded` for **359** executors while **497** ran tasks. A
running Added-minus-Removed counter bottoms out at **-46**, because 96 `ExecutorRemoved` events
cover only 48 distinct ids: every removed id is logged **twice**. The ids with no `Added` event
sit in 333-544, clustered high, and the file's first event is `SparkListenerLogStart`, so this is
not a truncated head. Every executor with no `Added` event scored zero executor-hours.

Fixed in `eventlog.py`: an executor first seen in a `TaskEnd` takes its `added_ts` from that
task's launch, a floor rather than a guess. On this log the run's cost moves **276.1 to 356.6
executor-hours (+29%)**, and the two findings rank:

```
[high  ]  346.2h  Executors 3% utilized: ~346 idle executor-hours held
[medium]    0.6h  Stage 9 spends 68% of task time waiting on shuffle fetch
```

The demotion is not caused by the seeding: stage 9's fetch wait is 0.59 executor-hours before the
change too, and fails the gate's 10-hour absolute floor either way. What the seeding changes is
the denominator, and therefore whether the fleet's cost is stated honestly.
