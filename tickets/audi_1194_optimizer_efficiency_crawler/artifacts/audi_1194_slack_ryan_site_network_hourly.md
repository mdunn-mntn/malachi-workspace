# site_network_hourly — the stage 9 ask is WITHDRAWN, 2026-08-26

**Do not send the message below.** Measuring every run in the archive overturned it. Stage 9's
fetch wait is a median **0.28%** of the run's executor-hours; the "57-90% of task time" the
detector reports is a true ratio on a tiny denominator, because stage 9 does almost no compute.
Raising `initialExecutors` would have targeted 0.3% of the job's cost, and cost more to do it.

**What the cost actually is.** Across the 30 heaviest runs: the job holds a median **241
executor-hours** to perform a median **27.5 hours of task work** — a 9x over-allocation, **2.5%
slot utilization** (max 40.7%). Fleet-wide over 302 runs, `idle_reserved_executors` fires on 236
and accounts for **18,334 of the job's 21,200 executor-hours (86%)**. That is the target.

**What is still unproven.** The right `maxExecutors` needs a trustworthy peak-concurrency figure,
and the event log makes that harder than it looks: task-start events whose end never lands (killed
at stage end, speculative) inflate a naive running count 50x, and executor-removed events for
executors added before the log window drive the executor count negative. Mean concurrency is
solid (task-hours / wall span ≈ 34 tasks against 2,160 slots held); peak is not, and peak is what
sizes the ceiling. Settle that before any PR changes the allocation.

**Also corrected:** the earlier claim that stage 15 "reads the same map output and waits ~0%" was
read as cold-vs-warm. It is neither. Stages 29/35 fetch **26x more blocks and 25x more bytes than
stage 9 with 0% wait** — the difference is that they do real work, so fetch time is a small share
of a large denominator. Map-side output spread is not the mechanism.

Evidence: `audi_1194_stage_read_parallelism.py` in this folder. Raw numbers in `summary.md`.

---

# Slack DM draft, Ryan Kleck, re: site_network_hourly Stage 9

Owner routing: `JobTeamConfig.TPA_EXPORT` -> `Team.TARGETING`, `#alerts-tpa-pipeline`.
Mechanism verified 2026-08-20 against 4 event logs; prevalence and cost verified 2026-08-26
against every run in the archive (302 runs, 2026-08-04..08-26).
Everything below the marker is the message; the evidence section is for follow-up questions.

## Message

Hey Ryan, can you set `initialExecutors` to ~300 on `site_network_hourly` for one hour so I can profile that run?

Stage 9 waits a median 56% of task time on shuffle fetch, on 252 of the last 302 runs. Not compute: CPU is 2.6%. Its map stage starts with 50 executors and lands 90% of output on 48-105. Later shuffles run with 400+ up and wait ~0%.

Those 302 runs held 21,200 executor-hours, the fleet's highest.

## Evidence

Measured with `airflow_optimizer` (AUDI-1194) on four event logs from
`gs://mntn-data-archive-prod/spark-events`.

### The stall, every run

| log | stage 9 tasks | fetch wait | blocks | bytes/block |
|---|---|---|---|---|
| app-20260817065122856-0420 | 366 | **73%** (17,379s of 23,716s) | 4,222,144 | 1,753 |
| app-20260817085115734-0691 | 128 | **64%** | 1,356,749 | 1,544 |
| app-20260817125114709-0168 | 622 | **44%** | 5,117,397 | 5,950 |
| app-20260820185132316-0176 | 74 | **58%** | 709,722 | 1,333 |

The crawl backlogs show the same stage at 44-73% on 8 of 8 runs sampled on 2026-08-17, and
on every sweep since 2026-08-07. Zero fetch failures, zero spill, CPU 608s of 23,716s run time.

### Prevalence over every run in the archive

Every event log the archive holds was parsed on 2026-08-26 (2,954 logs, 2026-08-04..08-26).
`site_network_hourly` ran 302 times in that window.

| | |
|---|---|
| runs | 302 |
| executor-hours held | **21,200** (the highest of any job in the fleet) |
| per-run executor-hours | min 0.2, median 51.3, max 371.2 |
| runs raising a fetch-wait finding | **254 of 302 (84%)** |
| of those, on stage 9 | **252** (the rest: stage 29 x2, 35 x1, 15 x1) |
| fetch wait | min 30%, **median 56%**, max 90% |
| runs also holding idle executors | 236 |

The four-log sample below is representative, not cherry-picked: it sits inside this distribution.

### What it is not

Block size and block count are not the cause. In the same app, stages 29 and 35 fetch
**23.4M blocks at 1,607 B** (5.5x more blocks, same size) with **1s** of fetch wait.

So raising `spark.sql.shuffle.partitions` is the wrong lever here and would make it worse by
multiplying block count. (The optimizer's stock advice for this detector said to raise it;
that text has been corrected.)

### What discriminates the stalling read from the clean one

Shuffle blocks are served by the executor that wrote them, so the reduce stage is
rate-limited by how many map-side executors hold the output:

| feeding map stage | executors live at its start | output spread over | 90% sits on | hottest holds | its reducer's fetch wait |
|---|---|---|---|---|---|
| stage 5 (feeds stage 9) | **50** | 159 | 77 | **24.6%** | 73% |
| stage 5, run 2 | **50** | 206 | 105 | **19.9%** | 64% |
| stage 6, run 3 | **50** | 146 | 48 | 2.2% | 44% |
| stage 5, run 4 | **50** | 191 | 85 | **18.1%** | 58% |
| stage 26 (feeds stage 29) | 306-398 | 481-500 | 422-436 | **0.3%** | 0% |

In all four logs the first big map stage starts with **exactly 50 executors** live
(`initialExecutors=50`), runs for 48-257s, and concentrates its output. By the time the
later map stages run, 306-500 executors are up and the output spreads across ~480 of them.
Their reducers do not stall.

### The one thing that does not fit

Stage 15 reads the **same** map output as stage 9, with comparable block count and size, and
waits ~0%. The likely reason is that stage 9 is the first reader (cold, off the map-side
executors' local disks) and stage 15 the second (warm), but the event log cannot settle that.
This is why the ask is a one-hour experiment rather than a config prescription.

### Cost context

17 SUCCEEDED runs on 2026-08-20 (04:50-19:50): **8,663 DCU-h total, mean 510/run, range
164-1,547** (`runtimeInfo.approximateUsage.milliDcuSeconds`, metered not estimated).
Stage 9 held 306 executors for 93s of an 823s app in the profiled run, at 16% utilization
(~31 idle executor-hours).

The DCU actually attributable to the stall is **not established** -- that is what the
experiment measures. Any dollar figure is also unproven while a committed-use discount is in
play: cutting DCU-seconds under a CUD floor saves nothing.

### Config as observed (event log, not source)

`spark.sql.shuffle.partitions=5000` (SparkSession builder, wins over the decorator),
`executor.cores=4`, `executor.memory=9600m`, `minExecutors=2`, `initialExecutors=50`,
`maxExecutors=500`, `executorAllocationRatio=0.3`, AQE on, `speculation` unset,
`shuffleTracking.enabled=true`, runtime 2.3.39.

### Reproduce

```
gsutil -o "GSUtil:check_hashes=never" cp gs://mntn-data-archive-prod/spark-events/<app>.zstd .
python3 -m airflow_optimizer.optimize <app>.zstd
PYTHONPATH=. python3 tickets/audi_1194_optimizer_efficiency_crawler/artifacts/audi_1194_shuffle_concentration.py <app>.zstd
```
