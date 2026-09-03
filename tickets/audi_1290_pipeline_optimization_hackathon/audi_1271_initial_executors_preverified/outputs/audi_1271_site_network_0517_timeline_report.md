# Executor utilization: registered vs busy

- App window (UTC): 00:51:27 -> 01:44:20  (52.9 min)
- Executors ever registered: 431; removed during run: 584
- Total executor-hours registered: 56.2
- Executor-hours actually running >=1 task: 10.3 (18.4% utilization)
- Idle-reserved executor-hours: 45.9
- Peak registered executors: 325

## Low-parallelism windows (busy executors <= 2 for >= 5 min)
- 01:18:37 -> 01:24:07 (6 min), registered executors 70-325

## Stages by duration (top 12)
| stage | tasks | start | end | dur_min | name |
|---|---|---|---|---|---|
| 27 | 11 | 01:40:26 | 01:44:19 | 3.9 | save at NativeMethodAccessorImpl.java:0 |
| 23 | 13817 | 01:36:18 | 01:39:51 | 3.5 | javaToPython at NativeMethodAccessorImpl.java:0 |
| 25 | 13817 | 01:39:53 | 01:40:25 | 0.5 | save at NativeMethodAccessorImpl.java:0 |
| 12 | 9506 | 01:17:50 | 01:18:11 | 0.3 | save at NativeMethodAccessorImpl.java:0 |
| 19 | 30 | 01:18:38 | 01:18:43 | 0.1 | save at NativeMethodAccessorImpl.java:0 |
| 22 | 1 | 01:36:14 | 01:36:18 | 0.1 | $anonfun$withThreadLocalCaptured$1 at FutureTask.java:264 |
| 2 | 1 | 01:09:04 | 01:09:07 | 0.1 | isEmpty at NativeMethodAccessorImpl.java:0 |
| 1 | 1 | 01:08:59 | 01:09:02 | 0.0 | parquet at NativeMethodAccessorImpl.java:0 |
| 0 | 1 | 00:51:39 | 00:51:41 | 0.0 | parquet at NativeMethodAccessorImpl.java:0 |
| 3 | 1 | 01:09:08 | 01:09:10 | 0.0 | isEmpty at NativeMethodAccessorImpl.java:0 |
| 24 | 1 | 01:39:52 | 01:39:53 | 0.0 | $anonfun$withThreadLocalCaptured$1 at FutureTask.java:264 |
| 4 | 1 | 01:09:11 | 01:09:12 | 0.0 | $anonfun$withThreadLocalCaptured$1 at FutureTask.java:264 |

## Executor removal reasons
- 583x Command exited with code 0
- 1x Executor decommission finished: spark scale down (181.1s) - Migration: 369/369 b
- -153 executors never removed before app end

## Task activity in the final hour
- Tasks finishing in the last 60 min: 13716
  - stage 5 (13473 tasks, javaToPython at NativeMethodAccessorImpl.java:0): 7802
  - stage 23 (13817 tasks, javaToPython at NativeMethodAccessorImpl.java:0): 4098
  - stage 25 (13817 tasks, save at NativeMethodAccessorImpl.java:0): 675
  - stage 9 (2250 tasks, javaToPython at NativeMethodAccessorImpl.java:0): 388
  - stage 15 (2250 tasks, save at NativeMethodAccessorImpl.java:0): 292
  - stage 12 (9506 tasks, save at NativeMethodAccessorImpl.java:0): 272
  - stage 11 (? tasks, ?): 84
  - stage 6 (9506 tasks, javaToPython at NativeMethodAccessorImpl.java:0): 64

- Idle tail (last task finish -> app end): 0.0 min
- After 01:43:27, busy executors never exceed 2 (registered at that moment: 57)
