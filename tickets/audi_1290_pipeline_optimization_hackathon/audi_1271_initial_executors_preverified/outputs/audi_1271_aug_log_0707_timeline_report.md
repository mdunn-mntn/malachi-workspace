# Executor utilization: registered vs busy

- App window (UTC): 01:16:44 -> 01:50:59  (34.2 min)
- Executors ever registered: 125; removed during run: 50
- Total executor-hours registered: 29.9
- Executor-hours actually running >=1 task: 1.7 (5.8% utilization)
- Idle-reserved executor-hours: 28.1
- Peak registered executors: 100

## Low-parallelism windows (busy executors <= 2 for >= 5 min)
- 01:16:54 -> 01:49:14 (32 min), registered executors 19-100

## Stages by duration (top 12)
| stage | tasks | start | end | dur_min | name |
|---|---|---|---|---|---|
| 7 | 9280 | 01:49:23 | 01:50:31 | 1.1 | save at NativeMethodAccessorImpl.java:0 |
| 9 | 56 | 01:49:27 | 01:50:28 | 1.0 | $anonfun$withThreadLocalCaptured$1 at FutureTask.java:264 |
| 11 | 667 | 01:50:32 | 01:50:47 | 0.2 | save at NativeMethodAccessorImpl.java:0 |
| 4 | 72 | 01:49:17 | 01:49:27 | 0.2 | save at NativeMethodAccessorImpl.java:0 |
| 14 | 571 | 01:50:48 | 01:50:52 | 0.1 | save at NativeMethodAccessorImpl.java:0 |
| 1 | 1 | 01:49:08 | 01:49:12 | 0.1 | isEmpty at NativeMethodAccessorImpl.java:0 |
| 3 | 1 | 01:49:17 | 01:49:21 | 0.1 | save at NativeMethodAccessorImpl.java:0 |
| 23 | 8 | 01:50:54 | 01:50:58 | 0.1 | save at NativeMethodAccessorImpl.java:0 |
| 0 | 1 | 01:49:03 | 01:49:06 | 0.0 | parquet at NativeMethodAccessorImpl.java:0 |
| 2 | 1 | 01:49:13 | 01:49:15 | 0.0 | parquet at NativeMethodAccessorImpl.java:0 |
| 18 | 500 | 01:50:53 | 01:50:54 | 0.0 | save at NativeMethodAccessorImpl.java:0 |
| 6 | 1 | 01:49:21 | 01:49:22 | 0.0 | $anonfun$withThreadLocalCaptured$1 at FutureTask.java:264 |

## Executor removal reasons
- 50x Command exited with code 0
- 75 executors never removed before app end

## Task activity in the final hour
- Tasks finishing in the last 60 min: 11159
  - stage 7 (9280 tasks, save at NativeMethodAccessorImpl.java:0): 9280
  - stage 11 (667 tasks, save at NativeMethodAccessorImpl.java:0): 667
  - stage 14 (571 tasks, save at NativeMethodAccessorImpl.java:0): 571
  - stage 18 (500 tasks, save at NativeMethodAccessorImpl.java:0): 500
  - stage 4 (72 tasks, save at NativeMethodAccessorImpl.java:0): 72
  - stage 9 (56 tasks, $anonfun$withThreadLocalCaptured$1 at FutureTask.java:264): 56
  - stage 23 (8 tasks, save at NativeMethodAccessorImpl.java:0): 8
  - stage 0 (1 tasks, parquet at NativeMethodAccessorImpl.java:0): 1

- Idle tail (last task finish -> app end): 0.0 min
- After 01:50:54, busy executors never exceed 2 (registered at that moment: 75)
