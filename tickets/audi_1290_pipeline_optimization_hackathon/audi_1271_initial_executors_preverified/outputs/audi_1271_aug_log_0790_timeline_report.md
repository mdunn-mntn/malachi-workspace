# Executor utilization: registered vs busy

- App window (UTC): 20:16:34 -> 21:13:48  (57.2 min)
- Executors ever registered: 122; removed during run: 50
- Total executor-hours registered: 55.3
- Executor-hours actually running >=1 task: 1.8 (3.3% utilization)
- Idle-reserved executor-hours: 53.4
- Peak registered executors: 100

## Low-parallelism windows (busy executors <= 2 for >= 5 min)
- 20:16:44 -> 20:54:44 (38 min), registered executors 22-100
- 20:56:44 -> 21:13:44 (17 min), registered executors 72-72

## Stages by duration (top 12)
| stage | tasks | start | end | dur_min | name |
|---|---|---|---|---|---|
| 7 | 10862 | 20:55:00 | 20:56:18 | 1.3 | save at NativeMethodAccessorImpl.java:0 |
| 9 | 56 | 20:55:05 | 20:56:14 | 1.2 | $anonfun$withThreadLocalCaptured$1 at FutureTask.java:264 |
| 11 | 665 | 20:56:19 | 20:56:33 | 0.2 | save at NativeMethodAccessorImpl.java:0 |
| 4 | 72 | 20:54:53 | 20:55:05 | 0.2 | save at NativeMethodAccessorImpl.java:0 |
| 3 | 1 | 20:54:53 | 20:54:58 | 0.1 | save at NativeMethodAccessorImpl.java:0 |
| 1 | 1 | 20:54:44 | 20:54:48 | 0.1 | isEmpty at NativeMethodAccessorImpl.java:0 |
| 14 | 571 | 20:56:34 | 20:56:37 | 0.1 | save at NativeMethodAccessorImpl.java:0 |
| 23 | 8 | 20:56:39 | 20:56:42 | 0.1 | save at NativeMethodAccessorImpl.java:0 |
| 0 | 1 | 20:54:40 | 20:54:42 | 0.0 | parquet at NativeMethodAccessorImpl.java:0 |
| 2 | 1 | 20:54:49 | 20:54:52 | 0.0 | parquet at NativeMethodAccessorImpl.java:0 |
| 18 | 500 | 20:56:37 | 20:56:38 | 0.0 | save at NativeMethodAccessorImpl.java:0 |
| 6 | 1 | 20:54:58 | 20:54:59 | 0.0 | $anonfun$withThreadLocalCaptured$1 at FutureTask.java:264 |

## Executor removal reasons
- 50x Command exited with code 0
- 72 executors never removed before app end

## Task activity in the final hour
- Tasks finishing in the last 60 min: 12739
  - stage 7 (10862 tasks, save at NativeMethodAccessorImpl.java:0): 10862
  - stage 11 (665 tasks, save at NativeMethodAccessorImpl.java:0): 665
  - stage 14 (571 tasks, save at NativeMethodAccessorImpl.java:0): 571
  - stage 18 (500 tasks, save at NativeMethodAccessorImpl.java:0): 500
  - stage 4 (72 tasks, save at NativeMethodAccessorImpl.java:0): 72
  - stage 9 (56 tasks, $anonfun$withThreadLocalCaptured$1 at FutureTask.java:264): 56
  - stage 23 (8 tasks, save at NativeMethodAccessorImpl.java:0): 8
  - stage 0 (1 tasks, parquet at NativeMethodAccessorImpl.java:0): 1

- Idle tail (last task finish -> app end): 17.1 min
- After 20:56:44, busy executors never exceed 2 (registered at that moment: 72)
