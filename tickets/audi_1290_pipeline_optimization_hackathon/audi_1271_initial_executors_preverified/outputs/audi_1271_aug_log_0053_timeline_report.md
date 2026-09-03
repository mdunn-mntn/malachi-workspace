# Executor utilization: registered vs busy

- App window (UTC): 12:16:21 -> 12:35:13  (18.9 min)
- Executors ever registered: 114; removed during run: 49
- Total executor-hours registered: 17.3
- Executor-hours actually running >=1 task: 2.4 (13.6% utilization)
- Idle-reserved executor-hours: 14.9
- Peak registered executors: 99

## Low-parallelism windows (busy executors <= 2 for >= 5 min)
- 12:16:31 -> 12:31:41 (15 min), registered executors 50-99

## Stages by duration (top 12)
| stage | tasks | start | end | dur_min | name |
|---|---|---|---|---|---|
| 7 | 9216 | 12:31:46 | 12:32:51 | 1.1 | save at NativeMethodAccessorImpl.java:0 |
| 9 | 56 | 12:31:47 | 12:32:46 | 1.0 | $anonfun$withThreadLocalCaptured$1 at FutureTask.java:264 |
| 33 | 9504 | 12:34:09 | 12:35:01 | 0.9 | save at NativeMethodAccessorImpl.java:0 |
| 11 | 571 | 12:32:52 | 12:33:00 | 0.1 | save at NativeMethodAccessorImpl.java:0 |
| 4 | 72 | 12:31:41 | 12:31:47 | 0.1 | save at NativeMethodAccessorImpl.java:0 |
| 35 | 572 | 12:35:02 | 12:35:07 | 0.1 | save at NativeMethodAccessorImpl.java:0 |
| 3 | 1 | 12:31:41 | 12:31:45 | 0.1 | save at NativeMethodAccessorImpl.java:0 |
| 1 | 1 | 12:31:33 | 12:31:36 | 0.1 | isEmpty at NativeMethodAccessorImpl.java:0 |
| 0 | 1 | 12:31:28 | 12:31:31 | 0.0 | parquet at NativeMethodAccessorImpl.java:0 |
| 14 | 246 | 12:33:01 | 12:33:04 | 0.0 | save at NativeMethodAccessorImpl.java:0 |
| 47 | 8 | 12:35:10 | 12:35:13 | 0.0 | save at NativeMethodAccessorImpl.java:0 |
| 2 | 1 | 12:31:37 | 12:31:39 | 0.0 | parquet at NativeMethodAccessorImpl.java:0 |

## Executor removal reasons
- 49x Command exited with code 0
- 65 executors never removed before app end

## Task activity in the final hour
- Tasks finishing in the last 60 min: 21171
  - stage 33 (9504 tasks, save at NativeMethodAccessorImpl.java:0): 9504
  - stage 7 (9216 tasks, save at NativeMethodAccessorImpl.java:0): 9216
  - stage 35 (572 tasks, save at NativeMethodAccessorImpl.java:0): 572
  - stage 11 (571 tasks, save at NativeMethodAccessorImpl.java:0): 571
  - stage 38 (333 tasks, save at NativeMethodAccessorImpl.java:0): 333
  - stage 42 (266 tasks, save at NativeMethodAccessorImpl.java:0): 266
  - stage 14 (246 tasks, save at NativeMethodAccessorImpl.java:0): 246
  - stage 18 (181 tasks, save at NativeMethodAccessorImpl.java:0): 181

- Idle tail (last task finish -> app end): 0.0 min
