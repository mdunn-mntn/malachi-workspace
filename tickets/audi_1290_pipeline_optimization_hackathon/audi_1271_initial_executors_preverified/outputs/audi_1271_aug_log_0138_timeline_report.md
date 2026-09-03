# Executor utilization: registered vs busy

- App window (UTC): 02:16:28 -> 02:22:25  (6.0 min)
- Executors ever registered: 124; removed during run: 50
- Total executor-hours registered: 6.1
- Executor-hours actually running >=1 task: 2.6 (42.2% utilization)
- Idle-reserved executor-hours: 3.5
- Peak registered executors: 100

## Low-parallelism windows (busy executors <= 2 for >= 5 min)

## Stages by duration (top 12)
| stage | tasks | start | end | dur_min | name |
|---|---|---|---|---|---|
| 7 | 9280 | 02:19:06 | 02:20:31 | 1.4 | save at NativeMethodAccessorImpl.java:0 |
| 9 | 56 | 02:19:06 | 02:20:27 | 1.4 | $anonfun$withThreadLocalCaptured$1 at FutureTask.java:264 |
| 33 | 9386 | 02:21:12 | 02:22:06 | 0.9 | save at NativeMethodAccessorImpl.java:0 |
| 11 | 423 | 02:20:33 | 02:20:42 | 0.2 | save at NativeMethodAccessorImpl.java:0 |
| 35 | 667 | 02:22:08 | 02:22:16 | 0.1 | save at NativeMethodAccessorImpl.java:0 |
| 4 | 72 | 02:18:59 | 02:19:06 | 0.1 | save at NativeMethodAccessorImpl.java:0 |
| 3 | 1 | 02:18:59 | 02:19:03 | 0.1 | save at NativeMethodAccessorImpl.java:0 |
| 1 | 1 | 02:18:50 | 02:18:54 | 0.1 | isEmpty at NativeMethodAccessorImpl.java:0 |
| 47 | 8 | 02:22:21 | 02:22:24 | 0.1 | save at NativeMethodAccessorImpl.java:0 |
| 23 | 8 | 02:20:47 | 02:20:50 | 0.1 | save at NativeMethodAccessorImpl.java:0 |
| 14 | 423 | 02:20:42 | 02:20:45 | 0.0 | save at NativeMethodAccessorImpl.java:0 |
| 38 | 571 | 02:22:17 | 02:22:19 | 0.0 | save at NativeMethodAccessorImpl.java:0 |

## Executor removal reasons
- 50x Command exited with code 0
- 74 executors never removed before app end

## Task activity in the final hour
- Tasks finishing in the last 60 min: 21954
  - stage 33 (9386 tasks, save at NativeMethodAccessorImpl.java:0): 9386
  - stage 7 (9280 tasks, save at NativeMethodAccessorImpl.java:0): 9280
  - stage 35 (667 tasks, save at NativeMethodAccessorImpl.java:0): 667
  - stage 38 (571 tasks, save at NativeMethodAccessorImpl.java:0): 571
  - stage 42 (500 tasks, save at NativeMethodAccessorImpl.java:0): 500
  - stage 11 (423 tasks, save at NativeMethodAccessorImpl.java:0): 423
  - stage 14 (423 tasks, save at NativeMethodAccessorImpl.java:0): 423
  - stage 18 (422 tasks, save at NativeMethodAccessorImpl.java:0): 422

- Idle tail (last task finish -> app end): 0.0 min
