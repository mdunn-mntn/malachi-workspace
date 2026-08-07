# Executor utilization: registered vs busy

- App window (UTC): 05:34:00 -> 07:19:42  (105.7 min)
- Executors ever registered: 240; removed during run: 0
- Total executor-hours registered: 396.2
- Executor-hours actually running >=1 task: 128.6 (32.5% utilization)
- Idle-reserved executor-hours: 267.5
- Peak registered executors: 240

## Low-parallelism windows (busy executors <= 2 for >= 5 min)
- 06:17:40 -> 07:19:40 (62 min), registered executors 240-240

## Stages by duration (top 12)
| stage | tasks | start | end | dur_min | name |
|---|---|---|---|---|---|
| 6 | 4915 | 05:58:33 | 07:18:41 | 80.1 | $anonfun$withThreadLocalCaptured$2 at CompletableFuture.java:1768 |
| 3 | 40000 | 05:35:26 | 05:58:31 | 23.1 | $anonfun$withThreadLocalCaptured$2 at CompletableFuture.java:1768 |
| 2 | 14000 | 05:35:26 | 05:47:14 | 11.8 | $anonfun$withThreadLocalCaptured$2 at CompletableFuture.java:1768 |
| 0 | 1 | 05:35:15 | 05:35:19 | 0.1 | load at NativeMethodAccessorImpl.java:0 |
| 1 | 1 | 05:35:21 | 05:35:24 | 0.0 | load at NativeMethodAccessorImpl.java:0 |

## Executor removal reasons
- 240 executors never removed before app end

## Task activity in the final hour
- Tasks finishing in the last 60 min: 1
  - stage 6 (4915 tasks, $anonfun$withThreadLocalCaptured$2 at CompletableFuture.java:1768): 1

- Idle tail (last task finish -> app end): 1.0 min
- After 06:17:40, busy executors never exceed 2 (registered at that moment: 240)
