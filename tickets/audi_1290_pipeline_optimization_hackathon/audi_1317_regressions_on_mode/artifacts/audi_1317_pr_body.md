The daily sweep writes each regression to the ledger, so a doubled spill or fetch wait reaches the digest and the cost dashboard the same morning.

**Base is `audi-1281-perf-regression-guard` (#1279), not main.**

**What**
- `regression_guard.reports()` judges every profiled DAG, reusing `evaluate()`. One key per stage and metric: `regression_disk_spill:3`.
- `sweep.py` folds them into the existing `ledger.record` call, leaving replay and resolution untouched.
- `digest.py` gains a regression section and a Slack count.

**Why**
The dashboard parses cost out of ledger titles, so the title ends with the run's executor-hours. The report carries `exec_h=0.0`, the field savings math uses.

**Validation**
- 4 real sweeps: 3 keys written, digest line carried, all resolved on schedule.
- 278 gated judgements over 100 run-days: 0 regressions.
- ruff 0.16.1 clean, 197 tests pass.
