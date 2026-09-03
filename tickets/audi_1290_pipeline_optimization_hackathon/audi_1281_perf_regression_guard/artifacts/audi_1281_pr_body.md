Adds a guard that fails when a job's disk spill or shuffle fetch-wait doubles against its own 30-day median, plus the per-stage metrics file it reads.

What
- stage_metrics.py: one row per stage per run, restored and republished like the ledger.
- regression_guard.py: CLI, exit 1 on a 2x regression, 2 with no baseline. Stages match by operation and task count, then id: Spark renumbers concurrent stages.
- Repairs the lint glob and stale test that turned this workflow red on main.

Why
The ledger holds spill only when a detector fires and sums executor-hours per day.

Validation
- ruff, 189 tests, compileall pass.
- intent_score_map, 17 runs: 0 regressions; seeded 2x exits 1, 1.5x exits 0.
- site_network_hourly stage 9 fetch-wait 0.83x ok; seeded 2x exits 1.
