# airflow_optimizer

Key-free, deterministic efficiency sweep over Airflow/Spark jobs that **succeed** (AUDI-1194). Reads a
finished job's Spark event log (Dataproc) or `EXPLAIN COST` plan + metrics (Databricks), runs the
optimization detectors, and ranks a cross-job backlog worst-first. The success-only counterpart to
`airflow_debugger/` (AUDI-1191, the failure debugger); the two run separately and share only the Spark
event-log parser.

## Use

```bash
# Single job: parse all 7 surfaces + every detector -> BLUF report grouped CODE / INFRA / FAILURE
python3 -m airflow_optimizer.optimize <spark_eventlog>

# Fleet crawl: optimize every job in a dir/glob of event logs, rank a cross-job backlog worst-first
python3 -m airflow_optimizer.crawl <event_log_dir_or_glob>
```

The weekly sweep (`.claude/scripts/oncall_weekly_optimizer.sh`) pulls the newest event logs from the GCS
prefix and runs `airflow_optimizer.crawl`.

## Pipeline

```
event log (.zstd) ─▶ eventlog (7-surface parse) ─▶ optimizations (detectors) ─▶ optimize (single-job BLUF)
                     jobs/stages/tasks/executors/                                        │
                     environment/SQL/storage                                   crawl (rank fleet backlog)
```

## Modules

- `eventlog` — full 7-surface Spark event-log parser (jobs / stages / tasks / executors / environment /
  SQL per-node metrics / storage; handles `.zstd`). The one artifact holding all 7 surfaces.
- `optimizations` — detectors over the plan text (`analyze_plan`: missing_statistics,
  shuffle_partition_sizing, broadcast_candidate, window_full_sort, repeated_scan) AND the event log
  (`analyze_run`: skew, disk_spill, gc_pressure, spot_preemption_cost, shuffle_fetch_instability),
  emitting `code` / `infra` / `failure` recommendations with real metrics.
- `optimize` — one-call single-job report.
- `crawl` — fleet crawl, ranks a cross-job backlog worst-first (the "check every DAG" mode).

## Notes

- Parser + detectors validated on real Spark event logs (`tests/fixtures/eventlog.zstd`,
  `eventlog_cache.zstd`). The prod crawl (2026-08-04) found a 242x skew on `Update Vertical Categorization`.
- Offline tests: `python3 -m airflow_optimizer.tests.test_{eventlog,optimizations}`.
