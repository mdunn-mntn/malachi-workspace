# airflow_optimizer

Key-free, deterministic efficiency sweep over Airflow/Spark jobs that **succeed** (AUDI-1194). Reads a
finished job's Spark event log, runs the optimization detectors, and ranks a cross-job backlog
worst-first. The success-only counterpart to `airflow_debugger/` (AUDI-1191, the failure debugger);
the two run separately and share only the Spark event-log parser.

The live input is **Dataproc event logs**, from two places: the flat `spark-events` archive (the 88
batch-operator models) and the per-uuid PHS temp-bucket dirs (ipdsc/tpa, via `phs`). `analyze_plan`
also reads a Databricks `EXPLAIN COST` plan. Acquiring one is **not in this package yet**, and the
specced route does not work: `jobs get-run-output` returns an empty `notebook_output` even on a
SUCCEEDED prod run, and job clusters set no `cluster_log_conf`. What does work (validated
2026-08-20) is running `EXPLAIN COST` against a SQL warehouse through the Statement Execution API,
which needs no dbt or cluster change (see the AUDI-1194 ticket artifacts).

## Use

```bash
# Single job: parse all 7 surfaces + every detector -> BLUF report grouped CODE / INFRA / FAILURE
python3 -m airflow_optimizer.optimize <spark_eventlog>

# Fleet crawl: optimize every job in a dir/glob of event logs, rank a cross-job backlog worst-first
python3 -m airflow_optimizer.crawl <event_log_dir_or_glob>
```

The daily sweep (the `spark_optimizer_daily` DAG in production, `oncall_daily_optimizer.sh` on a
laptop) pulls a full day of event logs from the
GCS prefix plus the PHS batches and runs `airflow_optimizer.crawl` over both.

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
  (`analyze_run`: skew, straggler, disk_spill, shuffle_partition_sizing, shuffle_fetch_wait,
  gc_pressure, spot_preemption_cost, idle_reserved_executors, cache_ineffective,
  shuffle_fetch_instability),
  emitting `code` / `infra` / `failure` recommendations with real metrics.
- `optimize` — one-call single-job report.
- `crawl` — fleet crawl, ranks a cross-job backlog worst-first (the "check every DAG" mode).
- `phs` — enumerates PHS-attached SUCCEEDED ipdsc/tpa batches (`gcloud dataproc batches list`) and
  fetches each one's per-uuid `spark-job-history` log. Needs standing `storage.objectViewer` on the
  Dataproc temp bucket (mntn-devops#4724); until that merges the reads 403 and are skipped.

## Notes

- Parser + detectors validated on real Spark event logs (`tests/fixtures/eventlog.zstd`,
  `eventlog_cache.zstd`). The prod crawl (2026-08-04) found a 242x skew on `Update Vertical Categorization`.
- Offline tests: `python3 -m airflow_optimizer.tests.test_{eventlog,optimizations}`.
