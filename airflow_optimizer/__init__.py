"""Airflow/Spark job optimization crawler (AUDI-1194).

Key-free, deterministic efficiency sweep over Airflow DAGs that SUCCEED. Split
from airflow_debugger (AUDI-1191, the failure debugger); the two share only the
Spark event-log parser. Reads a finished job's Spark event log, runs the
optimization detectors, and ranks a cross-job backlog worst-first.

The live input is Dataproc event logs, from two places: the flat spark-events
archive (the 88 batch-operator models) and the per-uuid PHS temp-bucket dirs
(ipdsc/tpa). analyze_plan also reads a Databricks EXPLAIN COST plan, but nothing
here acquires one - the 2026-08-03 probe showed jobs get-run-output carries no
plan text until a model emits EXPLAIN COST itself.

Modules:
- eventlog      : full 7-surface Spark event-log parser (jobs/stages/tasks/executors/environment/SQL/storage; handles .zstd)
- optimizations : optimization detectors over the plan text (analyze_plan) and the event log (analyze_run)
- optimize      : one-call single-job report (parse 7 surfaces + every detector -> BLUF grouped CODE/INFRA/FAILURE)
- crawl         : fleet crawl -> optimize every job, rank a cross-job backlog worst-first
- phs           : enumerate PHS-attached ipdsc/tpa batches and fetch their per-uuid event logs
"""

__all__ = [
    "crawl",
    "eventlog",
    "optimizations",
    "optimize",
    "phs",
]
