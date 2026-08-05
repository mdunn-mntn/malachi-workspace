"""Airflow/Spark job optimization crawler (AUDI-1194).

Key-free, deterministic efficiency sweep over Airflow DAGs that SUCCEED. Split
from airflow_debugger (AUDI-1191, the failure debugger); the two share only the
Spark event-log parser. Reads a finished job's Spark event log (Dataproc) or
EXPLAIN COST plan + metrics (Databricks), runs the optimization detectors, and
ranks a cross-job backlog worst-first.

Modules:
- eventlog      : full 7-surface Spark event-log parser (jobs/stages/tasks/executors/environment/SQL/storage; handles .zstd)
- optimizations : optimization detectors over the plan text (analyze_plan) and the event log (analyze_run)
- optimize      : one-call single-job report (parse 7 surfaces + every detector -> BLUF grouped CODE/INFRA/FAILURE)
- crawl         : fleet crawl -> optimize every job, rank a cross-job backlog worst-first
"""

__all__ = [
    "crawl",
    "eventlog",
    "optimizations",
    "optimize",
]
