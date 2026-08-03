# AUDI-1191 use-case #2 — Spark query optimization (scope + honest gap)

## BLUF

We are **not** yet capturing the full Spark detail needed to optimize queries — the failure
analyzers pull failure *state + error signature*, not the *query plan / per-node metrics /
optimizer-statistics* the Spark UI shows. The optimization **detector brain is now built**
(`airflow_debugger/optimizations.py`, validated on the real `targeted_signal` plan) and fed plan
text; the missing half is the **plan/metrics acquisition layer** (Spark REST / event log) plus a
**succeeded-DAG crawl** mode. Low-hanging fruit is real and already surfacing.

## What the analyzer already produces (validated on the real targeted_signal plan, 2026-07-31)

| Impact | Finding | Fix |
|---|---|---|
| **HIGH** | Missing stats on `product_categorization` (~13.5B rows scanned) | `ANALYZE TABLE product_categorization COMPUTE STATISTICS FOR ALL COLUMNS` |
| MEDIUM | Wide 182 GiB shuffle | `spark.sql.shuffle.partitions ~729` (~256 MiB each) / AQE coalesce |
| MEDIUM | RunningWindow forces a full sort of the wide shuffle | check PARTITION BY cardinality; keep WindowGroupLimit; pre-aggregate |

The missing-stats finding is the optimizer's OWN advisory (`missing = product_categorization`) turned
into an actionable fix. Missing stats is why the plan chose a 554M-row SortMergeJoin + multi-hour sorts
instead of a broadcast/right-sized shuffle; the 182 GiB shuffle at default partitions is the spill that
INC-009 saw. Both are concrete, owner-actionable wins on a currently-green-but-slow (1.8 h) job.

## The gap: are we getting all the detail Spark can provide? No.

Current analyzers (`dataproc_rca`, `databricks_rca`) pull: run/batch state, the error string, cluster
termination reason. They do NOT pull: the physical plan, per-node SQL metrics (rows / time / **spill**),
the `== Optimizer Statistics ==` state, WholeStageCodegen durations, shuffle/cache sizes, or the
SQL/DataFrame config. All of that is in the Spark UI SQL detail (the screenshots). Optimization needs it.

## Plan/metrics acquisition — where it lives + the key-free path (next build)

- **Databricks:** the Spark SQL detail is the Spark UI, backed by the **Spark REST API**
  `/api/v1/applications/<app_id>/sql/<executionId>` served through the workspace Spark-UI proxy —
  reachable with the `malachi@mountain.com` OAuth token as bearer. The physical-plan text +
  optimizer-stats block also appear in the driver/notebook output (`get-run-output` partially reaches
  it). **Next:** prove the exact REST path returns nodes+metrics key-free; fall back to plan-text.
- **Dataproc:** plan + metrics come from the Spark **event log** (`.zstd`) → `eventlog_profiler.py`
  already extracts spill/skew/recompute, but the event log is often **absent** (`eventLog.dir` unset —
  INC-005 had none; enabling emission is a prod lever). Plan text can also land in Cloud Logging driver
  output. **Next:** plan-text-from-driver-log now; push for event-log emission for the deep profile.

## Crawl mode — optimize succeeded DAGs, not just failures (the "check every DAG" vision)

The same day-dump that pulls failed logs can enumerate **succeeded** tasks and run `analyze_plan` on each
plan → a ranked, cross-DAG **optimization backlog** (low-hanging first: missing stats, wide shuffle,
uncached recompute). This is exactly the "scan every DAG/task, find what to optimize" ask. It depends on
the acquisition layer above (a success log alone does not carry the full plan — the plan comes from the
Spark UI / event log). Output = a backlog doc / xlsx the owning teams triage, quarterly-refreshable.

## Detector taxonomy (extensible, mirrors the failure signatures)

`missing_statistics` · `broadcast_candidate` · `shuffle_partition_sizing` · `window_full_sort` ·
`repeated_scan` (uncached recompute). Each returns impact + evidence + a concrete fix. Add detectors as
real plans surface new shapes (same growth path as the 14→22 failure signatures).

## Next-step decision

Which acquisition path first: **(a)** Databricks Spark REST `/sql/<id>` (richest — real per-node time +
spill, unlocks skew/spill detectors), or **(b)** plan-text-from-driver-log for both engines (cheapest,
works today, gets missing-stats + join-strategy + shuffle-size but no per-node timing)?
