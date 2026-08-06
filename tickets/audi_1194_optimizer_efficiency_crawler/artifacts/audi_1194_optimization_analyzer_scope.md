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
  reachable with the `malachi@mountain.com` OAuth token as bearer. **EVIDENCED 2026-08-03 (probe on
  the real INC-009 task run 616633605519362):** `databricks jobs get-run-output` returns ONLY
  `error / error_trace / metadata / notebook_output` — NO plan text, NO `sizeInBytes`, NO
  `ANALYZE TABLE`. So the plan/optimizer-stats are **not** in the cheap driver output; the
  "plan-text-first is cheap for Databricks" assumption is false. Getting the Databricks plan requires
  the **Spark REST** path (or the driver log4j log, if the CBO advisory lands there — unverified), or
  a prod change to `explain()` to stdout. **Next:** spike the Spark REST/proxy path key-free.

  **SPIKED 2026-08-03 (real run 616633605519362, cluster 5731-160716-16bfsjew, app 1624571757441950872) — BLOCKED for completed job clusters:**
  - `cluster_log_conf = None` → **no event log persisted** anywhere readable (unlike Dataproc's event log path).
  - `/driver-proxy-api/o/<org>/<cluster>/<port>/api/v1/applications` → **400 `INVALID_STATE: Cluster is in Terminated state`** (driver proxy is running-cluster-only; job clusters terminate right after the run).
  - `/sparkui/<cluster>/driver-0/api/v1/applications` → **500 `TEMPORARILY_UNAVAILABLE`** for a terminated cluster (the Historical Spark UI is HTML-rendered, no clean REST).
  - Not in `get-run-output`; not in SQL query history (dbt **python** model, not a warehouse query).

  **Verdict:** automated key-free acquisition of a completed Databricks job-cluster's Spark plan/metrics is **not viable via REST**. Two real unblocks, both needing an owner/Ryan prod change:
  1. **Enable `cluster_log_conf` event-log delivery to GCS** on the job cluster → then read the event log like Dataproc (`eventlog_profiler.py`). Durable, gives full metrics.
  2. **Add `explain(mode="cost")` (or `EXPLAIN COST`) to the dbt model** → plan + optimizer-stats print to stdout → land in `get-run-output` (already key-free). Lighter, gets the low-hanging missing-stats/join/shuffle findings.

  Until then: the analyzer runs on plans grabbed **manually** from the Spark UI (works today — delivered the targeted_signal wins).
- **Dataproc:** plan + metrics come from the Spark **event log** (`.zstd`) → `eventlog_profiler.py`
  already extracts spill/skew/recompute, but the event log is often **absent** (`eventLog.dir` unset —
  INC-005 had none; enabling emission is a prod lever). **EVIDENCED 2026-08-03:** the Cloud Logging
  driver output does NOT carry the physical plan / optimizer-stats either (INC-005's driver log = 0 plan
  markers; recent batches emit no driver plan text). So — symmetric with Databricks — Dataproc also has
  **no cheap plan source**; both engines need the event-log/explain enablement (IMP-023). **Next:** turn
  on `spark.eventLog.enabled` for one batch → read the `.zstd` with `eventlog_profiler.py`.

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
