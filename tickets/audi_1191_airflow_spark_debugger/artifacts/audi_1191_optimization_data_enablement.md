# AUDI-1191 — Enabling Spark optimization-data collection (prod-config spec)

**Status: DESIGN / reviewable-by-Ryan. No airflow-ti change made.** The probe (2026-08-03) proved the
rich Spark plan/metrics are NOT reachable key-free for a completed run today. This spec is the set of
small, mostly config-only changes that make them collectable going forward — grounded in the exact
airflow-ti attach points. Historical (pre-change) runs stay uncollectable; this is going-forward.

## BLUF

Turn on Spark **event-log delivery** on both engines and attach a **persistent history store**, so a
completed job's plan + per-node metrics land in GCS where our key-free analyzer (`optimizations.py` +
`eventlog_profiler.py`) can read them. The event log is the highest-leverage lever because it is the
**one artifact that captures all 7 Spark data surfaces** (jobs/stages/executors/environment/storage/SQL)
— see `audi_1191_spark_data_inventory.md`. Prefer it over `explain()`-to-stdout, which gets only the plan. Databricks is best enforced via the existing **cluster policy**
(one edit, org-wide); Dataproc is a **batch property** the framework already checks for.

## Dataproc (serverless batches — e.g. tpa_mntn_id_export)

**Lever (the framework already knows it):** `spark/utils/spark_job_monitor.py:144` literally warns
`Configure spark.eventLog.enabled=true in Dataproc batch properties` when it's off, and emits the
`MCP_EVENT_LOGGING_CONFIG_BASE64` breadcrumb when on. So the plumbing to *consume* the event log exists;
it just isn't being written.

1. In the batch `runtime_properties` (the `@compute.dataproc_batch(runtime_properties=...)` decorator),
   set:
   - `spark.eventLog.enabled=true`
   - `spark.eventLog.dir=gs://<mntn-dataproc-event-logs>/<env>/`
   - `spark.eventLog.compress=true` (the `.zstd` the profiler already expects)
   → the event log lands in GCS; `eventlog_profiler.py` reads it → full spill/skew/recompute metrics.
2. **Historical retention:** attach a **persistent Spark History Server** via
   `peripherals_config.spark_history_server_config` — the config shape ALREADY exists at
   `include/spark/data_source/ipdsc_emr_cluster.py:68` (`persistent_history_cluster.get(env)`). Point
   batches at a shared PHS so the Spark UI + event logs survive batch termination.
3. Needs read-only `storage.objectViewer` on the event-log bucket for the key-free analyzer (same ask
   already open for the staging bucket).

## Databricks (job clusters — e.g. targeted_signal / DbxDbtOperator + ModelPysparkDbxJobOperator)

**Gap:** `include/models/operators.py:481` builds the `new_cluster` with `policy_id`, `custom_tags`,
`spark_env_vars` — but **no `cluster_log_conf` and no event-log `spark_conf`**, so nothing is persisted
and the cluster's Spark UI dies with the cluster (probe: `400 Terminated` / `500 TEMPORARILY_UNAVAILABLE`).

Pick one (cheapest first):
1. **`EXPLAIN COST` / `df.explain(mode="cost")` in the dbt model** — prints the physical plan +
   `== Optimizer Statistics ==` (the missing-stats advisory) to stdout → lands in the already-key-free
   `get-run-output.notebook_output`. Lightest; gets the low-hanging findings (missing stats, join
   strategy, shuffle size) with **zero cluster change**. No per-node timing/spill.
2. **`cluster_log_conf` on `new_cluster`** (`operators.py:481`) → deliver driver log4j + event logs to
   `gs://<mntn-dbx-cluster-logs>/<cluster_id>/`. Post-termination readable key-free → the CBO
   missing-stats warning (log4j) + the Spark event log (full metrics via `eventlog_profiler.py`).
3. **Enforce via the cluster policy `001D160AE4052091`** (the `policy_id` already on every job cluster):
   add a `cluster_log_conf` requirement to the policy → **all** job clusters deliver logs from one edit,
   no per-model change. Cleanest org-wide; needs the policy owner.

## Historical data

Not cleanly recoverable for already-terminated runs (no event log was persisted; Historical Spark UI is
HTML-only, `get-run-output` lacks the plan). This is **going-forward only** — accept the gap; the value
compounds as jobs run under the new config.

## Rollout + cost

- Feature-flag / one low-risk DAG first; Ryan's review; never push main.
- Event-log storage is cheap (GCS). A persistent Spark History Server is a small always-on cluster —
  **flag GCP spend to Zach Schoenberger** (he watches Dataproc cost) before standing one up.
- Order: (1) Dataproc `eventLog.enabled` on one batch — cheapest, framework-ready; (2) Databricks
  `EXPLAIN COST` in one model — cheapest DBX; (3) then policy-level `cluster_log_conf` + PHS for scale.

## Analyzer readiness

`optimizations.py` runs on plan text today. When event logs flow, bridge it to `eventlog_profiler.py`'s
per-node output to light up the spill/skew detectors (the metrics `get-run-output` can't give). That
bridge is a workspace-side (ours) build, no prod dependency once the logs land.
