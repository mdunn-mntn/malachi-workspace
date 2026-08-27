---
name: reference_pyspark_optimization_skill
description: Spark job optimization = read .claude/skills/pyspark-optimization-databricks-dataproc/SKILL.md (compiled 2026-08-27, DBR 17.3 + Dataproc 2.3, 39 cited sources) THEN this doc's MNTN reconciliation annex — the profiled Dataproc fleet is SERVERLESS Spark 4.0, not image-2.3/3.5.3; speculation pinned false on GCS writers; fetch-wait is map-side; EXPLAIN COST detector limits.
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [pyspark optimization, spark optimization, spark tuning, pyspark-optimization-databricks-dataproc, photon, AQE, adaptive query execution, skew, salting, spill, broadcast join, shuffle, liquid clustering, z-order, EXPLAIN COST, manifest committer, fs.gs, DBR 17.3, spark 4.0 ANSI, dataproc image 2.3, spark 3.5.3, DCU, speculation, straggler]
domain: [infra, reference]
lifecycle: active
last_verified: 2026-08-27
---
# reference_pyspark_optimization_skill

## What this is

The general Spark tuning reference lives at
`.claude/skills/pyspark-optimization-databricks-dataproc/SKILL.md` — a compiled, citation-pinned
reference (2026-08-27 research pass: Databricks DBR 17.3 LTS / Spark 4.0.0 + Photon; Dataproc
image 2.3 / Spark 3.5.3; shared Spark internals; symptom→fix table; 39 sources with
verified/canonical status). It is installed verbatim; corrections and MNTN divergences are
appended HERE, never edited into the skill. **Where this annex and the skill conflict, this
annex wins for MNTN work** — it records what was observed on the actual fleet. Read BOTH before
tuning any job surfaced by the AUDI-1194 optimizer or AUDI-1191 debugger.

## Fleet reality vs the reference's assumptions

- **The profiled Dataproc fleet is SERVERLESS batches, not image-2.3 clusters.** The skill's
  Part B2 (image 2.3.13, Spark 3.5.3, YARN autoscaling, EFM, preemptible secondaries) does NOT
  describe the airflow-ti model fleet: those run as Dataproc Serverless batches in
  `mntn-prj-prod-00` and emit **Spark 4.0** event logs (`airflow_optimizer/eventlog.py:8`;
  dynamic-allocation behavior verified against Spark 4.0.0 source, see
  [[reference_dataproc_eventlog_profiling]]). Consequences: Spark 4.0 semantics (ANSI on,
  B1.7's checklist) apply on the Dataproc side too; there are no spot/preemptible workers to
  tune ("decommission"/"lost" are normal serverless scale-down strings — 0 preemptions in a
  300-run sample); billing is per-DCU (`milliDcuSeconds`), not VM+fee, and Dataproc is absent
  from `system.billing` ([[reference_spark_eventlog_cost_units]],
  [[feedback_dataproc_cost_awareness]]).
- **Unreconciled version contradiction, kept per the append rule:** the standard airflow-ti
  `spark.jars` pins `iceberg-spark-runtime-3.5_2.13` (`documentation/docs/airflow_ti_workflow.md:225`,
  implying Spark 3.5) while the serverless event logs and dynamic-allocation behavior are
  Spark 4.0. Hypothesis: the jar pin is stale or per-model; the check that settles it is reading
  `runtimeConfig.version` from `gcloud dataproc batches describe` on a live batch. No doc records
  the serverless runtime version. [[reference_airflow_ti]]
- **Real image clusters DO exist** — Fangorn inference runs 290×n2-standard-16 in
  `mntn-targeting-prj-prod` (image version unrecorded); Part B2 applies there, version
  unverified. [[reference_fangorn_inference_dataproc]]
- **Databricks is confirmed DBR 17.3** (`.claude/databricks_setup.md:16`,
  `spark_version: 17.3.x-scala2.13`) so Part B1 applies as written — with one delta:
  `runtime_engine: STANDARD`, so **Photon is NOT on for our classic job clusters** despite
  B1's "on by default" framing for warehouses/serverless. [[reference_databricks]]

## MNTN overrides to the skill's fixes

- **Straggler fix (`spark.speculation=true`) is owner-gated, never a prescription:** airflow-ti
  pins `spark.speculation=false` on every GCS-writing model (ManifestCommitter race —
  `advertiser_join.py`, `intent_score_map.py:54`). The gauntlet has already reverted one
  evidence-backed speculation change (`ipdsc_ds_35`, 2026-08-27).
  [[reference_dataproc_eventlog_profiling]] [[reference_airflow_ti]]
- **Shuffle fetch-wait is a MAP-SIDE question here, not partition sizing:** on
  `site_network_hourly`, stage 9 waited 73% on 4.2M blocks @~1.7 KiB while sibling stages read
  23.4M smaller blocks with ~0 wait — the lever is map-side executor spread
  (`initialExecutors`), and raising `spark.sql.shuffle.partitions` on already-tiny blocks makes
  it WORSE by multiplying block count. [[reference_dataproc_eventlog_profiling]]
- **EXPLAIN COST is narrower than the skill implies:** on real Databricks plans only the
  missing-statistics detector fires — Photon renames physical operators
  (`PhotonShuffleExchangeSink/Source`) and `Statistics(sizeInBytes=)` attaches to logical
  operators only; a planner error returns as RESULT TEXT with statement success, and any query
  against a transient (drop-and-recreate) table can never be planned after the fact.
  [[reference_databricks]]
- **Executor sizing:** the measured lever is `spark.executor.cores` ≈ 4–8, not node size —
  identical total cores on big nodes ran 3.5× slower at default 1-executor-per-node.
  [[reference_databricks]]
- **Builder config silently beats decorator config** in airflow-ti models (`getOrCreate` wins),
  so a shuffle-partition fix must change BOTH, and any decorator `runtime_properties` change
  requires regenerating `dags/model_task_config.json`. [[reference_airflow_ti]]
- **Cost units:** event-log executor-hours per [[reference_spark_eventlog_cost_units]]
  (`ExecutorAdded` is not a census; seed from first task launch); Databricks dollars per
  [[reference_databricks_billing_cost]] (job_run_timeline dedupe or 12× inflation; warehouse
  dollars apportioned, never measured). CUD floor caveat: cut DCU-seconds may save $0
  ([[feedback_dataproc_cost_awareness]]).

## Relationship to the AUDI-1194/1191 detectors

Detector fix text is canonical in `airflow_optimizer/optimizations.py`; this doc and the skill
do not restate it, and the three airflow-ti traps above bind every shipped fix. Detector
inventory and measured status (10 of 14 fire on real logs): [[project_airflow_optimizer]];
failure-side RCA: [[project_airflow_debugger]]. Canonical-source decision for fix text is
tracked in `improvements_backlog.md` (2026-08-27 row).

## Contradiction log (append-only, dated)

(empty)
