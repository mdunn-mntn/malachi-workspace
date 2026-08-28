# Phase plan: measure the 39 non-Spark DAGs (savings + billing), AUDI-1241 scope

Written 2026-08-28 pre-compaction. This is the next big build. The user's prompt to start it:

> Start the non-Spark profiling phase from
> tickets/audi_1194_optimizer_efficiency_crawler/artifacts/audi_1194_nonspark_phase_plan.md

## Why

39 of 72 active DAGs run no Spark and are invisible to the optimizer. Their cost surfaces:
BigQuery slots, Databricks-native jobs, K8s pods. The goal is the SAME loop that works for
Spark: profile daily → findings in the digest → fix PRs → measured savings in the ledger,
Mode dashboard (report e81786de8403), and the playbook fix log.

## Architecture (mirror the Spark path, one module per surface)

1. `bq_profile.py` — per task: INFORMATION_SCHEMA.JOBS(_BY_PROJECT) slot-ms, bytes billed,
   query text hash, per dag/task label attribution (BQ jobs carry airflow labels? VERIFY
   first: labels or job_id prefix convention; else match by service account + time window).
   Rate: slot-ms → dollars via reservation cost or on-demand $6.25/TiB (check which applies:
   org uses us-central1 slot reservation `dw-main-bronze:us-central1.background-jobs`).
2. `dbx_profile.py` — extend existing databricks.job_costs/query_costs (system tables, grants
   live) from report-only to FINDINGS (idle warehouses, oversized clusters, repeated scans).
3. `pod_profile.py` — K8s requested-vs-used from... (VERIFY the metrics source: GKE metrics
   in Cloud Monitoring; the deployment is Astro-hosted so pod metrics may need the Astro API).
4. Ledger: new `surface` field per row (spark|bq|dbx|pod) + per-surface exec-unit and rate
   (slot-h, DBU, pod-core-h). savings() math unchanged: before-rate minus after-rate per day.
5. Billing: blended_usd for each surface from the SAME billing export (service.description
   BigQuery / Databricks passthrough; pods are GKE SKUs). billing.py grows per-surface rates.
6. Digest: one section per surface; coverage report gains "profiled non-Spark" counts.

## Order of work (each its own PR, tools-only)

1. BQ profiler (biggest spend visibility, easiest attribution) + ledger surface field.
2. Billing per-surface rates.
3. Databricks findings (grants exist; system.query already read).
4. Pod profiler (needs metrics-source verification first; maybe drop if Astro blocks it).
5. Mode dashboard: per-surface savings split (layout is API-editable, report e81786de8403).

## Constraints that bite here

- BQ reads from the DAG run as `airflow-ti-prod` SA — needs INFORMATION_SCHEMA.JOBS access
  in the projects where task queries run (find them first; likely dw-main-* + analytics).
  Grants via mntn-devops PR (Cristina), same pattern as the billing grant (#5121).
- Workspace tree airflow_optimizer/ ↔ bundle include/spark_optimizer/ two-copy rule, sed port.
- Never git add . ; gauntlet before any PR (haiku default); BQ via bq_run.sh with
  --use_legacy_sql=false for backticked standard SQL.
- The hackathon merges the EXISTING 17 PR-READY Spark fixes; this phase is separate optimizer
  tooling work and lands under AUDI-1241 (checklist comment 2026-08-28).

## State at write time

All shipped and live: #1239-#1243 merged; #1244 open (priority rationale + IMP-087 alert
pagination), gauntlet running, Ryan to merge. Mode dashboard live with custom layout (KPIs,
SVG line chart, DAG bars, fixes table). Fix log auto-syncs to playbook 2908061697 from the
noon job. First measured saving: fangorn #1231, 575.6 exec-h/day = $160/day.
