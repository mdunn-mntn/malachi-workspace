---
name: reference_sqlmesh_repo
description: "SteelHouse/sqlmesh — the data-warehouse SQLMesh repo (silver/gold models). How to author + deploy a model, plus the two killer gotchas (Cloud Identity quota project; format-before-plan)."
metadata: 
  node_type: memory
  type: reference
  originSessionId: 604faaf9-ab5f-4b71-bb07-1a88aa0b430e
doc_type: memory
keywords: [sqlmesh, SteelHouse/sqlmesh, silver gold models, sqlmesh plan, sqlmesh format, cloud identity quota project, verify-impact, physical table freshness, ryan kleck, audi-1083]
domain: [repos, bigquery]
lifecycle: active
last_verified: 2026-07-27
---
**Repo:** `git@github.com:SteelHouse/sqlmesh.git`, cloned `~/Developer/work/mntn/sqlmesh`. Owns the
`dw-main-silver`/`gold` models (the SQLMesh half of the stack; airflow-ti is the separate Spark feature store).
Full how-to in **`knowledge/data_knowledge.md` § "SQLMesh — how to create + deploy a model"** — read that before deploying.

**Key facts (verified AUDI-1083, 2026-07-24, first hands-on deploy):**
- **A "job" IS the `.sql` model file** under `models/dw-main-{layer}/{schema}/` (MODEL block + query). The `cron` in the MODEL block IS the schedule; no separate DAG (unlike airflow-ti). JS UDFs allowed as `CREATE TEMPORARY FUNCTION` pre-statements (precedent `summarydata/conversion_signal_impressions.sql`).
- **Local env:** venv + `pip install -r requirements.txt` (sqlmesh from a fork), `export SSL_CERT_FILE=$(python -m certifi)`, `gcloud auth application-default login`. Cloud SQL state + BQ access is DP-granted (I have it). `sqlmesh info` tests both connections.
- **Deploy loop:** `sqlmesh format` → `sqlmesh plan dev_<user> --no-prompts --auto-apply` (required pre-PR gate, Ryan Kleck; `--no-prompts` alone aborts at the backfill confirm, need `--auto-apply`) → push → PR → CI → Ryan reviews/merges. **On merge a FULL model is backfilled to prod almost immediately** (not next cron).
- **GOTCHA 1 — Cloud Identity quota project:** the repo's prod-access guard does a Google-Group lookup that CRASHES an even a dev plan when your ADC quota project (`mntn-coredw-prod`) has Cloud Identity API disabled. Fix: `gcloud auth application-default set-quota-project dw-main-bronze` (enabled there).
- **GOTCHA 2 — format BEFORE plan:** reformatting AFTER planning changes the model fingerprint → invalidates the snapshot → CI `verify-impact` fails ("Missing … deployable impact snapshot … not applied in any environment for this tree"). Re-plan on the formatted code + `gh run rerun <id> --failed`.
- CI (`sqlmesh-checks`): "Check SQL Formatting" (`format --check`) + `verify-impact`. Ignore the "Node.js 20 deprecated" annotations (repo CI infra). First deployed model: `dw-main-silver.audience.mm_campaign_classifier` (AUDI-1083).
- **GOTCHA 3 — freshness/"did it run?" is on the PHYSICAL table, not the clean view.** The prod clean-name object is a virtual-layer VIEW, so `bq show` / `<dataset>.__TABLES__` shows it **0 rows / 0 bytes / stale last_modified** = a false "job failed / empty table" signal. Check the physical timestamp: `SELECT table_id, TIMESTAMP_MILLIS(last_modified_time), row_count FROM \`dw-main-silver.sqlmesh__<dataset>.__TABLES__\` WHERE table_id LIKE '%<table>%' ORDER BY 2 DESC` (physical = `<dataset>__<table>__<fingerprint>`; fingerprint changes each redeploy). Row-count/distribution sanity runs fine on the clean view; only freshness needs the physical. Verified 2026-07-27 on AUDI-1083 (view 0-rows/25h-stale vs physical 14,516 rows / 17h — daily FULL confirmed running).

Related: [[reference_audience_intent_scoring_dag]], [[reference_airflow_ti]], [[project_audi_1083_mm_classifier]], [[feedback_airflow_prod_safety]].
