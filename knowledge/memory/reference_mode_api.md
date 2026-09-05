---
name: reference_mode_api
description: Mode Analytics API for workspace mntn — auth via keychain mode_api_token/mode_api_secret basic pair; report creation is POST /api/mntn/reports with space_token (the documented /spaces/.../reports path 404s); report layout is editable HTML via PATCH (custom CSS/JS, window.datasets, mode-chart embeds); charts editable via PATCH view_vegas; schedules API rejects documented payloads (set in UI); BQ data source 48787 runs as mode-analytics@dw-main-bronze SA; adding a query + layout section to an existing report via API works end-to-end (POST reports/<token>/queries + layout PATCH); the BigQuery cost table reads ledger surface=bq only, so the optimizer's unattributed BQ bucket is not on Mode (AUDI-1278; would need bigquery.resourceViewer for mode-analytics@).
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [mode api, mode analytics api, mode_api_token, mode_api_secret, mode workspace mntn, mode report creation, space_token, mode report layout, PATCH layout, window.datasets, mode-chart, view_vegas, mode chart spec, mode schedules api, mode data source 48787, mode-analytics service account, spark optimizer savings dashboard, e81786de8403, audience intelligence space, report queries POST, add query to report, 3ead7301daa8, opt-bq layout section, unattributed not on mode, bigquery.resourceViewer mode-analytics, jobs.listAll, iam_bronze_extras.tf, AUDI-1278, JOBS_BY_PROJECT mode query option, decision 0007]
domain: [infra, tools]
lifecycle: active
last_verified: 2026-09-03
---
# Mode Analytics API (workspace `mntn`)

All confirmed empirically 2026-08-28 while building the Spark Optimizer Savings dashboard (AUDI-1194).

- **Auth:** HTTP basic pair, keychain items `mode_api_token` + `mode_api_secret`.
- **BQ data source id `48787`** executes as `mode-analytics@dw-main-bronze.iam.gserviceaccount.com` — found by running `SELECT SESSION_USER()` through a Mode query. Grant this SA on any dataset a Mode report must read.
- **Create a report:** `POST /api/mntn/reports` with a `space_token` param. The documented `/api/mntn/spaces/<token>/reports` path **404s**.
- **Report LAYOUT is editable HTML via `PATCH`** — custom CSS/JS allowed, query results reachable through the `window.datasets` global, charts embedded with `mode-chart` elements.
- **Chart specs are editable via `PATCH` on `view_vegas`.**
- **Schedules API rejects its documented payloads** — set schedules in the UI.
- **Refresh a report on demand:** `POST /api/mntn/reports/<token>/runs` (verified 2026-08-29 on `e81786de8403`, run `48140b50e8dc` succeeded).
- **Add a query to an existing report + a new layout section, end-to-end via API (verified
  2026-09-01 on `e81786de8403`):** `POST /api/mntn/reports/<token>/queries` created "BigQuery
  cost by task" (query token `3ead7301daa8`), then a layout `PATCH` added a new section
  (id `opt-bq`) embedding it; run `d2d0b89e9cef` succeeded.
- **Credential location — two records, both observed:** this doc recorded keychain items `mode_api_token`/`mode_api_secret` (2026-08-28); the 2026-08-29 refresh used `MODE_API_TOKEN`/`MODE_API_SECRET` from `~/.zshrc`. Hypothesis: same pair stored in both places. Settles by checking `security find-generic-password -s mode_api_token` vs `grep MODE_API ~/.zshrc`.
- Live artifact: report `e81786de8403` "Spark Optimizer Savings", Audience Intelligence space, custom layout (KPI cards, hand-drawn SVG line chart, DAG bar list, fixes table). Reads `mntn-prj-prod-00:optimizer.optimization_ledger` (see [[project_airflow_optimizer]]).

- **The "BigQuery cost by task" query (`3ead7301daa8`) reads `optimizer.optimization_ledger` `WHERE surface = 'bq'` only, so the optimizer's unattributed BQ bucket never reaches Mode** (the ledger skips it by design; AUDI-1278, 2026-09-02/03). Putting it on the dashboard (option A; D1 = the daily `optimizer_bq_<date>.md` report, decision 0007) means a Mode query over `dw-main-bronze.region-us-central1.INFORMATION_SCHEMA.JOBS_BY_PROJECT`. **(Superseded 2026-09-03, AUDI-1316: no grant needed.** `mode-analytics@dw-main-bronze` already holds `bigquery.jobs.listAll` via its `medallion_bronze_reader` role, confirmed by live project policy + role permission list.) The query is written and validated; add it to report `e81786de8403` alongside the existing six. See [[reference_bq_job_attribution]].

Dashboard-porting and TI-1037 history: [[reference_mode_dashboard_porting]], [[project_audi_1037_mode_dashboard]].

- **The unowned-BigQuery section is LIVE on `e81786de8403` (2026-09-03, AUDI-1316):** query `Unowned BigQuery jobs by day` (token `f513b6ed7755`) over `dw-main-bronze.region-us-central1.INFORMATION_SCHEMA.JOBS_BY_PROJECT`, layout section `opt-unowned`, added end-to-end over the API (POST queries, PATCH layout, POST runs). The successful run settles the last open question: no grant was needed, `medallion_bronze_reader` already carries `bigquery.jobs.listAll`.

- **Schedules ARE writable via the API (2026-09-05, AUDI-1325) — contradicts the "schedules API rejects its documented payloads" line above; both records kept.** `PATCH /api/mntn/reports/e81786de8403/schedules/d30b701e413d` returned 200 and moved `cron_hour` 6 -> 10 UTC. What works is the payload the object's own `_forms.edit.input` dictates, sent whole: `{"report_schedule":{"name":...,"cron":{"freq":"daily","hour":10,"time_zone":"UTC","day_of_week":null,"day_of_month":null,"minute":0},"params":{},"timeout":86100}}`. Hypothesis reconciling the two: the 2026-08-28 attempt used Mode's published doc shape (flat `cron_hour`/`cron_minute`), not the nested `cron` object the live `_forms` carries. Settle it by replaying a flat-field payload and recording the status. Rule of thumb: GET the object, read `_forms.<action>.input`, echo that shape back.

- **Report `e81786de8403` now carries eight queries (2026-09-05, AUDI-1325):** the corrected `Savings headline` (`5a66e5fad18c`, 4,635 bytes) and `Savings by surface` (`513a4a7a4a71`, 3,568 bytes) were replaced over `PATCH /api/mntn/reports/<report>/queries/<query>` with body `{"query":{"raw_query","name","data_source_id"}}`, and `Fixes not yet measurable` (token `baf7ca81c920`, 3,211 bytes) was created over `POST .../queries`. The layout was NOT edited, so the new query has no section on the page yet and the hero, empty-state and "DAGs fixed so far" copy still describe the old numbers.
