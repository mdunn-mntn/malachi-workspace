---
name: reference_mode_api
description: Mode Analytics API for workspace mntn — auth via keychain mode_api_token/mode_api_secret basic pair; report creation is POST /api/mntn/reports with space_token (the documented /spaces/.../reports path 404s); report layout is editable HTML via PATCH (custom CSS/JS, window.datasets, mode-chart embeds); charts editable via PATCH view_vegas; schedules API rejects documented payloads (set in UI); BQ data source 48787 runs as mode-analytics@dw-main-bronze SA; adding a query + layout section to an existing report via API works end-to-end (POST reports/<token>/queries + layout PATCH).
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [mode api, mode analytics api, mode_api_token, mode_api_secret, mode workspace mntn, mode report creation, space_token, mode report layout, PATCH layout, window.datasets, mode-chart, view_vegas, mode chart spec, mode schedules api, mode data source 48787, mode-analytics service account, spark optimizer savings dashboard, e81786de8403, audience intelligence space, report queries POST, add query to report, 3ead7301daa8, opt-bq layout section]
domain: [infra, tools]
lifecycle: active
last_verified: 2026-09-01
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

Dashboard-porting and TI-1037 history: [[reference_mode_dashboard_porting]], [[project_audi_1037_mode_dashboard]].
