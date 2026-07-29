---
name: reference_backend_reporting_ber_team
description: Backend Reporting (BER) squad owns the summarydata reporting models + CHAPI/ClickHouse/R2 graph metrics stack
metadata: 
  node_type: memory
  type: reference
  originSessionId: b2e26231-0715-4211-9711-2f60a8021621
doc_type: memory
keywords: [backend reporting, BER team, summarydata, all_facts, CHAPI, ClickHouse, graph.usersreached, Lizz Joslen, airflow-reporting]
domain: [routing-people, repos, data-catalog]
lifecycle: active
last_verified: 2026-06-24
---
The **Backend Reporting squad** (GitHub team `backend-reporting`, SQLMesh owner tag `ber`) owns the customer-facing reporting metric stack end-to-end: the BigQuery SQLMesh models (`summarydata.impression_facts`, `visit_facts`, `all_facts`, `ber_stg.visit_facts__base` in `SteelHouse/sqlmesh`) AND the CHAPI/ClickHouse load that R2 reads (`SteelHouse/airflow-reporting`, `dags/chapi/` → ClickHouse `all_facts_local_daily`). Verified via the repo's `owners.py` + commit authors (TI-1019, 2026-06-24).

Maintainers: **Lizz Joslen, Mike Rivera** (Aylwin Souza on squad). Route `graph.*` / `all_facts` / reporting-metric definition + change requests to a **BER Jira ticket** tagging Backend Reporting (not #reporting_helpdesk, which is Q&A only).

The R2 "graph" namespace (`graph.usersreached`, `graph.sitevisitors`, `graph.spend`, `graph.impressions`) = R2 UI metric layer → CHAPI ("ClickHouse API") → ClickHouse `all_facts_local_daily` ← hourly copy of BQ `summarydata.all_facts`. See [[reference_graph_usersreached_mixed_key]] for the `uniques` IP/cookie-key gotcha.
