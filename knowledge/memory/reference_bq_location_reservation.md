---
name: reference-bq-location-reservation
description: BQ jobs must run in us-central1 to hit the org slot reservation; dataset-less queries (inline external GCS tables) default to US multi-region and bill on-demand
metadata: 
  node_type: memory
  type: reference
  originSessionId: 1293bb5f-2247-43b7-b203-60b292e433ad
doc_type: memory
keywords: [bq location reservation, us-central1, slot reservation, background-jobs, external_table_definition, on-demand billing, bq_run.sh location, Alek Piasecki, US multi-region]
domain: [bigquery, infra]
lifecycle: active
last_verified: 2026-07-16
---
**BQ slot reservation = `dw-main-bronze:us-central1.background-jobs`, us-central1 ONLY.** Reservation assignments are per (project, location) — a job created in the US multi-region gets no reservation and bills on-demand at $6.25/TiB regardless of which project/tables it touches. All MNTN datasets are us-central1, so normal table queries auto-route correctly. The leak: **queries referencing NO dataset default to US multi-region** — inline `--external_table_definition` GCS-parquet queries (DDP runbook svs/wcv/pc) and trivial `SELECT 1`-style tests. AUDI-1089's July 2026 runbook re-run billed ~140 TiB ≈ $875 on-demand this way (caught by Alek Piasecki, ~$720/wk).

**Fixes in place (2026-07-16):** `~/.bigqueryrc` → `location = us-central1` (plain bq); `bq_run.sh` injects `--location=us-central1` unless caller overrides; workspace `.mcp.json` bigquery server `US` → `us-central1` (also un-broke the MCP tool on MNTN datasets). Verified: identical svs external query with the flag → reservation, 0 bytes billed. Override with `--location=US` only for `region-us` INFORMATION_SCHEMA reads. Trade-off (Alek, confirmed): reservation jobs may queue/throttle when the small slot pool is maxed — stagger big scans, escalate to Alek Piasecki if problematic, never revert to US/on-demand. GCS: `mntn-data-archive-prod`/`mntn-data-tpa-prod` = US-CENTRAL1; `mntn-data-partners` = US multi-region. Full writeup: `knowledge/data_catalog.md` § "BQ job location & slot-reservation routing". Related: [[reference-ddp-valuation-framework]].
