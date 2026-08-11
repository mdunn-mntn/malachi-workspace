---
name: project_structured_bq_catalog
description: workspace has a structured self-documenting per-table BQ catalog (knowledge/bq/, 204 docs) crawled from prose+live schema; MERGED to main 2026-07-20 (PR #1)
metadata: 
  node_type: memory
  type: project
  originSessionId: fc59db3f-b426-4cbe-9c11-c2bd5011531f
doc_type: memory
keywords: [structured_bq_catalog, per-table catalog, knowledge/bq, 263 tables, coverage_state, bq_introspect.sh, schema_synced, START_HERE.md, "PR #1", build_index.sh, lint_coverage, bq_introspect, coverage_state]
domain: [project, bigquery, data-catalog]
lifecycle: active
last_verified: 2026-07-20
---
The MNTN workspace gained a **structured, self-documenting BigQuery catalog** layer (adopted from the AI Workflow Kit), built July 2026 and **MERGED to `main` on 2026-07-20 via PR #1** (`mdunn-mntn/malachi-workspace#1`). It is live on main — no branch needed.

**What exists:**
- `knowledge/bq/<dataset>/<table>.md` — one doc per table, **263 tables** (grew from the initial 204 after crawling the `_UNDOCUMENTED.queue` on 2026-07-20): logdata(24), summarydata(85), core(70), aggregates(14), integrationprod(41), external(9), ber_stg(14), analytics_curated(3), tpa(1), audience(1), summarydata_archive(1). Coverage: **57 verified · 206 enriched · 0 skeleton · 0 stale**. `_UNDOCUMENTED.queue` is now EMPTY. Front-matter (YAML) + auto-synced schema + Purpose/Grain/Column-meanings/Joins/Gotchas/Cost + Changelog. Two dates: `schema_synced` (machine) vs `last_verified` (human); `coverage_state` = skeleton→enriched→verified. Note: `bq_introspect.sh` takes a 2nd table-allow-list arg + `GCP_PROJECT=dw-main-bronze` override for cross-project seeding (integrationprod/external/analytics_curated live in bronze).
- Generated indexes (rebuild: `.claude/scripts/build_index.sh`): `knowledge/START_HERE.md`, `_ROUTING.md`, `bq/_CATALOG_INDEX.md`, `bq/_TOPICS.md`, `bq/_COVERAGE.md`.
- `knowledge/glossary.md` (term→def+pointer), `knowledge/decisions/` (ADR-style settled disambiguations).
- Deterministic spine in `.claude/`: `bq_run.sh` (dry-run gate + perf log + net-new detection), 4 hooks, `bq_introspect.sh` (schema refresh), `lint_coverage.py`, agents in `.claude/agents/`, `workflows/` (ARCHITECTURE, INGEST_GUIDE, agent_pass_runbook).

**How it was built:** multi-agent ingest loop (cataloger → 2 adversarial reviewers → fixer) driven by the Workflow tool over `scratchpad/crawl.js`. The crawl **corrected** stale prose against live BQ (e.g. impression_log `epoch`=µs not seconds; bidder_bid_events TTL 10d not 90d; campaign_groups status_id→name is many-to-many). Prose docs (`data_catalog.md`, `data_knowledge.md`, …) remain the source of truth; the per-table layer is the derived, indexed layer that cross-links back. See [[project_qfai_transfer_interest]] is unrelated; see the crawl commits on the branch (`adopt-kit ...`).

**Retrieval for a fresh chat:** `knowledge/START_HERE.md` → the index maps → the one doc. Load indexes, not the tree. Until a table's doc is `enriched`/`verified`, its `data_catalog.md` section is the fallback.
