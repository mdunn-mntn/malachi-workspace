---
doc_type: routing
title: START HERE — how to find anything (MNTN workspace)
summary: "task → the minimal set of docs to open; bridges the prose knowledge base and the new per-table catalog"
last_verified: 2026-07-17
keywords: [start, routing, where, how to find, orientation, catalog]
---

# START HERE

The front door for the MNTN workspace. **Load indexes, not the whole tree** — open only the docs a
map names. Two knowledge layers coexist during the migration to the self-documenting system:

- **Prose knowledge (the mature source of truth):** `data_catalog.md`, `data_knowledge.md`,
  `mntn_business.md`, `experimentation.md`, `ds_catalog.md`. Big, curated, grep-able by `##` section.
- **Structured catalog (new, being crawled FROM the prose + live schema):** one doc per table under
  `knowledge/bq/<dataset>/<table>.md`, with generated indexes `_ROUTING.md` (keyword→doc),
  `bq/_TOPICS.md` (by domain), `bq/_CATALOG_INDEX.md`, `bq/_COVERAGE.md` (documented-at-what-depth).

Until a table's structured doc reaches `enriched`/`verified` in `bq/_COVERAGE.md`, its
`data_catalog.md` `## silver.<ds>.<table>` section remains the fallback source.

## Find anything in 4 steps
1. **Know the term?** `grep -ri "<term>" knowledge/_ROUTING.md` (structured) or
   `grep -n "<term>" knowledge/data_catalog.md knowledge/data_knowledge.md` (prose) → open what it names.
2. **Know the table?** `bq/_CATALOG_INDEX.md` → its `knowledge/bq/<ds>/<table>.md`; or its
   `## silver.<ds>.<table>` section in `data_catalog.md`.
3. **Know the domain?** `bq/_TOPICS.md` (structured) or the dataset headers in `data_catalog.md`.
4. **Documenting depth / what to enrich next?** `bq/_COVERAGE.md` (worst-first work queue).

Every query runs through `.claude/scripts/bq_run.sh` (logs cost + `sql_tables` to
`knowledge/bq_perf_log.jsonl`; the net-new hook flags undocumented tables). **Sample first.**

## Task → start-set (open these, nothing more)
| I need to… | open |
|---|---|
| a **table's schema / grain / gotchas** | `bq/_CATALOG_INDEX.md` → the table doc; else its `data_catalog.md` section + `data_knowledge.md` gotchas |
| **business logic / a metric definition** | `data_knowledge.md` (§ Business Logic / Advertising Concepts), `mntn_business.md`, `glossary.md` |
| **experiment / rollout / causal method** | `experimentation.md` § Standard Analysis Protocol |
| **pre/post · before-after a date · "did X change a KPI"** | `experimentation.md` § Standard Analysis Protocol (never naive pre/post — pair with CausalImpact); perf tables via `_ROUTING` **campaign daily rollup** / **long pre-period** → `summarydata.sum_by_campaign_by_day` (NOT `agg__daily_sum_by_campaign`, frozen to Sep 2025–Apr 2026) |
| **a data-source `DSxx`** | `ds_catalog.md` |
| **tune a slow/expensive query** | `bq/optimization_playbook.md`, `bq/query_cookbook.md`, the table's `## Observed cost`, and mine `bq_perf_log.jsonl` via `.claude/scripts/perf_digest.py` |
| **verify how a reported number was produced** | `.claude/scripts/bq_verify.py <ticket \| label \| sql_sha256>` → the exact SQL fingerprint + `job_id` (recovers full SQL via `bq show -j`) + git commit + cost. Every `bq_run.sh` run is provenance-stamped. |
| **an Airflow / pager / pipeline alert (on-call)** | `on-call/oncall_runbook.md` — §0 classify (alert vs ticket) → §2 catalog → §3 incidents — or run **`/oncall`**. Grep `_ROUTING.md` for the symptom (`sensor timeout`, `dataproc`, the DAG name). |
| **prior work on a topic** | `tickets/INDEX.md` → the ticket's `summary.md` |
| **where a file belongs** | `folder_definitions.md` (ticket structure authority) |

## The system (how this workspace documents itself)
- **Deterministic layer** (`.claude/`): the `bq_run.sh` wrapper + 8 hooks + `build_index.sh` +
  `lint_coverage.py` + `lint_tickets.py` keep the indexes true and flag doc-debt automatically;
  `health_scorecard.py` (SessionStart) and `request_digest.py` add self-improvement signals. See `.claude/README.md`.
- **Agents** (`.claude/agents/`): cataloger (skeleton→enriched), reviewer-adversarial ×2, fixer,
  synthesizer, perf-analyst, curator. The crawl uses this loop. See `workflows/ARCHITECTURE.md`.
- **Session startup + always-on behaviors + BQ rules:** `.claude/CLAUDE.md`.
