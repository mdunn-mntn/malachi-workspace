---
name: project_bq_optimization_discipline
description: "BQ query-optimization discipline: two anchor docs LIVE 2026-07-27 (query_cookbook + optimization_playbook); repeats-mode bug fixed; several tooling items deferred to cadence"
metadata: 
  node_type: memory
  type: project
  originSessionId: cc00f377-b575-43ed-84cf-3e31ce190e7a
doc_type: memory
keywords: [bq optimization, query_cookbook, optimization_playbook, perf_digest, fast-first bq, perf-analyst, bq_run.sh, build_index.sh, slow query tuning]
domain: [bigquery, workflow, project]
lifecycle: active
last_verified: 2026-07-27
---
The proactive-optimization half of the BQ workflow. The capture/provenance/cataloging half already
worked (`bq_run.sh` logs ~30 fields, raw `bq query` is hook-blocked, net-new tables auto-queue, per-table
catalog). This project built the missing half.

**Why:** `START_HERE.md:42` routed "tune a slow/expensive query" to `bq/optimization_playbook.md` +
`bq/query_cookbook.md`, but neither file existed. The optimization knowledge was real but scattered
across `data_catalog.md` / `data_knowledge.md` / 264 table docs.

**Built (LIVE 2026-07-27, commit fbaf936c):**
- `knowledge/bq/query_cookbook.md` (doc_type `bq_cookbook`) — §A cheapest-form query library, §B
  before/after tuning wins, §C fast-first approximation toolkit. Fast-first how-to = [[fast-first-bq]].
- `knowledge/bq/optimization_playbook.md` (doc_type `bq_playbook`) — fast-first→scale workflow, consolidated
  speed rules, `bq show -j <job_id>` join/stage attribution recipe.
- `perf_digest.py` two defects fixed (2026-07-27): `--mode repeats` keyed `sql_sha1` (wrapper logs
  `sql_sha256`) so it always printed `_(none)_`; `--mode phase-accuracy` let a cached 0-byte re-run
  overwrite the real full scan (last-write-wins) → ratio read 0.0. Both now correct (dogfooded on a live
  top-5-advertiser query; that test also caught the all_facts visit footguns, cookbook §A8).

**How to apply / infra facts learned:**
- `build_index.sh` pre-reserves doc_types `bq_cookbook` + `bq_playbook` (titles "Query cookbook" /
  "Optimization playbook"); it requires `doc_type` front-matter and folds `keywords:` into `_ROUTING.md`.
- `lint_coverage.py` skips any doc where `doc_type != bq_table` — so non-table bq/ guide docs need no
  coverage_state. Run `build_index.sh` after adding/editing a bq/ doc.
- The `perf-analyst` agent's two output sinks are these docs (cookbook §B, playbook § Observed rules).

**Deferred to cadence (NOT built; a future session / `/workflow-audit` should pick these up):**
- No pre-flight dry-run / `est_gb` / sample-gate exists. `config.env` declares `BQ_GB_ABORT/WARN/SAMPLE_SKIP_GB`
  and `enforce_bq_wrapper.sh`'s block message claims a "sample-first gate", but `bq_run.sh` implements
  none of it (intentional: no cost gating). Consequence: `perf_digest` prune-ratio is always inert.
- `perf_explain.py` (automate the `bq show -j` join-attribution recipe) — planned, not written.
- Systematic `## Observed cost` backfill: 9/264 table docs populated; fill top-N hot tables from
  `perf_digest --mode by-table`.
- Path/config drift: ~5 files reference a nested `knowledge/bq/bq_perf_log.jsonl` (real path is
  `knowledge/bq_perf_log.jsonl`); rotation size 40 MB (script) vs 20 MB (config.env).

Related: [[bq-workflow]], [[fast-first-bq]], [[project_structured_bq_catalog]], [[reference_bq_location_reservation]].
