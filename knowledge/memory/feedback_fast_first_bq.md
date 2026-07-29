---
name: fast-first-bq
description: "Fast-first BQ discipline: probe a small sample/APPROX for a directional answer in seconds before scaling to the exact full scan; optimize for wall-time/slots, not cost"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: cc00f377-b575-43ed-84cf-3e31ce190e7a
doc_type: memory
keywords: [fast_first_bq, sample probe, approx_count_distinct, tablesample, bq_run.sh, slot contention, wall-time, dry-run underestimate, optimization_playbook, query_cookbook]
domain: [bigquery, workflow]
lifecycle: active
last_verified: 2026-07-27
---
Answer BQ questions fast-first, then scale to exact. Get a directionally-correct answer in
seconds/rows before spending a big scan on a query whose shape might be wrong.

**Why:** MNTN runs on a reserved us-central1 slot pool and the standing rule is "stop considering cost"
([[bq-workflow]]). The real constraint is wall-time and slot contention: one large query at a time, a
hard 6-hour interactive wall, big queries queue behind each other. A wrong-shaped query that burns a 6h
slot is the expensive mistake, not the dollars.

**How to apply (the loop, documented in `knowledge/bq/optimization_playbook.md`):**
1. Shape probe (seconds): 1-day window / `TABLESAMPLE` / `FARM_FINGERPRINT`-sampled cohort, run with
   `bq_run.sh --phase sample --label "<name>"`. Confirm join keys, grain, columns — not the final number.
   Prefer a real 1-day sample over a dry-run (BQ dry-run under-estimates federated tables ~30×).
2. Approximate (seconds): `APPROX_COUNT_DISTINCT` / `HLL_COUNT.MERGE` / `APPROX_QUANTILES` for a ~1%-error
   directional read.
3. Confirm the sample predicted the full: `--phase full --label "<same name>"`, then
   `perf_digest.py --mode phase-accuracy` pairs them by label.
4. Scale to exact only when the decision needs the deliverable number.

**Recipes + rules:** `knowledge/bq/query_cookbook.md` (§C toolkit + §A query library) and
`knowledge/bq/optimization_playbook.md` (workflow + observed rules + join/stage attribution via
`bq show -j <job_id>`). Not a cost gate — never prompt "should I proceed?"; flag outliers descriptively
and run. Read-only creds: no scratch tables — materialize with CTEs / `CREATE TEMP FUNCTION` / BQ 24h
auto-cache, or push heavy repeats to Databricks ([[reference_databricks]]).
