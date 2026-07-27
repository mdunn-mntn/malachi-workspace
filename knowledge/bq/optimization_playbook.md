---
doc_type: bq_playbook
title: Optimization playbook
summary: "the fast-first->scale workflow + the cross-cutting BQ speed rules + how to attribute which join took longest"
keywords: [optimization playbook, tune slow query, tune expensive query, fast first, sample first, scale to exact, slot contention, one large query at a time, 6 hour wall, reservation, us-central1, partition pruning, TIMESTAMP not DATE, literal date, SELECT star, projection, join attribution, shuffle spill, query plan, materialization, Databricks]
last_verified: 2026-07-27
source: data_catalog.md + data_knowledge.md + memory (feedback_bq_workflow, reference_bq_location_reservation, reference_databricks) + perf tooling
tags: [optimization, cost, playbook]
---

# Optimization playbook

The rules for making a BigQuery query fast here, and the workflow for getting a directionally-correct
answer before you spend a big scan on the wrong shape. Companion to
[query_cookbook.md](query_cookbook.md) (the *recipes*; this is the *rules*). Mine the evidence behind
these with `.claude/scripts/perf_digest.py --mode all` over `knowledge/bq_perf_log.jsonl`.

**The constraint is time, not money.** MNTN runs on a reserved us-central1 slot pool and the standing
directive is "stop considering cost." So optimize for wall-time and slot availability, never dollars.
There is no cost gate and no "should I proceed?" prompt: flag an outlier scan descriptively and run it.

---

## The Fast-First → Scale workflow

The default order of operations for any non-trivial question. Each step answers "is my query shaped
right?" before the next spends more.

1. **Shape probe (seconds).** Run against a tiny slice first: a 1-day window, `TABLESAMPLE SYSTEM (n PERCENT)`
   on an unpartitioned table, or a `FARM_FINGERPRINT`-sampled cohort. Call it with
   `bq_run.sh --phase sample --label "<name>"`. You are confirming the join keys, the grain, and the
   result columns, not the final number. Prefer a real 1-day sample over a dry-run: BQ's dry-run
   under-estimates federated/external tables ~30× (§ Observed rules).
2. **Approximate (seconds).** Get the directional answer with `APPROX_COUNT_DISTINCT` / `HLL_COUNT.MERGE`
   / `APPROX_QUANTILES` / `APPROX_TOP_COUNT` (cookbook §C). ~1% error is almost always fine for a
   go/no-go read.
3. **Confirm the sample predicted the full.** Re-run the same query at full scope with
   `--phase full --label "<same name>"`, then `perf_digest.py --mode phase-accuracy` reports the
   full/sample ratio. A wild ratio means the sample was not representative — fix the shape before trusting it.
4. **Scale to exact only when the decision needs it.** Swap `APPROX_*` for exact counts, widen the
   window, drop the sample. Do this once, deliberately, for the number that goes in the deliverable.

Same `--label` across the sample and full runs is what lets the tooling pair them. `phase` is logged per
run in the perf log.

---

## Observed rules

Cross-cutting rules that hold across many tables and queries. The `perf-analyst` agent appends new ones
here on its weekly pass; per-table specifics live in each table doc's `## Observed cost`.

### Routing & slots
- **Jobs must run in us-central1.** The org reservation `dw-main-bronze:us-central1.background-jobs`
  only covers us-central1 jobs; a job in the US multi-region gets no reservation and bills on-demand at
  $6.25/TiB. `bq_run.sh` injects `--location=us-central1`; `~/.bigqueryrc` covers plain `bq`. The leak is
  **dataset-less queries** (inline `--external_table_definition` GCS scans, `SELECT 1` tests) which can't
  infer a location and default to US. Pass `--location=us-central1` explicitly on any external-table-only
  query. (AUDI-1089 leaked ~140 TiB ≈ $875 this way.) See [[reference_bq_location_reservation]].
- **One large query at a time.** The adhoc/reservation slot pool is intentionally small and does not
  auto-scale. Two concurrent 18 TB scans took 12+ hours each vs ~2 hours solo (TI-650). Run big scans
  sequentially; stagger the DDP external scans the same way. Concurrent small queries (<1 TB) are fine.
- **Hard 6-hour interactive wall.** Cannot be raised. Long multi-day `augmentor_log` / lift queries hit
  it and die with zero output. If a run risks >4h, push it to Databricks (no wall, GCS-native reads,
  memory-optimized clusters for shuffle-heavy joins). See [[reference_databricks]].

### Partition pruning
- **`TIMESTAMP()` not `DATE()`.** Silver log views (`logdata.*`, `summarydata.*`) are UNION ALL over a
  `time`-partitioned raw table and a history table. `WHERE time >= TIMESTAMP('...') AND time < TIMESTAMP('...')`
  pushes down and prunes; `WHERE DATE(time) BETWEEN ...` defeats pruning and scans every partition
  (9+ min vs near-instant). SQLMesh `@start_dt`/`@end_dt` are already TIMESTAMP — use directly.
- **Literal date, not a subquery.** `WHERE dt = (SELECT MAX(dt) ...)` does not prune (164.9B rows / 280s
  for a 1-day COUNT on ipdsc). Probe the latest `dt` in a cheap separate query, then inline the literal.
- **Every log/event table needs a date filter.** Even filtering by an id, `clickpass_log` without a date
  filter scans 2,238+ partitions (110 GB). Add the `time` bound to scan one.
- **The silver LOG tables (event/impression/viewability) have no data before 2025-01-01** (their history
  physicals start 2025-01-01). This is NOT true of other tables — `ui_visits`/`visits` reach 2023-01-01,
  `sum_by_campaign_by_day` 2024-01-01, CIL 2023-10-01, `all_facts` ~2020-10. Check a table's actual `MIN`
  before assuming a floor, and don't scan "to be thorough" past where the data starts. Pre-2025 *log* data
  lived only in Greenplum coreDW (deprecated 2026-04-30).

### Scan shape
- **Project columns; never `SELECT *`.** On wide tables `SELECT *` costs ~150× a narrow projection
  (measured 152× on the 84-column `sum_by_campaign_by_day`: 94.3 MB/day vs ~635 KB/day).
- **Aggregate for verification.** To confirm rows exist, `GROUP BY ... COUNT(*)` scans the same bytes as
  returning raw rows but shuffles far less output.
- **Don't double-aggregate.** A query emitting both per-entity and pooled rows runs the heavy join twice.
  Emit the fine grain and reconstruct the pooled number in Python (`pooled = SUM(per_entity)` when the
  grain keys are unique). This killed a TI-933 query at the 6h wall.
- **Query log tables individually, not one UNION ALL** when tracing across event/impression/viewability —
  three jobs skip tables early and get better slot allocation than one combined job.
- **Genuinely unpartitioned tables:** a `WHERE time` filter does NOT prune and can make cost worse; the
  only levers are column projection and `TABLESAMPLE` / `LIMIT`. Confirm partitioning with `bq show` first
  — the base `bidder_bid_events` IS hour-partitioned on `time` (the filter is mandatory there); some
  `_test`/`_optimized` variants are the unpartitioned case.

### Approximation & sizing
- **Prefer `APPROX_COUNT_DISTINCT` / `HLL_COUNT.MERGE`** over exact DISTINCT on full-partition scans
  (~1% error). Reach columns are BYTES HLL++ sketches — merge, never `SUM`.
- **3P (DS35) delivery is bursty** (~2-4 load-days/month per category). Size over ≥30 days and report the
  last-delivery `dt`, or query a single known load-day. Never trust a single-day/single-week 3P number.

### Read-only materialization
- No `CREATE TABLE`/DDL under our creds. Materialization = `WITH` CTEs, `CREATE TEMP FUNCTION` (session
  UDFs), and BQ's automatic 24h query cache. Route a genuinely heavy repeat to Databricks or one CTE
  query, not a scratch table.

---

## Attributing which join / stage took longest (and why)

The compact perf log keeps whole-query cost but strips the per-stage plan. The `job_id` is preserved, so
the plan is recoverable on demand. To find out *why* a query was slow:

1. Recover the job: `python3 .claude/scripts/bq_verify.py "<label | ticket | sql_sha256>"` prints the
   matching perf record(s) with the `job_id`.
2. Pull the full plan: `bq show --format=prettyjson -j <job_id>`.
3. Read `statistics.query.queryPlan[]` and rank stages by the bottleneck signal:
   - **`slotMs`** — where the compute went. The top-slotMs stage is your hot join/aggregate.
   - **`waitMsAvg` / `waitMsMax`** — the stage sat waiting for slots (contention, not the query's fault:
     re-check "one large query at a time").
   - **`shuffleOutputBytesSpilled` > 0** — the join spilled shuffle to disk: the join is too wide or the
     keys are skewed. Narrow the window, project fewer columns, or reduce the join fan-out.
   - **`recordsRead` >> `recordsWritten` on a JOIN stage** — fan-out blow-up (a non-unique join key
     multiplying rows). Check the key cardinality on a sample first.
   - **stage name / `computeMode`** — a late `Output`/`Join` stage reading billions of records points at
     the aggregate/output, not the scan (the TI-933 double-aggregation signature).

Record a durable finding as a one-liner in the relevant table doc's `## Observed cost` (or as a §B
before/after row in the cookbook if it generalizes). Automating steps 2-3 into a `perf_explain.py` helper
is a planned follow-up.
