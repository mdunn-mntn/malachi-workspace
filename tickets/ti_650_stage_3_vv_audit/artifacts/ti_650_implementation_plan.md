# VV IP Lineage — Implementation Plan

**Table:** `{dataset}.vv_ip_lineage` (silver layer — dataset TBD, e.g. `mes` or `logdata`)
**Owner:** targeting-infrastructure
**Repo:** `SteelHouse/sqlmesh` — new model file in `models/dw-main-silver/{dataset}/`

---

## 1. What the Table Is

One row per verified visit across all advertisers and all stages. Traces the IP address through every stage of the ad funnel: bid (auction) -> VAST (ad playback) -> redirect (clickpass) -> visit (site landing). Links each VV to its first-touch impression (Stage 1) and most recent prior VV (retargeting chain).

29 columns (v4). Partitioned by `trace_date` (DATE), clustered by `advertiser_id` + `vv_stage`.

Full schema: see `ti_650_column_reference.md`. V4 replaces `ft_*` columns with `s1_*` (chain traversal), removes boolean flags and `max_historical_stage`.

---

## 2. Source Tables

| Table | Role | Filter | Scan Size |
|-------|------|--------|-----------|
| `silver.logdata.clickpass_log` | Anchor — one row per VV | `time >= @start_dt AND time < @end_dt` | Target interval only |
| `silver.logdata.event_log` | CTV impression IPs (joined 3x: last-touch, prior VV LT, S1 chain LT) | `event_type_raw = 'vast_impression'` + 90-day lookback from `@start_dt` | ~90 days (single scan, ~3 TB) |
| `silver.logdata.cost_impression_log` | Display impression bid_ip (CIL.ip = bid_ip, 100% validated; replaces impression_log — has advertiser_id, ~20,000x smaller) | 90-day lookback from `@start_dt` | ~90 days (~800K rows/day/advertiser vs ~16B for impression_log) |
| `silver.summarydata.ui_visits` | Independent visit record (visit IP, impression IP, is_new) | `from_verified_impression = true` + +/- 7 day buffer | ~14 days |
| `silver.logdata.clickpass_log` (self) | Prior VV pool for retargeting chain | 90-day lookback from `@start_dt` | ~90 days |
| `bronze.integrationprod.campaigns` | Stage classification (`funnel_level`) | `deleted = FALSE` | Small dimension table |

**Optimization:** impression_log replaced by cost_impression_log (CIL) — CIL.ip IS bid_ip (100% validated, 794K rows). CIL has `advertiser_id` (impression_log does not), enabling ~20,000x row reduction for single-advertiser queries. Render IP lost — only differs from bid_ip 6.2% of the time (internal 10.x.x.x NAT). Each log table (event_log, CIL) is scanned once and joined 4 times. `COALESCE(el, cil)` in SELECT prefers CTV (event_log); CIL fills in NULLs for display inventory.

**Known performance issue:** BQ does not materialize CTEs — el_all is scanned 4 times (52 slot-hrs). See `ti_650_query_optimization_guide.md` for mitigation strategies (TEMP TABLE, semi-join, staging layer).

---

## 3. SQLMesh Model Config

```sql
MODEL (
  description 'One row per verified visit. Traces IP through bid -> VAST -> redirect -> visit.',
  owner 'targeting-infrastructure',
  tags ['ti', 'vv_lineage', 'mes'],
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column trace_date,
    lookback 48,
    batch_size 168,
    forward_only TRUE,
    on_destructive_change 'warn'
  ),
  start '2026-01-01',
  cron '@hourly',
  physical_properties (
    partition_expiration_days = 90,
    require_partition_filter = TRUE
  ),
  partitioned_by (trace_date),
  clustered_by (advertiser_id, vv_stage),
  grain (ad_served_id),
  gateway silver
);
```

| Setting | Value | Rationale |
|---------|-------|-----------|
| `cron '@hourly'` | Runs every hour | Matches existing silver model patterns (clickpass_log, visits) |
| `lookback 48` | Reprocess last 48 hours each run | Handles late-arriving clickpass/event data |
| `batch_size 168` | 7 days per backfill chunk | Prevents single massive query during initial backfill |
| `forward_only TRUE` | No auto-rebackfill on schema changes | MNTN convention — avoids surprise full-table rewrites |
| `partition_expiration_days = 90` | Auto-drop partitions older than 90 days | Keeps table size bounded; campaigns typically run 2 weeks to 2 months |
| `start '2026-01-01'` | Backfill from Jan 1, 2026 | ~65 days of initial backfill |

---

## 4. How Hourly Runs Work

Each hour, SQLMesh:

1. Determines the current interval (1 hour)
2. Also reprocesses the prior 48 hours (lookback)
3. For each interval: DELETEs existing rows for that `trace_date` range, then INSERTs fresh results
4. The SQL uses `@start_dt` / `@end_dt` — SQLMesh injects the interval timestamps automatically

**What happens per hourly run:**
- `clickpass_log` is filtered to the interval window (e.g., 48 hours)
- `event_log` is scanned for a 90-day window ending at `@end_dt` (hardcoded in SQL, not the SQLMesh lookback)
- `ui_visits` is scanned +/- 7 days from the interval window
- `prior_vv_pool` (clickpass_log self-join) scans 90 days back from `@start_dt`

**Idempotency:** SQLMesh handles DELETE+INSERT automatically. Re-running the same interval produces the same result. No manual cleanup needed.

---

## 5. Backfill Plan

### Initial Backfill (one-time)

- **Range:** 2026-01-01 to present (~65 days as of 2026-03-05)
- **Chunk size:** 168 hours (7 days) per batch — set via `batch_size`
- **Estimated chunks:** ~10 batches of 7 days each
- **SQLMesh handles this automatically** on first `plan apply` — it identifies all unprocessed intervals from `start` to now and works through them in 7-day batches
- **No manual intervention needed** — just apply the plan and let it run

### Performance Reference

- 1 advertiser, 1 week: ~9 minutes (on default BQ slots)
- All advertisers, 1 week: TBD (linear scale estimate: 9 min x ~3,000 advertisers would be impractical without optimization)
- **Dustin's recommendation:** Consider an hourly staging materialization of the source data, then run the final model over the pre-filtered data. This dramatically reduces scan size.

### Cost Estimate (on-demand pricing)

| Scenario | Scan | Cost |
|----------|------|------|
| Daily incremental (1 day, all advertisers) | ~4.7 TB | ~$29 |
| 60-day backfill (batched 7 days at a time) | ~9.4 TB total | ~$47 |
| Monthly ongoing | ~141 TB | ~$870 |

Note: cost_impression_log replaces impression_log (~negligible scan vs ~1.9 TB). event_log remains ~3 TB per 90-day scan (scanned 4x = ~12 TB effective). See `ti_650_query_optimization_guide.md` for TEMP TABLE strategy to reduce to single scan.

Note: MNTN uses reserved slots (`dw-main-bronze:us-central1:reservations/background-jobs`), not on-demand pricing. Actual cost depends on slot allocation.

---

## 6. Data Retention & Historical Data

| Question | Answer |
|----------|--------|
| How long do we keep data? | 90 days (`partition_expiration_days = 90`). Partitions older than 90 days are auto-dropped by BigQuery. |
| Why 90 days? | Campaigns typically run 2 weeks to 2 months. 90 days covers the full lifecycle of most campaigns plus buffer. |
| Can we query historical data beyond 90 days? | No — expired partitions are gone. If long-term historical analysis is needed, we'd either increase retention or create a separate archive/rollup table. |
| What about the event_log 90-day lookback? | Each run scans 90 days of event_log to find impressions for any VV in the target interval. This is a scan window, not a retention window — the event_log itself has its own retention. |

---

## 7. Decisions Needed from dplat

1. **Dataset name:** Where does this table live in the silver layer? Options:
   - `mes.vv_ip_lineage` (new `mes` dataset for MES audit tables)
   - `logdata.vv_ip_lineage` (alongside existing log tables)
   - Other?

2. **Staging layer:** Should we implement Dustin's two-layer approach (hourly staging materialization of source data, then daily final model)? Or start with the direct single-model approach and optimize later if performance is an issue?

3. **Slot allocation:** The initial backfill and the 90-day event_log scan are expensive. Any concerns with running this on the existing `background-jobs` reservation?

4. **Retention:** Is 90 days appropriate, or should we use a different window?

5. **Alerting:** The `targeting-infrastructure` owner is already registered in `owners.py` pointing to `#monitor-test`. Should this route to a different Slack channel?

---

## 8. Implementation Steps

1. **Get review from dplat on this plan** (decisions above)
2. **Send Zach the column reference PDF + preview query** for schema review
3. **Clone `SteelHouse/sqlmesh` repo**
4. **Add model file** at `models/dw-main-silver/{dataset}/vv_ip_lineage.sql`
5. **Test in dev environment:** `sqlmesh plan dev` — validates SQL, creates dev tables
6. **Validate output** against existing Q3 preview results (advertiser 37775, 2026-02-07)
7. **Promote to prod:** `sqlmesh plan` — triggers backfill from 2026-01-01
8. **Monitor initial backfill** — watch for slot pressure, query failures
9. **Verify steady-state hourly runs** — confirm 48-hour lookback handles late data correctly

---

## 9. Full SQL

See `queries/ti_650_sqlmesh_model.sql` for the complete model file (MODEL config + SQL query).
