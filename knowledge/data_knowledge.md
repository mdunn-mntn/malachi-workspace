# Data Knowledge — MNTN BigQuery

## Architecture

### Full Data Stack (top to bottom)
```
bronze.integrationprod  ← Postgres CDC replica (Datastream). Source for config/dimension data.
bronze.raw              ← Raw event tables (bidder, augmentor, pixel). Short TTLs.
  ↓ SQLMesh (bronze layer)
bronze.sqlmesh__raw     ← Bronze SQLMesh models (versioned)
bronze.raw.*  (VIEWs)  ← View aliases to bronze.sqlmesh__raw
  ↓ SQLMesh (silver layer)
silver.sqlmesh__logdata / summarydata / aggregates  ← Silver SQLMesh models (versioned)
silver.logdata / summarydata / aggregates  (VIEWs)  ← Clean view aliases (query these!)
silver.core  (VIEWs)   ← Direct views over bronze.integrationprod.core_* (no SQLMesh)
silver.fpa   (VIEWs)   ← Direct views over bronze.integrationprod.fpa_* (no SQLMesh)
```

### SQLMesh Repo & Model Conventions

**Repo:** `git@github.com:SteelHouse/sqlmesh.git`

**Directory structure mirrors medallion layers:**
```
models/
├── dw-main-bronze/raw/              ← bronze ingestion (hourly incremental)
├── dw-main-bronze/integrationprod/  ← CDC dimension tables
├── dw-main-silver/logdata/          ← VIEWs reshaping bronze → silver
├── dw-main-silver/ber_stg/          ← heavy incremental models (visits, conversions)
├── dw-main-silver/aggregates/       ← rollups
└── dw-main-gold/                    ← end-product tables
```

**Config:** `config.py` (not YAML). Gateways: `bronze`, `silver`, `gold`. Each maps to `dw-main-{layer}` project. State DB: GCP Postgres (`dw-main-bronze:us-central1:data-platform-state`). Dialect: `bigquery`.

**Common INCREMENTAL_BY_TIME_RANGE patterns (from existing models):**
- `cron '@hourly'` — standard for event-level tables
- `lookback 48` — reprocess 48 hours for late-arriving data
- `batch_size 168` (7 days) or `49` — chunks for backfill
- `forward_only TRUE` — no automatic rebackfill on schema changes
- `partition_expiration_days` — set in `physical_properties`
- Date filter: `time >= @start_dt AND time < @end_dt` (TIMESTAMP macros)
- Hardcoded lookbacks (e.g., 90-day event_log scan) go in the SQL, not the MODEL config

**Registered owners** (`owners.py`): `targeting-infrastructure`, `ber`, `RPLAT`, `bae`, `test`. Each has a Slack channel for audit/failure alerts.

### SQLMesh Table Tags (Supported vs. Unsupported)
SQLMesh model definitions can carry `tags` that link a table to a topic in the internal data
documentation app. A table is considered **supported** if it appears under a topic via a tag;
all other tables are **unsupported**.

Tags are added in the `MODEL()` block of a SQLMesh `.sql` model file:
```sql
MODEL (
  name logdata.impression_log,
  tags ['impressions_raw'],
  ...
);
```

Topic definitions (title, description, subtopics → tag mappings) live in a separate YAML file
maintained by the data platform team. Tags are part of the regular dev workflow and can be
linted/reviewed in PRs. When looking for the canonical table for a use case, filter by
"supported" in the data doc app — it immediately excludes dev/staging tables.

Reference: `documentation/docs/data_documentation_app.md`

### SQLMesh Versioned Table Pattern
`dw-main-silver.logdata`, `dw-main-silver.summarydata`, and `dw-main-silver.aggregates` are
**view layers only**. Every table is a VIEW pointing to a physically versioned table in the
corresponding `sqlmesh__*` dataset:

```
logdata.impression_log  (VIEW)
  → sqlmesh__logdata.logdata__impression_log__4185451957  (VIEW)
      → [upstream: coredw / Postgres source]

logdata.spend_log  (VIEW)
  → sqlmesh__logdata.logdata__spend_log__4068879977  (TABLE, HOUR partition on auction_timestamp)
```

The numeric hash suffix (e.g. `__4185451957`) is the SQLMesh model version. The view in `logdata`
always points to the current production version. Do NOT query hashed tables directly — always
use the clean alias in `logdata.*`.

### Partition Filter Best Practice — Silver Log Tables

**Critical:** Silver layer views (`logdata.*`, `summarydata.*`) are UNION ALL views of two underlying tables:
- **Recent table**: `dw-main-bronze.sqlmesh__raw.raw__*` — partitioned by `time` (TIMESTAMP, DAY partition)
- **History table**: `dw-main-bronze.sqlmesh__history.history__*` — partitioned by `date_column`

**Physical table retention (verified 2026-03-16):**

| Physical Table | Dataset | Earliest Data | TTL | Size (approx) |
|---|---|---|---|---|
| `history__event_log__1601996237` | `sqlmesh__history` | 2025-01-01 | none | ~12 TB |
| `history__impression_log__2959498407` | `sqlmesh__history` | 2025-01-01 | none | ~10 TB |
| `history__viewability_log__3987498553` | `sqlmesh__history` | 2025-04-08 | none | ~1.2 TB |
| `raw__event_log__2961306213` | `sqlmesh__raw` | 2026-01-01 | 365d | ~1.6 TB |
| `raw__impression_log__1553762165` | `sqlmesh__raw` | 2025-08-25 | 90d | — |
| `raw__viewability_log__4234484773` | `sqlmesh__raw` | 2025-12-31 | 90d | ~216 GB |

**No BQ table at any layer has data before 2025-01-01.** Pre-2025 data only exists in Greenplum coreDW (deprecated April 30, 2026). When searching for historical IP/impression data, querying the physical tables directly (bypassing the silver VIEW) does NOT extend coverage — it only avoids the UNION overhead.

BQ can push down filters to the underlying `time`-partitioned raw tables only with **direct TIMESTAMP comparisons**. Wrapping the column in `DATE()` defeats partition pruning.

**Correct (enables partition pruning):**
```sql
WHERE time >= TIMESTAMP('2026-02-04') AND time < TIMESTAMP('2026-02-11')
```

**Wrong (prevents partition pruning — scans all partitions):**
```sql
WHERE DATE(time) BETWEEN '2026-02-04' AND '2026-02-10'
```

SQLMesh model date parameters (`@start_dt`, `@end_dt`) are already TIMESTAMP type — use them directly without wrapping. Confirmed 2026-03-06 (queries using `DATE()` ran 9+ minutes vs near-instant with `TIMESTAMP()`).

### IP Search Optimization — Cross-Stage/Cross-Table Queries

Searching for a single IP across full table history (event_log, impression_log, viewability_log) is extremely expensive because these tables are partitioned by time, not IP. There is no IP index.

**Measured costs (TI-650, 2026-03-16):**

| Query Pattern | GB Billed | Wall Time | Partitions |
|---|---|---|---|
| event_log full scan (`DATE(time)`) | 14,677 GB | 35 min | 440 |
| impression_log full scan (`DATE(time)`) | 11,829 GB | 23 min | 440 |
| viewability_log full scan (`DATE(time)`) | 1,563 GB | 5 min | 343 |
| clickpass_log no date filter | 110 GB | 29s | 2,238 |
| IP funnel trace (single-day, TIMESTAMP) | 1,136 GB | 39s | 18 |

**Optimization rules for IP searches:**

1. **Use TIMESTAMP(), not DATE()** — `DATE(time)` defeats partition pruning on every table (documented above). Use `time >= TIMESTAMP('2025-07-01') AND time < TIMESTAMP('2026-03-01')` instead of `DATE(time) BETWEEN '2025-07-01' AND '2026-02-28'`.

2. **Narrow the date range** — If you know the campaign group creation date (e.g. 2025-07-11) and the VV date (e.g. 2026-02-04), search only that window. Don't scan from 2025-01-01 "to be thorough" — it adds months of unnecessary partitions.

3. **Drop LIKE wildcards for IP matching** — `ip LIKE '216.126.34.185%'` is unnecessary. IPs in silver log tables do not carry CIDR suffixes (`/32`, `/24`). Use exact match: `ip = '216.126.34.185'`. The LIKE adds string comparison overhead on billions of rows. **Exception:** `event_log.ip` has `/32` CIDR suffix on ALL pre-2026 data — use `SPLIT(ip, '/')[SAFE_OFFSET(0)]` when matching event_log IPs across time periods (see CIDR bullet under Stage Progression).

4. **Always add a date filter to clickpass_log** — Even when filtering by `ad_served_id`, clickpass_log has 2,238+ partitions. Without a date filter, BQ scans all of them (110 GB). Add `AND time >= TIMESTAMP('2026-02-04') AND time < TIMESTAMP('2026-02-05')` to scan 1 partition instead.

5. **Use aggregation for verification** — If you only need to confirm zero S1/S2 rows exist, use `GROUP BY campaign_group_id, campaign_id, funnel_level` with `COUNT(*)` instead of returning raw rows. Same bytes scanned but less output shuffling.

6. **Query tables individually, not UNION ALL** — The cross-stage UNION ALL query (event_log + viewability_log + impression_log) forces BQ to process all three in one job. Running them as three separate queries lets you skip tables early (e.g., if viewability_log returns 0 rows, you save time on interpretation, and individual queries may get better slot allocation).

Some `sqlmesh__logdata` tables are themselves VIEWs referencing other datasets:
- `bid_attempted_log` and `bid_events_log` → both reference `bidder_bid_events` (same data, different filters)
- `bid_logs` → references Beeswax bid_logs upstream
- `auction_log` → references `v_augmentor_log`
- `win_logs` → references Beeswax win_logs upstream
- `icloud_vv_log` → references icloud_vv table upstream

### Slot Contention — Never Run Large Queries Simultaneously

The adhoc BQ reservation has limited slots. Running two 4+ TB queries concurrently causes 3-5x runtime inflation (queries that take 2-5 min solo take 12+ hours concurrent). Always run large queries sequentially.

**Learned from TI-650 (2026-03-18):** Two 18 TB queries run simultaneously took 12+ hours each instead of ~2 hours solo. The adhoc reservation does not auto-scale — concurrent jobs compete for the same fixed slot pool.

**Rule:** One large query at a time. Queue the next after the previous completes. For small queries (<1 TB), concurrent execution is fine.

### Upstream Source Systems
- **Postgres (coredw)**: impression_log, click_log, clickpass_log, event_log, conversion_log,
  guid_log, viewability_log come from Postgres. Evidence: `conversion_log` has `inet` fields
  cast to STRING, and clickpass_log comments note "doesn't exist in coredw."
- **Bidder service**: bidder_bid_events, bidder_auction_events are written directly by the bidder.
- **Beeswax exchange**: bid_logs, win_logs come from Beeswax (external DSP).
- **Spend pipeline**: spend_log is produced by the spend/billing pipeline from bidder events + wins.

---

## Business Logic

### is_new Column
`is_new = TRUE` means the guid/cookie is a **first-time visitor to that advertiser's site**
(i.e., no prior recorded page view for that advertiser_id + guid combo). Appears in:
clickpass_log, guid_log, cost_impression_log, visits, ui_visits.

### visit_facts vs visits vs ui_visits
- `visits` (silver.summarydata): row-level, one row per visit event. Partitioned by time (DAY).
- `ui_visits` (silver.summarydata): row-level VIEW on top of `visits`, used by the UI.
  Same schema as visits but with added computed fields (visit_day, source_type, etc.).
  Note: `ip` column has a known upstream bug — two ip fields exist temporarily.
- `visit_facts` (silver.summarydata): **pre-aggregated**, one row per advertiser+campaign+geo+device+hour.
  Use for performance reporting. Partition: DAY on `hour`, cluster: advertiser_id, campaign_id.

### conversions vs conversion_facts
- `conversions`: row-level, one row per conversion event. 60-day rolling retention.
- `conversion_facts`: pre-aggregated by advertiser+campaign+geo+hour. Use for reporting.

### all_facts VIEW
`summarydata.all_facts` is the kitchen-sink reporting view: joins visit_facts + conversion_facts
+ spend_facts + more. Very wide (150+ columns). Use only when you need cross-metric analysis.
Prefer the individual facts tables when possible (faster, cheaper).

### competing_ prefixed columns
Columns prefixed with `competing_` in visit_facts and conversion_facts represent metrics for
impressions where the advertiser was NOT the attributed touch (i.e., they were "competing"
for credit). Used in incremental attribution analysis.

### probattr_ prefixed columns
Columns prefixed with `probattr_` = probabilistic attribution model metrics, as opposed to
deterministic last-touch or last-tv-touch attribution.

### attribution_model_id
- `attribution_model_type_id = 0` should be treated as `1` (last-touch) — known business rule.
  See `ui_visits` column description comment.
- Present in clickpass_log, ui_visits, ui_conversions, conversions, visits.
- Distinct values for advertiser 37775 (7-day sample): 1, 2, 3, 9, 10, 11. Model 9 most common (49%).

### guid (user/device cookie ID)
- Present in every log table: clickpass_log, event_log, cost_impression_log, impression_log, ui_visits, viewability_log, etc.
- Persists across multiple VVs — same user/device across visits. Top user: 123 VVs in 7 days (advertiser 37775).
- clickpass_log.guid matches ui_visits.guid 99.99% (same session context).
- clickpass_log.guid matches event_log.guid ~60% (different context — impression vs visit).
- `original_guid` (clickpass_log only) = pre-reattribution guid. Differs from guid in ~16% of VVs.
- `page_view_guid` (clickpass_log only) = GUID from page view signal.
- **guid as S1 resolution key (v11):** ~18-23% of previously unresolved S2/S3 VVs can be linked to an S1 VV via matching guid (same user at different IP). Additional ~7-9% can be linked to an S1 impression via guid. guid-based tiers are the highest-impact new resolution paths after IP-based methods.

### viewability_log and VV IP lineage
- viewability_log has ad_served_id, ip, bid_ip, guid, campaign_id, time — same schema as CIL for IP purposes.
- **Not useful for S1 resolution:** For advertiser 37775, zero S1 impressions exist in viewability_log. CIL already covers display impressions comprehensively.
- Zach suggested investigating it for display viewable inventory IPs, but empirical check shows no incremental coverage.
- **Multiple rows per ad_served_id (display only):** `viewability_type_id` 1=measurable, 2=viewable. Each display impression produces two viewability events. CTV does NOT go through viewability_log — uses event_log VAST events instead. When joining, either GROUP BY to dedup or keep both rows if you need the type breakdown.
- **viewability_log is the display equivalent of event_log:** Use it to trace viewable display impressions in the pipeline (clickpass → viewability_log → win_logs → bid_logs).

### pa_model_id
Probabilistic attribution model ID. Present in visit_facts, conversion_facts, visits, ui_visits.

### Epoch Units (CRITICAL — varies by table)
| Table | epoch column | Unit |
|-------|-------------|------|
| spend_log | auction_epoch | **nanoseconds** |
| bidder_bid_events | auction_epoch | **microseconds** |
| bidder_bid_events | epoch | **milliseconds** |
| bidder_auction_events | epoch | (milliseconds, not explicitly documented) |
| impression_log | epoch | (seconds — inherited from Postgres) |
| v_augmentor_log | epoch | **milliseconds** |
| clickpass_log | epoch | (seconds) |
Always verify epoch units before using for time math.

### spend_log.auction_id vs exchange_auction_id
- `auction_id` = `<exchange_id>.<auction_id>` — MNTN's composite identifier (also called mntn_auction_id)
- `exchange_auction_id` = the raw auction ID from the exchange itself

### device_type ENUM
Valid device_type values are defined in `dw-main-bronze.integrationprod.device_type`.
Always join there for human-readable labels.

### bidder_bid_events vs bid_attempted_log vs bid_events_log
All three expose the same underlying `bidder_bid_events` table:
- `bid_attempted_log` = attempted bids (may or may not have won)
- `bid_events_log` = same view as bid_attempted_log (currently identical, may have filter differences)
- Use `spend_log` for **won** impressions with cost data.

### partner_id
Appears in spend_log, bidder_bid_events, bidder_auction_events:
- Describes which exchange partner (Beeswax vs MNTN bidder)

### is_test Flag
`is_test = TRUE` in spend_log and bidder_auction_events means test/QA auctions — **exclude from
production analysis**.

**GOTCHA (TI-504):** `is_test` campaigns have structurally lower IVR — **8-53% of production campaigns even within the same intent tier**. Cannot compare test vs non-test campaign performance directly. Use within-test-group comparisons (control vs treatment) for experiments. The cause is unknown but likely involves delivery priority, creative/budget differences, or bidder behavior differences for test campaigns.

### advertiser_household_score (HHST)
Available on `cost_impression_log`. Segments impressions by intent tier:
- **HI (High Intent):** HHST >= 6666
- **MI (Mid Intent):** HHST 3333-6665
- **Max Reach / PP:** HHST 1-3332

Old prospecting campaigns serve **89-99% HI tier** traffic. Virtually no MI/PP historical baseline exists for prospecting campaigns.

### ip vs ip_raw vs original_ip vs bid_ip vs impression_ip

Zach explained the full IP column taxonomy on 2026-02-25 call and confirmed in docx review 2026-03-03:

| Column | Present in | What it is |
|--------|-----------|------------|
| `ip` | most tables | **The IP used for all logic** — enriched/preferred IP. VV tracing, targeting, and geo all use this. |
| `ip_raw` | clickpass_log, event_log, others | Raw IP before MNTN enrichment. Usually identical to `ip`. |
| `original_ip` | event_log, cost_impression_log, others | Pre-iCloud Private Relay IP — raw TCP connection IP from x-forwarded-for header. MNTN overrides this with a more accurate device IP stored in `ip`. Use `ip` for analysis; `original_ip` for audit/debug only. |
| `bid_ip` | event_log, click_log, impression_log | IP at bid/auction time for the associated impression. In event_log, `bid_ip` = win_log.ip = cost_impression_log.ip at 100% (validated 30,502 rows, TI-650). The gold column for IP lineage — no need to join win_log or CIL. **cost_impression_log.ip IS the bid_ip** — confirmed by joining CIL to impression_log on impression_id=ttd_impression_id: 794,050/794,050 (100%) match bid_ip; only 745,169 (93.8%) match render_ip. When they differ, render_ip is internal (10.x.x.x NAT/proxy), CIL.ip = public bid_ip. CIL also has `advertiser_id` (impression_log does not), making it far cheaper to query for single-advertiser analysis. |
| `impression_ip` | ui_visits | Bid IP carried forward from impression_log onto the visit record. Matches event_log.bid_ip at 95.8–100% (mismatch ~2–4% for CTV-heavy advertisers where impression_ip may reference a different impression than last-touch ad_served_id). Fallback for non-CTV VVs where event_log has no row. |

**Rule:** Use `ip` for analysis. `bid_ip` to trace back to bid time. `impression_ip` as non-CTV fallback. `original_ip` only for pre-relay audit.

### icloud_ tables
`icloud_vv_log`, `icloud_guids`, `icloud_ipv4`, `icloud_ipv6` relate to Apple iCloud Private Relay
traffic handling. IPs from iCloud relay require special treatment for geo-targeting.

---

## Data Quality Notes

### Known Issues
1. **ui_visits.ip**: upstream bug — two IP columns (ip + ip_raw) present. Comment says "revert once ip is fixed upstream."
2. **clickpass_log.first_touch_time**: "doesn't exist in coredw, but keep it just in case" — unreliable, may be NULL.
3. **conversion_log._col_23**: unnamed JSON column (raw artifact from Postgres migration).
4. **cost_impression_log.recency_elapsed_time**: INTERVAL type — BQ doesn't support INTERVAL in all contexts.
5. **clickpass_log.first_touch_ad_served_id NULL (~40%)**: The lookup for `first_touch_ad_served_id` requires a CTV impression with `funnel_level=1` and `objective_id=1` from the same campaign group. The system searches on **both bid_ip AND ip of the attributable impression** (Sharad, 2026-03-04). Open question: does "both" mean OR (either match) or AND (both must match)? A9a results show ft_null is 54.85% when mutated vs 38.19% when not (+16.66pp delta), but mutation explains only ~15% of NULLs. Sharad: *"The fact that we are not able to find such records for a high number of VVs points to some issue in the targeting."* The 40% NULL rate is a known problem, not a design choice.

### BigQuery Behavioral Gotchas
- **GENERATE_UUID() is non-deterministic across CTE references (TI-650, 2026-03-19):** BQ CTEs are NOT guaranteed to be materialized. If a CTE uses `GENERATE_UUID()` and is referenced in multiple places (e.g., 3 UNION ALL blocks), each reference re-evaluates the function and gets a DIFFERENT UUID. Fix: use a deterministic hash like `TO_HEX(MD5(key_column))` or `FORMAT('%s-...', SUBSTR(TO_HEX(MD5(key)), ...))` when the UUID must be stable across references.
- **Display impression timing gap (TI-650, 2026-03-19):** Display impressions can be served 2-4 weeks before the user visits the site and triggers the VV. When tracing S3 VVs back to their impression via `ad_served_id`, a ±7d window around the VV time misses 35% of display impressions. CTV is same-day. **Use ±30d for the 5-source trace when display campaigns are in scope.** Verified on 24+ advertisers.

### Retention / TTL
| Table | Retention |
|-------|-----------|
| silver.logdata.clickpass_log | **No TTL** — confirmed 2026-03-03 (expirationTime: none, no partition expiry) |
| silver.logdata.event_log | **No TTL** — confirmed 2026-03-03 (expirationTime: none, no partition expiry) |
| bidder_bid_events | 90 days (expirationMs on partition) |
| bid_logs (silver.logdata) | **TTL confirmed empirically (TI-650, 2026-03-23):** 10/10 tested ad_served_ids had impression_log records but bid_logs records gone (no time filter). impression_log.ip for display is internal NAT (10.105.x.x) — useless without bid_logs join. |
| bid_logs_enriched | 90 days |
| event_log_filtered | 60 days |
| conversions | 60 days |
| spend_log_tmp (in logdata) | Unknown — likely short-term staging |
| spend_log | No expiry — HOUR partitioned |

---

## Join Keys Reference

| Join | Left | Right | Key |
|------|------|-------|-----|
| Visit → Impression | summarydata.visits | logdata.impression_log | ad_served_id |
| Conversion → Visit | summarydata.conversions | summarydata.visits | guid + advertiser_id |
| Spend → Bidder | logdata.spend_log | logdata.bidder_bid_events | auction_id / bid_id |
| Bidder bid → auction | bidder_bid_events | bidder_auction_events | auction_id |
| Any → Campaign | any | silver.core.* | campaign_id |
| Any → Advertiser | any | silver.core.* | advertiser_id |
| device_type label | any | bronze.integrationprod.device_type | device_type |

---

## Dataset Disambiguation

### logdata vs sqlmesh__logdata
Query `logdata.*` — it always points to the current version. Never query `sqlmesh__logdata.*` directly.

### summarydata vs sqlmesh__summarydata
Same rule: query `summarydata.*`. Never query `sqlmesh__summarydata.*` directly.

### spend_log vs win_logs vs cost_impression_log
- `spend_log`: MNTN bidder pipeline output — wins with cost data. Source of truth for billing.
- `win_logs`: Beeswax win notification log — external exchange perspective.
- `cost_impression_log`: enriched impression-level spend, joined with geo/device/segment data.
  90-day rolling. Best for impression-level cost analysis.

### silver.core vs bronze.integrationprod
`silver.core` is a thin view layer — every table is `SELECT * FROM bronze.integrationprod.core_*`.
For schemas, always reference `bronze.integrationprod`. The `core_` prefix is stripped in silver.core
(e.g. `integrationprod.core_flights` → `silver.core.flights`).

### bronze.raw physical vs silver enriched (key differences)
| Aspect | bronze.raw | silver.logdata |
|--------|-----------|----------------|
| device_type | INTEGER | STRING (already joined to ENUM) |
| auction_timestamp | INTEGER (raw epoch) | TIMESTAMP (converted) |
| geo_type / video_placement | INTEGER | STRING |
| bid_placed / bid_dropped | BOOLEAN (present) | Absent — filtered out |
| _source_file / _batch_id | Present | Absent |
| site_page / site_referrer / content_* | Present (raw) | Absent or condensed |
| TTL | 90 days (bidder), 10 days (augmentor) | No expiry on most silver tables |

### aggregates vs facts tables
- `silver.aggregates.agg__daily_sum_by_campaign`: **pre-computed daily rollup**, best for campaign-level
  trend analysis. Cheaper to query than visit_facts/conversion_facts.
- `silver.summarydata.visit_facts / conversion_facts / spend_facts`: **hourly granularity**, more
  flexible for custom date bucketing, geo breakdowns, and device-level analysis.
- Use `agg__daily_sum_by_campaign` when you just need daily campaign totals.
- Use the individual facts tables when you need sub-day granularity or geo/device dimensions.

---

## Entity Hierarchy & Key Relationships

```
advertisers (bronze.integrationprod.advertisers)
  └── campaign_groups (campaign_group_id → advertiser_id)
        └── core_flights (flight_id → campaign_group_id)   ← budget period
        └── campaigns (campaign_id → campaign_group_id)
              └── core_creative_groups (group_id → campaign_id)
                    └── core_creative_groups_x_creatives
                          └── core_creatives (creative_id → advertiser_id)
```

### Extended Join Keys (Phase 2 additions)
| Join | Left | Right | Key |
|------|------|-------|-----|
| Campaign → Campaign Group | campaigns | campaign_groups | campaign_group_id |
| Campaign Group → Advertiser | campaign_groups | advertisers | advertiser_id |
| Campaign Group → Flight | campaign_groups | core_flights | active_flight_id |
| Creative → Advertiser | core_creatives | advertisers | advertiser_id |
| Creative Group → Campaign | core_creative_groups | campaigns | campaign_id |
| Any spend → Flight | spend_log | core_flights | flight_id |
| Conversion source | summarydata.conversions | integrationprod.data_sources | conversion_source_id |
| PMP deal → Campaign Group | core_private_marketplace_deals | campaign_groups | campaign_group_id |
| Audience size | aggregates.audience_hll_by_day | integrationprod.audience_segments | segment_id |
| MNTN → Beeswax advertiser | integrationprod.advertisers | beeswax_advertiser_mappings | advertiser_id |
| MNTN → Beeswax line item | integrationprod.campaigns | beeswax_line_item_mappings | campaign_id |

---

## Datastream Replication (bronze.integrationprod)

All `bronze.integrationprod` tables are Postgres replicas via GCP Datastream (CDC).
Most tables include a `datastream_metadata RECORD`:
- `uuid` — Datastream replication event UUID
- `source_timestamp` — Epoch ms of the Postgres WAL change event

**Do not use `datastream_metadata.source_timestamp` as a proxy for `update_time`** — it is
the CDC event timestamp, not the application-layer update time. Use `update_time` instead.

Tables without `datastream_metadata` (e.g. `advertisers`, `campaigns`, `campaign_groups`)
are likely replicated via a different mechanism or are missing the field intentionally.

---

## TTL / Retention Summary (Phase 2 additions)

| Table | Project.Dataset | Retention |
|-------|----------------|-----------|
| bronze.raw.bidder_bid_events | dw-main-bronze.raw | 90 days |
| bronze.raw.bidder_auction_events | dw-main-bronze.raw | 90 days |
| bronze.raw.bidder_beeswax_win_notifications | dw-main-bronze.raw | 90 days |
| bronze.raw.bidder_win_notifications | dw-main-bronze.raw | 90 days |
| bronze.raw.bidder_price_events | dw-main-bronze.raw | 90 days |
| bronze.raw.augmentor_log | dw-main-bronze.raw | **10 days** (+ partition filter required) |
| bronze.raw.bid_price_log | dw-main-bronze.raw | **10 days** (+ partition filter required) |
| bronze.raw.tmul_daily | dw-main-bronze.raw | **14 days** |
| bronze.raw.page_view_signal_log | dw-main-bronze.raw | 90 days |

---

## is_test / deleted Filters (bronze.integrationprod)

Always apply these filters when joining dimension tables for production analysis:
```sql
-- Advertisers
WHERE deleted = FALSE AND is_test = FALSE

-- Campaigns
WHERE deleted = FALSE AND is_test = FALSE

-- Campaign Groups
WHERE deleted = FALSE AND is_test = FALSE
```
Missing these filters will include internal test accounts and deleted entities in counts/metrics.

---

## Advertising Concepts & Domain Logic

### RTC (Real-Time Conquest)
Real-Time Conquest is a CTV prospecting targeting mode. The bidder targets IP addresses identified
as households actively watching competitors based on real-time data.

**How to identify RTC impressions:**
- In `logdata.cost_impression_log`, filter: `model_params ~ 'realtime_conquest_score=10000'`
- Campaign filter: `funnel_level = 1` (pure prospecting), `channel_id = 8` (CTV)
- `data_source_id = 19` = RTC data source
- RTC GA release: August 13, 2025

**IVR, CPM, CPV** are the primary KPIs for RTC monitoring. RTC impressions generally show higher
IVR than non-RTC on the same campaign segment.

### NTB (New-to-Brand) — Definitive Clarification
`is_new = TRUE` means the IP/household has not had a prior page view or purchase for that advertiser
**within the client-side JavaScript pixel's lookback window.**

**CRITICAL:** `is_new` is determined by a **client-side JavaScript pixel** (not a backend table lookup
and not auditable via SQL joins). The pixel fires and checks the browser's first-party data.
This means:
- The NTB flag is NOT derived from MNTN's internal data (no SQL query can reproduce it exactly)
- 41–56% disagreement between `clickpass_log.is_new` and `ui_visits.is_new` across advertisers —
  this disagreement is real and expected; they represent different evaluation points
- Cross-device visits are the primary driver of NTB misclassification (61.2% mutation rate when
  cross-device is involved)

### Pre/Post Analysis Pattern
Standard pattern for measuring feature release impact:
1. Define a release date (e.g., July 15/22, 2025 for vertical classification changes)
2. Require 7+ days of data in both pre and post windows
3. Use `summarydata.sum_by_campaign_group_by_day` (Greenplum) for daily rollups
4. Use `audience.audience_segments` with `expression_type_id = 2` (TPA) to filter campaign groups
5. Use `fpa.advertiser_verticals` with `type = 1` for primary vertical only
6. Use `dso.valid_campaign_groups` to filter to valid (active, non-test) campaign groups
7. Use `r2.advertiser_settings` to filter on `reporting_style = 'last_touch'` when attribution matters
8. Use `competing_*` columns in visit_facts/conversion_facts for non-last-touch advertisers

### Jaguar / DS13 / Audience Intent Scoring
Jaguar is MNTN's IP scoring model that predicts household purchase intent.

**Architecture:**
- Input: `bronze.raw.tmul_daily` → membership DB → bidder
- Scores stored in `cost_impression_log.model_params` as key=value pairs (e.g., `score=0.8523`)
- `data_source_id = 13` — canonical `data_sources.name` is **"MNTN Vertical Categorization"**. In war-room language (Bryce Wagg, 2026-04-22) this DS is called **"Peak Performance"** and carries the Jaguar intent scoring. Same DS id, two names depending on the audience — code = Vertical Categorization, product = Peak Performance.
- **Canonical Peak Performance segment-level detector (TI-896, verified):** expression must contain ALL THREE of `"score_type":"rtc"`, `"data_source_id":13`, `"data_source_id":19`. Neither DS13-alone nor DS13+DS19-without-RTC is sufficient — DS13-alone over-counts by ~12pp (legacy Vertical Categorization use); DS13+DS19-without-RTC has ~1% false-positive baseline (legacy hybrid Interest+Keywords audiences that predate PP). All three signals together give near-zero pre-Oct-2025 baseline, matching the formal launch date.
- **Template-level (in `archives_audiences_archives`) uses the compact `{"interest":{"include":[{"or":[...DS13...DS19...]}]}}` schema**; segment-level (`archives_audience_segment_archives`) uses the translated `{"select":[...],"categories":{"where":{...}},"geos":{...}}` form, which adds auxiliary DS ids (DS14 global flag, holdout MD5 bucketing). Structural "pure DS13+DS19" checks only work at template level.
- Scores applied at bid time — not stored long-term in BQ event tables
- Pipeline is DS13 (not DS2 or DS4)

### Ecommerce Classifier
Domain-level ecommerce classifier that assigns an `ecommerce_score` to each domain.

**Key details:**
- Input data: `s3://mntn-data-archive-prod/site_visit_signal_batch_ecommerce_test/classified_data/dt=<date>/hh=<hour>/`
- 251M website visits used for training/evaluation
- `registered_domain` column is the join key
- Recommended thresholds: P90 ≈ 0.9181 (whitelist), P10 ≈ 0.0002 (blocklist)
- Downstream work: TI-200 whitelist/blocklist uses these thresholds

### Vertical Classification
`fpa.advertiser_verticals` (Greenplum/BQ) stores the advertiser→vertical mapping.
- `type = 1` = primary vertical (use this for filtering)
- Vertical IDs: 101001, 119001, 120002 referenced in TI-221/TI-270 analyses
- **`advertiser_name` column is UNRELIABLE** — it's a denormalized, write-once field that is never updated. Two known issues:
  1. **Empty name regression (since 2025-12-23):** 79–82% of new advertisers have empty string. 4,366+ advertisers affected.
  2. **Stale names:** 7% of populated names differ from current `advertisers.company_name` (customers renamed after FPA row was created).
  - **Always JOIN to `advertisers.company_name`** for the current name — never use `fpa_advertiser_verticals.advertiser_name`.
- **Shortcut for vertical/bucket lookups:** `tpa.dim_vertical` in coredb (created by Ryan Kleck) — PK is `vertical_id`, pre-joined with bucket info (`bucket_id`, `bucket_name`, `vertical_bucket_name`, `verticals_in_bucket`). Use this instead of self-joining `advertiser_verticals` type=0 + type=1 when you just need the vertical→bucket mapping.

---

## IPDSC Pipeline & MES Architecture

### What is IPDSC
IPDSC (IP Data Source Category) is the process that maps IP addresses to audience segment
category_ids by data source. It is the bridge between HEM (hashed email) CRM uploads and
the IPs that the bidder actually targets.

**IPDSC data location (BQ):**
- GCS path: `gs://mntn-data-archive-prod/ipdsc/dt=<date>/data_source_id=<id>/`
- BQ external table: `dw-main-bronze.external.ipdsc__v1`
- Format: Parquet, partitioned by `dt` (STRING) and `data_source_id` (INTEGER)
- No expiration — historical data available

**How to query ipdsc__v1:**
```sql
SELECT DISTINCT ip, dscid.element AS category_id
FROM `dw-main-bronze.external.ipdsc__v1` t
  , UNNEST(t.data_source_category_ids.list) AS dscid
WHERE t.data_source_id = 4          -- DS4 = CRM
  AND t.dt = '2025-11-25'           -- choose a date during campaign flight
  AND dscid.element IN (17077, 17079)  -- audience_upload_ids / category_ids
```

**Key fact:** `category_id` in ipdsc__v1 = `audience_upload_id` in tpa.audience_upload_hashed_emails
= `data_source_category_id` in integrationprod.audience_uploads — these are all the SAME value.

### MES (Membership Enrichment Service) Pipeline
MES is the enrichment service that processes impressions and validates audience membership.

**IPDSC block list (data_source_ids never used for targeting):**
- DS 2 — OPM/real-time (blocked in MES inner join)
- DS 14 — (blocked)
- DS 42 — (blocked)

**35-day lookback** for non-Oracle data sources in MES.

**impression_enrichment.py**: The MES pipeline uses an inner join against ipdsc — IPs not in
the ipdsc file for the relevant data source are dropped. This is the root cause of HH discrepancy
investigations (TI-644, MM-44) where targeting audiences appear smaller than expected.

### Data Source (DS) Type Reference
| DS ID | Name | Type | In IPDSC | In tmul_daily | Notes |
|-------|------|------|----------|---------------|-------|
| 2 | MNTN First Party / OPM | Real-time | NO | YES | Always in tmul_daily; never in ipdsc block list |
| 3 | (Third Party) | — | — | YES | In tmul_daily |
| 4 | CRM | Batch upload | YES | NO | HEM → IP via Verisk identity graph; in ipdsc, NOT in tmul_daily rows |
| 13 | Audience Intent Scoring | Jaguar model | — | — | Score stored in model_params |
| 14 | — | — | YES | — | Blocked in MES |
| 16 | MNTN Taxonomy | Taxonomy | NO | — | Real-time; not in ipdsc |
| 19 | RTC | Real-Time Conquest | — | — | `realtime_conquest_score=10000` in model_params |
| 21 | MNTN Conversion | Real-time | NO | — | Conversion-based exclusions |
| 34 | MNTN Pageview | Real-time | NO | — | Page view-based exclusions |
| 42 | — | — | — | — | Blocked in MES |

### CRM Upload Flow (DS 4)
1. Advertiser uploads CSV of hashed emails (HEMs) → stored in `tpa.audience_upload_hashed_emails`
2. Verisk identity graph resolves HEMs → IP addresses
3. IPs stored in `external.ipdsc__v1` (GCS-backed Parquet) for the relevant `dt` and `data_source_id=4`
4. Bidder reads ipdsc → targets those IPs
5. `audience_upload_ips` is **empty for email uploads** — only populated for direct IP uploads

**Match rate (HEM → IP):** ~61–63% typical (stored in `integrationprod.audience_uploads.match_rate`).
`match_rate * entry_count` = estimated IP count (use ipdsc query for actual count).

**HEM deduplication:** Filter `pre_hash_case = 'UPPERCASE'` to count unique emails — each HEM
is stored in UPPERCASE, LOWERCASE, and ORIGINAL case variants.

**Empty HEM hash:** SHA256 of empty string =
`e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
This appears when a row has no email value. Exclude from counts.

### LiveRamp Cross-IP Identity Linkage
**Canonical DS ids (verified 2026-04-22 against `bronze.integrationprod.data_sources`):**
- **DS3 = "MNTN Third Party"** (not LiveRamp — this note's original framing was wrong)
- **DS11 = "LiveRamp"** (audience-segment-based)
- **DS35 = "LiveRamp IP"** (IP-based variant — this is what Bryce called out as "3P" in the TI-896 war-room scope)

LiveRamp maps IPs to a shared identity graph (household/person level). When LiveRamp links IP_A ↔ IP_B,
both IPs receive identical segment membership entries in `tpa_membership_update_log` with `data_source_id` in {11, 35}.

**Full canonical DS map — query `bronze.integrationprod.data_sources` before relying on any tribal-knowledge DS id.** Known gotchas from that lookup (2026-04-22):
- DS2 = MNTN First Party
- DS19 = MNTN Matched (= "Keywords" in war-room/product language per Bryce)
- DS38 = MNTN UI Audience Keywords (different thing — UI-keyword strings, ~40M rows)
- Per-advertiser audiences appear in the 1000+ range with names like "{AID} - First Party Audience" / "Third Party Audience" / "Control Group Audience" / "Extension Audience". Any MM/3P/CRM count that excludes these will undercount.

**Key limitation for cross-stage tracing:** If an S1 impression is served to IP_A, and LiveRamp links IP_A to IP_B,
IP_B enters the S2 targeting segment. The S2 VV at IP_B cannot be traced back to the S1 impression at IP_A
because there is no IP↔IP linkage table in BQ. This accounts for ~20% of unresolved S2/S3 VVs in the
VV IP lineage audit (TI-650).

**How to detect LiveRamp-linked IPs:** Find IPs entering the same DS3 segments at the same timestamp (±1 min)
in `tpa_membership_update_log`. Segment overlap >50% indicates identity-level linkage (not coincidental).
Example: 96/140 segments shared (68.6% overlap) between linked IPs.

**No IP→IP mapping table exists in BQ.** `ipdsc__v1` only maps IP → data_source_id, not IP → IP.
LiveRamp's identity graph is external to MNTN's data warehouse.

---

## Campaign Holdout / Incrementality Measurement

### 10% Holdout Group (All Campaigns)
Every campaign has a **10% holdout group** — IPs that are never served impressions.

**Mechanism (Matt Brorby, 2026-04-07):**
- When an IP enters the targeting pipeline, it goes through a hash function
- Last 2 digits of the hash determine group assignment: < 10 → holdout, ≥ 10 → targeted
- This is **pure random assignment by IP** — the holdout should have the same intent tier distribution as the targeted 90%
- The holdout group is used for the incrementality dashboard in the UI

**How the holdout works (Nicholas + Zach, 2026-04-07):**
- The holdout is embedded IN the audience segment expression JSON as a where clause
- **1000 buckets** — holdout = range 0-99 (10%), targeted = range 100-999 (90%)
- The hash uses a **prefix (e.g., ex46)** — this is DIFFERENT from experiment bucket hashing (which hashes on the IP address directly). They are independent random assignments.
- Expression lives in `audience_segment_campaigns.expression` (expression_type = 2 only — type 1 is legacy, not read)
- Literally has the name "holdout" in the JSON — can be identified by parsing the expression

**How to get holdout IP lists:**
- **Option 1 — MD5 bucket hash (PREFERRED, Zach 2026-04-07):** Compute the bucket for any IP directly. No TMUL needed. **This is the cheapest approach by far.**

```sql
-- Greenplum/Postgres (Zach's confirmed function):
SELECT md5('{AID}:100.17.100.240') AS ip_hash,
       (('x' || substr(md5('{AID}:100.17.100.240'), 1, 16))::bit(64)::bigint % 1000) AS bucket;
-- If bucket is 0-99 inclusive → holdout. 100-999 → targeted.
```

**Critical details:**
- The hash input is **`{AID}:{IP}`** — the advertiser ID is prefixed. This means holdout assignment is **per-advertiser per-IP**, not global per-IP.
- Takes first 16 hex chars of the MD5, casts to 64-bit integer, mod 1000
- Zach calls this the `rust_equivalent` — the SQL replicates what the Rust-based audience service does at runtime
- 1000 buckets: 0-99 = holdout (10%), 100-999 = targeted (90%)
- Replace `{AID}` with the actual advertiser_id (integer)
- **BQ equivalent needs testing** — MD5 returns bytes in BQ, not hex string. Will need `TO_HEX(MD5(...))` and then a cast approach.

- **Option 2 (Zach, expensive fallback):** Use `external.tpa_membership_update_log__v2` (TMUL v2) — logs which IPs are in which segments. **Expensive for 30-day windows** — dry_run first. Only use if the hash approach doesn't match.
- **Option 3 (not yet built):** Nick wants Jordan/Zach to build a tool that takes an audience expression and returns matching IPs. Doesn't exist yet.

**Key tables for audience expressions:**
- `audience_segment_campaigns` — the real table. Maps 1:1 with campaign_id. Contains the expression JSON. Filter: `expression_type = 2`.
- `audience.audiences` — just a wrapper/template. Don't use for analysis.
- Nick (experimentation team) has a streamlined query to extract expressions — ask him.

**Important: Stage 1 campaigns only for holdout analysis.** S2/S3 campaigns target people already hit by S1 ads — they're downstream of the targeting decision. Use `funnel_level = 1`.

**MemDB holdout hash (Ryan Kleck, 2026-04-07):** MemDB already hashes IPs for the existing 10% holdout mechanism. This same mechanism could potentially be extended/reused for decile-based audience splits (AUD-5221/TI-831). Investigate before building a new hashing system.

---

## Intent Scoring Architecture (advertiser_household_score / HHST)

Reference diagram: `documentation/architecture/audience_intent_scoring.png`

### Venn Diagram: Bucket > Vertical > Keywords
- **DS13** = buckets and verticals
- **Bucket** = Industry (broad)
- **Vertical** = Subindustry (narrow, within a bucket)
- **Keywords** = DS19 keyword matches (narrowest, within a vertical)

### Campaign Level Scores (HHST on cost_impression_log)
| Tier | HHST Range | Criteria | Score Assignment |
|------|-----------|----------|-----------------|
| **High Intent (HI)** | 6666-10000 (always 10000) | IPs that fall in the **vertical** (subindustry) | Always flat 10000 |
| **Mid Intent (MI)** | 3333-6665 | IPs in the **bucket** (industry) but NOT the vertical. Ranked by # of page views, most recent page view. | Variable within range |
| **Max Reach / Peak Performance (PP)** | 1-3332 | All other IPs in the audience not in the bucket or vertical | Random score in range |

### Advertiser Level Scores (same ranges, different criteria)
| Tier | HHST Range | Criteria |
|------|-----------|----------|
| **HI** | 6666-10000 (always 10000) | IPs in the vertical |
| **MI** | 3333-6665 | IPs in bucket but NOT vertical, **>1 page view** in the bucket |
| **Max Reach** | 1-3332 | IPs in bucket but NOT vertical, **exactly 1 page view** — random score |

### Current Production Reality (as of 2026-04-08)
- **69.9%** of impressions: HHST = 10000 (HI — all scored IPs get flat 10000)
- **28.7%** of impressions: HHST = -1 (unscored — no Fangorn score / not in any segment)
- **1.4%** of impressions: HHST in MI range (3333-6665) — real variation exists here
- **~0%** in PP/Max Reach range (1-3332)
- **~0%** in HI sub-range (6666-9999) — everything is flat 10000

**Implication:** Per-tier incrementality analysis is not meaningful until continuous scoring replaces the flat 10000. Aggregate analysis (holdout vs targeted, all tiers pooled) is the correct approach.

### Special Values
- **10000** = High Intent (HI) — flat score for all vertical-matched IPs. Currently 69.9% of impressions.
- **8000** = Peak Performance (PP) — **was active Jan-Feb 2026, currently minimal** (as of 2026-04-08). Targeting logic: serve HI (10000) first, then expand to PP (8000) if pacing allows. Waterfall: HI → PP. Top advertisers with PP data: 34185, 36232, 37158, 34838. Most PP activity ended by late February. Sporadic single-digit impressions in March-April.
- **3333-6665** = Mid Intent (MI) — bucket-matched IPs not in the vertical. 1.4% of impressions.
- **-1** = unscored (no Fangorn/intent score assigned). 28.7% of impressions.
- **-4** = rare edge case (9 impressions observed)

**Once PP (8000) goes live**, per-tier incrementality analysis becomes possible: compare HI (10000) vs PP (8000) vs MI (3333-6665) holdout/targeted visit rates.

### Intent Tier Thresholds (Prospecting Scoring Pipeline)
Source: `gs://household-scoring-prod/output/scoring/prospecting_intent/` — daily per IP/advertiser/campaign. Scores retained only **35 days** in active storage.

| Tier | Score Range | Criteria |
|------|------------|----------|
| **High Intent** | 10000 | Vertical + keyword match |
| **Peak** | 7000-9999 | Vertical only (no keyword) |
| **Mid Intent** | 3333-6999 | Keyword only (no vertical) |
| **Max Reach** | <3333 | Neither vertical nor keyword |

**Coverage rates in ITT analysis (Alex Knorr pre-analysis, April 2026):**
- High intent: **3.4% median** coverage of treatment group (NOT 14% as initially estimated in meetings)
- Peak: **0.2%** median coverage
- Mid intent: **0.04%** median coverage
- LATE (Wald estimator) only credible above ~4-5% coverage → only high-intent barely crosses this threshold
- 10 advertisers across 8 verticals, 25-day post-period (Mar 21 – Apr 14)
- External table: `dw-main-bronze.external.TI_835_prospecting_scores`
- Full report: `reports/TI_835_Pre_Analysis_v4.html` in SteelHouse/databricks_targeting (branch TI-835)

**Use for incrementality analysis:**
- Compare visit rates between 10% holdout (no impressions ever) vs 90% targeted group
- Use ITT (Intent to Treat): compare ALL IPs in 90% group, not just those who actually received impressions
- Why ITT: only a fraction of the 90% actually gets impressions (budget-constrained). Comparing only impression-recipients vs holdout introduces selection bias (impression receipt correlates with behavioral differences like watching TV at that time)

**guid_log vs clickpass_log for incrementality (TI-835):**
- **guid_log** captures ALL pixel visits (direct, organic, paid, VV — everything). Both holdout and targeted IPs visit at the same rate → ~0% lift. This measures total site traffic, which MNTN ads barely move.
- **clickpass_log** captures VV-attributed visits only (user clicked through from MNTN ad). Targeted group has 2-8x more attributed visits than holdout → massive lift. This measures MNTN attribution signal.
- Use clickpass_log for "does our targeting drive attributed visits?" Use guid_log for "does our targeting drive total site traffic?"

**BQ holdout hash (ported from Greenplum, TI-835):**
```sql
-- BQ equivalent of MD5('{AID}:{IP}') unsigned mod 1000:
MOD(
  ABS(
    CAST(
      CONCAT('0x', SUBSTR(TO_HEX(MD5(CONCAT(CAST(advertiser_id AS STRING), ':', ip))), 1, 16))
    AS INT64)
  ),
  1000
)
-- Bucket 0-99 = holdout (10%), 100-999 = targeted (90%)
-- Per-advertiser per-IP assignment
```

**Key gotcha — ITT dilution:**
- If the audience is 10M but the budget only reaches 10% of them, the 90% targeted group's visit rate is diluted by the 80% who were eligible but never actually received an impression
- The true treatment effect is on the impression-recipients, but ITT gives you the unbiased average effect across the full eligible group
- For a finer analysis (actual treatment effect on the treated), need to model the impression-receipt probability — more complex, not needed for initial analysis

---

## Audience System Architecture

### audience.audiences vs audience.audience_segments (Greenplum)
These are two distinct concepts, often confused:

| Table | Purpose | Used for targeting? |
|-------|---------|-------------------|
| `audience.audiences` | Named audience templates (reusable definitions) | NO — template only |
| `audience.audience_segments` | Actual targeting expressions for campaigns | YES — this drives delivery |

**Implication:** Querying `audiences` alone will NOT reveal which audience was actually used for
targeting. Always join to `audience.audience_segments` for campaign-level analysis.

- **`expression_type_id` = 1 vs 2 (corrected 2026-04-23 via Jordan + empirical check):** Type 1 is NOT "legacy / ignore it" as earlier framed — it's the **OPM (first-party retargeting) audience system** with text-format expressions against pixel/user attributes. Type 2 is TPA (third-party targeting) in JSON format. Both are live.
  - **`expression_type_id = 1` (OPM / FPA):** text-format expressions like `UserNumPageViews <= 22`, `UserLastVisitTime <= 0,day`, `UserPageViews contains [...]`, `UserNumVisits >= 1 AND UserAvgVisitDuration <= 60,min`. Matches users by MNTN pixel / site-tracking attributes. Used for retargeting audiences.
  - **`expression_type_id = 2` (TPA):** JSON expressions with `categories.where.value[]` structure, referencing `data_source_id` + `category_ids`. This is the prospecting / audience-targeting system.
  - **Link between the two:** when a TPA expression contains `"data_source_id": 2, "category_ids": [X, Y]`, the category_ids X/Y are **OPM `audience_id` values** (from the expression_type_id=1 table). So DS2 in a TPA expression = pointer to an OPM retargeting audience. Same mechanism for `blockFirstParty` exclusion (adds DS2 exclusion categories pointing at OPM page-view / conversion audiences).
  - **TI-896 earlier filter `expression_type_id = 2 AND is_targeted = TRUE`** is correct for the TPA/prospecting lane — OPM (type 1) is a separate system. But type 1 is NOT "to ignore" broadly; it's the FPA data used elsewhere in the bidder.
- **Per-advertiser data source IDs** (`data_sources.data_source_type_id = 2`, IDs ≥1000) with names like `{AID} - First Party Audience`, `{AID} - Third Party Audience`, `{AID} - Control Group Audience`, `{AID} - Extension Audience` — **do NOT appear in segment-archive expression JSON**. Cohort sample (April 2026): 0 expression references to these DS IDs. Audience-bucket detectors that try to match by name pattern (e.g., `name LIKE '% - First Party Audience'`) won't fire on segment expressions. Only global DS IDs (1-99 range) appear. Source: TI-896 verification 2026-04-22.
- **DS2 is OPM segments, NOT Mountain Matched** (Alyson + Zach 2026-04-22, Slack). Bryce's earlier mapping (DS2 = MM) was wrong. DS2 in segment-archive expressions usage breakdown: ~99.7% in inclusion clauses (`op: "any"`), ~0.3% in exclusion clauses (`op: "not"` per V4 in TI-896). Treat DS2 trajectory as OPM-segment-targeting, not MM.
- **DS2 architecture (authoritative, per Jordan Piepkow Slack 2026-04-23, referencing `SegmentExpressionService.kt:134-178` and `DataSource.kt:31`):** DS2 = "MNTN First Party", `CommonDataSource.MNTNFirstParty`. DS2 is a **container for first-party (OPM) segment IDs** — each DS2 `category_id` inside an audience expression is a **pointer to an OPM segment** (pageview segment, conversion segment, etc.), NOT the expression itself.
  - **When processing TPA expressions**, the expression service extracts DS2 categories to detect `isFpaAudience` (whether the audience uses first-party data).
  - **OPM-type expressions get wrapped** into a TpaExpressionV1 with `MNTNFirstParty.dataSourceId (2)` — encoding "this OPM segment should be resolved via DS2."
  - **`AdvertiserConfiguration.blockFirstParty`** flag controls whether first-party data gets excluded. When ON, exclusion categories are added under DS2 to block page-view / conversion segments from targeting (`SegmentExpressionService.kt:662, 690`).
  - **Python migration script** strips DS2 entries from expressions during processing (alongside DS8/IP List) — they're removed from or/and clauses. DS2 references are somewhat vestigial / tooling-dependent.
  - **DS2 appears in an expression when** the audience references OPM first-party data (targeting or blocking). Audiences with no first-party references (pure 3P, pure CRM, pure keyword) won't have DS2 entries.
  - **Membership cache:** `tmul_daily` holds DS2 + DS3 segment memberships per IP (14-day TTL) for fast bidder lookups.
  - **Category_id space is independent** — DS2 category_ids (278889, 514936, 555664, 565599, etc.) don't appear under DS13/DS19/DS35 (verified in TI-896 2026-04-22 via ipdsc sample).
- **DS2 disappearance observed Oct 27 2025 in TI-896** (spend-weighted share 73-79% → 42-46%) is therefore most likely a product-side change: (a) MMv3 default audience template stopped auto-adding OPM pageview/conversion references, or (b) `blockFirstParty` default behavior changed so fewer advertisers have first-party exclusion active. Real audience-side change worth surfacing; not a pipeline artifact.
- **Mountain Matched the product (MM 2.0) is not detectable by simple DS-id matching.** It's the umbrella audience system with score tiers (Max Reach / Mid Intent / Peak Performance / High Intent per state table; see `mntn_business.md`). Open question for Ryan / Zach: what's the authoritative MM-product detector at the database level?
- **Canonical `data_sources.name` vs Bryce war-room labels (TI-896, 2026-04-22):**
  - DS2 canonical name = "MNTN First Party"; Bryce called it "Mountain Matched (MM)" — wrong, per Alyson + Zach 2026-04-22 it's OPM
  - DS19 canonical name = "MNTN Matched"; Bryce called it "Keywords"
  - Use canonical / authoritative labels (per Zach), not Bryce's product labels.
- **Audience-bucket detector convention (used in TI-221, TI-270, TI-896):** filter `expression LIKE '%"data_source_id":N,%'` (Postgres) or `REGEXP_CONTAINS(expression, r'"data_source_id"\s*:\s*N\b')` (BQ). Always with `expression_type_id = 2 AND is_targeted = TRUE`.
- `audience.audience_segment_campaigns` maps which audience segment is active for each campaign — **1:1 mapping with campaign_id** (not campaign_group_id). This is the real thing that drives delivery.

### Audience Expression Structure (Nicholas, 2026-04-07)
The expression JSON (type 2) has 3-4 overarching AND clauses:
1. **selects** — category selections
2. **categories** — DS19 keywords, data source filters, CRM blocks, visitor/converter lookbacks (30d)
3. **geos** — geography targeting (usually US)
4. **holdout/buckets** (optional) — bucket range for holdout (0-99 out of 1000) or experiment groups (e.g., 0-500)

The relationship chain: `campaign_groups` (template) → `campaigns` (actual bidding entities) → `audience_segment_campaigns` (1:1 with campaign) → `audience_segments` (expression JSON).

`audience.audiences` is just another template wrapper — like campaign_groups is to campaigns. Don't query it directly for analysis.

### audience.campaign_segment_history Contamination
`audience.campaign_segment_history` blends both `audiences` (template) and `audience_segments`
(targeting). This table is the source of contamination bugs — using it to determine "what audience
was this campaign using" is unreliable because template objects appear alongside active targeting.
Use `audience_segment_campaigns` + `audience_segments` instead.

**BQ equivalent:** `summarydata.v_campaign_group_segment_history` (VIEW) — same issue applies;
verify against `audience_segment_campaigns` for production analysis.

### Prospecting vs Retargeting (Audience Type)
- `funnel_level = 1` = prospecting (never-served households)
- `funnel_level = 2` or higher = retargeting
- For RTC monitoring: always filter `funnel_level = 1`

---

## Stage 3 VV Pipeline & IP Mutation Audit

### Campaign Stage Definitions (Zach, 2026-03-03 & 2026-03-04)

Stages are campaign targeting stages, not event types. Each stage targets a different IP audience.

| Stage | Segment populated by | Business meaning |
|-------|---------------------|-----------------|
| Stage 1 | Campaign setup (initial audience — customer data, lookalike, etc.) | All targetable IPs |
| Stage 2 | Stage 1 VAST Impression IPs | Users who were served an ad and it played |
| Stage 3 | **IPs that had a verified visit** (from any stage's impression) | Retargeting audience |

**Key rules:**
- Stage 2 is populated ONLY from Stage 1 VAST IPs.
- Stage 3 = any IP that had a VV. Two paths: (1) Stage 1 impression → VV → Stage 3, or (2) Stage 1 → Stage 2 impression → VV → Stage 3. Attribution doesn't follow the stage sequence — a VV can be attributed to any stage's impression.
- **Cross-stage key is vast_ip (event_log.ip), NOT bid_ip (empirically proven, 2026-03-10).** Either/or join: `prev.vast_start_ip = next.bid_ip OR prev.vast_impression_ip = next.bid_ip`. VAST event order: impression fires FIRST (creative loaded), start fires SECOND (playback begins). vast_start marginally better cross-stage (+256 matches on 487K pairs) but difference is noise. Either/or gains +351 matches (0.05%) — adopted in v9. 1.558% match neither (structural — CGNAT/SSAI/IPv6/VPN). No deterministic cross-stage ID exists besides IP (Finding #28).
- **bid_ip ≠ vast_ip in ~1.2% of impressions (3.54M/288.7M).** Five mechanisms cause the difference: CGNAT /24 rotation (35%), CGNAT wider /16 pool (25%), carrier /8 reallocation (6%), SSAI proxy — VAST callback from AWS server not user device (6%), dual-stack IPv4→IPv6 (12%), other — VPN/CDN/genuine network switch (16%).
- **event_log.ip has CIDR notation (`/32` or `/128` suffix) on ALL pre-2026 data.** Confirmed: 526M rows Nov-Dec 2025 = 100% CIDR, 393M rows Jan-Feb 2026 = 0% CIDR. Other tables (impression_log, viewability_log, clickpass_log, bid_logs, win_logs) have NO CIDR suffix in Feb 2026 data — this is event_log-specific. `/32` = IPv4, `/128` = IPv6. Exact string matching (`ip = 'x.x.x.x'`) will miss pre-2026 rows. Fix: `SPLIT(ip, '/')[SAFE_OFFSET(0)]` for reliable matching across time periods. `bid_ip` column does NOT have this issue (always bare IP). Any cross-stage query with lookback into 2025 MUST strip CIDR. Discovered TI-650 v16 2026-03-13.
- **CRM campaign_groups:** Identifiable by campaign_group name containing "CRM". These may use different targeting pools (data_source_id=4, email→IP). However, every VV in the prospecting funnel (objective_id IN 1, 5, 6) MUST follow the IP path (S3 bid_ip → prior S2/S1 VV, S2 bid_ip → S1 event_log). If resolution fails, the cause is insufficient lookback, table TTL, or a data bug — not identity graph bypass.
- **4 IPs per stage in audit table (collapsed from 6):** vast_start_ip, vast_impression_ip (both event_log.ip, different event_type_raw, 99.85% identical), serve_ip (impression_log.ip, 93.6% = bid_ip), bid_ip (= win_ip = segment_ip, 100%). Dropped: win_ip (=bid_ip 100%) and segment_ip (=bid_ip 100%, Zach confirmed). win_logs.impression_ip_address is infrastructure/CDN IP, not user.
- `first_touch_ad_served_id` always points to a Stage 1 impression (by definition: `funnel_level=1, objective_id=1`). `ad_served_id` (last touch) can point to Stage 1, 2, or 3.
- Stage 3 exists for retargeting — "last touch is king in ad tech" (Zach). Users who already visited are highest-intent, so we keep serving to maintain last-touch attribution credit.
- Scale: Stage 1 ~8.5M IPs → ~10K get impressions → ~2K enter Stage 3 (Zach's example).
- **Campaign groups are exclusive.** A VV in campaign_group 1 for an advertiser does NOT allow another campaign_group 2 for the same advertiser to target that IP at a higher stage. Each campaign_group's funnel is independent. Concurrent or previous campaign_groups have zero bearing on each other's stage progression. (Zach, 2026-03-05)
- **IPs accumulate stages within a campaign_group, never removed.** Frequency capping (14-day) handles dedup, not targeting removal. Budget: S1 ~75-80%, S2 ~5-10%, S3 = remainder.
- **Campaign ID = Stage (1:1).** Determine via campaigns.funnel_level (1=S1, 2=S2, 3=S3, 4=Ego). Bidder has no concept of stages.
- **objective_id reference (from `core.objectives` + Ray). Prospecting filter = IN (1, 5, 6):**
  - 1 = Prospecting (CTV Prospecting)
  - 2 = Onsite (ads on customer's own website)
  - 3 = Prospecting (duplicate of 1, not actively used)
  - 4 = Retargeting
  - 5 = Multi-Touch (S2 prospecting, newer naming convention)
  - 6 = Multi-Touch Full Funnel (MT+ = Stage 3, newer naming convention)
  - 7 = Ego (employee targeting — targeting advertiser's own employees)
- **CRITICAL: objective_id is UNRELIABLE as a stage indicator (Ray, 2026-03-11).** During the "TV Only" UI migration, the UI team stopped setting objective_id correctly and pivoted to using funnel_level instead. Result: 48,934 "Beeswax Television Multi-Touch Plus" campaigns (S3/MT+) have `objective_id=1` instead of `6`. The old parent/child campaign_group structure (parent=obj 1/channel 8, child=obj 5+6/channel 1) was collapsed into a single campaign_group with 3 campaigns, and objectives were not updated. **funnel_level is the authoritative stage indicator, not objective_id.**
- **CRITICAL: funnel_level ≠ prospecting.** Retargeting campaigns (objective_id=4) exist at every funnel_level (1/2/3). For prospecting-only analysis, use `objective_id NOT IN (4, 7)` (safer) or `objective_id IN (1, 5, 6)` per Ray — both work because mislabeled MT+ campaigns have objective_id=1 (included either way). Excludes Retargeting (4), Ego (7). `objective_id IN (1,5,6)` also excludes Onsite (2) and unused dup (3).
- **campaign_group_id scoping required for cross-stage IP linking (TI-650 v14).** All cross-stage IP matching MUST be within the same `campaign_group_id` — matching across groups is coincidental, not funnel trace. `campaign_group_id` is unique across advertisers (only `0` is shared as null/default). Using `advertiser_id` instead inflates resolution rates ~5pp. Zach directive 2026-03-12.
- **objective_id × funnel_level distribution (2026-03-12):** S1: obj=1 (52K prosp) + obj=4 (20K retarget). S2: obj=1 (43K broken) + obj=5 (64K MT prosp) + obj=4 (19K retarget). S3: obj=1 (43K broken) + obj=6 (60K MT+ prosp) + obj=4 (19K retarget). The 43K S2/S3 campaigns with obj=1 are from the TV Only UI migration break.
- **Zero-chain advertisers in multi-advertiser analysis:** 4/10 top advertisers had zero S3→S2→S1 chain resolution despite having S2 campaigns in the campaigns table. Cause: those S2 campaigns had zero prospecting vast events in the 90-day lookback — they serve only retargeting (obj=4).
- **VV attribution = stack model.** Impressions stacked; page view checks top (most recent). Everything behind is ineligible.
- **VVS cross-device linking (Sharad, confirmed):** The Verified Visit Service links visits to impressions in two layers: (1) **IP match** — find impressions served to the same IP as the page view IP (primary), (2) **GA Client ID expansion** — using the page view's GA Client ID, find all IPs that Client ID has been seen with in the previous few days, then look for impressions on any of those IPs. Validations and filtering applied at each layer. See: Nimeshi Fernando's "Verified Visit Service (VVS) Business Logic" Confluence doc.
- **VVS determination logic (Nimeshi Fernando, Confluence):** Full decision tree: (1) advertiser_id valid? → (2) IP blocklist check (`segmentation.ip_blocklist`) → (3) GUID blocklist check (`segmentation.guid_blocklist`) → (4) cross-device config check (`vvs.cross_device_config` in Aurora DB) → (5) GUID match (`attribution_model_id=1`) → (6) IP match (`attribution_model_id=2`, includes CTV household_whitelist + iCloud IPv4 filter + GUID-to-IP count check) → (7) repeat with `viewable=false` impressions → (8) GA Client ID match (`attribution_model_id=3`, via `cookie.gaid_ip_mapping`) → eligibility checks (duplicate visit, TTL/acquisition window, advertiser TTL 45-day max) → (12) referral blocking / tamp detection (utm_source, utm_medium, utm_campaign, utm_content, gclid, cid, cmmmc). TRPX fires every page view; only first in session is eligible. VV window = 14-45 days per advertiser.
- **VVS attribution_model_id reference:** 1-3 = Last Touch (guid/ip/ga_client_id), 4-6 = Last TV Touch (guid/ip/ga_client_id), 7-8 = Offline Attribution, 9-14 = Competing (guid/ip/ga_client_id variants), 15-16 = Impression-based (ip). Non-competing = 1-8, Competing = 9-14. Competing VVs stored in `competing_vv` Kafka topic.
- **PV_GUID_LOCK:** VVS stores impression GUID + page view GUID. PV GUID TTL = 30 min of inactivity (resets each TRPX fire). Handles IP changes mid-session. Advertisers with `pv_guid_lock = true` in `advertiser_configs`.
- **TRPX (tracking pixel):** Installed on advertiser webpages. Fires HTTP POST to VVS on every page view. Sends: ip, guid, gaid (GA Client ID), advertiserId, UTM params, referrer, userAgent, xForwardedFor, epoch. Response: `isSuccessful=true` (Last Touch VV) or `isSuccessful=false` (rejection or Competing VV). TRPX also sends GA data to attribution-consumer → Measurement Protocol → advertiser's GA property → logged in `analytics_request_log`.
- **CTV vs display attribution:** No preference between media types — treated identically in last-touch attribution. (Sharad, ATT, 2026-03-06)
- **Non-viewable display impressions:** Appear ONLY in `impression_log`, never in `event_log`. `event_log` only contains viewable CTV impressions (`vast_impression` events). For IP lineage tracing, `COALESCE(event_log.bid_ip, impression_log.bid_ip)` is required — `event_log` preferred; `impression_log` fallback for non-viewable display. (Sharad, ATT, 2026-03-06)
- **bid_ip COALESCE fallback pattern (TI-650 validation, Zach confirmed 2026-03-24):** `event_log.bid_ip` was intentionally designed to match `bid_logs.ip` — safe to use as fallback when bid_logs records are purged (90d TTL). Four tables store `bid_ip` as a denormalized column: `event_log`, `impression_log`, `viewability_log`, `click_log`. When `bid_logs.ip` is NULL (purged), COALESCE from these. Pattern: `COALESCE(NULLIF(bid_logs.ip, '0.0.0.0'), impression_log.bid_ip, event_log.bid_ip, viewability_log.bid_ip)`. Note: bid_ip may not exist in viewability_log for all events — Zach was uncertain if it was added for display viewability. Recovers ~50% of bid_logs-purged VVs; remaining ~50% have NULL bid_ip across all tables (pipeline didn't write it). `clickpass_log` does NOT have a bid_ip column — Zach acknowledged this is a gap ("that's a whole other thing").
- **bid_logs has multiple rows per auction_id (Zach, 2026-03-24):** A single auction is eligible for multiple campaigns, so `bid_logs` can have multiple rows for the same `auction_id` (one per campaign). Always dedup: `QUALIFY ROW_NUMBER() OVER (PARTITION BY il.ad_served_id ORDER BY b.time ASC) = 1` when joining via `impression_log.ttd_impression_id = bid_logs.auction_id`.
- **GCP data floor: January 1, 2025 (Zach, 2026-03-24).** No BQ table (silver or bronze) has data before 2025-01-01. Pre-2025 data only exists in Greenplum coreDW (deprecated April 30, 2026). All-time lookback queries are bounded by this floor.
- **90-day lookback sufficient for 99%+ of advertisers (Zach, 2026-03-24).** Most advertisers hit 99% resolution at 60 days. 90 days is overkill for the general case. Only WGU (31357) is a known outlier requiring >90d lookback. Other long-lookback cases (Zazzle, Ferguson Home) may be "neon pixel accounts" with special advertiser configurations — check `advertiser_configs` table for custom VV attribution windows. Optimization: run 90d lookback first, then all-time scan only for unresolved VVs.
- **Advertiser configurations for VV attribution windows:** `advertiser_configs` table (likely `bronze.integrationprod`) contains per-advertiser settings including custom attribution windows. Advertisers with extended lookback (like WGU) have special configs. Use this to identify which accounts will have edge cases in VV→impression resolution. (Zach, 2026-03-24)
- **Table design:** must support ALL stages per VV row. Stage 1 VV = S2/S3 cols NULL. Stage 3 VV = entire row full. Pipeline via SQLMesh. 90-day retention.
- **Deployment guidance (Dustin/dplat, 2026-03-05):** Silver layer is the correct location. SQLMesh recommended — handles orchestration and idempotency. Consider hourly materialization of source data first, then run the larger model over the reduced dataset. Set retention in the SQLMesh model or at table creation. Tag the table with the owning team. For very large batch processes, Spark + Airflow may be better.

### The IP Pipeline per Stage (empirically validated 2026-03-10)

Within a single CTV ad serve, there are only **2 distinct user IPs** per stage:

| IP | Table | Column | What it is | Validated |
|----|-------|--------|------------|-----------|
| **bid_ip** | event_log | bid_ip | Targeting identity (= win_ip = serve_ip = segment_ip) | bid=win: 38.2M rows, 47 differ (0.0001%) |
| **vast_ip** | event_log | ip | VAST playback IP — **enters next stage's segment** | bid≠vast: 1.02% (CGNAT /24 rotation) |
| redirect_ip | clickpass_log | ip | Redirect/visit IP (mutation boundary) | — |
| visit_ip | ui_visits | ip | Page view IP | — |
| impression_ip | ui_visits | impression_ip | Pixel-side IP (mobile/CGNAT fallback) | — |

Additional validations:
- vast_impression_ip ≈ vast_start_ip: 99.95% match (374/812,609 differ)
- win_logs.impression_ip_address: infrastructure/CDN IP (68.67.x.x MNTN infra, AWS IPs), NOT user IP
- **win_logs Beeswax→MNTN ID mapping (validated 2026-03-13):** `campaign_alt_id` = MNTN campaign_group_id; `line_item_alt_id` (STRING→INT64) = MNTN campaign_id; `creative_alt_id` = MNTN creative_id (unverified). Join: `CAST(w.line_item_alt_id AS INT64) = c.campaign_id`. Also join to event_log via `win_logs.auction_id = event_log.td_impression_id`.
- **CTV vs display identification:** At campaign level: `campaigns.channel_id = 8` = Television/CTV, `= 1` = Multi-Touch/display. At impression level: `win_logs.placement_type` (`VIDEO`/`BANNER`), `cost_impression_log.partner_ad_format` (`VIDEO`/`BANNER`). A single campaign_group can contain both CTV and display campaigns.
- **Impression trace paths — VV back to bid (confirmed by Zach 2026-03-13):** Key difference: for display, impression_log comes BEFORE win_logs (opposite of CTV). For viewable display, auction_id/ad_served_id let you skip impression_log and go straight to win_logs.
  - CTV: clickpass → event_log → win_logs → impression_log → bid_logs
  - Display viewable: clickpass → viewability_log → win_logs → bid_logs
  - Display non-viewable: clickpass → impression_log → win_logs → bid_logs
- **CTV chronological timestamp order (confirmed 2026-03-20, 225K VVs):** bid_time ≤ win_time ≤ impression_log_time ≤ event_log_time ≤ vv_time. Win notification precedes impression log entry in CTV pipeline. Display order: bid_time ≤ win_time ≤ impression_log_time ≤ vv_time (viewability_time ≤ vv_time).
- **0.0.0.0 placeholder IPs in event_log:** Some CTV VVs resolve to `0.0.0.0` via event_log. These are invalid/placeholder IPs — treat as equivalent to NULL for resolution purposes.
- **Display impression timing gap (TI-650, 2026-03-19):** Display impressions can be served 2-4 weeks before the user visits the site and triggers the VV. When tracing S3 VVs back to their impression via `ad_served_id`, a ±7d window around the VV time misses 35% of display impressions (Kindred Bravely: 768/2203 = 35% no_ip at ±7d, 0% at ±30d). CTV is same-day. **Use ±30d for the 5-source trace when display campaigns are in scope.** Verified on 3 advertisers: ±30d recovers 100% of impressions.
- **channels reference table:** `bronze.integrationprod.channels` — 10 rows. 1=Multi-Touch, 2=Email, 3=In-App, 4=Mobile Web, 5=Platform Fee, 6=Real Time Offers, 7=Social, 8=Television, 9=Ad Serving, 10=Onsite Offers.
- **v15 forensic trace (2026-03-12, 50 VVs):** IP is 100% identical across ALL 8 source tables (event_log, impression_log, CIL, bid_logs, win_logs, clickpass_log, ui_visits). serve_ip = bid_ip at 100%. Adding any source table to S1 pool has zero impact on resolution. The 8% unresolved S3 VVs entered via identity graph, not via MNTN impression.
- **bid_events_log is nearly empty** — only advertiser 32167 has data. Not useful for general IP lookups. Use bid_logs (Beeswax-native) instead.

**Cross-stage link (CORRECTED v20, 2026-03-16):**
- **S1 → S2 (impression-based):** `S2.bid_ip ≈ S1.vast_start_ip OR S1.vast_impression_ip`. S2 targeting = "had an S1 impression." Search event_log/viewability_log/impression_log for the S1 impression IP.
- **S1/S2 → S3 (VV-based):** `S3.bid_ip = prior_S1_or_S2.clickpass_log.ip`. S3 targeting = "had a prior S1 or S2 **verified visit**." Search `clickpass_log` for prior S1/S2 VV, NOT impression tables. Then: `prior_VV.ad_served_id → CIL.ip` to get the prior impression's bid_ip (may differ from VV ip due to cross-device!). For S2 VV → S1 chain: use the S2 impression's bid_ip to search S1 event_log.
- **Key insight:** In cross-device scenarios (CTV ad → phone visit), the VV clickpass IP ≠ the impression bid IP. The VV's clickpass IP is what enters the next stage's targeting segment. Prior analysis (v14-v18) searched impression tables and found zero — because the IP never had an S1/S2 impression, only an S2 VV. This was the ~8% "unresolved ceiling" — many are now traceable via the clickpass_log VV bridge.
- `first_touch_ad_served_id` links S3/S2→S1 directly (skips S2) but only 25-51% available.

### IP Mutation Key Findings (TI-650)
- **100% of mutation occurs at the VAST→redirect boundary** (Stage 3, CIL→EL or EL→redirect)
- **Aggregate mutation rate:** ~21.2% (14.28% inter-impression bid IP mutation for multi-impression VVs)
- **Cross-device is the primary driver:** 61.2% mutation when cross-device flag is set
- **Mutation range:** 1.2–33.4% across 15 advertisers in the reference dataset
- **Retargeting VV rate:** 59.8% of CTV VVs have a prior VV on the same bid_ip (= Stage 3 retargeting)
- **first_touch_ad_served_id bridge:** verified — 99.4% of ft UUIDs resolve to a real vast_impression
  in event_log. 30% of VVs with ft_id are multi-impression (ft ≠ lt).
- **Phantom NTB estimate:** ~4,006 events/day for advertiser 37775 (caused by IP mutation making
  verified NTB visits look like they came from households already in the ad graph)
- **Win → CIL join:** 100% reliable — always use `win_log.ad_served_id → CIL.ad_served_id`

### Cross-Stage VV Linking Research (TI-650, 2026-03-09)

The current audit table joins prior_vv_pool on IP (bid_ip or redirect_ip match), but IPs change
between stages, so ~60% of S2/S3 VVs cannot find their S1 origin via IP alone. Research into
alternative cross-stage identifiers:

**first_touch_ad_served_id** — the best available cross-stage link:
- Population rate: 44.2% globally (all advertisers, Feb 4-11 2026)
- For adv 37775: S1=91% populated, S2=32%, S3=34%
- When populated AND found in clickpass_log, 100% point to Stage 1 impressions (0% S2, 0% S3)
- However, ~40% of ft_populated S2/S3 rows cannot find their ft_asid in the 90-day CP lookback
  (these may reference impressions older than 90 days or from pre-BQ era)
- IP match rate (S2/S3 VV IP vs S1 ft VV IP): S2=64.5%, S3=71.9%
- GUID match rate (S2/S3 VV guid vs S1 ft VV guid): S2=25.2%, S3=42.9%
  (lower than IP because GUIDs are browser-specific — cross-device VVs get different GUIDs)

**guid (browser cookie)** — limited cross-stage utility:
- 607 GUIDs appear in all 3 stages (S1+S2+S3) within same campaign_group (1 week, adv 37775)
- 8,348 GUIDs appear in 2 stages (1,805 S1+S2; 2,382 S1+S3; 4,161 S2+S3)
- 185,265 GUIDs appear in only 1 stage (94% of all GUIDs)
- Problem: GUIDs are browser-level, CTV impressions use device GUIDs, visits use browser GUIDs.
  Cross-device attribution (CTV ad → mobile/desktop visit) always yields different GUIDs.

**ga_client_ids (Google Analytics Client ID)** — even more limited:
- 159 GA Client IDs span all 3 stages; 2,852 span 2 stages; 152,697 span only 1 stage
- GA Client ID is browser-specific AND requires GA to be installed — not available for all visits.
- Population rate: ~74-77% across stages for adv 37775.

**For S2/S3 VVs where first_touch IS NULL (the hard cases):**
- S2: 35,832 ft-NULL VVs — only 878 (2.5%) have a GUID match to any S1 VV in same campaign_group,
  571 (1.6%) have an IP match, 150 (0.4%) have a GA match. 34,702 (96.8%) have NO match at all.
- S3: 42,778 ft-NULL VVs — only 1,370 (3.2%) GUID match, 1,502 (3.5%) IP match, 245 (0.6%) GA
  match. 40,396 (94.4%) have NO match at all.
- **Conclusion:** For ft-NULL VVs, none of the available identifiers can reliably trace back to S1.
  These VVs are fundamentally unlinkable with current data — the S1 impression that triggered the
  funnel progression is either too old, or the VVS determined the VV without an S1 match
  (e.g., via GA Client ID cross-device expansion to an IP with no S1 clickpass record).

**event_log.td_impression_id = cost_impression_log.impression_id** — confirmed join:
- 100% populated in event_log (38.7M/38.7M on 1 day)
- Joins reliably to CIL.impression_id (ad_served_id matches at near-100%)
- Useful for enriching impression-level data but does NOT help cross-stage linking
  (it links within a single impression event, not across funnel stages)

**cost_impression_log.model_params** — no audience/segment identifiers:
- Contains: geo_version, device_type_group, flight_id, campaign_id, campaign_group_id,
  advertiser_id, pmp_deal_id, household_score, advertiser_household_score, realtime_conquest_score
- Does NOT contain: segment_id, audience_id, audience_upload_id, or any targeting segment reference
- Cannot determine which audience segment was targeted for a given impression

**Bottom line (CORRECTED 2026-03-16):** Cross-stage linking depends on which stage you're entering. S1→S2 uses **vast_ip** (impression-based). S1/S2→S3 uses **clickpass_log.ip** (VV-based). The prior analysis that found a ~92% IP ceiling for S3 was searching the wrong table — impression tables instead of clickpass_log. The VV's clickpass IP is what enters S3 targeting, and in cross-device cases it differs from the impression bid IP entirely.

**v20 resolution rates (10 advertisers, Feb 4–11, 90-day lookback, prospecting obj 1,5,6, campaign_group_id scoped, VV bridge):**
- S2: 97.95–99.87% (unchanged — S2→S1 impression-based link was already correct)
- S3: 74.54–99.47% (massive improvement from v14's 58.56–96.56%)
- VV bridge resolves most cross-device S3 VVs that were previously unresolvable
- adv 37775: 91.98% → **99.05%** (+7.07pp, unresolved dropped 1,761 → 75)
- adv 42097: 61.59% → **98.48%** (+36.89pp — was the worst, now nearly resolved)
- adv 34835: 81.45% → **99.34%** (+17.89pp)
- adv 31357: 58.56% → **74.54%** (+15.98pp, still lowest — heavy identity-graph population)

**The "92% ceiling" was wrong (corrected 2026-03-16).** Prior analysis (v14-v18) found ~92% S3 resolution via impression tables. This was an artifact of searching the wrong table. With the VV bridge (clickpass_log), the true ceiling is **~99%** for most advertisers. The remaining ~1% are IPs with no prior MNTN VV or impression in the same campaign group (true identity-graph-only entries).

**Retargeting pool impact (tested 2026-03-12):** Adding retargeting campaigns (obj=4) to the S1 pool resolves 110 additional S3 VVs for adv 37775 — IPs whose first MNTN impression was retargeting, not prospecting. Business decision: audit scope = "first prospecting touch" vs "first MNTN touch."

**Irreducible floor (updated 2026-03-16):** With VV bridge, only 75 S3 VVs unresolved for adv 37775 (0.33% of CIL cohort). These have no prior S1/S2 VV or impression IP match within the campaign group. Plus 1,074 VVs with no CIL record (pipeline gap, not TTL). Prior 567-unresolved GUID bridge analysis used the wrong cross-stage methodology — most of those 567 are now resolved via VV bridge.

**campaign_group_id scoping (Zach directive, 2026-03-12):** Cross-stage IP linking MUST be scoped within the same `campaign_group_id`. A VV in one campaign group cannot be linked to an impression in a different campaign group — that would be a coincidental IP match, not a real funnel trace. `campaign_group_id` is unique across advertisers. This constraint must be enforced in the production `vv_ip_lineage` model.

**Former "zero-chain" advertisers now resolved:** 4/10 advertisers (31276, 31357, 34835, 42097) had zero S3→S2→S1 chain in v14 because no S2 VAST event matched S3 bid_ip. v20 reveals they all had substantial VV-based chains — the old query was searching event_log instead of clickpass_log. Example: adv 42097 went from 0 chain resolutions to 11,399 via S2 VV chain.

**S1 pool lookback: 90 days is sufficient for 100% resolution (TI-650 v21c, 2026-03-17).** Initial analysis using MIN(impression_time) showed 186-day max gap, but this was biased — selecting the oldest of multiple matches. Using MOST RECENT prior S1 match: max 69 days, median 6 days, P95 29 days, P99 35 days. Verified: full v8 query (5-source IP trace + 4-table S1 pool + CIDR fix) with 90d lookback = 68,498/68,498 = **100% resolved**, matching the 180d result. **For production: use 90d lookback** — confirmed sufficient, halves scan cost vs 180d.

### Notable Advertisers
- **31357 = WGU (Western Governors University).** ~30% of MNTN monthly spend (entire business). Largest single advertiser. Has abnormally long S3 lookback window per Zach. Online degree program. Any analysis using this advertiser as test case should note it's an extreme outlier in spend and funnel depth. (Zach confirmed 2026-03-17)

### attribution_model_id Clarification (from TI-650)
- `ad_served_id` = **last-touch** attribution — the most recent impression that led to the VV
- `first_touch` = the first impression in the multi-impression sequence (NULL ~40% — permanent,
  confirmed by Zach: "no post processing" means first_touch NULL is not backfilled)
- `first_touch` NULL rate inversely correlates with age (54% at <1hr, 18% at 14-21 days) —
  this confirms batch processing, not a lookback limitation
- 91.76% of VVs have ad_served_id more recent than first_touch (as expected for last-touch)

### MES Pipeline Architecture (3-Stage Verified Visit Model)
Each stage can be up to 30 days apart. IP mutation BETWEEN stages is expected.
IP mutation WITHIN a stage (Stage 3's internal Bid→CIL→EL→Redirect→Visit chain) is the audit subject.

Stage 3 VV production audit table: `audit.vv_ip_lineage` (renamed from stage3_vv_ip_lineage)
- All stages (S1/S2/S3), partitioned by `trace_date`, clustered by `advertiser_id` + `vv_stage`
- **v12 architecture (target):** 2-link S1 resolution (`imp_direct`: S1 vast_start_ip = bid_ip; `imp_visit`: S1 vast_start_ip = impression_ip). Replaces v11's 10-tier cascade — all other tiers empirically proven redundant.
- Stage-based column naming: `s3_*`/`s2_*`/`s1_*` (each stage has 5 IPs + impression_time + guid)
- 90-day lookback (Zach confirmed max chain = 88 days: 14+30+14+30). Production default 120d for WGU outlier.
- **S3 resolution (v3, 10 advertisers, bid_ip only): 99.83%** (138,317/138,557 resolved, 44 unresolved)
- **S3 resolution (v2, 20 advertisers, 5-source): 99.83%** (225,491/225,872 resolved, 381 unresolved)
- Previous "~11% ceiling" was inflated by retargeting campaigns. Zach: retargeting not relevant. Filter with `objective_id IN (1, 5, 6)`.
- Remaining unresolved = lookback window insufficient, source table TTL truncation, or data bug. NOT identity graph entry — every VV MUST follow the IP path. 100% resolution achievable with sufficient lookback.
- Full schema reference: `tickets/ti_650_stage_3_vv_audit/artifacts/ti_650_column_reference.md`

---

## Greenplum (coreDW) Patterns

### coreDW Deprecation
**Deprecation date: April 30, 2026.** After this date, coreDW (Greenplum) will no longer receive updates.
BQ silver is the validated replacement — the full-scale run of Stage 3 VV audit matched GP within 0.12pp.

**Important:** BQ bronze.raw is a **non-random subset of Greenplum data (~25% of GP volume).**
Always use BQ silver (not bronze.raw) as the GP replacement.

### Greenplum-Specific SQL Syntax
Common GP-specific patterns that don't translate directly to BQ:
- `~` operator = regex match (BQ: use `REGEXP_CONTAINS`)
- `::` casting (BQ: use `CAST()`)
- `host(ip)` = strip /32 CIDR notation from inet type (BQ: use `NET.IP_FROM_STRING` or `SPLIT`)
- INET type: `ui_visits.ip` and other IP columns in GP are INET type — use `host(ip)` to get plain string
- Temp tables: `CREATE TEMP TABLE` is standard GP syntax; BQ uses CTEs or `CREATE TEMP TABLE` differently

### Key Greenplum Tables (not in BQ catalog)
These exist in Greenplum coreDW but may not have BQ equivalents:

| Table | Schema | Purpose |
|-------|--------|---------|
| `tpa.membership_updates_logs` | Greenplum | TPA membership update log |
| `summarydata.sum_by_campaign_group_by_day` | Greenplum | Daily pre-aggregated rollup by campaign group |
| `dso.valid_campaign_groups` | Greenplum | Active/valid campaign groups for DSO analysis |
| `fpa.advertiser_verticals` | Greenplum | Advertiser → vertical mapping (`type=1` = primary) |
| `r2.advertiser_settings` | Greenplum | Advertiser-level settings (`reporting_style='last_touch'`) |
| `audience.campaign_segment_history` | Greenplum | Campaign segment change history (see contamination warning above) |
| `summarydata.v_campaign_group_segment_history` | Greenplum | VIEW — segment history by campaign group |
| `audience.audience_segment_campaigns` | Greenplum | Maps active audience segment → campaign_group |
| `audience.data_sources` | Greenplum | Data source registry (DS IDs, names, types) |
| `geo.locations` | Greenplum | Geo location reference table (location_id, state/country names) |
| `public.campaign_groups` (aliased as `campaign_groups_raw`) | Greenplum | Campaign group dimension |
| `logdata.impression_log` | Greenplum | All bids (won or lost) — IP columns: ip, ip_raw, bid_ip, original_ip |

### IP Columns in Greenplum impression_log
`logdata.impression_log` has 4 distinct IP columns:
- `ip` — INET type, the primary IP (cast to text for joins: `il.ip::text`)
- `ip_raw` — raw IP before processing
- `bid_ip` — the IP used in the bid
- `original_ip` — pre-proxy original IP

---

## Email / Conversion Analysis Patterns

### Email Columns in conversion_log
`logdata.conversion_log` has two email-related columns:
- `email` — hashed email from pixel (SHA256)
- `email_data` — additional email metadata

**Empty email hash:** `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
(SHA256 of empty string) — appears when the pixel fires but no email value is present. Always
exclude this hash when counting qualifying emailled conversions.

**Email prevalence threshold:** ~0.5 is used in NTB/email analysis to determine which advertisers
have sufficient email data to include in the analysis.

### conversion_log.query Field — Two Email Extraction Patterns (Greenplum)

The `query` column in `logdata.conversion_log` stores pixel query string data in two different
formats. Classify first, then extract accordingly:

```sql
-- Step 1: Classify format
CASE
  WHEN query LIKE '{%' AND query NOT LIKE '%{%22%' THEN 'json'
  WHEN query LIKE '%=%' THEN 'querystring'
  ELSE 'other'
END AS query_format

-- Step 2: Extract email_data based on format
CASE
  WHEN query_format = 'json'
    AND (query::json->>'email_data') IS NOT NULL
    THEN query::json->>'email_data'
  WHEN query_format = 'querystring'
    AND query LIKE '%email_data=%'
    THEN split_part(split_part(query, 'email_data=', 2), '&', 1)
END AS email_data
```

Both `email` and `email_data` fields can be the source of email signals — always check both and
use `COALESCE(email, email_data)` for combined NTB analysis. Prevalence threshold: ≥0.5
(50%) to include an advertiser in NTB email analysis.

### ui_conversions vs conversions
`summarydata.ui_conversions` (Greenplum) uses `order_amt` for purchase amount.
**Do NOT use `order_amt_usd`** — this column is NULL in ui_conversions. Use `order_amt` directly.

### ui_visits.impression_time
`ui_visits.impression_time` = the timestamp of the original impression (NOT the visit time).
Use this when filtering visits by the period when ads were being served (e.g., "visits attributed
to impressions during Oct 17 – Dec 4, 2025").

---

## tmul_daily vs tpa_membership_update_log — Schema Differences

These two tables cover related data but have different structures and must NOT be used interchangeably.

| Aspect | tmul_daily | tpa_membership_update_log |
|--------|-----------|--------------------------|
| Type | Snapshot (daily state) | Change log (deltas) |
| Partition | `time` TIMESTAMP (hourly, 08:00 UTC) | `dt` STRING + `hh` STRING (zero-padded) |
| TTL | **14 days** | No stated expiration; data from 2025-11-21 |
| Data Sources | DS 2 and DS 3 ONLY | DS 2 and DS 3 (DS 4 NOT confirmed) |
| Size | ~32B rows, ~14.5TB | Unknown |
| `id` column | IP address | IP address |
| Unnest path | `UNNEST(td.in_segments.list) AS isl` → `isl.element.segment_id`, `isl.element.advertiser_id`, `isl.element.campaign_id` | `UNNEST(td.in_segments.segments) AS isl` → `isl.segment_id` (no `.element`) |
| Snapshot time | 08:00 UTC daily | Event-driven |

**Key gotcha:** DS 4 (CRM data) does NOT appear in tmul_daily at the row level. CRM membership
is resolved via the identity graph and stored in ipdsc__v1 instead.

---

## Additional Gotchas (TI-748 findings)

- **`funnel_level` is on `campaigns`, NOT `campaign_groups`**: `campaign_groups` has no `funnel_level` column. Use `campaigns.funnel_level = 1` for prospecting filter.
- **`agg__daily_sum_by_campaign` effective start: 2025-09-01**: Despite the GCP data floor of 2025-01-01, the aggregates table only has data from September 2025 onwards.
- **`agg__daily_sum_by_campaign` STALE since 2026-03-31** (confirmed 2026-04-27, TI-837 Phase 2): MAX(day) is 2026-03-31; no April 2026 data. For analysis windows after 2026-03-31, use `cost_impression_log` directly (not aggregates) or `sum_by_campaign_by_day` (max=2026-04-14). Pipeline owner unknown — flag if blocking work.
- **`uniques` in `agg__daily_sum_by_campaign` is unreliable for per-advertiser analysis**: The column exists but often contains zeros or values that don't aggregate meaningfully at the campaign level. Do not use VVR (vv/uniques) as a metric from this table.
- **Low-impression weeks produce extreme rate metrics**: When campaigns pause but VVs still attribute (lookback window), you get weeks with e.g. 7 impressions and 2,564 VVs (IVR=366). Always filter weeks with <1,000 impressions when computing rate metrics.
- **`r2_advertiser_settings` has no `deleted` column**: Unlike most integrationprod tables, this table has no deleted/is_test flags. All rows are valid.
- **`sum_by_campaign_by_day` starts 2024-01-01**: 15+ months of history. Use for experiments needing long pre-periods (52 weeks). `agg__daily_sum_by_campaign` only starts Sep 2025. Same columns except `uniques` is HLL BYTES (not usable as integer count).
- **`media_plan_publishers.name` matches `sum_by_ctv_network_by_day.domain` exactly**: "CBS" = "CBS", "HBO Max" = "HBO Max". Cross-validated against `cost_impression_log` — same publishers, same rank order.
- **`sum_by_ctv_network_by_day` is validated against `cost_impression_log`**: Top-5 publishers match (same names, same ranking). Count differences are from campaign-level filtering. Valid proxy for publisher-level analysis.
- **CRITICAL — scope media plan queries to plan campaign groups only**: When analyzing media plan delivery, ONLY include impressions from campaigns in `media_plan.campaign_group_id`. Including ALL advertiser campaigns produces wildly incorrect results (appeared like plans weren't followed when they actually were ±3%). Lesson learned painfully in TI-748.
- **`media_plan_publishers.badge_state`**: `RECOMMENDED` (user accepted MNTN allocation), `USER_MODIFIED` (user changed), `USER_ADDED` (user added a publisher). Use to filter recommended-only plans.
- **Media Plan algorithm pipeline (Chris Addy + olympus repo, 2026-03-27)**: semantic search (FAISS + Gemini embeddings, top 40) → spend capacity filter (≥$0.50/hr, added Feb 3 2026) → multi-signal scoring → softmax(alpha=5.0) allocation → drop <0.5% → enforce min=10/max=15 → cap 12% per network → LLM rationale (Gemini 2.0 Flash). Score weights: performance 25%, quality 25%, semantic 20%, ML prediction 10%, spendability 8%, CPM efficiency 6%, scale 4%, accessibility 2%. Performance composite blends advertiser (50%), vertical (30%), network (20%) history. The spend capacity filter acts as a hard gate BEFORE scoring — low-inventory networks get filtered out before scoring happens.
- **CRITICAL config change Feb 3, 2026** (olympus commit `555234f`, PERML-412): `max_networks` changed from 25 → 15, `min_networks` from 15 → 10, AND spend capacity filtering added. Every plan before this date has 25-26 publishers; every plan after has exactly 16. This single change explains the TI-748 performance split.
- **ML model feature skew**: The ML prediction model (10% weight) has 52 input features but only 11 receive real data at inference — 41 are zeroed. Top-importance features (advertiser_mean_score, trend features) are completely missing. Fix planned but not yet implemented (olympus `specs/backlog/ml-primary-scoring-mode.md`).
- **Media Plan config parameters**: `alpha=5.0` (softmax temperature — higher = more concentrated), `max_networks=15`, `min_networks=10`, `max_allocation=12%`, `min_allocation=0.5%`. Plans with >15 publishers were likely generated under old config or overrides.
- **Per-publisher scores available in API response**: `score_semantic`, `score_performance_advertiser`, `score_performance_vertical`, `score_performance_network`, `spendability_score`, `score_cpm_efficiency`, `score_scale`, `score` (final combined). Chris Addy checking if persisted to BQ or just API response logs.
- **Media Plan algorithm source code**: `github.com/steelhouse/olympus` repo. Has docs populated for Claude Code exploration. Contact: Chris Addy (tech lead).
- **`deliverability_classification`**: Categorical delivery risk — "high" (expect full spend), "medium" (moderate underspend risk), "low" (high underspend risk). Computed by guardrail model: per-network daily spend thresholds, audience size, blocked networks, budget constraints. Final = worst individual guardrail. In-flight override: >3 days in + >90% pace → upgraded to "high". HHI (Herfindahl index) tracked in metrics but NOT a classification factor yet.
- **Flex Targeting = typically 10% budget reserve**: Not assigned to specific networks. Bidder uses for real-time optimization — un-recommended publisher impressions (e.g., Tubi Entertainment) come entirely from this flex pool. Explains ±3% deviation between recommended and actual allocations.
- **Without media plan, bidder does NOT optimize network allocation**: Impressions go to a huge inventory pool managed by the inventory team's deal commitments (e.g., "told HBO we'd spend $1M in Q1"). No performance-based network optimization. Manual adjustments only when customers complain about concentration. This explains pre-adoption spread of 131-183 publishers.
- **Media plan generated for BOTH Prospecting and Retargeting**: But only surfaced to customer for Prospecting campaigns. Retargeting plans run on backend — customer has no knowledge of them and cannot edit.
- **Beta selection bias**: Advertisers are hand-picked by PEX/CS (identified candidates with past interest), then validated by Toph (production ops) for pacing risk. NOT randomized. Important confound for causal analysis.
- **M1 beta released 2025-10-20**: TV-Only Prospecting only. No opt-in — required for all AIDs in beta. Changes only before campaign launch (mid-flight not supported in M1).
- **M2 (EOM January 2026)**: UI reskin — media plan auto-applied when user clicks the step. Happy path = don't edit. Secret bypass: skip media plan step entirely.
- **Dynamic media plan coming (TBD release)**: Recurring regeneration/rebalancing during campaign flight. Blocks planned experiment — can't test static version if dynamic is imminent.
- **`advertiser_configurations.conversion_lookback_window`**: NULL for most advertisers (use system default of 30 days). Only ~620 advertisers have non-null values (mostly 30, some 33/35/45/60 days).

---

## Keyword Targeting & BUK (Bottoms Up Keywords)

### Mountain Match V2 (Current Production)
- Flow: Scrape advertiser homepage (Common Crawl) → LLM describes products/services → LLM generates 20 parent keywords → LLM expands each to 10 search terms (200 total) → Embedding alignment maps to closest DS19 `data_source_category_id` → collapses to <200 unique DS19 keywords → audience expression
- Autopilot endpoint handles the LLM steps; Search Term endpoint handles DS19 alignment
- Parent keywords = user-facing labels in UI. Child keywords = DS19 IDs in the audience expression (not shown to customers)
- Once generated, keywords are static — no dynamic updates based on customer behavior or seasonality
- If homepage scrape fails, falls back to URL-based description (usually poor quality)

### BUK (Bottoms Up Keywords) — ALS Model
- Flow: `good_log` + `conversion_log` (30-day window) → build advertiser × DS19 keyword interaction matrix → Train implicit ALS model → Generate ranked DS19 recommendations per advertiser → k-means cluster by embedding into ~20 groups → LLM generates parent keyword labels/descriptions → audience expression
- **Confidence signal**: Weighted blend of distinct IP count, conversion count, cart volume, avg daily IPs, avg daily conversions (log1p transformed, configurable weights per signal)
- **Score adjustments**: (1) Popularity penalty — log odds ratio of keyword rarity, suppresses generic keywords like "accessories", "web services". (2) Advertiser lift — how popular keyword is for this advertiser vs. global average
- **Threshold**: Single percentile-based cutoff on model scores (~top 42%). Replaces fixed 200-keyword rule. Stronger-signal advertisers naturally get more keywords
- **Cold start**: Advertisers not in training data can't get ALS recommendations. Current fallback = vertical averages. Planned = MM V2 fallback
- **"Web services" pollution**: Google Tag Manager URLs get classified as web services by the LLM, associating this keyword with nearly every advertiser. Popularity penalty suppresses it

### Data Sources Overview
- **`bronze.integrationprod.data_sources`**: Master dimension table for all data sources
- **`bronze.integrationprod.categories`**: Category names/descriptions per data source (hierarchical)
- **`bronze.integrationprod.keyword_categories`**: Keyword strings for DS38 (MNTN UI Audience Keywords, ~40M rows)
- **`bronze.external.ipdsc__v1`**: IP-to-data-source-category mapping. Columns: `ip`, `data_source_id`, `data_source_category_ids` (ARRAY), `dt`. Filter by `dt` and `data_source_id`

### DS13: "MNTN Vertical Categorization"
- Manually curated vertical buckets used to classify advertisers into industry categories
- 37 top-level verticals (Apparel, Electronics, Pets, Real Estate, etc.) with 148 leaf categories
- Lookup: `categories WHERE data_source_id = 13`
- The `data_source_category_id` values ARE the vertical identifiers

### DS19: "MNTN Matched" (Keyword Matching)
- The keyword matching data source — `data_source_category_id` values are linked to targetable keywords
- `visible = false` in `data_sources` table (internal only)
- ~20,000 keywords representing product categories (e.g., "Dog Beds" = 905072, "Pet Accessories" = 922262)
- Used in audience expressions for both MM V2 and BUK
- In `ipdsc__v1`: each IP has an array of DS19 `data_source_category_ids` it's been matched to
- NOTE: DS19 category IDs do NOT have rows in the `categories` table with `data_source_id = 19`. They share IDs with DS16 ("MNTN Taxonomy Data") in the `categories` table. The human-readable product category names (e.g., "Dog Beds" for ID 905072) come from the URL classification pipeline: advertiser URLs are classified via LLM/embedding into a product category name + DS19 `data_source_category_id`. These name mappings live in the BUK feature store (Databricks/Airflow pipeline output), not in a BQ dimension table
- DS38 ("MNTN UI Audience Keywords") in `keyword_categories` contains user-facing keyword strings (~40M rows) but uses different `data_source_category_id` values than DS19

### DS16: "MNTN Taxonomy Data"
- Per-advertiser taxonomy tree with hierarchy: ROOT → AdvertiserID → {PageViews, Conversions, Impressions, VV, Wins} → {Prospecting, MultiTouch, Retargeting} → CampaignGroupID → CampaignID
- 1.6M+ categories in `categories WHERE data_source_id = 16`
- Shares `data_source_category_id` values with DS19 (same IDs appear in both contexts)

### Shopper Graph API
- Internal API at `shopper-graph.in.mountain.com/autopilot?advertiser_id={id}` (Tailscale VPN required)
- Returns both MM V2 keywords and BUK keywords per advertiser in a single response
- BUK payload includes parent keyword groups with child keyword IDs and model version hash
- Currently: every DAG retrain overwrites ALL advertiser keywords (not idempotent — planned fix)

### Continuous Scoring (Fangorn + Keywords)
- Combines Fangorn intent score (s, 0-1) with BUK keyword evidence score (K, 0-1)
- Keyword score: `K = 1 - exp(-β * Σ 1/log2(rank+1))` where sum is over matched keywords
- Blending options: geometric `F = s^(1-γ) · K^γ` or linear `F = (1-γ)s + γK`. γ=0.25 (intent-dominant)
- Missing score fallback: `COALESCE(fangorn_score, keywords_score, 0)`
- Final score mapped to bidder scale: <0.6 = Max Reach (0-3333), 0.6-0.8 = Mid Intent (3333-6666), 0.8+ = High Intent (6666-10000)
- Rollout: (1) Fangorn release, (2) continuous scoring with Fangorn + MM V2 equal-rank keywords, (3) wire in BUK rankings
- DDP site visit signals already incorporated into BUK model training (confirmed Alex 2026-03-31)

### Keyword Value: Advertiser-Specific, Not Universal (TI-804, 2026-04-02)
- Per-advertiser keyword ranking: **184x visit rate differential** (top-5 vs bottom keywords)
- Global keyword ranking (across all advertisers): only **3x range**, correlation with BUK rank = 0.11
- Keyword quality is advertiser-specific: "Dog Beds" is rank-1 for K9 Ballistics, irrelevant for Rocket Lawyer
- BUK's ALS collaborative filtering captures this per-advertiser signal; MM V2's LLM homepage scrape cannot
- 93% of tested advertisers show >10x lift, all 15 verticals positive
- Validates continuous scoring for keywords (not just verticals) — the keyword signal is real and massive

### BUK Pipeline
- Runs as Airflow DAG in `airflow-ti` repo (SteelHouse/airflow-ti)
- Training and prediction on Databricks (job compute = 1/4 cost of interactive)
- Predictions output to GCS: `gs://targeting-infra-vertex-pipelines-prod/bottom-up-keywords/batch-predictions/dt={date}/` (parquet, ~18MB, includes advertiser_name, vertical_name, product_category, rank, score_adj)
- Feature store: recently migrated from Databricks-only to Airflow VS (Vertex/Spark)
- Local dev: Astronomer (`astro dev start/stop`), uv venv with python 3.11

### BUK Beta Customers (as of 2026-03-31)
- 40279 West Bend Insurance — live campaign
- 45594 Samy's Camera — live campaign
- 33129 Apollo.io, 37336 Global Rescue, 33610 Amsterdam Printing, 48687 Apolla, 35374 Experience Scottsdale — talked, not all live yet

## Feature Store & Bidstream Feature Analysis (TI-789/790)

### Key Finding: Pre-Visit vs Feedback Features
When building IP-level feature vectors to predict visits (IVR), features split into two categories:
- **Pre-visit features** (available at bid time): bidstream, impression, and win data. AUC ~0.896.
- **Feedback features** (available after site visit): guid_log pixel events, conversion_log purchase data. AUC ~0.999 but leaky — presence of guid_log data implies a visit already happened.

**Do NOT mix pre-visit and feedback features in a single targeting model** — guid_log/conversion_log features will dominate and mask the real predictive signal from bidstream features. Use feedback features for retraining, scoring returning visitors, and identity resolution.

### Top Pre-Visit Features for Targeting (by SHAP)
1. `al_avg_segments` (augmentor_log) — average MNTN segments on the IP
2. `ci_pct_new` (cost_impression_log) — % impressions where IP is "new"
3. `ci_pct_rtc` (cost_impression_log) — % RTC-targeted impressions
4. `ci_total_cost` (cost_impression_log) — total media spend on IP
5. `wl_avg_price` (win_logs) — average clearing price
6. `al_n_auctions` (augmentor_log) — auction volume for this IP
7. `wl_n_models` (win_logs) — device model diversity

### Bronze-Only Fields in augmentor_log
The silver view (`v_augmentor_log`) drops several fields present in bronze (`raw.augmentor_log`):
- `iab_categories` — IAB content taxonomy (30% fill). Key for vertical classification.
- `categories` — additional content categories (13% fill)
- `isp` — ISP name (10% fill)
- `page`, `referrer` — page URL and referrer (15%, 4%)
- `is_blocked`, `blocking_site` — brand safety (0% — no signal)

Must use `bronze.raw.augmentor_log` for iab_categories, NOT the silver view.

### content_genre in bidder_auction_events
- 87% fill, 37K+ distinct raw values
- Case-inconsistent: "Entertainment" vs "entertainment" vs "GENRE_COMEDY"
- Comma-delimited multi-genre: "sitcom,comedy"
- Normalize: `LOWER(SPLIT(content_genre, ',')[SAFE_OFFSET(0)])`, strip `genre_` prefix
- After normalization: ~50-100 clean genres
- Strong IP-level differentiation: residential IPs show 99%+ concentration in single genres

### guid_log product Field is JSON in Silver
In silver.logdata.guid_log, `product` is JSON type (not RECORD like bronze):
- Use `JSON_VALUE(product, '$.CATEGORY')`, not `product.CATEGORY`
- Fields: CATEGORY, BRAND, NAME, AMOUNT, SKU, INVENTORY_COUNT, CURRENCY, IMG_URL, REFERRER, AVAILABLE_UNTIL

### conversion_log query Field is JSON with key_value Array
Structure: `{"key_value": [{"KEY": "shoid", "value": "xxx"}, ...]}`
- Use `TO_JSON_STRING(query)` with `REGEXP_CONTAINS` for searching
- Key fields inside: `shoamt` (75%), `shpt` (74%), `ga_client_id` (67%), `email_data` (2.3%), `androidId/idfa/adid` (~3%)

### cost_impression_log Gotchas
- `recency_elapsed_time` is INTERVAL type — extract with `EXTRACT(SECOND FROM ...) + EXTRACT(MINUTE FROM ...)*60 + ...`
- `household_score = -1` means unscored
- `advertiser_household_score = 10000` means RTC conquest
- `partner_ad_format` is authoritative for VIDEO vs BANNER

### augmentor_log TTL and Archives
- BQ TTL: 10 days. Parquet archive: ~30 days at `gs://mntn-data-archive-prod/augmentor_log/region={east,west}/dt=YYYY-MM-DD/hh=HH`
- Ryan's pipeline (`aug_log_ip_vertical_id_hourly.py`) reads from parquet, runs hourly, maps domains to vertical IDs via tldextract
- Output: `gs://mntn-data-archive-prod/feature_store/feature_group_1_source/` partitioned by dt/hh
- Pipeline code: `steelhouse/airflow-ti` repo, `models/feature_store/feature_group_1_source/`

### BQ dry-run is unreliable on federated tables (confirmed 2026-04-27)
A query touching `household_scoring__prospecting_intent__v1` (Parquet external table over `gs://household-scoring-prod/...`) dry-runs at 610 GB but actually processes **18.1 TB** when run — ~30× under-estimate. The estimator cannot see into the federated source's actual scan footprint. Rule: for any query that joins a federated/external table, treat the dry-run as a lower bound only. Sample on a smaller window (1 day) before committing to a larger run.

### `bq query` crashes on SQL strings starting with `--` (workaround: pipe via stdin) (2026-04-27)
The `bq` CLI uses Google's `absl` flag parser. When the SQL is passed as a positional argument that begins with `--` (e.g., a SQL block-leading comment line `-- TI-XXX: ...`), absl interprets the entire SQL string as an unknown flag and tries to compute Levenshtein distance suggestions against known flags. The recursive distance function blows Python's recursion limit and crashes with `RecursionError: maximum recursion depth exceeded` from `_damerau_levenshtein`. No query is dispatched — the bq process never reaches BigQuery.

**Workaround:** pipe SQL to `bq query` via stdin instead of passing as a positional argument. Works through the `bq_run.sh` wrapper too:
```bash
bash .claude/scripts/bq_run.sh --ticket "TI-XXX" --label "..." \
  --use_legacy_sql=false --format=prettyjson --max_rows=500 --project_id=dw-main-silver \
  < path/to/query.sql > output.json
```

Alternative: strip the leading `--` comment from the SQL before passing positionally (workable but brittle). Stdin is the durable fix.

### `clickpass_log` cannot do apples-to-apples holdout comparisons (2026-04-27)
By definition, a `clickpass_log` row requires (a) a MNTN impression AND (b) a subsequent visit matched to that impression. Holdout IPs (per-advertiser hash bucket 0-99) by construction don't get served impressions for that advertiser → essentially cannot generate clickpass rows for that advertiser. So any clickpass-based "lift" calculation between treatment and holdout will show enormous lift simply because the table's existence requires the treatment.

The non-zero clickpass entries observed for holdouts are spillover: the holdout hash is per-(AID, IP), so an IP that's a Zazzle holdout might be served Ferguson Home ads, get a Ferguson clickpass row, and visit Zazzle in the same window — but the visit gets attributed to Ferguson, not Zazzle. These are tiny.

**Implication for incrementality measurement:** use `guid_log` as the honest test of total-traffic causation. Use `clickpass_log` only to quantify *attribution capture* — the wedge between the two = visits MNTN takes credit for that would have happened anyway. For Zazzle high-intent ATT (TI-837, 1-day, 2026-04-27): clickpass lift +1.49pp / guid lift +1.30pp → ~78% of MNTN-attributed visits would have happened anyway via other channels.

### Canonical prospecting_intent table (used in TI-837)
`dw-main-bronze.external.household_scoring__prospecting_intent__v1` — federated Parquet table over `gs://household-scoring-prod/output/scoring/prospecting_intent/year=YYYY/month=MM/day=DD/`. 10-day rolling retention in BQ; deeper history (35-day) accessible via raw GCS. Schema: `ip, advertiser_id, campaign_group_id, campaign_id, household_score, year, month, day`.

**Intent tier from household_score** (per Alex Knorr's TI-835 thresholds):
- 10000 = `high` (vertical + keyword match)
- 7000-9999 = `peak` (vertical only)
- 3333-6999 = `mid` (keyword only)
- <3333 = `max_reach`

**Coverage gap:** keyword-only advertisers (e.g., WGU 31357) are NOT in this table — they're scored via a different pipeline (Mountain Match V2 / DS19 keyword match without DS13 vertical scoring). For TI-835's 9-advertiser sample: 7 are present (Ferguson Home, Ancient Nutrition, First Watch, HexClad, Clayton Homes, Zazzle, Northern Tool); 2 are absent (Angi 32766, REVOLVE 53308) — same exclusion pattern as WGU.

### augmentor_log `mntn_segments` for holdout IPs (confirmed 2026-04-22)
When a holdout IP appears in `augmentor_log`, the `mntn_segments` array **does NOT contain the segment the IP is a holdout of**. The filter logic strips the segment at audience evaluation time — holdouts are not qualified to bid on, so the matching segment is never attached to the row.

This means you **cannot** use `mntn_segments` as a proxy for "would we have bid on this IP if not for the holdout." The implication for ghost-bidding / lift measurement:
- Pick the target audience externally — reconstruct it from `prospecting_scores` / DS13 + DS19 overlap or from `audience_segments.expression`
- Intersect that targetable IP universe with the holdout hash (`MD5('{AID}:{IP}')` mod 1000 ∈ 0-99)
- Look those IPs up in `augmentor_log` inside the campaign window — appearances prove biddability (IP was seen by the augmentor, so it was eligible for *some* bid request) even without the segment match on the row itself
- Matt Brorby verified on a few test IPs; Malachi and Alex Knorr corroborated on Zoom 2026-04-22

Confusingly, the 2026-04-20 verification that "10% of unique IPs in augmentor_log hash into buckets 0-99" is still correct — holdout IPs are present in the log, just not attached to the segment they're a holdout of. The two facts are consistent.

**Also:** advertiser_id 90 is a MNTN PSA advertiser. PSA impressions are served to holdout IPs intentionally (they show up in `cost_impression_log` for buckets 0-99). Exclude AID 90 from any holdout-based lift analysis — Alex Knorr hit this as a confusing anomaly before the PSA fact was surfaced.

### Parquet vs BQ Schema Differences (confirmed TI-810)
- **augmentor_log parquet LIST fields** (`pmp`, `iab_categories`, `mntn_segments`): Parquet legacy LIST format = `struct<list: array<struct<element: T>>>`. `F.size(F.col("pmp.list"))` fails in Spark — interpreted as map subscript. Use `F.col("pmp").isNotNull()` instead.
- **guid_log `product` column**: STRUCT in parquet (`{amount, brand, category, currency, ...}`), but appears as flat columns in BQ silver view. Cannot compare to string `"null"` — use `.isNotNull()`.
- **General rule**: Always inspect raw parquet schema before writing Spark aggregations — BQ silver views enrich/flatten types differently from raw parquet.

### private_marketplace_deals Table
- Reference table for PMP deal names and IDs — not built by us, comes from Beeswax/exchanges
- DS42 (select team) converts PMP string IDs to integer `data_source_category_id` values
- Has `name`, `floor_price`, `channel_id` columns
- Example: `SELECT * FROM private_marketplace_deals WHERE lower(name) LIKE '%nba%'` for sports deals

### IPv6 in augmentor_log
- When IP field is blank, IPv6 field is often populated
- IPv6 can link to household ID via identity graph
- Consider as fallback for blank-IP rows when building IP-level features

### IAB Categories — Practical Limitations (from Alex's TI-791 analysis)
- Top categories are too generic: "Arts & Entertainment", "Television" dominate across all device types
- Only ~20% of CTV rows have any IAB categories
- IAB alone is insufficient for vertical classification — need domain parsing too
- Simple rules mapping (not embeddings) is more practical given the generic taxonomy
- CTV vs non-CTV is the primary delimiter — classification logic should split on device_type first

### Scale Reference (per day, 2026-03-29)
| Table | Distinct IPs | Rows |
|-------|-------------|------|
| guid_log | 31.2M | ~340M |
| win_logs | 11.7M | ~68M |
| cost_impression_log | 11.7M | ~68M |
| conversion_log | 10.7M | ~63M |
| clickpass_log (visits) | 563K | ~1M |
| augmentor_log | 23.6M (1hr) | 1.2B/hr |
| bidder_auction_events | — | 112M/hr |

Daily IVR base rate: ~4.8% (563K visitors / 11.7M impressed IPs).

<!-- slack-extracted: 2026-04-08-full -->
- ### BigQuery Standardized Timezone Conversion Functions

Standardized functions are available in BigQuery (bronze, silver, and gold datasets) to convert UTC timestamps to advertiser-local time, consistent with CoreDW behavior:

- `public.timetz(time, time_zone)` → returns `DATETIME`
- `public.hourtz(time, time_zone)` → returns `DATETIME`
- `public.datetz(time, time_zone)` → returns `DATE`

**Why these exist:** Created late in the CoreDW→BigQuery migration after inconsistencies were found with native approaches like `TIMESTAMP_TRUNC(..., time_zone)` and `DATETIME_TRUNC(..., time_zone)`.

**Reporting pipeline convention:**
- `timestamp` columns → UTC
- `datetime` columns → Advertiser local time

Use these functions instead of native BigQuery truncation functions when timezone conversion is needed.
- ### CoreDB DDL Change Management — Migration to Alembic (In Progress)

As of April 2026, CoreDB DDL changes are executed manually without a migration framework. This makes it difficult to link schema changes to tickets, enforce parity across prod/QA/dev environments, or reason about triggers.

An effort is actively underway (led by the platform team, target mid-May 2026) to adopt a migration system similar to Alembic that would:
- Link DPLAT tickets to pull requests
- Enforce schema parity between prod, QA, and dev
- Enable local testing

Until then, DDL changes to CoreDB are coordinated via the #data-platform channel.
- ### Data Engineering MCP Server

An internal Model Context Protocol (MCP) server is available for data engineering tooling at `https://data-eng-ai.in.mountain.com/`. It is deployed via GitHub Actions to Argo.

**Current tools include:**
- GCS folder size lookups
- Row count queries against Parquet files
- Parquet schema inspection
- TI on-call utilities
- Dataproc batch analysis with code change recommendations (partially functional)

**How to contribute:** Merge tool additions to `main`, then run the GitHub Action to generate and deploy an Argo PR.

**Slack bot:** A Slack bot interface is also available for interacting with the MCP (e.g., Parquet schema queries). Contact the owner (Ryan Kleck) to be added to the bot's channel.

<!-- slack-extracted: 2026-04-08-review -->
- ### BigQuery Write Patterns — Bulk Loading vs. Direct INSERT

BigQuery is optimized for bulk loading rather than row-by-row `INSERT` statements. Direct `INSERT` queries have a daily quota and are discouraged by Google's quota structure.

**Preferred ingestion patterns:**
- Write Parquet files to GCS, then load to BigQuery (standard pattern for services ingesting from external sources)
- Use the BigQuery Python client's load job method (e.g., `load_table_from_dataframe`), which uses load jobs rather than streaming inserts
- Spark jobs writing directly to BigQuery are also an established pattern

**Load job limits:**
- 1,500 load jobs per table per day
- 100,000 load jobs per project per day
- Max job size: 15 TB per load job
- Jobs fail if runtime exceeds 6 hours

**Anti-pattern:** Do not insert data into an OLTP database (e.g., CoreDB) and then dump it to BigQuery. Write directly to GCS/BigQuery from the service.
- ### Fangorn Continuous Scoring — IVR Evaluation Methodology

IVR rates for Fangorn are not evaluated at the individual keyword level (there is no 1:1 keyword-to-visit association). Instead, evaluation is done at the **IP level using a unified score**:

1. Each IP is assigned a unified score based on its keyword rankings from BUK.
2. IPs are binned by unified score (e.g., IPs scoring ≥ 0.9).
3. Visits and impressions are summed across all IPs in a score bin to calculate IVR for that bin.

This approach means a visit is not attributed to a single keyword — it's attributed to the IP's overall intent score. The result shows how IPs with high unified intent scores perform relative to lower-scored IPs.

**Implication:** Adding 100% of keywords to High Intent is suboptimal because individual keywords have vastly different IVR rates and don't all perform at the average HI level. Continuous scoring addresses this by ordering keywords more precisely by intent signal strength.
- ### Audience Size Expansion — Keyword Recommendation Complexity

Recommending a specific keyword addition to achieve a target IP count increase (e.g., "add 50,000 IPs") is not straightforward due to IP intersection:

- The marginal IP gain from adding a keyword cannot be estimated without a full evaluation against MemDB, because IP sets across keywords overlap.
- Evaluating how much a category grows an audience in real time is considered a hard blocker for any productized recommendation.
- DAR (Dynamic Audience Recommendations) produces a ranked list of keywords; marginal IP gain could theoretically be computed sequentially from rank 1 to rank N, but this functionality does not exist today.
- IPDSC could be used as an approximation, but would be slow.

**Current workaround:** The existing MemDB hash mechanism (used for holdout bucketing) could potentially be reused for approximate audience sizing, but no automated recommendation tooling exists.
- ### `core.advertiser_default_values` — Campaign Budget Floor/Ceiling Overrides

The `core.advertiser_default_values` table allows per-advertiser overrides of campaign model settings, including budget floor and ceiling by stage (e.g., stage 15, 23, 25, 42, 43) and campaign status.

**Key behavior:**
- Values are stored as a JSONB column `campaign_values` with keys like `budget_floor`, `budget_ceiling`, and `campaign_status_id`
- Stage 23 and 43 with `campaign_status_id: 7` disable those stages (e.g., for advertisers not tracking Verified Visits)
- Settings flow downstream and affect active campaigns when updated

**Known issue (April 2026):** Many advertisers created before the budget minimum feature was introduced are missing records in this table. When an advertiser has CART disabled, the default values record may not have been created, causing budget floor/ceiling to not apply correctly. 

**Workaround:** Toggle CART ON then OFF in the Advertiser Info page in Command Center — this creates the missing record, fixes existing budget allocations on live campaigns, and syncs everything downstream. A migration to back-populate all affected advertisers is tracked in PRO-497.

<!-- slack-extracted: 2026-04-14 -->
- **Offline Upload Values — Future-Dated Conversions Issue**

The table `ui.offline_upload_values` can contain conversions with future timestamps if the customer uploads data that includes future-dated records. This was confirmed via `upload_id = 27358`, which had a `max(time)` of `2026-04-17` at a time when that date had not yet occurred.

**Root cause:** The upstream discrepancy originates from the third-party data provider (xdd). Data can fail to match hashed values at the time of the initial run but recover later.

**Resolution pattern:** Re-triggering the pipeline after xdd data recovers will produce correct match results (e.g., 1,458 conversions surfaced on rerun).

**Action item for data quality:** Ensure offline uploads do not contain conversions with future dates before processing. No other downstream action is required once data recovers. (via Lilit, #reporting_helpdesk_ask_anything, 2026-04-13)

<!-- slack-extracted: 2026-04-16 -->
- **Fangorn Experiment — Mid-Intent + Peak Performance Logic Bug:** A logic error was identified in the Mid-Intent + Peak Performance audience expression during the Fangorn experiment. The bug affected the Treatment side only.

**Root cause:** On the Control side, the audience expression uses a 6-digit `vertical_id` for High Intent and a 3-digit `vertical_id` for Mid Intent. On the Treatment side, the `advertiser_id` was mistakenly used for both High and Mid Intent instead.

When Peak Performance was enabled and the threshold dropped to 3333:
- **Treatment (correct behavior):** ORs in the 6-digit `vertical_id` → targets the vertical OR (bucket AND keywords)
- **Control (buggy behavior):** ORed in the `advertiser_id` → accidentally targeted Bucket OR Keywords, which simplified to just the bucket at the 3333 threshold

**Fix for rollout:** Mimic DS13 exactly — use a 6-digit `vertical_id` for High Intent and a 3-digit `vertical_id` for Mid Intent on the Treatment side. The `advertiser_id` will no longer be used for intent-tier differentiation in audience expressions. (via Ryan Kleck, #dev_fangorn-model_ex, 2026-04-01)
- **Datastream Replication — Primary Key Policy:** When adding tables to Datastream replication, BigQuery does not use serialized/synthetic primary key columns. PKs should be data columns that are meaningfully unique (e.g., `location_id`), not auto-generated serial columns. Adding a serial column purely for replication purposes is discouraged because it is not representative of the data. (via Dustin Niehoff, #data-platform, 2026-04-01)

<!-- slack-extracted: 2026-04-17 -->
- **cost_impression_log — Customer-Centric Spend View and PSA Exclusion**

`cost_impression_log` is designed to depict spend and costs from a customer-centric perspective. PSAs (Public Service Announcements — impressions served when no paid ad is available) are excluded from `cost_impression_log` because they are not charged to the customer. This means `cost_impression_log` spend figures will not match raw cache or Beeswax totals when PSAs are present for a given campaign. The discrepancy is expected behavior, not a data quality issue.

Practical implication: If a campaign shows higher spend in Beeswax/cache than in `cost_impression_log`, check whether PSAs were served for that campaign ID during the period in question. Query `impression_log` filtering on `original_cid` to confirm PSA volume. (via ray, #reporting_helpdesk_ask_anything, 2026-04-16)
- **PSA Root Cause Pattern — Ad-Service Cache and NULL update_time**

A PSA spike incident (April 15–16, 2026) revealed a structural risk in the ad-service cache refresh logic:

- The cache performs full refreshes filtering on `update_time`, but a significant number of historical VAST metadata rows have **NULL `update_time`** because that column was not always tracked.
- Those NULL-`update_time` rows are never picked up in refresh cycles, so they are effectively frozen in cache.
- After ticket CDS-3414 (~April 1, 2026) changed from full updates to batch updates, any creative IDs (CRIDs) composed entirely of NULL-`update_time` rows stopped being refreshed.
- Redis has a 14-day TTL, so those rows naturally expired around April 15, resulting in missing Redis keys → full PSAs for affected campaigns.
- **Impact was limited to older creatives** (pre-dating `update_time` tracking). Newer Select creatives with populated `update_time` were unaffected.
- Remediation: fix deployed ~12:53 ET April 16. Follow-up work planned to ensure `update_time` is properly maintained going forward. (via bermudez, #mission-control, 2026-04-16)

<!-- slack-extracted: 2026-04-21 -->
- **DS46 — ML Audience Intent Scoring Model (Fangorn):** DS46 is the data source ID for MNTN's ML-based audience intent scoring model, associated with the Fangorn scoring system. As of 2026-04-20, DS46 was deliberately turned off and is NOT in production. The team ran an experiment using it, then stopped populating IPDSC (IP-to-DSC mapping) with DS46 scores in order to allow MemDB (MembershipDB) to clear its cache, which has a 30-day TTL. Once MemDB has cleared the old data, DS46 population will resume and the model will roll out. Declining row counts in DS46 monitoring during this period are expected and not indicative of a bug. Two BigQuery tables can be used to monitor DS46 volume: `dw-main-bronze.external_ddm.data_source_sizes` (high-level, filter `data_source_id = 46`) and `dw-main-bronze.external_ddm.data_source_category_sizes` (exploded by DSCID category). (via Ryan Kleck, #mission-control, 2026-04-20)
- **Fangorn Continuous Scoring — Two Flavors:** There are two variants of continuous scoring involving Fangorn: (1) Fangorn-only continuous scoring — scores go directly to the bidder team; no MembershipDB dependency. (2) Fangorn + Keyword continuous scoring — requires Fangorn scores to be in MembershipDB. The reason scores must be in MemDB for the second variant is to support proper audience size display in the UI. Sequencing: Fangorn-only continuous scoring ships first (no MemDB dependency), then Fangorn/Keyword continuous scoring (requires MemDB). (via Ryan Kleck, #tgt-infrastructure-squad, 2026-04-20)
- **mntn-analytics-raw GCS Bucket — TTL and Cost Risk:** The GCS bucket `mntn-analytics-raw` stores all Kafka data dumps and has a 1-year TTL. As of April 2026, at its current growth rate it was projected to cost $310K/month by September 2026. The data platform team was evaluating reducing the TTL to 90 days. Primary consumers are various applications and the data warehouse. This is a cost-critical decision requiring input from all teams that consume Kafka data from this bucket. (via scotty, #chapter-data-engineering, 2026-04-20)

<!-- slack-extracted: 2026-04-22 -->
- **tpa.categories — Data Source (DS) 11 vs. DS 35**

In the `tpa.categories` table, Data Source 11 is deprecated. Data Source 35 is the current live source. When the same audience category ID appears under both DS 11 and DS 35 (e.g., for Liveramp audiences), DS 35 should be used. (via zach.schoenberger, #targeting-squad, 2026-04-21)
- **Pacing Incident (2026-04-21) — Root Cause: Daily Budget Doubling via flightspendraw / flightimpressionsserved**

A systemic pacing failure affecting 1000+ CGIDs (>90% underspend on 4/20, followed by overspend on 4/21) was traced to daily budgets doubling day-over-day. The root cause involved the `flightspendraw` and `flightimpressionsserved` fields. Contributing factors:
- A BID deployment on 2026-04-21 00:30 UTC (splitting geo version and intent score reads) was initially suspected but ruled unlikely to be the root cause and was reverted as a precaution.
- The long-term fix requires a DevOps change to a DB table TTL (tracked in DEV-7296).
- Short-term mitigations: PAC manually adjusted daily budget caps and synced to Beeswax; HHST was manually reset by Forrest.

**Impact:** No underspend on 4/21, but significant overspend. Full overspend impact list shared with PEX the following day. (via Johnny, #mission-control, 2026-04-21)

<!-- slack-extracted: 2026-04-24 -->
- ### analytics_request_log — GA3 Volume Drop Root Cause Pattern

A drop in `analytics_request_log` total volume that is isolated to GA3 (while GA4 volume remains normal) indicates a reduction in cross-device requests, not a data pipeline failure. Diagnostic indicators:

1. **GA4 volume stays normal** — GA4 is `cross_device = false`, so it is unaffected by cross-device resolution issues.
2. **Distinct `ad_served_id` volume stays consistent** — all relevant matched impressions are still represented; the drop is in cross-device enrichment only.
3. **Distinct `ga_client_id` volume drops in tandem with GA3 volume** — the ratio of `ga_client_id`s per IP drives GA3 cross-device request volume.
4. **The `analytics_request_log` to `clickpass_log` monitor will not alert** in this scenario, because matched impressions are still present.

Root cause observed 2026-04-22 (hr19 UTC): truncation of `partner_sync_by_advertiser_v3` in ScyllaDB caused the drop. Restoring that table restored GA3 volume. (via Nish, #mission-control, 2026-04-23)
- ### Experimental Campaigns — Sync Disabled (Monitoring Implication)

Experimental (EX) campaigns have syncing to BX (the bidder exchange layer) intentionally disabled as part of the experiment design. This has a known monitoring side effect: any audit or monitor that relies on synced data (e.g., publisher/network blocklist sync, app bundle sync) will fire incorrectly for these campaigns even when the campaign configuration itself is correct.

**Recommended handling:** Exclude experimental CGIDs from sync-dependent monitors on a per-campaign basis, or implement logic to suppress alerts for campaigns flagged as experimental. Creating a dedicated App Bundle Sync Monitor that accounts for this distinction has been identified as a future improvement (DM-4375).

**Example (Audit #9, 2026-04-23):** An advertiser updated their AID block list after the experiment campaign was already running. The update was never propagated to BX because syncing was disabled, causing Audit #9 (DSP Control - Publisher & Network Blocking) to fire. The audit fire was technically correct but operationally expected given the sync-disabled state. (via Tom Manuel, #mission-control, 2026-04-23)

<!-- slack-extracted: 2026-04-26 -->
- ## RabbitMQ Consumer Outage Pattern — Segment/Campaign Mis-Targeting Risk

When RabbitMQ consumers stop processing messages (due to acknowledgment timeout errors or other failures), the following impacts occur:

- **Segment or campaign mis-targeting** — audience updates handled through the batch-job path (RabbitMQ) are delayed or dropped.
- **Intent scores not updated** — intent scoring pipeline depends on RabbitMQ consumers; a multi-day outage means stale intent scores for the affected period.
- **Spend impact** — similar to the 4/1 incident, campaign groups with recently updated audiences may see no spend or significant underspend during the consumer downtime window.
- **Kafka path is independent** — Kafka consumers can remain functional even when RabbitMQ consumers fail. This means audience updates flowing through Kafka (e.g., standard audience refresh) may still work while batch-job-path updates (RabbitMQ) do not.

**Detection gap:** Current alerting does NOT surface when a large backlog of messages is sitting in the RabbitMQ queue waiting to be consumed. The `message-delivered` metric dropping and the `messages ready` backlog count rising are the two Grafana signals to watch.

**Grafana dashboard for RabbitMQ:** `grafana.prod.in.mountain.com/d/Kn5xm-gZk/rabbitmq-overview` — use the `mntn-gke-bidder-01-prometheus` datasource, `bidder` namespace, `rabbitmq-rtb` cluster.
- Panel 14: message-delivered metric (drop = consumers stopped)
- Panel 9: messages ready/backlog count (rise = consumers falling behind)

**Baseline note:** There is always a baseline of ~2M unprocessed messages in the backlog for historical reasons (Bidder team to confirm). Monitor for count increasing *above* this baseline.

**Incident timeline (April 2026):** Issue began between 4/22 ~04:00 UTC and 4/23 ~01:00 UTC; consumers recovered ~4/25 14:15 UTC. Estimated ~2-day impact window. A prior similar incident occurred 4/1 (caused by Aerospike issue), resulting in no spend/underspend for CGs with newly updated audiences from 4/1 hr0 UTC through 4/2 hr13 UTC. (via Changxing Cao, #production-ops, 2026-04-25)

<!-- slack-extracted: 2026-04-28 -->
- ## summarydata.visits.elapsed_time — Time-to-Visit After Impression

`summarydata.visits.elapsed_time` captures the delta between an impression's timestamp and the subsequent page view timestamp for verified visits. Key details:
- Applies to records sourced from VVS's clickpass_log output (i.e., `visits.click = false`).
- The impression used for attribution is logged by clickpass_log at redirect time, and elapsed_time is computed from that impression's `time` field.
- Useful for calculating lift on visits and understanding per-advertiser visit lag distributions.
- Filters on `click = false` are recommended to isolate VV-sourced records. (via ray, #reporting_helpdesk_ask_anything, 2026-04-28)
- ## HHST (Household Score Threshold) — Behavior, Recent Changes, and Known Issues

### What HHST Does
HHST controls the minimum intent score required for a household to be targeted. Lowering the threshold expands the audience pool; raising it restricts to higher-intent households. The system reacts to campaign pacing: underspending campaigns trigger HHST decreases to expand reach.

### March 2026 Change — Switch from Beeswax Win Notifications to CIL
On approximately March 16, 2026, the HHST DAG was changed to use the Cost Impression Log (CIL) as the pacing signal instead of Beeswax (Bx) win notifications. Previously, Bx win notifications caused HHST to not properly adjust for some campaigns. The CIL-based approach reduced instances of HHST failing to respond to underspending campaigns.

**Side effect:** The CIL change coincided with a decline in audience quality metrics (pv_perc) and IVR beginning mid-March 2026. Increased sensitivity to pacing has caused more aggressive HHST decreases on underperforming campaigns.

### April 1, 2026 — Sub-Second Bidding Migration
A sub-second bidding solution was rolled out on April 1, 2026, migrating more campaign groups to that solution. This contributed to additional underspend, which in turn drove further HHST decreases.

### Asymmetry in HHST Bump Logic
The HHST bump-up procedure is slow relative to the drop-down procedure:
- Campaigns pacing well in mid-intent receive a +300 point bump.
- Underspending campaigns can drop thousands of points (e.g., 10,000 → 3,400 in a single adjustment).
- Recovery from a large drop can take ~2 weeks even when the campaign subsequently paces well.

### Pending Fix — Max Reach Scoring
The root cause of slow HHST recovery is that max-reach IPs stopped being scored, removing fine-grained HHST control over the max-reach bucket. A proposed fix assigns random scores to max-reach IPs at bid time (bidder-side), restoring fine-grained HHST control within the max-reach bucket. (via Tofer, #production-ops, 2026-04-27)
- ## Audience Size Drop to Zero — Pixel Mapping / OPM Expression Conflicts

When a campaign group's audience size drops to zero overnight without a visible UI change in the activity log, a common root cause is that the advertiser's pixel data and exclusion rules have converged — i.e., the IPs being collected by the pixel are the same set of IPs being excluded by the campaign's audience configuration (e.g., Login Domain / Current Student Exclusion overlapping with a 1+ Page View audience).

This is distinct from segment deprecation. Possible triggers include changes to the advertiser's pixel mapping or the OPM expression logic, even if the pixel continues firing. Resolution requires the advertiser to update their pixel configuration or audience exclusion rules to separate the included and excluded populations. (via zach.schoenberger, #mission-control, 2026-04-27)

## Databricks ↔ BigQuery — read paths for the big logs (TI-837, 2026-04-28)

For high-volume scans (augmentor_log, guid_log specifically), reading directly from GCS via Spark on Databricks is dramatically cheaper and faster than scanning through BigQuery. BQ slot contention + scan billing avoided. Per Victor Savitskiy (#data-platform):

| Table | GCS path | BQ-only? | Read pattern |
|---|---|---|---|
| `bronze.raw.augmentor_log` | `gs://mntn-data-archive-prod/augmentor_log/` | No | **Read GCS Parquet directly via Spark.** Full historical archive, no 10-day TTL constraint. |
| `silver.logdata.guid_log` | `gs://mntn-data-archive-prod/guid_log/` | No | **Read GCS Parquet directly via Spark.** |
| `silver.logdata.cost_impression_log` | n/a | **BQ-only** | Spark BigQuery connector, table-only mode (resolves SQLMesh physical at runtime). Efficient streaming, no materialization needed. |
| `silver.logdata.clickpass_log` | n/a (complicated view) | BQ-only | Spark BigQuery connector with `materializationDataset` + `viewsEnabled=true`. BQ materializes a temp table; output-size limit ~200M rows on the result. Medium data size, queryable. |
| `bronze.external.household_scoring__prospecting_intent__v1` | `gs://household-scoring-prod/output/scoring/prospecting_intent/` | No | Hive-partitioned Parquet (year/month/day). Read from GCS directly. |
| `bronze.integrationprod.campaigns` | n/a | BQ-only | Tiny (508k rows / 100 MB). Either BQ or coredb. |

**Spark BigQuery connector pattern (Victor):**

```python
# Option 1: views/queries supported, ~200M output row limit, extra materialization cost
spark.read.format("bigquery") \
  .option("parentProject", "dw-main-bronze") \
  .option("billingProject", "dw-main-bronze") \
  .option("project", "dw-main-bronze") \
  .option("materializationDataset", "external") \
  .option("viewsEnabled", "true") \
  .load("table_or_query")

# Option 2: tables-only, no size limit, no extra cost
spark.read.format("bigquery") \
  .option("parentProject", "dw-main-bronze") \
  .option("billingProject", "dw-main-bronze") \
  .option("project", "dw-main-bronze") \
  .load("table_name")
```

**All 3 project-related properties must be set** or extra costs apply.

**Third option:** `airflow_vs` reader in `airflow_ti` — gives choice of compute engine (databricks / dataproc / dataproc-serverless). Useful for production pipelines, not interactive.

**Speedup estimate (TI-837 case):** v1 = 87 min wall on BQ for 30 advertisers, 7-day window. Hypothesis (untested): reading augmentor + guid from GCS via Spark on a high-compute Databricks cluster could cut to 10-20 min while also avoiding BQ scan billing. Awaiting first run to confirm.

