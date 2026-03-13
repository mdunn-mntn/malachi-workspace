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

Some `sqlmesh__logdata` tables are themselves VIEWs referencing other datasets:
- `bid_attempted_log` and `bid_events_log` → both reference `bidder_bid_events` (same data, different filters)
- `bid_logs` → references Beeswax bid_logs upstream
- `auction_log` → references `v_augmentor_log`
- `win_logs` → references Beeswax win_logs upstream
- `icloud_vv_log` → references icloud_vv table upstream

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

### Retention / TTL
| Table | Retention |
|-------|-----------|
| silver.logdata.clickpass_log | **No TTL** — confirmed 2026-03-03 (expirationTime: none, no partition expiry) |
| silver.logdata.event_log | **No TTL** — confirmed 2026-03-03 (expirationTime: none, no partition expiry) |
| bidder_bid_events | 90 days (expirationMs on partition) |
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
- `data_source_id = 13` = Audience Intent Scoring (Jaguar) in the `audience.data_sources` table
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
`fpa.advertiser_verticals` (Greenplum) stores the advertiser→vertical mapping.
- `type = 1` = primary vertical (use this for filtering)
- Vertical IDs: 101001, 119001, 120002 referenced in TI-221/TI-270 analyses

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

### LiveRamp (DS 3) Cross-IP Identity Linkage
LiveRamp maps IPs to a shared identity graph (household/person level). When LiveRamp links IP_A ↔ IP_B,
both IPs receive identical segment membership entries in `tpa_membership_update_log` with `data_source_id=3`.

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

## Audience System Architecture

### audience.audiences vs audience.audience_segments (Greenplum)
These are two distinct concepts, often confused:

| Table | Purpose | Used for targeting? |
|-------|---------|-------------------|
| `audience.audiences` | Named audience templates (reusable definitions) | NO — template only |
| `audience.audience_segments` | Actual targeting expressions for campaigns | YES — this drives delivery |

**Implication:** Querying `audiences` alone will NOT reveal which audience was actually used for
targeting. Always join to `audience.audience_segments` for campaign-level analysis.

- `expression_type_id = 2` = TPA (Third Party Audience) expression type in audience_segments
- `audience.audience_segment_campaigns` maps which audience segment is active for each campaign_group

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
- **event_log.ip has CIDR notation (`/32` or `/128` suffix) on ALL data before 2026-01-01.** Exact cutover: Dec 31, 2025 → Jan 1, 2026 (100% CIDR before, 0% after). `/32` = IPv4, `/128` = IPv6. Exact string matching (`ip = 'x.x.x.x'`) will miss pre-2026 rows. Fix: `SPLIT(ip, '/')[OFFSET(0)]` for reliable matching. `bid_ip` column does NOT have this issue (always bare IP). Any cross-stage query with lookback into 2025 MUST strip CIDR. Discovered TI-650 v16 2026-03-13.
- **CRM campaign_groups bypass the S1→S2→S3 VAST funnel.** IPs enter CRM campaign_group targeting pools via identity resolution (data_source_id=4, email→IP), NOT through a prior VAST impression. These IPs will have no S1/S2 event_log records within the campaign_group. Identifiable by campaign_group name containing "CRM". Standard cross-stage IP trace cannot resolve these — they are structurally unresolvable via IP matching. Discovered TI-650 v16 2026-03-13.
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
- **channels reference table:** `bronze.integrationprod.channels` — 10 rows. 1=Multi-Touch, 2=Email, 3=In-App, 4=Mobile Web, 5=Platform Fee, 6=Real Time Offers, 7=Social, 8=Television, 9=Ad Serving, 10=Onsite Offers.
- **v15 forensic trace (2026-03-12, 50 VVs):** IP is 100% identical across ALL 8 source tables (event_log, impression_log, CIL, bid_logs, win_logs, clickpass_log, ui_visits). serve_ip = bid_ip at 100%. Adding any source table to S1 pool has zero impact on resolution. The 8% unresolved S3 VVs entered via identity graph, not via MNTN impression.
- **bid_events_log is nearly empty** — only advertiser 32167 has data. Not useful for general IP lookups. Use bid_logs (Beeswax-native) instead.

**Cross-stage link:** `next_stage.bid_ip ≈ prev_stage.vast_start_ip OR prev_stage.vast_impression_ip` (either/or join, ~1.2% differ — CGNAT/SSAI/IPv6/VPN). IP is the ONLY cross-stage link. first_touch_ad_served_id links S3/S2→S1 directly (skips S2) but only 25-51% available.

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

**Bottom line (updated 2026-03-12):** The primary cross-stage link is **vast_ip** — the VAST impression IP enters the next stage's segment, so `next_stage.bid_ip ≈ prev_stage.vast_ip`.

**v13 resolution rates (10 advertisers, Feb 4–11, 90-day lookback, prospecting obj 1,5,6):**
- S2: 97.95–99.87% (single hop to S1, near-perfect)
- S3: 62.51–97.83% (full S3→S2→S1 chain + S3→S1 direct fallback)
- S3→S2→S1 chain matters for 6/10 advertisers (up to 74% of S3 VVs resolve through chain)
- Chain added 134 net new S3 resolutions for adv 37775 (96.80→97.36%)

**Retargeting pool impact (tested 2026-03-12):** Adding retargeting campaigns (obj=4) to the S1 pool resolves 110 additional S3 VVs for adv 37775 — IPs whose first MNTN impression was retargeting, not prospecting. Business decision: audit scope = "first prospecting touch" vs "first MNTN touch."

**Irreducible floor (updated 2026-03-12):** 567 IP-unresolved → 484 resolved via GUID bridge (85.4%) → **83 truly irreducible** (0.36% of CIL cohort, only 10 primary attribution = 0.04%). Plus 1,074 VVs with no CIL record — NOT TTL expiration (all < 30 days old), pipeline gap recoverable via event_log bid_ip fallback.

**campaign_group_id scoping (Zach directive, 2026-03-12):** Cross-stage IP linking MUST be scoped within the same `campaign_group_id`. A VV in one campaign group cannot be linked to an impression in a different campaign group — that would be a coincidental IP match, not a real funnel trace. `campaign_group_id` is unique across advertisers. This constraint must be enforced in the production `vv_ip_lineage` model.

**Zero-chain advertisers:** 4/10 had zero S3→S2→S1 chain resolution. Cause: no active S2 prospecting impressions in the 90-day lookback (S2 campaigns exist but serve only retargeting).

The previous "~11% unresolved" ceiling included retargeting campaigns — Zach confirmed retargeting is NOT relevant to this audit. GUID bridge via `guid_identity_daily` resolves ~82% of the remaining IP-unresolvable VVs.

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
- v8 architecture: 7-tier S1 resolution (VV chain → impression chain → visit IP → cp_ft fallback)
- 180-day event_log lookback (was 30/90 — S3→S1 chains can span 104+ days)
- s1_imp_pool: earliest S1 impression per bid_ip (ORDER BY time ASC, not DESC — temporal bug fix)
- **S1 coverage (all campaigns): S1 100%, S2 76.6%, S3 80.3%** (adv 37775, v11, 7-day trace)
- **S1 coverage (prospecting CTV only): 98.56%** (S2, adv 37775). Primary VV unresolved: 0.34%.
- Previous "~11% ceiling" was inflated by retargeting campaigns. Zach: retargeting not relevant.
- Remaining 1.44% = LiveRamp identity graph entries (S1 impression on different CGNAT IP).
- impression_ip (from ui_visits) differs from bid_ip for 5.3% of S3 VVs. Does NOT rescue new cases — re-attributes from cp_ft fallback.
- A4b dedup: fixed (dedup bug found and corrected during audit)

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
