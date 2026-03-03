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
```

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

### ip vs ip_raw
Many tables have both `ip` (potentially masked/enriched) and `ip_raw` (original). The `original_ip`
column (where present) is the pre-proxy original. In ui_visits/visits, `impression_ip` is the IP
at impression time.

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

### Retention / TTL
| Table | Retention |
|-------|-----------|
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

### The 5-Checkpoint IP Trace
Within a single CTV ad serve, the IP can change at each stage:

| # | Checkpoint | Table | IP Column | Stability |
|---|------------|-------|-----------|-----------|
| 1 | Win IP | logdata.win_log | ip (inet /32) | — (baseline) |
| 2 | CIL IP | logdata.cost_impression_log | ip (text) | 100% stable Win→CIL |
| 3 | EL IP | logdata.event_log | ip (inet /32) | 96.2% stable CIL→EL |
| 4 | CP IP | logdata.clickpass_log | ip (inet /32) | — |
| 5 | Visit IP | summarydata.ui_visits | ip (inet /32) | mutation range 5.9–33.4% |

**Simplified 2-hop join:** `event_log.bid_ip` = `win_log.ip` at 100% — this enables joining
win_log → CIL → event_log without traversing all 5 checkpoints explicitly.

### IP Mutation Key Findings (TI-650)
- **100% of mutation occurs at the VAST→redirect boundary** (Stage 3, CIL→EL or EL→redirect)
- **Aggregate mutation rate:** ~21.2% (14.28% inter-impression bid IP mutation for multi-impression VVs)
- **Cross-device is the primary driver:** 61.2% mutation when cross-device flag is set
- **Mutation range:** 1.2–33.4% across 15 advertisers in the reference dataset
- **Phantom NTB estimate:** ~4,006 events/day for advertiser 37775 (caused by IP mutation making
  verified NTB visits look like they came from households already in the ad graph)
- **Win → CIL join:** 100% reliable — always use `win_log.ad_served_id → CIL.ad_served_id`

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

Stage 3 VV production audit table: `audit.stage3_vv_ip_lineage`
- Partitioned by `trace_date`, clustered by `advertiser_id`
- Requires 30-day event_log lookback for reliable join coverage
- A4b dedup: fixed (dedup bug found and corrected during audit)
- BQ silver data gap: 2026-01-31+ has incomplete data (coreDW deprecation transition)

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
