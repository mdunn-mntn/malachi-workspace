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
