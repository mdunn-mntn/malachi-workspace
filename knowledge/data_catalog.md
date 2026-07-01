# Data Catalog — MNTN BigQuery
Last updated: 2026-03-12 | Phase 2 complete + Phase 3 additions + silver.fpa (TI-737)

## Catalog Index
- [silver.logdata](#silver-logdata)
- [silver.summarydata](#silver-summarydata)
- [silver.core](#silver-core)
- [silver.aggregates](#silver-aggregates)
- [bronze.raw](#bronze-raw)
- [bronze.integrationprod](#bronze-integrationprod)
- [bronze.coredw](#bronze-coredw)
- [bronze.external](#bronze-external) — ipdsc__v1 (CRM IP resolution)
- [bronze.tpa](#bronze-tpa) — audience_upload_hashed_emails, audience_upload_ips
- [audit](#audit-bq-dataset) — vv_ip_lineage
- [silver.fpa](#silver-fpa) — advertiser_verticals, categories
- [Greenplum Tables Reference](#greenplum-coredw-tables-reference)

---

## SQLMesh Naming Convention
All tables in `logdata`, `summarydata`, and `aggregates` are VIEWs → `sqlmesh__*` versioned tables.
Format: `<dataset>__<table_name>__<version_hash>`. Always query the clean alias. See data_knowledge.md.

## Datastream Replication Pattern
Most `bronze.integrationprod` tables include a `datastream_metadata RECORD` with:
- `uuid` — Datastream replication event UUID
- `source_timestamp` — Epoch of the source Postgres change event
This confirms `bronze.integrationprod` is a Postgres replica via GCP Datastream (CDC).

---

# silver.logdata

**Project:** dw-main-silver | **Dataset:** logdata
All tables in this dataset are VIEWs pointing to `sqlmesh__logdata`.
**Retention:** Earliest data is 2025-01-01 for most tables (event_log, impression_log). viewability_log starts 2025-04-08. No BQ layer (silver or bronze.raw) has data before 2025-01-01. Pre-2025 data only in Greenplum coreDW (deprecated April 30, 2026). Per-physical-table retention details in `data_knowledge.md` under "Partition Filter Best Practice".

---

## silver.logdata.impression_log
- **Type:** VIEW → `sqlmesh__logdata.logdata__impression_log__4185451957` (VIEW → upstream Postgres/coredw)
- **Partition:** Inherited from upstream (not set at view level)
- **Clustering:** None
- **Use for:** Ad impression events served by MNTN
- **Key columns:**

| Column | Type | Notes |
|--------|------|-------|
| guid | STRING | User identifier (cookie) |
| time | TIMESTAMP | Impression time (UTC) |
| epoch | INTEGER | Seconds epoch |
| advertiser_id | INTEGER | Join → core.advertisers |
| campaign_id | INTEGER | Join → core.campaigns |
| creative_id | INTEGER | Join → core.creatives |
| group_id | INTEGER | Campaign group / ad group ID |
| exchange_id | INTEGER | Ad exchange |
| exchange_io | STRING | Exchange insertion order |
| domain | STRING | Impression domain |
| subdomain | STRING | Impression subdomain |
| cpm | BIGNUMERIC | Cost per mille |
| cpi | INTEGER | Cost per impression (micros?) |
| section_id | INTEGER | Placement section |
| ad_served_id | STRING | Unique impression identifier — primary join key |
| user_agent | JSON | Parsed user agent (JSON) |
| browser | STRING | Browser name |
| ip | STRING | Enriched IP |
| ip_raw | STRING | Raw IP |
| bid_ip | STRING | IP at bid time |
| original_ip | STRING | Pre-proxy IP |
| geoname_id | INTEGER | MaxMind geo ID |
| country | STRING | ISO country code |
| metro_id | INTEGER | DMA metro ID |
| region | STRING | State/region |
| city | STRING | City |
| postal_code | STRING | Zip code |
| device | STRING | Device string |
| is_baseline | BOOLEAN | Whether impression is baseline |
| cache_buster | BOOLEAN | Cache buster flag |
| ttd_impression_id | STRING | Trade Desk impression ID |
| deal_id | STRING | PMP deal ID |
| app_bundle | STRING | Mobile app bundle (CTV/mobile) |
| publisher | STRING | Publisher name |
| ga_tracking_id | STRING | Google Analytics tracking ID |
| ga_client_id | STRING | GA client ID |
| td_site | STRING | TD site identifier |
| td_id | STRING | TD identifier |
| original_aid | INTEGER | Original advertiser ID |
| original_cid | INTEGER | Original campaign ID |
| creative_size_id | INTEGER | Creative size reference |
| server_host_name | STRING | Serving host |
| impression_log_file | STRING | Source log file name |

- **Query tip:** Always filter on `time` (date range). Use `ad_served_id` to join to visits/conversions.

---

## silver.logdata.clickpass_log
- **Type:** VIEW → `sqlmesh__logdata.logdata__clickpass_log__218519243` (VIEW → upstream Postgres)
- **Underlying physical:** `dw-main-bronze.history.clickpass_log_physical` (1 B rows / 2.9 TB, DAY-partitioned on `time`)
- **Partition:** None at view level. **No TTL** — confirmed 2026-03-03 (expirationTime: none). Use `DATE(time)` for date filters.
- **GCS archive:** **None as a clean dump — view is "complicated" per Victor.** Read via Spark BigQuery connector with `materializationDataset=external` + `viewsEnabled=true` (BQ materializes a temp table for the view). Output-size limit ~200M rows applies if pulling rows back to driver, but aggregations are fine. Medium data size — queryable. (via Victor Savitskiy 2026-04-28, TI-837)
- **Use for:** Verified visit log — one row per verified visit redirect. "clickpass" is the old term for verified visit (VV). Contains ALL VV types: CTV and display. **Not** click-only; not CTV-only. (Confirmed by Zach: "vv can happen for display as well and would be here.") `ui_visits` is the superset that adds display clicks and non-VV traffic.
- **What it actually is (Zach Schoenberger 2026-04-30):** **visits = clicks + VVs**, MNTN-attributed. One row per attributed visit: MNTN served impression → user visited advertiser site within ~30 days → MNTN pixel matched the visit back to the impression. NOT page views — that's `guid_log`.
- **36 columns** (confirmed schema 2026-03-03)

| Column | Type | Notes |
|--------|------|-------|
| guid | STRING | User cookie ID |
| time | TIMESTAMP | VV redirect time — use DATE(time) for filtering, no partition column |
| epoch | INTEGER | Seconds |
| advertiser_id | INTEGER | |
| campaign_id | INTEGER | |
| creative_id | INTEGER | |
| creative_group_id | INTEGER | |
| click_url | STRING | Original click URL |
| destination_click_url | STRING | Final destination URL |
| destination_with_suffix | STRING | Destination with tracking suffix |
| ip | STRING | **Primary IP** — enriched IP at redirect time. Use this for VV IP analysis. |
| ip_raw | STRING | Raw IP before enrichment (known upstream issue: two ip columns — ip + ip_raw) |
| is_new | BOOLEAN | NTB flag — determined by client-side JS pixel, NOT a DB lookup. Disagrees with ui_visits.is_new 41–56% of the time — expected, architectural. |
| is_control_group | BOOLEAN | Control group exclusion flag |
| is_cross_device | BOOLEAN | Ad on one device, visit on another. Cross-device = 61% of IP mutation. |
| referer | STRING | Referring URL |
| parent_referer | STRING | Parent frame referrer |
| query | STRING | Query params |
| user_agent | STRING | Browser/device user agent |
| ad_served_id | STRING | **Primary join key** — UUID linking this VV to its ad impression. Always last-touch (most recent impression). Join to event_log on this for IP trace. |
| original_guid | STRING | Pre-cross-device GUID |
| impression_time | TIMESTAMP | Time of the ad impression that triggered this VV. Gap to `time` is always ≤30 days (confirmed 3.25M VVs). |
| impression_epoch | INTEGER | Impression epoch (seconds) |
| page_view_guid | STRING | GUID from page view signal |
| viewable | BOOLEAN | Was impression viewable |
| first_touch_ad_served_id | STRING | UUID of first impression for this user/advertiser. NULL ~40% of the time — populated at write time, no batch backfill. (Confirmed by Zach: "clickpass_log is a real time log. there is no post processing.") |
| first_touch_time | TIMESTAMP | Time of first touch impression |
| attribution_model_id | INTEGER | Attribution model used |
| app_bundle | STRING | |
| publisher | STRING | |
| blocked_source | STRING | IVT/fraud block reason |
| additional_parameters | STRING | Extra tracking params |
| click_elapsed | INTEGER | ms since impression |
| view_elapsed | INTEGER | ms since last view |
| ga_tracking_ids | STRING | |
| ga_client_ids | STRING | |

- **Key audit findings (TI-650, 2026-03-03):** clickpass_log is 99.6% proxy for ui_visits VVs. redirect_ip (clickpass.ip) = visit_ip (ui_visits.ip) at 99.93%+. All IP mutation occurs between event_log.ip (VAST) and clickpass.ip (redirect) — zero at the visit hop.
- **Join tips:** `ad_served_id` → event_log for bid_ip and VAST IP. `ad_served_id` → CAST(ui_visits.ad_served_id AS STRING) + `from_verified_impression = true` for visit IP. Use 30-day EL lookback (impression_time is always ≤30 days before VV time).
- **Gotcha:** No `dt` partition column — filter on `DATE(time)`. Bronze raw.clickpass_log has ~25% of silver volume (upstream filter) — always use silver.

---

## silver.logdata.conversion_log
- **Type:** VIEW → `sqlmesh__logdata.logdata__conversion_log__3338353553` (VIEW → upstream Postgres)
- **Use for:** Pixel-fire conversion events (advertiser site conversions)

| Column | Type | Notes |
|--------|------|-------|
| guid | STRING | User cookie |
| time | TIMESTAMP | Conversion time |
| epoch_time | INTEGER | Seconds |
| advertiser_id | INTEGER | |
| order_id | STRING | Advertiser order ID |
| order_amt | NUMERIC | Order value in local currency |
| order_curr | STRING | Currency code |
| order_amt_usd | BIGNUMERIC | Order value in USD |
| ip | STRING | |
| ip_raw | STRING | |
| original_ip | STRING | |
| conversion_type | STRING | Type of conversion event |
| conversion_source_id | INTEGER | Conversion source reference |
| email | STRING | Hashed email (PII) |
| phone | STRING | Hashed phone (PII) |
| query | JSON | Query params as JSON |
| _col_23 | JSON | **Unnamed column** — raw artifact from Postgres migration |
| browser | STRING | |
| operating_system | STRING | |
| device_type | STRING | |
| browser_version | STRING | |
| is_mobile_device | BOOLEAN | |
| referer | STRING | |
| ga_tracking_id | STRING | |
| ga_client_id | STRING | |

**Caveat — silver hides corrupt order_amt** (TI-832, 2026-05-06). Bronze (`dw-main-bronze.raw.conversion_log`) contains thousands of rows from 4 advertisers (34957 Harley, 33903 Bioharvest, 32023 Tarte, 63746 Networking Today) with corrupt `order_amt` in the $1B–$7.4T range — likely timestamp leakage / unit-conversion bugs at the pixel layer. Silver SQLMesh strips these. **If investigating data quality on conversions, query `bronze.raw.conversion_log` not silver — or you'll see zero issues that absolutely exist upstream.** Pixel ops (Ashley Pineda Varela) flagged via TI-832 outlier sheet.

---

## feature_store/feature_group_1_source/conv_log_ip (parquet, daily)
- **Type:** Parquet at `gs://mntn-data-archive-prod/feature_store/feature_group_1_source/conv_log_ip/dt=YYYY-MM-DD/`
- **Source repo:** [airflow-ti `models/feature_store/feature_group_1_source/conv_log_ip.py`](https://github.com/SteelHouse/airflow-ti/blob/main/models/feature_store/feature_group_1_source/conv_log_ip.py)
- **Use for:** IP-grain daily conversion rollup. Inputs to Layer-2 derived models for Fangorn V1/V2.
- **Filter applied:** drops `pixel_isolation=true` advertisers + null/0.0.0.0/127.0.0.1 IPs.

| Column | Aggregation | Notes |
|--------|------------|-------|
| dt, ip | grain | Partition + IP |
| conversion_count | `COUNT(*)` | Daily conv volume |
| total_order_amount | `SUM(order_amt)` | Currency-mixed |
| usd_order_amount | `SUM(order_amt WHERE order_curr=USD)` | TI-832 — currency-clean (~99% covered) |
| order_amount_count | `COUNT(WHERE order_amt > 0)` | |
| max_order_amount | `MAX(order_amt)` | Includes corrupt values from the 4 advertisers above |
| conversion_time_min, conversion_time_max | `MIN`/`MAX(time)` | TI-832 — feeds L2 recency math |
| order_id_hll, conversion_type_hll, conversion_source_hll, advertiser_id_hll | PySpark Datasketches HLL | Mergeable in L2 via `F.hll_union_agg` + `F.hll_sketch_estimate` (NOT the ZetaSketch UDF) |

History starts ~2026-04-09. Daily TTL on the regular path; monthly snapshot path retains forever (see `conv_log_derived_ip` below).

---

## feature_store/feature_group_2_derived/conv_log_derived_ip (parquet, daily)
- **Type:** Parquet at `gs://mntn-data-archive-prod/feature_store/feature_group_2_derived/conv_log_derived_ip/dt=YYYY-MM-DD/`
- **Monthly snapshot:** `gs://mntn-data-archive-prod/feature_store/feature_group_2_derived_monthly/conv_log_derived_ip/dt=YYYY-MM-01/` (fires on day 15 of each month with `run_date = first of month`; forever-retained)
- **Source repo:** [airflow-ti `models/feature_store/feature_group_2_derived/conv_log_derived_ip.py`](https://github.com/SteelHouse/airflow-ti/blob/main/models/feature_store/feature_group_2_derived/conv_log_derived_ip.py)
- **Use for:** IP-grain rolling/forward conversion features. Built for Fangorn V2 (Matt Brorby's conversion-target XGBoost). SHAP-validated on TI-832 — see `tickets/ti_832_feature_store_roas_cpa/outputs/ti_832_shap_combined.csv`.
- **Partition convention:** base partition is `dt = run_date + 1` (as-of-next-day boundary); monthly snapshot partition is `dt = run_date` (first of month).

| Column | Notes |
|--------|-------|
| snapshot_date, ip | grain |
| cv_n_conv_{7,14,30}d | Conversion count per window |
| cv_usd_amt_{7,14,30}d | USD-only revenue per window |
| cv_max_amt_30d | Single largest order in 30d (SHAP top-3 feature) |
| cv_avg_amt_30d | `usd_amt_30d / n_conv_30d` |
| cv_n_orders_30d, cv_n_adv_30d | HLL-merged distinct counts |
| last_conversion_time_max, last_conversion_day, cv_days_since_last | Recency; sentinel `999` = no conversion in window |
| cv_n_conv_forward_{7,14}d_outcome, cv_usd_amt_forward_{7,14}d_outcome | Training labels — monthly snapshot only |
| first_conversion_time_min_forward_outcome, first_conversion_day_forward_outcome, cv_days_until_first_conv_forward_outcome | Time-to-first-conversion outcomes — monthly snapshot only |

---

## silver.logdata.click_log
- **Type:** VIEW → `sqlmesh__logdata.logdata__click_log__3304395312` (VIEW → upstream Postgres)
- **Use for:** Raw click events (exchange-level clicks, distinct from clickpass)

| Column | Type | Notes |
|--------|------|-------|
| guid | STRING | |
| time | TIMESTAMP | |
| epoch | INTEGER | Seconds |
| advertiser_id | INTEGER | |
| exchange_id | INTEGER | |
| campaign_id | INTEGER | |
| creative_id | INTEGER | |
| group_id | INTEGER | |
| ad_served_id | STRING | |
| ip | STRING | |
| ip_raw | STRING | |
| bid_ip | STRING | |
| original_ip | STRING | |
| first_touch_ad_served_id | STRING | |
| landing_page | STRING | |
| referrer | STRING | |
| country | STRING | |
| region | STRING | |
| metro_id | INTEGER | |
| postal_code | STRING | |
| app_bundle | STRING | |
| publisher | STRING | |

---

## silver.logdata.event_log
- **Type:** VIEW → `sqlmesh__logdata.logdata__event_log__314628680` (VIEW → upstream Postgres)
- **Partition:** None at view level. **No TTL** — confirmed 2026-03-03 (expirationTime: none). Use `DATE(time)` for date filters.
- **Use for:** Ad event log including VAST video events (vast_impression, vast_start, vast_firstQuartile, etc.) and general pixel events. Primary source for IP-at-VAST-playback and bid_ip. **38 columns** (confirmed schema 2026-03-03).

| Column | Type | Notes |
|--------|------|-------|
| guid | STRING | |
| time | TIMESTAMP | Event time |
| epoch | INTEGER | Seconds |
| advertiser_id | INTEGER | |
| campaign_id | INTEGER | |
| creative_id | INTEGER | |
| exchange_id | INTEGER | |
| ad_served_id | STRING | **Primary join key** — links to clickpass_log and cost_impression_log |
| domain | STRING | |
| subdomain | STRING | |
| group_id | INTEGER | |
| user_agent | STRING | |
| ip | STRING | **VAST playback IP** — IP of the CTV device during VAST ad playback. ≠ bid_ip ~3.5% of the time. **CIDR suffix on pre-2026 data:** ALL rows before 2026-01-01 have `/32` (IPv4) or `/128` (IPv6) suffix (526M rows confirmed). Post-2026 = bare IP. Use `SPLIT(ip, '/')[SAFE_OFFSET(0)]` when matching across time periods. Other log tables do NOT have this issue. |
| ip_raw | STRING | Raw IP before enrichment |
| is_mobile_device | BOOLEAN | |
| browser | STRING | |
| operating_system | STRING | |
| device_type | STRING | STRING in silver (already enriched — e.g. "CTV", "Mobile"). INTEGER in bronze.raw. |
| browser_version | STRING | |
| event_type_id | INTEGER | Event type reference |
| event_type_raw | STRING | Raw event type string — filter on `'vast_impression'` for VAST IP trace |
| geoname_id | INTEGER | |
| country | STRING | |
| metro_id | INTEGER | |
| region | STRING | |
| city | STRING | |
| postal_code | STRING | |
| continent | STRING | |
| locale_code | STRING | |
| time_zone | STRING | |
| device | STRING | |
| deal_id | STRING | |
| td_impression_id | STRING | |
| root_video | STRING | |
| bid_ip | STRING | **Bid IP** — IP at auction/win time. = win_log.ip and cost_impression_log.ip at 100% (validated 30,502 rows). This is the gold column — eliminates need to join win_log or CIL. |
| original_ip | STRING | Pre-iCloud Private Relay IP — the raw connection IP before MNTN's IP enrichment override. `ip` = the enriched/preferred IP used for all logic. `original_ip` = raw header IP for audit/debug. |
| app_bundle | STRING | |
| publisher | STRING | |

- **Key audit findings (TI-650, 2026-03-03):** `bid_ip` = win_log.ip at 100% — eliminates need for CIL/win_log joins. VAST events (vast_impression) = the IP at CTV playback. Multiple event types share one `ad_served_id` — always dedup with `ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time)` and take rn=1 for vast_impression.
- **VAST filter:** `event_type_raw = 'vast_impression'` for IP trace. Other types: vast_start, vast_firstQuartile, vast_midpoint, vast_thirdQuartile, vast_complete — all share the same IPs.
- **30-day lookback required:** A clickpass VV can occur up to 30 days after its VAST event. Confirmed: 100% of 3.25M VVs have impression_time within 30 days of visit time. Using 20-day lookback causes +3–5pp mutation offset.
- **Non-CTV:** Display/mobile ads don't fire VAST events — no event_log row. `el_matched = false` in VV trace = non-CTV inventory.

---

## silver.logdata.guid_log
- **Type:** VIEW → `sqlmesh__logdata.logdata__guid_log__614422669` (VIEW → upstream Postgres)
- **Underlying physical:** `dw-main-bronze.history.guid_log_physical` (TABLE, 107 B rows / 366 TB, DAY-partitioned on `time`)
- **GCS archive:** `gs://mntn-data-archive-prod/guid_log/` — read directly from GCS via Spark for high-volume scans on Databricks. (via Victor Savitskiy 2026-04-28, TI-837)
- **What it actually is (Zach Schoenberger 2026-04-30):** **page-view events.** One row per page view on an advertiser site by a tracked household — fires for every page view regardless of whether MNTN ever served an ad. **Not** the same as visits — clickpass_log is visits.
- **Use for:** site-traffic / page-view-level analysis; cause-agnostic "did this IP hit the advertiser site?" signal at IP-day granularity (after dedup).

| Column | Type | Notes |
|--------|------|-------|
| guid | STRING | |
| time | TIMESTAMP | |
| epoch | INTEGER | Seconds |
| advertiser_id | INTEGER | |
| ip | STRING | |
| ip_raw | STRING | |
| original_ip | STRING | |
| is_new | BOOLEAN | First cookie for this advertiser |
| is_control_group | BOOLEAN | |
| is_cookied | BOOLEAN | Whether cookie was set |
| referer | STRING | |
| parent_referer | STRING | |
| is_mobile_device | BOOLEAN | |
| browser | STRING | |
| operating_system | STRING | |
| device_type | STRING | |
| browser_version | STRING | |
| mobile | BOOLEAN | |
| cache_buster | STRING | |
| ga_gid | STRING | |
| ga_tracking_id | STRING | |
| ga_client_id | STRING | |
| ga_gclid | STRING | |
| ga_utm_campaign | STRING | |
| ga_utm_source | STRING | |
| ga_utm_medium | STRING | |
| email | STRING | Hashed (PII) |
| phone | STRING | Hashed (PII) |
| available_ga | STRING | |
| query | STRING | |
| user_agent | JSON | |
| product | JSON | Product data |
| cart | JSON | Cart data |
| product_currency | STRING | |
| product_inventory_count | INTEGER | |
| product_referer | STRING | |
| product_sku | STRING | |
| product_name | STRING | |

---

## silver.logdata.spend_log
- **Type:** VIEW → `sqlmesh__logdata.logdata__spend_log__4068879977` (**TABLE** — physical)
- **Partition:** HOUR on `auction_timestamp`
- **Clustering:** None
- **Use for:** Won auction records with cost/billing data. Source of truth for spend.

| Column | Type | Notes |
|--------|------|-------|
| advertiser_id | INTEGER | |
| campaign_id | INTEGER | |
| campaign_group_id | INTEGER | |
| creative_id | INTEGER | |
| flight_id | INTEGER | |
| term_id | INTEGER | |
| product_id | INTEGER | |
| partner_id | INTEGER | Beeswax vs MNTN bidder |
| auction_id | STRING | `<exchange_id>.<auction_id>` = mntn_auction_id |
| exchange_auction_id | STRING | Raw exchange auction ID |
| auction_epoch | INTEGER | **NANOSECONDS** |
| auction_timestamp | TIMESTAMP | Auction receipt time (partition key) |
| auction_type | INTEGER | First-price vs second-price |
| bid_id | STRING | Unique bid identifier (uuidv7) |
| bid_price_micros | INTEGER | Bid price in micros (local currency) |
| bid_price_micros_usd | INTEGER | Bid price in micros USD |
| win_cost_micros_usd | INTEGER | Actual win cost micros USD |
| impression_id | STRING | Impression ID picked for bid |
| impression_timestamp | TIMESTAMP | Impression render time |
| impression_bid_floor | FLOAT | |
| impression_expiration | INTEGER | Expiration window |
| creation_timestamp | TIMESTAMP | Win notification receipt time |
| flight_end_timestamp | TIMESTAMP | |
| device_type | STRING | See bronze.integrationprod.device_type |
| device_ua | STRING | |
| platform_device_ifa | STRING | |
| platform_os | STRING | |
| placement_type | STRING | VIDEO, BANNER, NATIVE |
| environment_type | STRING | |
| inventory_source | STRING | Beeswax exchange names |
| publisher_id | STRING | |
| publisher_name | STRING | |
| site_domain | STRING | |
| site_id | STRING | |
| site_name | STRING | |
| deal_id | STRING | Also known as pmp_deal_id |
| ip | STRING | |
| app_bundle | STRING | |
| app_id | STRING | |
| app_name | STRING | |
| geo_version | STRING | |
| is_test | BOOLEAN | **Exclude from production analysis** |
| model_params | STRING | |
| advertiser_intent_score | INTEGER | |
| campaign_intent_score | INTEGER | |
| segment_intent_score | INTEGER | |
| segment_intent_score_ttl | INTEGER | |

- **Query tip:** Always filter on `auction_timestamp`. Exclude `is_test = TRUE`.

---

## silver.logdata.bidder_bid_events
- **Type:** VIEW → `sqlmesh__logdata.logdata__bidder_bid_events__3013815525` (**TABLE** — physical)
- **Partition:** HOUR on `time` (90-day TTL)
- **Use for:** All bid decisions from MNTN bidder (bid + no-bid reasons)

| Column | Type | Notes |
|--------|------|-------|
| time | TIMESTAMP | Partition key |
| epoch | INTEGER | **MILLISECONDS** |
| auction_epoch | INTEGER | **MICROSECONDS** |
| auction_id | STRING | |
| exchange_auction_id | STRING | |
| bid_id | STRING | |
| advertiser_id | INTEGER | |
| campaign_id | INTEGER | |
| campaign_group_id | INTEGER | |
| creative_id | INTEGER | |
| flight_id | INTEGER | |
| term_id | INTEGER | |
| product_id | INTEGER | |
| partner_id | INTEGER | |
| segment_id | INTEGER | |
| channel_id | INTEGER | |
| objective_id | INTEGER | |
| line_item | INTEGER | |
| price | INTEGER | Bid price micros |
| impression_bid_floor | FLOAT | |
| household_score | INTEGER | |
| household_score_threshold | INTEGER | |
| advertiser_household_score | INTEGER | |
| conquest_score | INTEGER | |
| conquest_score_ttl | INTEGER | |
| budget_pace | FLOAT | |
| pace_multiplier | FLOAT | |
| price_cap_multiplier | FLOAT | |
| recency | INTEGER | |
| recency_threshold | INTEGER | |
| flight_end_timestamp | TIMESTAMP | |
| placement_type | STRING | |
| inventory_source | STRING | |
| publisher | STRING | |
| publisher_domain | STRING | |
| publisher_id | STRING | |
| publisher_name | STRING | |
| publisher_price_threshold | INTEGER | |
| selected_pmp_deal_id | STRING | |
| selected_pmp_deal_is_fixed_price | BOOLEAN | |
| pmp_deal_bid_floor | FLOAT | |
| pmp_deal_ids | RECORD | LIST |
| device | STRING | |
| device_ua | STRING | |
| ip | STRING | |
| ifa | STRING | Device IFA |
| region | STRING | |
| width | INTEGER | |
| height | INTEGER | |
| duration | INTEGER | |
| is_ctv | STRING | |
| is_test | BOOLEAN | |
| env | STRING | Environment |
| beeswax_crid | INTEGER | Beeswax creative ID |
| threshold_failure_reasons | STRING | Why bid was filtered |
| campaign_frequency_cap | STRING | |
| campaign_group_frequency_cap | STRING | |
| agent_params | STRING | Bidding agent parameters |
| pacing_debug_data | STRING | |
| targeted_segments | RECORD | LIST |
| campaign_impressions | RECORD | LIST |
| campaign_group_impressions | RECORD | LIST |
| term_ids | RECORD | LIST |
| tow_hours | RECORD | Time-of-week hours |

- **Note:** 90-day TTL. Exclude `is_test = TRUE`.

---

## silver.logdata.bidder_auction_events
- **Type:** VIEW → `sqlmesh__logdata.logdata__bidder_auction_events__3563801775` (**TABLE** — physical)
- **Partition:** HOUR on `time`
- **Use for:** All auctions seen by MNTN bidder (including dropped/no-bid)

| Column | Type | Notes |
|--------|------|-------|
| time | TIMESTAMP | Partition key |
| epoch | INTEGER | |
| auction_id | STRING | |
| mntn_auction_id | STRING | MNTN's composite auction ID |
| exchange_auction_id | STRING | |
| exchange_id | INTEGER | |
| auction_type | INTEGER | |
| partner_id | INTEGER | |
| auction_dropped | BOOLEAN | Whether auction was dropped |
| auction_dropped_reason | STRING | |
| placement_type | STRING | |
| environment_type | STRING | |
| inventory_source | STRING | |
| publisher_id | STRING | |
| publisher_domain | STRING | |
| publisher_name | STRING | |
| site_id | STRING | |
| site_domain | STRING | |
| site_name | STRING | |
| app_bundle | STRING | |
| app_id | STRING | |
| app_name | STRING | |
| app_domain | STRING | |
| device_type | STRING | |
| device_ua | STRING | |
| device_ifa | STRING | |
| device_ip | STRING | |
| device_ipv6 | STRING | |
| device_os | STRING | |
| geo_city | STRING | |
| geo_country | STRING | |
| geo_metro | STRING | |
| geo_region | STRING | |
| geo_zip | STRING | |
| geo_lat | FLOAT | |
| geo_lon | FLOAT | |
| geo_type | STRING | |
| geo_version | STRING | |
| region | STRING | |
| pmp_deal_ids | RECORD | LIST |
| segment_ids | RECORD | LIST |
| video_placement | STRING | |
| request_id | INTEGER | |
| is_test | BOOLEAN | |

---

## silver.logdata.bid_logs_enriched
- **Type:** VIEW → `sqlmesh__logdata.logdata__bid_logs_enriched__277062179` (**TABLE** — physical)
- **Partition:** HOUR on `time` (90-day TTL)
- **Use for:** Enriched bidder bid events (same schema as bidder_bid_events, join with auction data)
- **Note:** Schema identical to `bidder_bid_events` — same columns. Difference is enrichment applied.

---

## silver.logdata.bid_attempted_log
- **Type:** VIEW → `sqlmesh__logdata.logdata__bid_attempted_log__1519082903` (VIEW → bidder_bid_events)
- **Use for:** Alias for bidder_bid_events — attempted bids. Same data.
- **Note:** `bid_events_log` is the same underlying view.

## silver.logdata.bid_events_log
- **Type:** VIEW → `sqlmesh__logdata.logdata__bid_events_log__772626469` (VIEW → bidder_bid_events UNION ALL bid_price_log)
- **Note:** Same as `bid_attempted_log` — both reference `bidder_bid_events`.
- **CRITICAL: Very sparse data (2026-03-12).** Only advertiser 32167 found in this table (checked Feb 2026). Most advertisers have NO records. Not useful for general advertiser analysis. Use `bid_logs` (Beeswax-native) instead for bid IP lookups.
- **Columns:** Has `auction_id`, `advertiser_id` (MNTN), `campaign_group_id`, `ip`, `time`. Despite having `auction_id`, the format may differ from `event_log.td_impression_id` — 0/50 matched in v15 forensic trace for adv 37775.

---

## silver.logdata.cost_impression_log
- **Type:** VIEW → `sqlmesh__logdata.logdata__cost_impression_log__2498930125` (**TABLE** — physical, 71 B rows / 56 TB)
- **Partition:** DAY on `time`
- **Clustering:** advertiser_id, impression_id
- **GCS archive:** **None — BigQuery-only dataset.** Stream from BQ via Spark BigQuery connector (efficient with the table-only mode; SQLMesh physical name resolved at runtime). (via Victor Savitskiy 2026-04-28, TI-837)
- **Use for:** Impression-level spend enriched with geo, device, segment data.
- **⚠️ RETENTION CORRECTION (AUDI-1070, verified 2026-06-30):** NOT 90-day rolling — CIL retains **multi-year history**. Empirically probed back to **2024 and earlier** (82.5M rows on 2024-06-15; 84.7M on 2025-02-01; agent MIN(time)≈2023-10). A Jan-2024→present per-impression analysis IS feasible from CIL. Cost-control: always partition-prune on `time` (one day ≈ 0.68 GB) + exploit `advertiser_id` clustering; a 4-month × 3-AID score scan billed ~12 GB. **MCP `bigquery` tool fails here** (`--location US`; dataset isn't US) — use `bq`/`bq_run.sh`.
- **⚠️ SCORE COLUMNS (AUDI-1070):** `advertiser_household_score` (MM-tuned per-advertiser) and `household_score` (general) are INT 0–10000 (−1/NULL = unscored). **BOTH columns are 100% NULL before 2025-06-01 and ~0% NULL from 2025-06-01 onward — a sharp, platform-wide, single-week cutover (verified AUDI-1070 2026-06-30: 100% NULL wk of 2025-05-25 → 0% NULL wk of 2025-06-01).** This is a **CIL LOGGING change** (the score columns began being written into cost_impression_log on 2025-06-01), **NOT** a scoring-pipeline onset — the bidder scored households before this date (scores lived upstream/in the bidder), CIL just didn't carry the columns. **Consequence: the TYPED COLUMNS cannot answer "score distribution / % under 8000 / scored-fraction over time" before 2025-06-01.** BUT scores are RECOVERABLE one cutover earlier — they first appear in the `model_params` STRING on **2025-05-06** (another clean 0%→100% overnight cutover). So the recoverable CIL score floor is **2025-05-06**, not 2025-06-01: `COALESCE(advertiser_household_score, SAFE_CAST(REGEXP_EXTRACT(model_params, r'advertiser_household_score=(-?\d+)') AS INT64))` (same pattern for household_score). **No CIL score history of any kind exists before 2025-05-06.** Do not read the NULL→populated transition as a performance event. Unscored encoding: **HS = −1; AHS = NULL/−1** (the two diverge — retargeting rows have HS=−1 but AHS=10000). Where populated, AHS scored impressions are nearly all at/near max (~9,900) → AHS is effectively **binary (scored vs unscored)**; the meaningful signal is the **scored fraction**, not the level. **RTC caveat: `realtime_conquest_score=…` is logged on ~100% of rows regardless of whether RTC fired — do NOT exclude rows merely containing that token.** Genuine RTC = `realtime_conquest_score=10000`; value −1 = RTC not active (the case for Caraway/Avon/HexClad). CIL partition-prunes on `DATE(time)`, clusters on `advertiser_id`.

| Column | Type | Notes |
|--------|------|-------|
| advertiser_id | INTEGER | |
| campaign_id | INTEGER | |
| group_id | INTEGER | |
| creative_id | INTEGER | |
| impression_id | STRING | Unique impression |
| ad_served_id | STRING | |
| guid | STRING | |
| time | TIMESTAMP | Partition key |
| epoch | INTEGER | |
| partner_time | TIMESTAMP | |
| partner_id | INTEGER | |
| ip | STRING | |
| partner_ip | STRING | |
| media_cost | NUMERIC | |
| media_spend | BIGNUMERIC | |
| data_spend | BIGNUMERIC | |
| platform_spend | BIGNUMERIC | |
| site | STRING | |
| domain | STRING | |
| raw_domain | STRING | |
| subdomain | STRING | |
| country | STRING | |
| metro_id | INTEGER | |
| region | STRING | |
| city | STRING | |
| postal_code | STRING | |
| private_marketplace_id | STRING | |
| supply_vendor | STRING | |
| operating_system_family | STRING | |
| operating_system | STRING | |
| browser | STRING | |
| user_agent | STRING | |
| device_type | STRING | Beeswax device type: SET_TOP_BOX, CONNECTED_TV, MOBILE, PC, TABLET, GAMES_CONSOLE |
| sh_device | STRING | MNTN device classification (often NULL for Beeswax impressions) |
| ott_device | STRING | `bw_batch` (Beeswax batch) or `mb_rt` (real-time) |
| publisher_type_id | INTEGER | 1=CTV/OTT, 2=premium, 3=web/display |
| unlinked | BOOLEAN | Impression not linked to a guid |
| partner_ad_format | STRING | **CTV vs display indicator:** `VIDEO`=CTV, `BANNER`=display, `BANNER_AND_VIDEO`=mixed |
| partner_site | STRING | |
| is_new | BOOLEAN | |
| geo_version | INTEGER | |
| household_score | INTEGER | |
| advertiser_household_score | INTEGER | |
| model_params | STRING | |
| batch_epoch | INTEGER | |
| source_batch_epoch | INTEGER | |
| recency_elapsed_time | INTERVAL | Time since last impression (INTERVAL type) |

---

## silver.logdata.viewability_log
- **Type:** VIEW → `sqlmesh__logdata.logdata__viewability_log__702576036` (VIEW → upstream)
- **Use for:** Display impression viewability events. **Display equivalent of event_log** — use for tracing viewable display VVs back to their impression. For non-viewable display VVs, use impression_log instead. CTV does not use this table (CTV uses event_log with vast_start/vast_impression).

| Column | Type | Notes |
|--------|------|-------|
| guid | STRING | |
| time | TIMESTAMP | |
| epoch_time | INTEGER | |
| ad_served_id | STRING | |
| exchange_id | INTEGER | |
| advertiser_id | INTEGER | |
| campaign_id | INTEGER | |
| creative_id | INTEGER | |
| group_id | INTEGER | |
| domain | STRING | |
| subdomain | STRING | |
| viewability_type_id | INTEGER | 1=measurable, 2=viewable. Multiple rows per ad_served_id (one per type). Display only — CTV uses event_log VAST events instead. |
| ip | STRING | |
| ip_raw | STRING | |
| bid_ip | STRING | |
| original_ip | STRING | |
| mntn_ip | STRING | |
| publisher | STRING | |
| user_agent | STRING | |
| is_mobile_device | BOOLEAN | |
| browser | STRING | |
| operating_system | STRING | |
| device_type | STRING | |
| browser_version | STRING | |

---

## silver.logdata.win_logs
- **Type:** VIEW → `sqlmesh__logdata.logdata__win_logs__1170758268` (VIEW → Beeswax win_logs)
- **Use for:** Beeswax win notification log (external DSP perspective on wins)
- **Note:** Very wide table (130+ columns). Beeswax-native schema. Use `spend_log` for MNTN-native billing.
- **CRITICAL: Uses Beeswax IDs, not MNTN IDs.** `advertiser_id`, `campaign_id`, `line_item_id`, `creative_id` are Beeswax-internal IDs. However, `_alt_id` columns map back to MNTN:
  - `campaign_alt_id` (INT64) = MNTN `campaign_group_id`
  - `line_item_alt_id` (STRING, cast to INT64) = MNTN `campaign_id`
  - `creative_alt_id` (STRING) = MNTN `creative_id` (from integrationprod.creatives? — unverified)
  - `creative_name` (STRING) = also contains MNTN `campaign_id` (as string) — appears redundant with line_item_alt_id
  - Join to MNTN campaigns: `CAST(w.line_item_alt_id AS INT64) = c.campaign_id`
  - Join to event_log: `win_logs.auction_id = event_log.td_impression_id`
- **Impression type indicators (validated 2026-03-13):**
  - `placement_type`: `VIDEO` = CTV, `BANNER` = display
  - `environment_type`: `APP` = CTV, `WEB` = display
  - `platform_device_type`: `SET_TOP_BOX`/`CONNECTED_TV` = CTV, `PC`/`MOBILE`/`TABLET` = display
  - `banner_width`/`banner_height`: `-1` = CTV (no banner), actual sizes (300x250, 728x90, etc.) = display
- **IP columns (validated 2026-03-10, 38.2M rows):** `ip` = bid/win IP (= event_log.bid_ip at 99.9999%). `impression_ip_address` = infrastructure/CDN IP (68.67.x.x MNTN infra, AWS IPs) — NOT user IP. 8 IP-related columns total: ip, ip_raw, ip_range, ipv6_address, ip_address_hashed, ipv6_address_hashed, clicks_ip_address, impression_ip_address.
- **Key columns:** account_id, campaign_id, campaign_alt_id, advertiser_id, creative_id, creative_alt_id,
  line_item_id, line_item_alt_id, auction_id, time, epoch,
  win_cost_micros_usd, bid_price_micros_usd, clearing_price_micros_usd, placement_type,
  environment_type, platform_device_type, inventory_source, is_test, flight_id

---

## silver.logdata.auction_log
- **Type:** VIEW → `sqlmesh__logdata.logdata__auction_log__507400019` (VIEW → v_augmentor_log)
- **Use for:** Auction-level events from augmentor service

---

## silver.logdata.v_augmentor_log
- **Type:** VIEW → `sqlmesh__logdata.logdata__v_augmentor_log__2626104662` (VIEW → augmentor_log upstream)
- **Use for:** Pre-bid augmentation events with geo parsing

| Column | Type | Notes |
|--------|------|-------|
| time | TIMESTAMP | |
| epoch | INTEGER | **MILLISECONDS** |
| domain | STRING | |
| app_bundle | STRING | |
| environment_type | STRING | |
| placement_type | STRING | |
| device_type | STRING | |
| inventory_source | STRING | |
| network | STRING | |
| os | STRING | |
| site_name | STRING | |
| ip | STRING | |
| ipv6 | STRING | |
| ifa | STRING | |
| user_agent | STRING | |
| video_placement | STRING | |
| geo_parsed | RECORD | Struct: geo_city, geo_country, geo_ip, geo_latitude, geo_longitude, geo_metro, geo_region, geo_zip |
| mntn_segments | RECORD | LIST of segment IDs |
| pmp | RECORD | LIST of PMP deals |

---

## silver.logdata.icloud_vv_log
- **Type:** VIEW → `sqlmesh__logdata.logdata__icloud_vv_log__1701206424` (VIEW → icloud_vv upstream)
- **Use for:** iCloud Private Relay view-through events (Apple device traffic)
- **Schema:** Similar to clickpass_log (guid, time, epoch, advertiser_id, campaign_id, click_url, ip, is_new, etc.)

---

## silver.logdata.page_view_signal_log
- **Type:** VIEW → `sqlmesh__logdata.logdata__page_view_signal_log__461032789` (VIEW → page_view_signal upstream)
- **Use for:** Page view signals from MNTN pixel (structured event format)

| Column | Type | Notes |
|--------|------|-------|
| event_id | STRING | |
| time | TIMESTAMP | |
| guid | STRING | Extracted from ids RECORD |
| advertiser_id | INTEGER | |
| data_source_id | INTEGER | |
| ip | STRING | |
| ids | RECORD | LIST of id name/value pairs |
| user_agent | RECORD | Struct: browser, browser_version, device_type, is_mobile_device, operating_system, raw, advanced |
| query_str | STRING | |
| url | STRING | |
| referer | STRING | |
| ad_served_id | STRING | |

---

## silver.logdata.event_log_filtered
- **Type:** VIEW → `sqlmesh__logdata.logdata__event_log_filtered__2760749612` (**TABLE** — physical)
- **Partition:** DAY on `time` (60-day TTL)
- **Use for:** Filtered subset of event_log (IVT/fraud removed)

---

## silver.logdata.realtime_spend_last_3d
- **Type:** VIEW → `sqlmesh__logdata.logdata__realtime_spend_last_3d__2690208900` (VIEW)
- **Use for:** Rolling 3-day spend aggregation for pacing/realtime dashboards
- **Note:** Derived from spend_log. Do not use for historical analysis.

---

## silver.logdata.spend_log_tmp
- **Type:** TABLE (direct, not through SQLMesh)
- **Use for:** Staging table for spend_log pipeline. Likely transient.
- **Schema:** Same as spend_log (advertiser_id, campaign_id, auction_id, auction_timestamp, etc.)

---

## silver.logdata.bid_logs
- **Type:** VIEW → `sqlmesh__logdata.logdata__bid_logs__932945987` (VIEW → Beeswax bid_logs)
- **Use for:** Beeswax bid log (external DSP bid records). Beeswax-native schema.

---

# silver.summarydata

**Project:** dw-main-silver | **Dataset:** summarydata
All tables are VIEWs pointing to `sqlmesh__summarydata`.

---

## silver.summarydata.visits
- **Type:** VIEW → `sqlmesh__summarydata.summarydata__visits__2108051348` (**TABLE** — physical)
- **Partition:** DAY on `time`
- **Clustering:** advertiser_id, ad_served_id, time
- **Use for:** Row-level site visit events. One row per visit attributed to an impression.

| Column | Type | Notes |
|--------|------|-------|
| advertiser_id | INTEGER | |
| guid | STRING | Visitor cookie |
| time | TIMESTAMP | Visit time (partition key) |
| epoch | INTEGER | |
| impression_time | TIMESTAMP | Attributed impression time |
| impression_epoch | INTEGER | |
| elapsed_time | INTERVAL | Time from impression to visit |
| ad_served_id | STRING | Links to impression_log |
| impression_id | STRING | |
| impression_ip | STRING | IP at impression time |
| ip | STRING | IP at visit time |
| ip_raw | STRING | |
| exchange_id | INTEGER | |
| section_id | INTEGER | |
| channel_id | INTEGER | |
| campaign_id | INTEGER | |
| group_id | INTEGER | |
| creative_id | INTEGER | |
| domain | STRING | |
| subdomain | STRING | |
| country | STRING | |
| region | STRING | |
| metro_id | INTEGER | |
| city | STRING | |
| postal_code | STRING | |
| private_marketplace_id | STRING | |
| supply_vendor | STRING | |
| device_type | STRING | |
| click | BOOLEAN | Was this a click-through visit |
| is_cross_device | BOOLEAN | |
| efficient | BOOLEAN | Efficient attribution flag |
| from_verified_impression | BOOLEAN | TRUE = visit is attributed via MNTN's verified impression attribution (used by the UI). Filter: `from_verified_impression = TRUE` to match what the UI reports. |
| is_new | BOOLEAN | First visit for this advertiser |
| visits_assist | BOOLEAN | Assist attribution flag |
| attribution_model_id | INTEGER | |
| first_touch_ad_served_id | STRING | |
| pa_model_id | INTEGER | Probabilistic attribution model |
| recency_elapsed_time | INTERVAL | |

- **Join:** `ad_served_id` → impression_log

---

## silver.summarydata.ui_visits
- **Type:** VIEW → `sqlmesh__summarydata.summarydata__ui_visits__903315386` (VIEW on visits)
- **Use for:** UI-facing view of visits with additional computed columns
- **Schema:** Same as visits, plus:
  - `visit_day` (FLOAT): Day number within attribution window
  - `source_type` (STRING): Visit attribution source
  - `is_competing` (BOOLEAN): Competing attribution
  - `is_pa` (BOOLEAN): Probabilistic attribution flag
  - `attribution_model_type_id` (INTEGER): **Note: if = 0, treat as 1 (last-touch)**
- **Known issue:** `ip` column has upstream bug — two IP fields present temporarily.

---

## silver.summarydata.conversions
- **Type:** VIEW → `sqlmesh__summarydata.summarydata__conversions__2893062813` (**TABLE** — physical)
- **Partition:** DAY on `time` (60-day TTL)
- **Use for:** Row-level conversion events attributed to impressions. 60-day rolling.

| Column | Type | Notes |
|--------|------|-------|
| advertiser_id | INTEGER | |
| guid | STRING | |
| time | TIMESTAMP | Conversion time (partition key) |
| epoch | INTEGER | |
| event_time | TIMESTAMP | Pixel fire time |
| event_epoch | INTEGER | |
| elapsed_time | INTERVAL | Impression → conversion time |
| ip | STRING | |
| impression_time | TIMESTAMP | |
| impression_epoch | INTEGER | |
| impression_ip | STRING | |
| impression_elapsed_time | INTERVAL | |
| ad_served_id | STRING | |
| impression_id | STRING | |
| exchange_id | INTEGER | |
| section_id | INTEGER | |
| channel_id | INTEGER | |
| campaign_id | INTEGER | |
| group_id | INTEGER | |
| creative_id | INTEGER | |
| domain | STRING | |
| subdomain | STRING | |
| order_id | STRING | |
| order_amt | NUMERIC | |
| order_curr | STRING | |
| click | BOOLEAN | |
| click_through | BOOLEAN | Click-through vs view-through |
| disputed | BOOLEAN | |
| query | JSON | |
| from_verified_impression | BOOLEAN | TRUE = visit is attributed via MNTN's verified impression attribution (used by the UI). Filter: `from_verified_impression = TRUE` to match what the UI reports. |
| is_cross_device | BOOLEAN | |
| attribution_model_id | INTEGER | |
| country | STRING | |
| region | STRING | |
| metro_id | INTEGER | |
| city | STRING | |
| postal_code | STRING | |
| private_marketplace_id | STRING | |
| supply_vendor | STRING | |
| device_type | STRING | |
| conversion_type | STRING | |
| conversion_source_id | INTEGER | |
| conversion_assist | BOOLEAN | |
| pa_model_id | INTEGER | |
| recency_elapsed_time | INTERVAL | |

---

## silver.summarydata.impression_facts
- **Type:** VIEW → `sqlmesh__summarydata.summarydata__impression_facts__3555911259` (**TABLE** — physical, ~1.8 TB)
- **Partition:** DAY on `hour`
- **Clustering:** NONE (verified 2026-05-28 — `bq show` returns `"clustering": null`)
- **Use for:** Hourly impression aggregates by campaign/geo/device. Primary impressions reporting table; stays fresh through current day (unlike `sum_by_campaign_by_day` rollups).
- **Optimization gotcha (TI-961, verified 2026-05-28):** Because there's NO clustering, filtering to a small advertiser list does NOT reduce bytes processed. The only knobs are date range (partition pruning works) + which fact tables to pull. A 60-day pull across all prospecting advertisers is ~206 GB; cutting to 14 days is ~50 GB; restricting to 50 advertisers makes zero difference at the bytes-processed level. **Asymmetric with `visit_facts`/`conversion_facts`/`spend_facts` which ARE clustered on advertiser/campaign** — keep in mind when joining across these tables.
- **CORRECTION (TI-1019, verified 2026-06-24 by reading the SQLMesh model + reconstructing the HLL in BQ): `uniques` is NOT keyed on raw `device_ip`.** The SQLMesh source (`SteelHouse/sqlmesh` → `models/dw-main-silver/summarydata/impression_facts.sql`, owner `ber`; legacy twin = `SteelHouse/db_repo` → `coredw/lds/functions/populate_impression_facts.sql`) builds `uniques`/`uniques_arr` as an HLL over a **channel-conditional IP-OR-GUID key**: `HLL_COUNT.INIT(CASE WHEN c.channel_id = 8 OR c.objective_id IN (5,6) THEN l.ip ELSE l.guid END)`. So CTV (channel_id 8) / video-objective (5,6) rows count distinct **resolved `cost_impression_log.ip`**; ALL OTHER (display) rows count distinct **`guid`** (cookie/device id). It reads FROM `logdata.cost_impression_log` (served/WON impressions) with `unlinked = FALSE AND ad_served_id IS NOT NULL`. **It is therefore NOT `device_ip` and NOT a pre-bid / augmentor / all-seen-IP universe — same served log as our calculator, different distinct key.** `existing_users_reached`/`new_users_reached` use the identical key, split by `is_new`.
- **This explains the ~2× "32.1M users reached" vs 15.7M served-IP gap (WGU AID 31357, 30d, reconstructed in BQ 2026-06-24):** distinct `ip` = 15.53M (≈ the 15.7M CIL count, confirming SAME served universe); distinct `guid` = 37.09M; the `uniques` blend (CTV→ip, display→guid) = **32.46M ≈ the 32.1M HLL number** (HLL ≈ exact; ~1% sketch error). WGU rows = 60% CTV-or-video (205M/343M) but the 40% display contributes 37M distinct GUIDs — that GUID fan-out (one IP → many browser/cookie GUIDs) is the entire 2×. **Bottom line: `graph.usersreached`/`uniques` over-counts the served universe via display GUIDs; for a served-IP MDE/IVR denominator use `cost_impression_log.ip` (15.7M → 10.70%), NOT `uniques` (32.1M → 5.92%).** Numerator `site_visitors` (from `visit_facts`) is resolved-IP-grained, so it does not match `uniques`' grain. Supersedes the earlier "raw device_ip" claim in the `all_facts` entry below (which was inference; this is the model source). **Reporting-stack identity:** `graph.usersreached`/`graph.sitevisitors` are R2/CHAPI metric names; physical source = ClickHouse `summarydata.all_facts_local_daily.uniques`/`site_visitors`, loaded by Airflow `load_reporting_data.load_all_facts` from BQ `dw-main-silver.summarydata.v_all_facts` (= SQLMesh `all_facts`) via GCS Parquet → `v_ext_all_facts` → ClickHouse. So R2/graph = the **ClickHouse copy of BQ `all_facts`**, owned by BER/data-platform (chapi/airflow-reporting). See data_knowledge.md "Reporting graph table / R2 / CHAPI" for the full lineage + ownership.

## silver.summarydata.visit_facts
- **Type:** VIEW → `sqlmesh__summarydata.summarydata__visit_facts__427634656` (**TABLE** — physical)
- **Partition:** DAY on `hour`
- **Clustering:** advertiser_id, campaign_id
- **Use for:** Pre-aggregated visit metrics by campaign/geo/device/hour. Primary reporting table.

| Column | Type | Notes |
|--------|------|-------|
| hour | TIMESTAMP | Partition key |
| advertiser_id | INTEGER | |
| campaign_group_id | INTEGER | |
| campaign_id | INTEGER | |
| channel_id | INTEGER | |
| objective_id | INTEGER | |
| group_id | INTEGER | |
| creative_id | INTEGER | |
| private_marketplace_id | STRING | |
| country | STRING | |
| metro_id | INTEGER | |
| region | STRING | |
| city | STRING | |
| postal_code | STRING | |
| domain | STRING | |
| supply_vendor | STRING | |
| device_type | STRING | |
| pa_model_id | STRING | |
| clicks | INTEGER | |
| views | INTEGER | Attributed site visits (view-through) |
| efficient_views | INTEGER | |
| new_visitors | INTEGER | First-time visitors |
| site_visitors | INTEGER | |
| new_site_visitors | INTEGER | |
| existing_site_visitors | INTEGER | |
| last_tv_touch_clicks | INTEGER | |
| last_tv_touch_views | INTEGER | |
| last_touch_clicks | INTEGER | |
| last_touch_views | INTEGER | |
| visits_assist | INTEGER | |
| competing_views | INTEGER | |
| competing_* | INTEGER | Various competing attribution metrics |
| probattr_* | INTEGER | Probabilistic attribution metrics |
| first_day_visits through seventh_day_visits | INTEGER | Day-bucketed visit counts |
| *_arr | STRING | HyperLogLog++ serialized arrays for unique counts |

---

## silver.summarydata.conversion_facts
- **Type:** VIEW → `sqlmesh__summarydata.summarydata__conversion_facts__3549666587` (**TABLE** — physical)
- **Partition:** DAY on `hour`
- **Clustering:** advertiser_id, campaign_id
- **Use for:** Pre-aggregated conversion metrics by campaign/geo/device/hour.

| Column | Type | Notes |
|--------|------|-------|
| hour | TIMESTAMP | |
| advertiser_id | INTEGER | |
| campaign_group_id | INTEGER | |
| campaign_id | INTEGER | |
| channel_id / objective_id / group_id / creative_id | INTEGER | |
| conversion_type | STRING | |
| conversion_source_id | INTEGER | |
| pa_model_id | INTEGER | |
| click_conversions | INTEGER | |
| click_order_value | NUMERIC | |
| view_conversions | INTEGER | |
| view_order_value | NUMERIC | |
| last_touch_* | INTEGER/NUMERIC | Last-touch attribution |
| last_tv_touch_* | INTEGER/NUMERIC | Last TV touch attribution |
| conversions_assist_* | INTEGER/NUMERIC | Assist attribution |
| competing_* | INTEGER/NUMERIC | Competing attribution |
| probattr_* | INTEGER/NUMERIC | Probabilistic attribution |

---

## silver.summarydata.spend_facts
- **Type:** VIEW → `sqlmesh__summarydata.summarydata__spend_facts__1266598596` (**TABLE** — physical)
- **Partition:** DAY on `hour`
- **Clustering:** advertiser_id, campaign_id
- **Use for:** Pre-aggregated spend by campaign/geo/device/hour.

| Column | Type | Notes |
|--------|------|-------|
| hour | TIMESTAMP | |
| advertiser_id | INTEGER | |
| campaign_group_id | INTEGER | |
| campaign_id | INTEGER | |
| channel_id / objective_id / group_id / creative_id | INTEGER | |
| private_marketplace_id | STRING | |
| country / metro_id / region / city / postal_code | STRING/INT | Geo dimensions |
| domain | STRING | |
| supply_vendor | STRING | |
| device_type | STRING | |
| media_spend | BIGNUMERIC | |
| data_spend | BIGNUMERIC | |
| platform_spend | BIGNUMERIC | |
| ctv_spend | BIGNUMERIC | |
| unlinked_spend | BIGNUMERIC | |

---

## silver.summarydata.all_facts
- **Type:** VIEW → `sqlmesh__summarydata.summarydata__all_facts__2291495033` (VIEW — joins facts)
- **Use for:** Combined reporting view: spend + visits + conversions + impressions. 150+ columns.
- **Warning:** Very wide. Prefer individual facts tables when possible.
- **How it's populated (verified from `SteelHouse/sqlmesh` model `models/dw-main-silver/summarydata/all_facts.sql`, owner `ber`, 2026-06-24):** hourly SQLMesh `INCREMENTAL_BY_TIME_RANGE` model (legacy Greenplum source = `lds.populate_all_facts()`). `UNION ALL` of three row-types: (1) `impression_facts` FULL OUTER JOIN `visit_facts` FULL OUTER JOIN `spend_facts` on **19 dimension keys** (hour, advertiser, campaign_group, campaign, channel, objective, group, creative, pmp, country, metro, region, city, postal, domain, supply_vendor, device_type, pa_model); (2) `conversion_facts` (conversion-only rows); (3) `site_facts` (site-only rows). HLL sketches (`uniques`, `site_visitors`, …) pass through **unchanged** (no re-keying). `bids = 0` (comment: "No bid_facts in SQLMesh"). CTV domains rewritten via `public.to_domain()` when `channel_id = 8`. So `graph.usersreached` = `uniques` (impression side, from `impression_facts`) and `graph.sitevisitors` = `site_visitors` (visit side, from `visit_facts`) — **different grains**. R2 reads a ClickHouse copy (`all_facts_local_daily`) via CHAPI; load DAG = `SteelHouse/airflow-reporting` `dags/chapi/`. **Owner = Backend Reporting squad (`ber`)** — owns BOTH the SQLMesh model and the CHAPI/ClickHouse load (verified via `owners.py` + commit authors: Lizz Joslen, Mike Rivera; Aylwin Souza on squad). Route reporting-metric/`graph.*`/`all_facts` change requests to a BER Jira ticket tagging Backend Reporting.
- **ClickHouse 30-day merge mechanics (verified from chapi/airflow-reporting code, 2026-06-25):** the BQ HLL++ sketch columns (`uniques`, `*_users_reached`) are **NOT loaded to ClickHouse — they're dead** (BQ HLL++ isn't ClickHouse-mergeable). R2/CHAPI uses the **`*_arr` raw-ID arrays**: `uniques_arr` → ClickHouse `all_facts_local_daily` (`Array(Nullable(String))`, hourly) → MV `uniqArrayState(uniques_arr)` → `all_facts_local_by_day` (`AggregateFunction(uniqArrayState, …)`). A 30-day `graph.usersreached` = `toInt64(uniqArrayMerge(uniques_arr))` over the window — an HLL **merge across days, NOT a `SUM()`** (hourly grain is fine; the window dedup is query-time; proven by chapi `HouseholdsReachedQuerySqlTest.kt`). Any NEW graph reach metric must emit an **`_arr` array** (e.g. `users_reached_ip_arr = ARRAY_AGG(l.ip)`), not an HLL sketch, and is a coordinated change across `sqlmesh` (model) + `chapi` (ClickHouse DDL on `all_facts_local_daily` + 2 `_by_day` tables, the MV, and `r2-metadata.xml` `type="HLL"`) + `airflow-reporting` (`dags/chapi/conf/reporting_config.json` + BQ export view `v_all_facts`) + a ~30d backfill (MVs aren't retroactive).
- **Key columns include:** All columns from visit_facts + conversion_facts + spend_facts, plus
  display_impressions, ctv_impressions, media_cost, fee_cost, vast_* video metrics,
  uniques (BYTES — HLL), *_arr serialized arrays, probattr_* columns, competing_* columns.
- **Gotcha (TI-1044, verified 2026-06-23):** the unique-count columns `uniques`, `visitors`,
  `site_visitors`, `new_site_visitors` are **HLL sketches stored as BYTES** — `SUM()` errors
  ("cannot coerce BYTES"); use `HLL_COUNT.MERGE(col)` to get a daily unique count, or
  `HLL_COUNT.MERGE_PARTIAL` to re-aggregate. The `*_arr` variants are STRING-encoded sketches.
  Scalar INT counts (no HLL): `views`, `clicks`, `view_conversions`, `click_conversions`,
  `new_visitors`, `raw_visits`, `raw_conversions`, `first_touch_visits`, `bids`,
  `display_impressions`, `ctv_impressions`.
- **Spend units (TI-1044):** `ctv_spend`/`media_spend` (BIGNUMERIC) and `view_order_value`/
  `click_order_value` (NUMERIC) are in **whole USD, NOT micros** — do not ÷1e6. (`ctv_spend` is
  advertiser-billed CPM spend, ~2× the `spend_log.win_cost_micros_usd` media cost.) `channel_id 8`
  = CTV (Beeswax Television); `channel_id 1` = display. Per-advertised-unique visit rate =
  `HLL_COUNT.MERGE(site_visitors)/HLL_COUNT.MERGE(uniques)`; CVR = `(SUM(view_conversions)+
  SUM(click_conversions))/HLL_COUNT.MERGE(uniques)`.
- **`uniques` (= `graph.usersreached`) is a channel-conditional IP-OR-GUID HLL over the SAME served `cost_impression_log` — NOT `device_ip`, NOT a broader universe (TI-1019, verified 2026-06-24 from the SQLMesh model + BQ reconstruction).** Full mechanism is in the corrected `impression_facts` entry above (search "channel-conditional IP-OR-GUID key") and in `data_knowledge.md` ("What the reporting graph table actually IS"). Key facts:
  - `uniques` = `HLL_COUNT.INIT(CASE WHEN channel_id = 8 OR objective_id IN (5,6) THEN ip ELSE guid END)` from `cost_impression_log` (`unlinked=FALSE AND ad_served_id IS NOT NULL`). CTV/video → distinct **`ip`**; display → distinct **`guid`** (cookie).
  - WGU 30d: `uniques`=32.1M, `site_visitors`=1.90M, IVR=5.92% vs our `cost_impression_log` distinct-`ip` IVR=10.70%. The 2× is **display being counted by cookie/guid (~18.4M, fans out ~2.4× per IP), not a different table.** CTV leg alone = 14.06M `ip` ≈ served CTV `ip` (12.7M); display guid leg = 18.40M; ≈ 32.46M total. (Earlier "raw device_ip" / "cross-table universe" notes were wrong — corrected here.)
  - "graph" is the **R2** metric layer → **CHAPI** (ClickHouse API) → ClickHouse `all_facts_local_daily` ← hourly copy of BQ `all_facts`. Owner: SQLMesh model = `ber` team (`SteelHouse/sqlmesh`); CHAPI load DAGs = data-platform (`SteelHouse/airflow-reporting`). Metric-def routing: #reporting_helpdesk_ask_anything (Ray).
  - **For the MDE baseline use `count(distinct ip) from cost_impression_log` (15.7M → 10.70%)** — the per-IP unit the holdout (`MD5(advertiser_id:ip)`; 0/2.36M served IPs in holdout buckets) and VV attribution run on (Zach Schoenberger, authority: holdout = targeting on event-log `ip`; VV = ip-match event-log↔guid-log, no md5). `graph.usersreached` mixes IP+cookie namespaces and over-counts display, so it's the wrong denominator.

---

## silver.summarydata.site_facts
- **Type:** VIEW → `sqlmesh__summarydata.summarydata__site_facts__2462066249` (**TABLE** — physical)
- **Partition:** DAY on `hour`
- **Clustering:** advertiser_id, conversion_source_id
- **Use for:** Aggregated site visitor metrics by advertiser/conversion_source.

| Column | Type | Notes |
|--------|------|-------|
| hour | TIMESTAMP | |
| advertiser_id | INTEGER | |
| conversion_source_id | INTEGER | |
| conversion_type | STRING | |
| visitors | BYTES | HLL++ serialized |
| visitors_arr | STRING | |
| new_to_file | INTEGER | |
| raw_visits | INTEGER | |
| raw_conversions | INTEGER | |
| raw_order_value | NUMERIC | |
| raw_existing_site_visitors | BYTES | HLL++ |
| raw_new_site_visitors | BYTES | HLL++ |

---

## silver.summarydata.offline_facts
- **Type:** VIEW → `sqlmesh__summarydata.summarydata__offline_facts__3339806120` (VIEW)
- **Use for:** Offline conversion attribution facts (uploaded conversion data)

---

## silver.summarydata.last_tv_touch_conversions
- **Type:** VIEW → `sqlmesh__summarydata.summarydata__last_tv_touch_conversions__1806020579` (**TABLE** — physical)
- **Partition:** DAY on `time`
- **Use for:** Conversions attributed to last TV ad touch specifically. Row-level.
- **Note:** `impression_ip` is STRING (converted from Postgres inet type)

---

## silver.summarydata.last_tv_touch_visits
- **Type:** VIEW → `sqlmesh__summarydata.summarydata__last_tv_touch_visits__3655239346`
- **Use for:** Site visits attributed to last TV touch.

---

## silver.summarydata.offline_conversions
- **Type:** VIEW → `sqlmesh__summarydata.summarydata__offline_conversions__292378007` (VIEW)
- **Use for:** Offline/uploaded conversion events matched to ad exposure.

---

## silver.summarydata.visits (alias: ui_visits)
See entries above.

---

## silver.summarydata.advertiser_sales_cycle_by_day
- **Type:** VIEW → `sqlmesh__summarydata.summarydata__advertiser_sales_cycle_by_day__969350298` (**TABLE** — physical)
- **Partition:** DAY on `day`
- **Use for:** Sales cycle (time from first page view to conversion) by advertiser.

| Column | Type | Notes |
|--------|------|-------|
| day | DATE | Partition key |
| advertiser_id | INTEGER | |
| guid | STRING | |
| ip | STRING | |
| ga_client_id | STRING | |
| conversion_time | TIMESTAMP | |
| conversion_epoch | INTEGER | |
| first_page_view_time | TIMESTAMP | |
| first_page_view_epoch | INTEGER | |
| sales_cycle_time | INTEGER | Seconds from first page view to conversion |
| is_new | BOOLEAN | |

---

## silver.summarydata.guid_ip_log_visitors
- **Type:** VIEW → `sqlmesh__summarydata.summarydata__guid_ip_log_visitors__2172532229`
- **Use for:** Visitor identity matching — GUID × IP associations.

## silver.summarydata.icloud_guids / icloud_ipv4 / icloud_ipv6
- **Type:** VIEWs → sqlmesh__summarydata
- **Use for:** Apple iCloud Private Relay identity resolution tables.

## silver.summarydata.sum_by_campaign_by_day
- **Type:** VIEW → `sqlmesh__summarydata`
- **Partition:** DAY on `day`
- **Date range:** 2024-01-01 to present (15+ months — longer than agg__daily_sum_by_campaign which starts Sep 2025)
- **Use for:** Daily campaign-level KPI aggregation. **Best table for experiments needing long pre-periods** (52-week pre-period for CausalImpact).
- **STALENESS GOTCHA (verified 2026-05-01):** This rollup view is currently lagging by ~17 days (max=2026-04-14 when current date is 2026-05-01). Same lag affects `sum_by_campaign_group_by_day` and `sum_by_advertiser_by_day`. Verify max(day) before using for recent-window analysis. The downstream `silver.aggregates.agg__daily_sum_by_campaign` is even worse (empty since 2026-03-31). **For fresh data, query the underlying fact tables directly: `silver.summarydata.{impression_facts, visit_facts, conversion_facts, spend_facts}`** — these are at hour grain and stay fresh through current day.

| Column | Type | Notes |
|---|---|---|
| advertiser_id | INT64 | |
| campaign_id | INT64 | Join to `campaigns` for campaign_group_id, funnel_level |
| day | DATE | Partition key |
| impressions | INT64 | |
| clicks | INT64 | VV component (click-through visits) |
| views | INT64 | VV component (view-through visits) |
| click_conversions | INT64 | |
| view_conversions | INT64 | |
| competing_views | INT64 | For industry_standard attribution |
| competing_view_conversions | INT64 | For industry_standard attribution |
| click_order_value | NUMERIC | |
| view_order_value | NUMERIC | |
| competing_view_order_value | NUMERIC | |
| media_spend, data_spend, platform_spend | NUMERIC | Total spend = sum of all three |
| vast_start, vast_complete | INT64 | Video completion tracking (VCR = complete/start) |
| video_impressions, display_impressions | INT64 | CTV vs display split |
| uniques | BYTES | HLL sketch — NOT usable as integer count at campaign level |

**Also available at other granularities:**
- `sum_by_campaign_group_by_day` — same columns, grouped by campaign_group_id instead of campaign_id
- `sum_by_advertiser_by_day` — advertiser-level daily
- `sum_by_creative_by_day` — creative-level daily
- `sum_by_region_by_day` — geographic daily
- All go back to 2024-01-01.

**⚠️ ATTRIBUTION-VARIANT COLUMNS — separate models, NOT additive buckets; never SUM them (verified AUDI-1070, Avon AID 31921, 2026-06-30):** this table carries multiple attribution variants of the same conversions:
- **Default:** `views, clicks, view_conversions, click_conversions, view_order_value, click_order_value` — **INCLUDES CTV** (a CTV view-conversion is a `view_conversion`). Clincher: the 100%-CTV prospecting campaign ("Beeswax Television") shows its full conversions in the DEFAULT columns — if default excluded CTV that would be ~0.
- **`last_touch_*` vs `last_tv_touch_*`:** **two SEPARATE last-touch models** — display/overall-last-touch vs TV-last-touch — that can credit the *same* conversion to *different* campaigns. They are NOT subsets and NOT additive. They only **coincide at the account total** (`default` ≈ `last_touch` ≈ 17.33× ROAS for Avon), and **diverge per stage** (fl=1: default 11.0 / last_touch 4.99 / last_tv_touch 7.47; fl=2: default 36 / last_touch 49 / last_tv 0.04). `last_tv_touch` revenue concentrates in the CTV prospecting stage.
- **`conversions_assist_*` / `visits_assist`:** multi-touch assist credit. · **`competing_*`:** industry_standard (first-touch) variant. · **`probattr_*`:** probabilistic.

**⛔ Do NOT use `last_touch + last_tv_touch` (or `default + last_tv_touch`) to "add CTV back" — it DOUBLE-COUNTS** any conversion exposed to both a display and a TV touch (each model credits it separately): account → ROAS **23.5**, which **overshoots** the UI's 22.1. It only lands near the UI by coincidence (the double-count ≈ the real CHAPI uplift).

**✅ CORRECTION (verified 2026-06-30 via CHAPI source `SteelHouse/chapi` + an EXACT BQ reproduction): you CAN rebuild the UI number — the 17.3→22.1 gap is the `competing_*` (FIRST-TOUCH) columns, NOT an unrecoverable "engine" difference and NOT `last_tv_touch`.** The client UI runs CHAPI's **NEW / `industry_standard`** reporting style = the last-touch (default) cols **+ `competing_*`** (which CHAPI labels FirstTouchVisits / FirstTouchConversions / FirstTouchOrderValue). Reproduced against `silver.summarydata.all_facts` (time col `hour`) to the dollar/visit, BOTH years: Verified Visits `clicks+views+competing_views` = **692,888 / 598,436 = UI EXACT**; Order Value `+competing_view_order_value` → **ROAS 22.09 (UI 22.12) / 26.36 (UI 26.36)**; **CPA & CVR EXACT** ($2.39/$2.03, 4.41%/5.26%); Households `HLL_COUNT.MERGE(uniques)` ~1–2% under (HLL engine only — BQ vs CH `uniqArrayMerge`). `last_tv_touch` is NEVER in the UI headline — the lt+tv sum's 23.5× was a coincidental overshoot. Legacy/`last_touch` advertisers (`reporting_style`, api2.*) drop `competing_*`. **So spend/impressions AND attributed visits/conv/rev all reproduce exactly; only HLL reach brackets.** Query: `tickets/audi_1070_yoy_decline_caraway_avon_hexclad/queries/avon_chapi_exact_reproduction.sql`.

**SCOPE GOTCHA — the client "Performance Report - MoM" chart is the PROSPECTING CAMPAIGN GROUP (use `objective_id`, not `funnel_level`):** the pink-visits/blue-spend/green-ROAS MoM chart shows the **prospecting group = `objective_id != 4`** (S1 Prospecting `259556` + Multi-Touch S2/S3 follow-ups `259558/9`,`330396/7` + Ego), NOT AID-wide. Proof: its monthly blue spend bars sum to **$56,833 / $46,614 to the dollar** (= the prospecting figure), not the AID-wide $73K/$64K. The UI summary **cards = ALL campaigns** (prospecting group + retargeting). The ROAS lift from ~8× (prospecting) to ~17–26× (account) is the **dedicated TV Retargeting campaigns (`objective_id = 4`)** — ~50× pooled (individual line items 80–278×) on 22% of spend / 64% of revenue. **Do NOT attribute the lift to "mid-funnel S2/S3"** — the non-retargeting S2/S3 campaigns (`330396/7`) actually run ~0–13× ROAS; and `funnel_level` is a messy separator (some `objective_id=4` retargeting campaigns are `funnel_level=1`). Join `campaign_id → bronze.integrationprod.campaigns.campaign_id` for `objective_id`/`funnel_level` (PK is `campaign_id`, NOT `id`; there is no `silver.core.campaigns`).

## silver.summarydata.sum_by_ctv_network_by_day
- **Type:** VIEW → `sqlmesh__summarydata`
- **Partition:** DAY on `day`
- **Date range:** 2025-01-01 to present
- **Use for:** Publisher/network-level performance. One row per advertiser × campaign × publisher × day.

| Column | Type | Notes |
|---|---|---|
| advertiser_id | INT64 | |
| campaign_id | INT64 | |
| domain | STRING | Publisher/network name (e.g., "CBS", "NBC", "HBO Max"). **Matches `media_plan_publishers.name` exactly** (cross-validated TI-748). |
| day | DATE | Partition key |
| impressions | INT64 | |
| clicks | INT64 | |
| views | INT64 | |
| click_conversions, view_conversions | INT64 | |
| click_order_value, view_order_value | NUMERIC | |

**Cross-validation:** Publisher distribution matches `cost_impression_log` (same top-5 publishers in same rank order, validated for CWRV Sales Feb 2026). Counts differ slightly due to campaign filtering but proportions are consistent.

**Gotchas:**
- Does NOT have competing_views/conversions columns — use industry_standard attribution from sum_by_campaign_by_day instead for total VV/conversions
- `domain` naming is the human-readable publisher name, not a URL domain

---

# silver.core
**Status:** Pending — ~50 tables. Likely contains: campaigns, advertisers, creatives, campaign_groups, placements, etc.

---

# silver.aggregates
**Status:** Pending

---

# bronze.raw
**Status:** Pending — ~40 tables. Raw ingestion layer.

---

# bronze.coredw
**Status:** Pending — small dataset (~2 tables).

---

# bronze.integrationprod
**Status:** Pending — known to contain ENUM reference tables (e.g., device_type).

---

# Notes on HyperLogLog++ (BYTES) Columns
Many unique-count columns in facts tables (uniques, visitors, *_arr) are stored as BYTES
(HyperLogLog++ sketches) for approximate distinct counting. Use BigQuery's HLL functions
(`HLL_COUNT.MERGE`, `HLL_COUNT.EXTRACT`) to work with these. The `*_arr` STRING variants
are serialized array representations.

---

# silver.core

**Project:** dw-main-silver | **Dataset:** core
All ~67 tables are VIEWs pointing directly to `bronze.integrationprod.core_*` tables (no SQLMesh here).
Pattern: `silver.core.flights` → `SELECT * FROM bronze.integrationprod.core_flights`

**Table inventory (all VIEWs):**
advertiser_account_types, advertiser_channel_margins, advertiser_padding_overrides,
advertisers_impression_tracking_urls, advertisers_visit_tracking_urls, advertisers_x_features,
advertisers_x_hotels, attribution_models, audiences, beta_advertisers, blocked_ip_addresses,
budget_types, campaign_group_channel_margins, campaign_group_x_audiences,
campaign_group_x_private_marketplace_deals, campaign_padding_overrides, campaign_statuses,
campaign_x_audiences, channel_margins, creative_groups, creative_groups_x_creatives,
creative_sizes, creative_video_meta_informations, creatives, currency_codes, device_type_groups,
fact__v3_conversions, features, flight_billing_types, flights, goal_types, hotels, icloud_blacklist,
margin_sources, media_plan, media_plan_publishers, mobile_apps, objectives, partner_types,
partners, pixel_integration_types, pixel_integrations, price_models,
private_marketplace_deal_impression_rates_log, private_marketplace_deals,
private_marketplace_families, private_marketplace_groups, private_marketplace_levels, products,
r2_roles_x_advertisers, segment_types, segmentation_defaults, select_advertiser_margins,
select_margins, ttd_advertiser_channel_margins, ui_flight_x_media_plan, v_advertiser_channel_margins,
v_campaign_group_channel_margins, v_channel_margins, v_icloud_blacklist

**For schemas of all silver.core tables, see [bronze.integrationprod](#bronze-integrationprod) — the actual source tables are prefixed `core_*`.**

## silver.core.media_plan

**Source:** `bronze.integrationprod.core_media_plan` (CDC from Postgres)

| Column | Type | Description |
|---|---|---|
| media_plan_id | INT64 | PK |
| advertiser_id | INT64 | FK to advertisers |
| campaign_group_id | INT64 | FK to campaign_groups — which campaign group uses this plan |
| media_plan_status_id | INT64 | 1=draft(?), 3=active, 8=inactive(?) |
| create_time | TIMESTAMP | When the plan was created |
| update_time | TIMESTAMP | Last update |
| original_recommendations | JSON | Publisher recommendations with budget %s and rationale |
| deliverability_classification | STRING | e.g. "medium" |
| is_manual | BOOL | Whether the plan was manually created vs auto-generated |
| datastream_metadata | RECORD | CDC metadata |

**Key query:** `SELECT DISTINCT advertiser_id FROM core.media_plan WHERE media_plan_status_id=3` — active media plan users.

**Gotchas:**
- Being on the beta list doesn't mean active — must check `media_plan_status_id=3`
- One advertiser can have many plans (one per campaign_group)
- `original_recommendations` contains JSON with publisher names, budget percentages, and rationale
- `deliverability_classification`: categorical delivery risk (high/medium/low). Worst individual guardrail wins. In-flight override: >3 days and >90% target pace → upgraded to "high".
- **Per-publisher scores NOT in BQ** — computed transiently in memory. Stored as JSON in GCS: `media-plan-artifacts` bucket, path `media-plan/{version}/{advertiser_id}/{plan_id}/response.json`. Scores include: score_semantic, score_performance_advertiser/vertical/network, score_quality, score_spendability, score_cpm_efficiency, score_scale, score_combined (all with normalized variants).
- **Config history:** `max_networks` was 18 initially (Oct 2025), bumped to 25, then reduced to 15 on Feb 3 2026 (olympus commit 555234f, PERML-412). `min_allocation` was 1% (old) → 0.5% (current). Plans created before Feb 2026 have 25-26 publishers; after have 16.

## silver.core.media_plan_publishers

**Source:** `bronze.integrationprod.core_media_plan_publishers` (CDC from Postgres)

| Column | Type | Description |
|---|---|---|
| media_plan_id | INT64 | FK to media_plan |
| name | STRING | Publisher/network name (e.g., "CBS", "NBC", "HBO Max") — matches `domain` in `sum_by_ctv_network_by_day` exactly |
| percentage | STRING (cast to INT) | Budget allocation % for this publisher within the plan |
| badge_state | STRING | `RECOMMENDED` (user accepted), `USER_MODIFIED` (user changed), `USER_ADDED` (user added) |
| rank | STRING (cast to INT) | Priority rank within the plan |
| rationale | STRING | Algorithm's reasoning for this publisher selection |

**Key queries:**
- Recommended-only plans: `WHERE badge_state = 'RECOMMENDED'` on all publishers for a plan
- Publisher concentration: `COUNT(DISTINCT name)` per plan — 16 publishers outperforms 26 (TI-748 finding)

**Gotchas:**
- `percentage` and `rank` are STRING type, cast to INT for math
- Publisher names match `sum_by_ctv_network_by_day.domain` exactly (cross-validated TI-748)
- Every plan includes a "Flex" publisher (7-10%) for bidder flexibility

## bronze.integrationprod.r2_advertiser_settings

| Column | Type | Description |
|---|---|---|
| advertiser_id | INT64 | FK to advertisers |
| reporting_style | STRING | `industry_standard` or `last_touch` — determines attribution model |

**Key query:** Check attribution model: `SELECT advertiser_id, reporting_style FROM integrationprod.r2_advertiser_settings WHERE advertiser_id = X`

**Note:** No `deleted` column on this table (unlike most integrationprod tables).

**⚠️ FIELD-LEVEL HISTORY EXISTS — `bronze.integrationprod.archives_advertiser_setting_archives`** (AUDI-1070). The live `r2_advertiser_settings` row's `update_time` is only the MOST RECENT change; `reporting_style` can flip many times. The archive table has one row per `version` with `reporting_style` + `create_time`/`update_time` — use it to resolve the **effective attribution lens as-of any date** (e.g. during a YoY analysis window). Real example: Avon 31921 & HexClad 34611 oscillated `industry_standard`↔`last_touch` **dozens of times** across 2024-2025; both were effectively `last_touch` during Feb-May 2025 but `industry_standard`(FT) during Feb-May 2026 — so any client-facing YoY for them mixed lenses (LT-2025 vs FT-2026), which alone manufactured ~50pp of an apparent visit/ROAS decline. **Always resolve the as-of lens for BOTH years before comparing.** As-of query pattern: UNION the archive rows + the live row, take `ARRAY_AGG(reporting_style ORDER BY COALESCE(update_time,create_time) DESC LIMIT 1)` where `eff_time <= window_date`.

---

# silver.aggregates

**Project:** dw-main-silver | **Dataset:** aggregates
All named views follow the SQLMesh pattern → `sqlmesh__aggregates`. The `_bqc_*` tables are
BigQuery Connector internal cache tables — do not query directly.

---

## silver.aggregates.agg__daily_sum_by_campaign
- **Type:** VIEW → `sqlmesh__aggregates.aggregates__agg__daily_sum_by_campaign__11365516`
- **Partition:** DAY on `day`
- **Use for:** Daily campaign-level reporting rollup. Best single table for campaign performance.

| Column | Type | Notes |
|--------|------|-------|
| day | DATE | Partition key |
| advertiser_id | INTEGER | |
| campaign_id | INTEGER | |
| impressions / display_impressions / video_impressions | INTEGER | |
| view_impressions / view_viewed / view_untrackable | INTEGER | Viewability metrics |
| clicks / views | INTEGER | |
| click_conversions / view_conversions | INTEGER | |
| click_order_value / view_order_value | NUMERIC | |
| media_cost / data_cost / fee_cost / partner_cost | NUMERIC | |
| media_spend / data_spend / platform_spend / video_spend | NUMERIC | |
| vast_start / vast_firstquartile / vast_midpoint / vast_thirdquartile / vast_complete | INTEGER | Video funnel |
| uniques / site_visitors / new_site_visitors / existing_site_visitors | INTEGER | |
| new_users_reached / existing_users_reached | INTEGER | |
| last_tv_touch_* | INTEGER/NUMERIC | Last TV touch attribution |
| last_touch_* | INTEGER/NUMERIC | Last touch attribution |
| visits_assist / conversions_assist_* | INTEGER/NUMERIC | |
| competing_* | INTEGER/NUMERIC | Competing attribution metrics |

- **Query tip:** Filter on `day`. Join to `bronze.integrationprod.campaigns` on `campaign_id`.

---

## silver.aggregates.campaign_group_log_aggregation
- **Type:** VIEW → `sqlmesh__aggregates`
- **Use for:** Per-minute bidder pacing data — bid decisions, spend vs cap, term eligibility.
- **Key columns:** bid_time, auction_time, bid_time_minute, advertiser_id, campaign_group_id, campaign_id,
  flight_id, auction_id, has_price, price, publisher, threshold_failure_reasons,
  flight_campaign_group_spend/cap, daily_campaign_group_spend/cap, terms (JSON), picked_term_id

---

## silver.aggregates.campaign_group_log_agg_min
- **Type:** VIEW → `sqlmesh__aggregates`
- **Use for:** Minute-level bid volume and pacing cap summary per campaign group / flight.
- **Key columns:** advertiser_id, campaign_group_id, flight_id, bid_time,
  total_bid_requests, total_200_bids, total_204_bids, bid_price_per_hour,
  max_flight_campaign_group_spend/cap, max_daily_campaign_group_spend/cap

---

## silver.aggregates.campaign_log_agg_min / campaign_log_agg_min_mntn_bidder
- **Type:** VIEWs
- **Use for:** Same as campaign_group_log_agg_min but at campaign level.

## silver.aggregates.campaign_group_log_aggregation_mntn_bidder
- **Type:** VIEW
- **Use for:** Same as campaign_group_log_aggregation but filtered to MNTN bidder only (not Beeswax).

---

## silver.aggregates.terms_log_agg_min
- **Type:** VIEW → `sqlmesh__aggregates`
- **Use for:** Term-level (segment/audience term) bid pacing detail.
- **Key columns:** bid_time, advertiser_id, flight_id, campaign_group_id, campaign_id,
  eligible_term_id, picked_term_id, term_id, dimension, total_bid_requests, total_bids,
  total_204_bids, avg_win_rate, max_term_spend/cap, throttling_percentage

---

## silver.aggregates.terms_log_agg_min_mntn_bidder
- **Type:** VIEW — Same as terms_log_agg_min filtered to MNTN bidder only.

---

## silver.aggregates.guid_identity_daily
- **Type:** VIEW → `sqlmesh__aggregates`
- **Use for:** Daily GUID identity graph — maps guid → ip, ga_client_id, device, phone, email.
- **Key columns:** day, guid, ip, original_ip, ga_client_id, ua_raw, phone, email, event_count, distinct_seconds

---

## silver.aggregates.audience_hll_by_day
- **Type:** VIEW → `sqlmesh__aggregates`
- **Use for:** Daily audience segment size estimates (HyperLogLog++ sketches).
- **Key columns:** dt, segment_id, hll (BYTES — HLL sketch for unique count)

---

## silver.aggregates.pmp_impression_rates
- **Type:** VIEW → `sqlmesh__aggregates`
- **Use for:** PMP deal impression availability rates.
- **Key columns:** partner_deal_id, days_with_data, distinct_ips, avg_daily_impression_rate, yesterdays_inventory

---

## silver.aggregates.augmentor_identity_daily
- **Type:** VIEW — Daily augmentor-sourced identity signal.

## silver.aggregates.tmul_holdout_segments / tpa_membership_update_log_uber / tpa_membership_updates_log_insegments
- **Type:** VIEWs — TPA (Third Party Audience) membership tracking views.

---

# bronze.raw

**Project:** dw-main-bronze | **Dataset:** raw
Mixed dataset: physical raw event tables (written by bidder/augmentor services) + VIEWs pointing
to `bronze.sqlmesh__raw` (same SQLMesh pattern as silver, but at bronze layer).

**Bronze raw views** (impression_log, clickpass_log, conversion_log, visits, etc.) → `bronze.sqlmesh__raw.raw__*`
These are the bronze-layer SQLMesh models that eventually feed silver.

---

## bronze.raw.bidder_bid_events (**PRIMARY RAW SOURCE**)
- **Type:** TABLE (physical — written by MNTN bidder service)
- **Partition:** HOUR (no field specified — ingestion time), 90-day TTL
- **Use for:** Raw bid decisions straight from the bidder. The source for silver.logdata.bidder_bid_events.
- **Key differences from silver:** `device_type` is INTEGER (not STRING), `auction_timestamp` is INTEGER
  (not TIMESTAMP), contains `_source_file` and `_batch_id` batch ingestion metadata.
- **Note:** `bid_placed` and `bid_dropped` BOOLEAN flags present here, absent in silver enriched version.

| Column | Type | Notes |
|--------|------|-------|
| mntn_auction_id | STRING | |
| partner_id | INTEGER | |
| exchange_id | INTEGER | |
| auction_id | STRING | |
| auction_timestamp | INTEGER | **Raw epoch integer** (not TIMESTAMP — silver converts) |
| bid_id | STRING | |
| impression_id | STRING | |
| advertiser_id | INTEGER | |
| campaign_group_id | INTEGER | |
| campaign_id | INTEGER | |
| creative_id | INTEGER | |
| flight_id | INTEGER | |
| bid_price | INTEGER | Micros |
| bid_placed | BOOLEAN | Whether a bid was actually placed (200 response) |
| bid_dropped | BOOLEAN | Whether bid was dropped |
| bid_dropped_reason | STRING | |
| term_id / term_ids | INTEGER/RECORD | |
| segment_id | INTEGER | |
| device_type | INTEGER | **INTEGER here** — join to device_type ENUM for label |
| device_ua / device_ip / device_ipv6 / device_ifa / device_os | STRING | |
| recency / recency_threshold | INTEGER | |
| pace_multiplier / budget_pace / price_cap_multiplier | FLOAT | |
| campaign_frequency_cap / campaign_group_frequency_cap | RECORD | |
| campaign_impressions / campaign_group_impressions | RECORD | |
| pmp_deal_ids / pmp_deal_id / pmp_deal_bid_floor | RECORD/STRING/FLOAT | |
| time_of_week_hours | RECORD | |
| _source_file | STRING | Batch source file |
| _batch_id | STRING | Batch ID |

---

## bronze.raw.bidder_auction_events (**PRIMARY RAW SOURCE**)
- **Type:** TABLE (physical), HOUR partition, 90-day TTL
- **Use for:** All auctions received by the bidder (including dropped). Raw source for silver.logdata.bidder_auction_events.
- **Key differences from silver:** `device_type` and `geo_type` are INTEGER; `video_placement` is INTEGER;
  `auction_timestamp` is INTEGER; has `content_*`, `site_page`, `site_referrer`, `site_categories` not in silver.

| Column | Type | Notes |
|--------|------|-------|
| mntn_auction_id | STRING | |
| partner_id / exchange_id | INTEGER | |
| auction_id | STRING | |
| auction_timestamp | INTEGER | Raw epoch integer |
| auction_type | INTEGER | |
| geo_country / geo_region / geo_city / geo_metro / geo_zip | STRING | |
| geo_lat / geo_lon | FLOAT | |
| geo_type | INTEGER | **INTEGER in raw** (STRING in silver) |
| geo_version | STRING | |
| device_type | INTEGER | **INTEGER in raw** |
| device_ua / device_ip / device_ipv6 / device_ifa / device_os / device_os_version | STRING | |
| publisher / publisher_id / publisher_name / publisher_domain | STRING | |
| app_id / app_name / app_domain / app_bundle | STRING | |
| site_id / site_name / site_domain / site_page / site_referrer | STRING | |
| site_categories | RECORD | LIST |
| content_network / content_channel / content_genre / content_series | STRING | |
| segment_ids | RECORD | LIST |
| pmp_deal_ids | RECORD | LIST |
| environment_type | STRING | |
| placement_type | STRING | |
| video_placement | INTEGER | **INTEGER in raw** |
| inventory_source | STRING | |
| auction_dropped | BOOLEAN | |
| auction_dropped_reason | STRING | |
| is_test | BOOLEAN | |
| _source_file / _batch_id | STRING | |

---

## bronze.raw.bidder_beeswax_win_notifications
- **Type:** TABLE (physical), HOUR partition, 90-day TTL
- **Use for:** Win notifications from Beeswax exchange (external DSP). Raw source for spend pipeline.
- **Key columns:** advertiser_id, campaign_id, campaign_group_id, flight_id, auction_id, beeswax_auction_id,
  auction_timestamp (INTEGER), creation_timestamp (INTEGER), impression_id, impression_timestamp (INTEGER),
  price (micros), placement_type, inventory_source, pmp_deal_id, device_type, device_ip, geo_version

---

## bronze.raw.bidder_win_notifications
- **Type:** TABLE (physical), HOUR partition, 90-day TTL
- **Use for:** MNTN bidder win notifications (non-Beeswax wins). Includes bid_id for join to bidder_bid_events.
- **Key columns:** advertiser_id, campaign_id, flight_id, bid_id, bid_price (micros), win_price (micros),
  auction_id, mntn_auction_id, auction_timestamp (INTEGER), impression_id, impression_timestamp (INTEGER),
  notification_timestamp (INTEGER), partner_id, exchange_id, pmp_deal_id, device_type (INTEGER)

---

## bronze.raw.bidder_price_events
- **Type:** TABLE (physical), DAY partition, 90-day TTL
- **Use for:** Price events from bidder (pre-bid price decisions). Similar structure to bidder_bid_events.
- **Key columns:** bid_id, impression_id, advertiser_id, campaign_id, flight_id, bid_price (micros),
  bid_placed, bid_dropped_reason, device_type (STRING here), pace_multiplier, budget_pace

---

## bronze.raw.augmentor_log (**PRIMARY RAW SOURCE**)
- **Type:** TABLE (physical), HOUR partition on `time`, **10-day TTL**, clustering: ip
- **Total volume:** ~293 B rows / 884 TB logical (current 10-day window).
- **Partition filter REQUIRED** (requirePartitionFilter: true)
- **Use for:** Raw augmentor service events — pre-bid request enrichment. Raw source for v_augmentor_log.
- **GCS archive:** `gs://mntn-data-archive-prod/augmentor_log/` — full historical archive in Parquet, no TTL constraint. **Read directly from GCS via Spark** for high-volume scans on Databricks (bypasses BQ slot contention + scan billing). (via Victor Savitskiy 2026-04-28, TI-837)
- **GCS partition layout (verified 2026-04-29, TI-837):**
  - Top level: `region={east,west}/`
  - Date level: `dt=YYYY-MM-DD/` (a single string column, NOT separate year/month/day)
  - Full path example: `gs://mntn-data-archive-prod/augmentor_log/region=east/dt=2026-04-23/`
  - Earliest partition: ~`2026-03-30` (archive history is ~30 days; not infinite as the "no TTL" framing suggests)
  - For a complete daily scan you must read BOTH `region=east` and `region=west`. Spark loads both if you read the parent path with `.filter("dt = '2026-04-23'")`, but explicit per-region paths give better partition pruning.

- **Row-level schema gotcha:** `augmentor_log` has **NO `advertiser_id` column at the row level**. Augmentor is a per-bid-request log — one row per upstream bid eval, not per advertiser. The only advertiser-relatable signal at this layer is `mntn_segments` (array of segment IDs that evaluated this IP). To attribute an augmentor row to an advertiser, you must join `mntn_segments` ↦ `audience_segments.expression` ↦ advertiser. See TI-837 v5 SQL `queries/ti_837_lift_analysis_30adv_7day_v5_segments.sql` for the canonical pattern.

| Column | Type | Notes |
|--------|------|-------|
| time | TIMESTAMP | Partition key (**required in filter**) |
| ip | STRING | Clustering key |
| time_stamp | STRING | String representation of timestamp |
| epoch | LONG (INT64) | Unix epoch (verify unit per record — augmentor uses microseconds) |
| hh | INTEGER | Hour bucket (0-23) |
| domain / app_bundle / site_name | STRING | |
| placement_type / environment_type / inventory_source | STRING | |
| device_type / video_placement | STRING | |
| os / user_agent / ifa / network / isp | STRING | |
| geo | STRING | Raw geo string |
| mntn_segments | ARRAY\<INTEGER\> | Segments that evaluated this IP (no advertiser_id linkage at row level) |
| pmp | ARRAY\<STRING\> | PMP deal IDs |
| iab_categories / categories | ARRAY\<STRING\> | |
| is_blocked | BOOLEAN | |
| blocking_site | STRING | |
| ipv6 | STRING | |
| page / referrer | STRING | |
| publisher_id / publisher_name / publisher_domain | STRING | |
| _source_file / _batch_id | STRING | ETL provenance |

- **Warning:** Very short TTL (10 days) on the BQ hot table. Partition filter required — always include `time` in BQ WHERE clauses.
- **Spark / Databricks read pattern:**
  ```python
  # Cleanest: explicit region + dt path → no partition pruning ambiguity
  df = spark.read.parquet("gs://mntn-data-archive-prod/augmentor_log/region=east/dt=2026-04-23/")

  # Both regions, range of days (parent path + filters)
  df = (spark.read.parquet("gs://mntn-data-archive-prod/augmentor_log/")
        .filter("dt BETWEEN '2026-04-20' AND '2026-04-26'"))
  ```
  Smoke test: `~/.databricks-py312/bin/python .claude/scripts/databricks_smoke.py`

---

## bronze.raw.bid_price_log
- **Type:** TABLE (physical), HOUR partition on `time`, **10-day TTL**, clustering: ip
- **Partition filter REQUIRED**
- **Use for:** Legacy bid price log from old bidder architecture. May overlap with bidder_bid_events.
- **Key columns:** time, ip, buyer_id, has_price, price, bid_id, auction_id, advertiser_id, campaign_id,
  creative_id, recency, household_score, conquest_score, threshold_failure_reasons, auction_epoch

---

## bronze.raw.page_view_signal_log
- **Type:** TABLE (physical), HOUR partition, 90-day TTL
- **Use for:** Raw page view signal events from MNTN pixel. Source for silver.logdata.page_view_signal_log.
- **Note:** `account_id` (STRING) present here but absent in silver version. No `ip` column at raw level.

| Column | Type | Notes |
|--------|------|-------|
| account_id | STRING | Present only at raw level |
| advertiser_id | INTEGER | |
| data_source_id | INTEGER | |
| data_source_key | STRING | |
| epoch | INTEGER | |
| event_id | STRING | |
| ids | RECORD | LIST of name/value pairs |
| query_str | STRING | |
| referer / url | STRING | |
| user_agent | RECORD | Struct: browser, browser_version, device_type, etc. |
| _source_file / _batch_id | STRING | |

---

## bronze.raw.tmul_daily
- **Type:** TABLE (physical), HOUR partition on `time`, **14-day TTL**
- **Use for:** Daily snapshot of IP → audience segment membership. The primary source for understanding
  which IPs are in which segments at any point in time (within 14-day window).
- **Scale:** ~32B rows, ~14.5TB
- **Snapshot time:** Daily at 08:00 UTC
- **CRITICAL:** Contains **DS 2 and DS 3 ONLY**. DS 4 (CRM) does NOT appear here.
  CRM membership is resolved via the identity graph → ipdsc__v1.

| Column | Type | Notes |
|--------|------|-------|
| id | STRING | **IP address** (despite generic name) |
| time / activity_time | TIMESTAMP | Partition column — daily snapshot at 08:00 UTC |
| data_source_id | INTEGER | Only 2 and 3 in practice |
| in_segments | RECORD | LIST of segments joined — unnest with `.list` wrapper |
| out_segments | RECORD | LIST of segments left |
| metadata_info | RECORD | |
| scores | RECORD | Key-value score pairs |
| delta | BOOLEAN | Whether this is a delta update |

**Unnest pattern:**
```sql
UNNEST(td.in_segments.list) AS isl → isl.element.segment_id, isl.element.advertiser_id, isl.element.campaign_id
```
Note: `.list` wrapper + `.element` — different from tpa_membership_update_log which uses `.segments` directly.

---

## bronze.raw (VIEWs — bronze SQLMesh layer)
The following are VIEWs in bronze.raw pointing to `bronze.sqlmesh__raw.*` (same SQLMesh pattern):
impression_log, clickpass_log, conversion_log, visits, click_log, event_log, guid_log,
viewability_log, bid_logs, win_logs, cost_impression_log, competing_vv, icloud_vv,
geo_maxmind_location_data, geo_maxmind_network_locations, tpa_membership_update_log, etc.

---

## bronze.raw (EXTERNAL tables)
- `icloud_ipv4_ext` — External table (Apple iCloud IP ranges, external source)
- `public__ip_info` — External table (public IP metadata)

---

# bronze.integrationprod

**Project:** dw-main-bronze | **Dataset:** integrationprod
The operational database replica — Postgres CDC via GCP Datastream. This is the source of truth
for all campaign/advertiser/creative/flight configuration. silver.core is a direct view layer over
the `core_*` tables here.

**Scale:** 200+ tables. Key prefixes:
- `core_*` — Exposed via silver.core views
- `audience_*` — Audience management system
- `beeswax_*` — Beeswax exchange mapping tables
- `dso_*` — Demand-side optimization budget/pacing configs
- `archives_*` — Change history / audit trail
- `bidder_*` / `camperbid_*` — Bidder config and ML training data

---

## bronze.integrationprod.advertisers
- **Type:** TABLE (Postgres replica via Datastream)
- **Primary key:** advertiser_id
- **Use for:** Advertiser account configuration. Master advertiser dimension.

| Column | Type | Notes |
|--------|------|-------|
| advertiser_id | INTEGER | PK |
| company_name | STRING | |
| active | BOOLEAN | |
| deleted | BOOLEAN | Filter: `deleted = FALSE` for active accounts |
| is_test | BOOLEAN | **Exclude from production analysis** |
| time_zone | STRING | Advertiser's reporting timezone |
| currency / display_currency | STRING | |
| country_iso_code | STRING | |
| advertiser_vertical_id | INTEGER | Join → (no direct table found — likely lookup) |
| status_id | INTEGER | |
| create_time / update_time | TIMESTAMP | |
| click_conversion_window | STRING | Attribution window (interval string) |
| view_conversion_window | STRING | |
| conversion_window | STRING | |
| invoice_conversion_window | STRING | |
| control_group_percentage | NUMERIC | % traffic in control group |
| segmentation_active / segmentation_new_active | BOOLEAN | |
| clickpass_enabled | BOOLEAN | |
| clickpass_click_ttl / clickpass_view_ttl / clickpass_window | STRING | |
| dpp_enabled | BOOLEAN | |
| product_version_id | INTEGER | |

- **Query tip:** Always filter `deleted = FALSE AND is_test = FALSE` for production data.

---

## bronze.integrationprod.campaigns
- **Type:** TABLE (Postgres replica)
- **Primary key:** campaign_id
- **Use for:** Campaign configuration. Note: campaigns belong to campaign_groups.

| Column | Type | Notes |
|--------|------|-------|
| campaign_id | INTEGER | PK |
| campaign_group_id | INTEGER | FK → campaign_groups |
| advertiser_id | INTEGER | FK → advertisers |
| name | STRING | |
| deleted | BOOLEAN | |
| is_test | BOOLEAN | |
| campaign_status_id | INTEGER | Join → core_campaign_statuses |
| objective_id | INTEGER | Join → core_objectives |
| channel_id | INTEGER | FK → channels. **Authoritative for CTV vs display.** 8=Television(CTV), 1=Multi-Touch(display). See channels table. |
| funnel_level | INTEGER | Stage indicator (1=S1 Prospecting, 2=S2 Multi-Touch, 3=S3 MT Plus, 4=Ego). **More reliable than objective_id.** |
| partner_id | INTEGER | Join → core_partners |
| start_time / end_time | TIMESTAMP | |
| create_time / update_time | TIMESTAMP | |
| audience_type_id | INTEGER | |
| segmentation_default_id | INTEGER | |
| dso_manage_budget | BOOLEAN | Whether DSO auto-manages budget |
| frequency_cap_type_id | INTEGER | |

---

## bronze.integrationprod.campaign_groups
- **Type:** TABLE (Postgres replica)
- **Primary key:** campaign_group_id
- **Use for:** Campaign group (flight/line item equivalent). The primary budget allocation unit.

| Column | Type | Notes |
|--------|------|-------|
| campaign_group_id | INTEGER | PK |
| advertiser_id | INTEGER | FK → advertisers |
| name | STRING | |
| deleted | BOOLEAN | |
| is_test | BOOLEAN | |
| campaign_group_status_id | INTEGER | |
| campaign_group_status | STRING | Denormalized status name |
| objective_id | INTEGER | |
| goal_type_id / goal_type_name | INTEGER/STRING | |
| goal_value | NUMERIC | |
| budget | NUMERIC | Total flight budget |
| budget_type_id | INTEGER | Join → core_budget_types |
| active_flight_id | INTEGER | FK → core_flights |
| start_time / end_time | TIMESTAMP | |
| first_launch_time | TIMESTAMP | |
| product_id | INTEGER | Join → core_products |
| ctv_creatives_status_id / display_creatives_status_id / ui_creatives_status_id | INTEGER | |
| frequency_cap_impressions / frequency_cap_duration | INTEGER/STRING | |
| has_audience | BOOLEAN | |
| testing_type | STRING | A/B testing type |
| parent_campaign_group_id | INTEGER | For nested campaign groups |
| update_time | TIMESTAMP | **GREATEST(ui_ui_flights.update_time, campaign_groups_raw.update_time)** — captures both campaign_group AND child flight modifications. Verified 2026-06-03: 98.8% of cgs have update_time ≥ max(flight, cg_raw); the rest are SQLMesh refresh tail. Use for any "campaign was modified" check. |
| update_time_raw | TIMESTAMP | Raw campaign_group row update_time only (does NOT include flight updates). Use only when you specifically want to detect direct cg-level changes excluding flight edits. |

### update_time semantics (verified 2026-06-03 — TI-ADHOC advertiser scoring filter)
- `update_time` in `campaign_groups` (and `public_campaign_groups`) is a computed column = `GREATEST(ui_flights.update_time, campaign_groups_raw.update_time)`. Source: Postgres view definition confirmed by Victor Savitskiy.
- Empirical correlation across 17,499 cgs in last 90 days: 98.8% follow the GREATEST formula exactly. 14.7% have `update_time = flight_max_update` (most recent change was a flight edit); 84.5% have `update_time = cg_raw_update`.
- **Status changes always bump update_time** — 4,869 status transitions in archive history, 0 with no update_time change.
- For "advertiser modified anything campaign-related in last 24h" checks (e.g., scoring filter rules), use `update_time`, not `update_time_raw`.

---

## bronze.integrationprod.audience_audience_segments
- **Type:** TABLE — per-audience segment overlay rows. Captures *audience-segment-level* expressions, distinct from `audience_audiences.expression` (which captures the audience-level expression).
- **Primary key:** audience_segment_id
- **Use for:** Identifying Fangorn-on advertisers from BigQuery (when Postgres `tpa.fangorn_advertiser_inclusion` is not accessible). Per memory `reference_fangorn_audience_overlay`: Fangorn switch uses audience-overlay → adds DS46 segments to `audience_audience_segments` while leaving DS13/DS19 in the base `audience_audiences` table.
- **Key columns:** audience_segment_id, audience_id (FK → audience_audiences), campaign_id, segment_id, **expression** (STRING — JSON with `"data_source_id":N` references), expression_type_id, is_targeted, update_time, create_time
- **Density:** ~312k rows total; ~1.6k rows reference DS46 (Fangorn) across ~464 distinct advertisers (BQ snapshot 2026-06-10).
- **Fangorn-state identification pattern:**
  ```sql
  WITH ds46_audiences AS (
    SELECT DISTINCT audience_id
    FROM `dw-main-bronze.integrationprod.audience_audience_segments`
    WHERE expression_type_id = 2
      AND REGEXP_CONTAINS(expression, r'"data_source_id":46')
  ),
  ds46_cgids AS (
    SELECT DISTINCT cg.campaign_group_id
    FROM `dw-main-bronze.integrationprod.audience_audience_x_campaign_groups` axcg
    JOIN ds46_audiences USING (audience_id)
    JOIN `dw-main-bronze.integrationprod.public_campaign_groups` cg
      ON cg.campaign_group_id = axcg.campaign_group_id
      OR cg.parent_campaign_group_id = axcg.campaign_group_id
  )
  SELECT DISTINCT c.advertiser_id
  FROM `dw-main-bronze.integrationprod.campaigns` c
  JOIN ds46_cgids USING (campaign_group_id)
  WHERE c.deleted = FALSE AND c.is_test = FALSE
  ```
- **Authoritative source caveat:** Postgres `tpa.fangorn_advertiser_inclusion` is authoritative for tier mapping; the BQ DS46-overlay count (464 advertisers) lags Postgres' rolled-out count (770 advertisers across Tiers 1-4) because recently-flipped advertisers may not yet have their audience-segments updated. Use BQ proxy only when Postgres isn't reachable; cross-check counts before claiming exact tier identification.
- **Discovered 2026-06-10** (TI-961 control-composition diagnostic).

---

## bronze.integrationprod.archives_campaign_group_archives
- **Type:** TABLE — version history of every `campaign_groups` row (Datastream-replicated audit trail)
- **Primary key:** campaign_group_archive_id
- **Use for:** Reconstructing campaign_group state as-of any historical point in time. One row per version per campaign_group. Same columns as `campaign_groups` plus a `version` integer.
- **Density:** ~7 versions per campaign_group on average (90-day window). ~106k rows for ~15k campaign_groups in last 90d.
- **Key columns:** All fields from `campaign_groups` (start_time, end_time, update_time, campaign_group_status_id, budget, etc.) PLUS `version` (monotonic per cg). Use `MAX(version)` or `MAX(update_time)` to find the version effective at a given timestamp.
- **Pattern — "what was the cg state on day D":**
  ```sql
  -- For each campaign_group, the version effective on day D
  WITH versions AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY campaign_group_id ORDER BY version DESC) AS rn
    FROM `dw-main-bronze.integrationprod.archives_campaign_group_archives`
    WHERE update_time <= TIMESTAMP(<day_d>)
  )
  SELECT * FROM versions WHERE rn = 1
  ```
- **Pattern — "any cg modification per day for rule-2-style filtering":**
  ```sql
  -- Any cg.update_time within [D-1d, D]
  SELECT advertiser_id, ANY_VALUE(campaign_group_id) AS cg_id
  FROM archives_campaign_group_archives
  WHERE update_time BETWEEN TIMESTAMP_SUB(D, INTERVAL 24 HOUR) AND D
  GROUP BY advertiser_id
  ```
- **Use cases proven:** historical simulation of advertiser scoring filter (TI-ADHOC, 2026-06-03), status-flip detection for rule coverage validation.
- **Sister table:** `archives_ui_flight_archives` for flight-level version history. **No `archives_advertisers_*` table exists in bronze.integrationprod** — advertiser version history is in `archives.advertiser_archives` in coreDB only.

---

## bronze.integrationprod.channels
- **Type:** TABLE (Postgres replica)
- **Primary key:** channel_id
- **Use for:** Reference table for campaign channel types. Join to `campaigns.channel_id`.
- **Row count:** 10

| channel_id | name |
|---|---|
| 1 | Multi-Touch (display/web) |
| 2 | Email |
| 3 | In-App |
| 4 | Mobile Web |
| 5 | Platform Fee |
| 6 | Real Time Offers |
| 7 | Social |
| 8 | Television (CTV) |
| 9 | Ad Serving |
| 10 | Onsite Offers |

---

## bronze.integrationprod.creative_sizes
- **Type:** TABLE (Postgres replica) — exposed as silver.core.creative_sizes
- **Primary key:** creative_size_id
- **Use for:** Reference table for creative dimensions and type flags.
- **Key columns:**

| Column | Type | Notes |
|--------|------|-------|
| creative_size_id | INTEGER | PK. Join to creatives.creative_size_id |
| width / height | INTEGER | Pixel dimensions |
| description | STRING | Human-readable name (e.g. "HD Video", "Medium Rectangle") |
| video | BOOLEAN | TRUE for video creatives |
| ctv | BOOLEAN | TRUE for CTV-eligible sizes (only creative_size_id 39=Vertical HD 1080x1920, 93=HD Video 1920x1080) |
| web | BOOLEAN | TRUE for web display sizes |
| mobile | BOOLEAN | TRUE for mobile display sizes |

---

## bronze.integrationprod.core_flights
- **Type:** TABLE (Postgres replica) — exposed as silver.core.flights
- **Primary key:** flight_id
- **Use for:** Financial commitment periods for a campaign group (budget × time window).

| Column | Type | Notes |
|--------|------|-------|
| flight_id | INTEGER | PK |
| campaign_group_id | INTEGER | FK → campaign_groups |
| start_time / end_time | TIMESTAMP | Flight period |
| budget | NUMERIC | Flight budget allocation |
| budget_type_id | INTEGER | |
| status_id | INTEGER | |
| ui_flight_id | INTEGER | |
| create_time / update_time | TIMESTAMP | |
| datastream_metadata | RECORD | uuid, source_timestamp |

> **⚠ GOTCHA — `campaign_groups.active_flight_id` is often STALE; do NOT use it as the current flight.** Verified
> TI-1037 2026-06-18: OTF live group 80057's `active_flight_id`=344166 points to a flight with `budget`=2.5 and a
> **2022-12 → 2023-01** window, while the *actual* current flight is **1041900** (2026-06-03 → 2026-07-01, budget 70000).
> To get the operative flight, read the latest `dso_campaign_group_flight_budgets.flight_id` (see below) and join THAT
> to `core_flights` — not `campaign_groups.active_flight_id`.

---

## DSO-managed budget tables (the operative budget for `dso_manage_budget=TRUE` campaigns) — TI-1037 2026-06-18
For DSO-managed campaigns (the common case for prospecting — all OTF campaigns are DSO-managed), the live operative
budget is NOT `campaign_groups.budget` (static) or `core_flights` via `active_flight_id` (stale). It is in the DSO
budget tables (`bronze.integrationprod.dso_*`), **versioned by `update_time`** (DSO writes a new row on each change —
take the latest). `campaigns.dso_manage_budget` (BOOLEAN) flags whether to use these.

| Table | Grain | Key budget cols | Use |
|---|---|---|---|
| `dso_campaign_group_daily_budgets` | campaign_group_id | `budget`, `budget_bx`, `budget_bidder` | **Operative daily $ cap** (OTF latest ≈ $1,690/day). |
| `dso_campaign_group_flight_budgets` | campaign_group_id | `budget`, **`flight_id`** | **Flight-level allocation** (OTF latest ≈ $44,170) + the REAL current `flight_id` (join → `core_flights` for the true window). |
| `dso_campaign_group_budgets` | campaign_group_id | `budget`, `duration` (sec) | Hourly/period rate (OTF ≈ $88.96 / `duration`=3600s = ~$89/hr). |
| `dso_campaign_budgets` / `dso_campaign_flight_budgets` / `dso_daily_budgets` | campaign_id | `budget` | Same, at campaign grain. |

- All carry `advertiser_id`, `transaction_id`, `create_time`, `update_time`. Latest row per group = `QUALIFY ROW_NUMBER() OVER (PARTITION BY campaign_group_id ORDER BY update_time DESC)=1`.
- **Budget-resolution order (for TI-1037 step 9 deliverability):** if `dso_manage_budget` → DSO flight budget (latest) for the cap + its `flight_id`→`core_flights` for the window, DSO daily for the per-day pace; else `core_flights.budget` for the flight covering `as_of` (by date range, not `active_flight_id`) → fallback `campaign_groups.budget`. As-of history: `archives_campaign_group_archives` (`MAX(version) ≤ as_of`).

## logdata.spend_log (spend numerator for pacing / deliverability) — TI-1037 2026-06-18
Realized spend per auction win. Use for step-9 pacing (spend ÷ budget) and any spend-vs-budget question.

| Column | Type | Notes |
|---|---|---|
| `win_cost_micros_usd` | INTEGER | **Spend in micros USD — ÷ 1e6 = dollars.** SUM for total spend. |
| `campaign_id` / `campaign_group_id` / `advertiser_id` | INTEGER | Grain for filtering/joining to budget. |
| `auction_epoch` | INTEGER | **NANOSECONDS** (per the epoch-units gotcha) — prefer the TIMESTAMP cols. |
| `auction_timestamp` / `impression_timestamp` / `time` | TIMESTAMP | `time` is the partition field — **date-filter on it** (required). |
| `flight_end_timestamp` | TIMESTAMP | Flight end as seen at win time. |
| `advertiser_intent_score` / `campaign_intent_score` | INTEGER | Per-win intent scores. |

- Pacing: `pacing_to_date = (SUM(win_cost_micros_usd)/1e6 over [flight_start, min(as_of, flight_end)]) / (flight_budget × elapsed_days/flight_days)`; the "fully delivered" bar is **96%** of budget (Chris Addy, see data_knowledge.md "How deliverability is actually set").

---

## bronze.integrationprod.core_creatives
- **Type:** TABLE (Postgres replica) — exposed as silver.core.creatives
- **Primary key:** creative_id

| Column | Type | Notes |
|--------|------|-------|
| creative_id | INTEGER | PK |
| advertiser_id | INTEGER | FK → advertisers |
| creative_size_id | INTEGER | Join → core_creative_sizes |
| name | STRING | |
| media_type_id | INTEGER | |
| active / approved / deleted | BOOLEAN | |
| is_control_creative | BOOLEAN | Control group creative |
| adcode | STRING | Ad HTML/JS code |
| length | INTEGER | Video length (seconds) |
| caas_video_id | STRING | Creative-as-a-Service video ID |
| create_time / update_time | TIMESTAMP | |

---

## bronze.integrationprod.core_creative_groups
- **Type:** TABLE — exposed as silver.core.creative_groups
- **Primary key:** group_id
- **Use for:** Creative groups (sets of creatives for A/B testing and rotation within a campaign)

| Column | Type | Notes |
|--------|------|-------|
| group_id | INTEGER | PK |
| campaign_id | INTEGER | FK → campaigns |
| name / description | STRING | |
| weight / escalating_order | INTEGER | Rotation/escalation config |
| active | BOOLEAN | |
| is_control | BOOLEAN | Control group flag |
| control_percentage | INTEGER | |
| imp_limit_day / imp_limit_user_day | INTEGER | |
| ui_creative_group_id | INTEGER | |

---

## bronze.integrationprod.core_objectives
- **Type:** TABLE — exposed as silver.core.objectives
- **Primary key:** objective_id
- **Use for:** Campaign/campaign_group objective ENUM (Retargeting, Prospecting, etc.)

| Column | Type | Notes |
|--------|------|-------|
| objective_id | INTEGER | PK |
| name | STRING | |
| description | STRING | |

---

## bronze.integrationprod.device_type (**ENUM reference**)
- **Type:** TABLE
- **Use for:** Device type label lookup. Join on `device_type` INTEGER from bronze.raw tables.
- **Columns:** id (INTEGER), name (STRING)
- **Note:** silver.logdata tables use STRING device_type (already joined). bronze.raw has INTEGER.

---

## bronze.integrationprod.channels
- **Type:** TABLE
- **Use for:** Channel ENUM (CTV, Display, etc.)
- **Columns:** channel_id (INTEGER), name (STRING)

---

## bronze.integrationprod.core_products
- **Type:** TABLE
- **Use for:** Product type (e.g. Mountain, Select, etc.)
- **Columns:** product_id (INTEGER), name (STRING), create_time, update_time, datastream_metadata

---

## bronze.integrationprod.core_partners
- **Type:** TABLE
- **Use for:** Partner (exchange partner) reference. partner_id used in spend_log, bidder tables.
- **Columns:** partner_id (INTEGER), name, description, created_at, partner_type_id, datastream_metadata

---

## bronze.integrationprod.core_creative_sizes
- **Type:** TABLE
- **Use for:** Creative dimension ENUM (width × height × type)
- **Columns:** creative_size_id, width, height, description, video (BOOL), web (BOOL), mobile (BOOL), ctv (BOOL)

---

## bronze.integrationprod.core_attribution_models
- **Type:** TABLE — exposed as silver.core.attribution_models
- **Use for:** Attribution model config. Includes both standard and competing model pairs.

| Column | Type | Notes |
|--------|------|-------|
| attribution_model_id | INTEGER | PK |
| name | STRING | |
| attribution_model_type_id | INTEGER | Join to determine last-touch vs probabilistic |
| counterpart_attribution_model_id | INTEGER | Linked competing model ID |

---

## bronze.integrationprod.core_private_marketplace_deals
- **Type:** TABLE — exposed as silver.core.private_marketplace_deals
- **Primary key:** private_marketplace_deal_id
- **Use for:** PMP deal configuration per advertiser/campaign_group.

| Column | Type | Notes |
|--------|------|-------|
| private_marketplace_deal_id | INTEGER | PK |
| partner_deal_id | STRING | Exchange-side deal ID |
| advertiser_id / campaign_group_id | INTEGER | FK |
| partner_id | INTEGER | Exchange partner |
| floor_price | NUMERIC | |
| start_time / end_time | TIMESTAMP | |
| active | BOOLEAN | |
| deal_type_id | INTEGER | |
| pricing_model | STRING | |
| publisher_id | INTEGER | |
| channel_id | INTEGER | |

---

## bronze.integrationprod.audience_segments
- **Type:** TABLE
- **Use for:** Advertiser-level audience segment definitions (targeting expressions). NOTE: this bronze table does NOT have `campaign_id` or `is_targeted` — those columns live on the silver view `audience.audience_segments` (see below).
- **Columns:** advertiser_id, expression (STRING), expression_type_id, segment_id, create_time, update_time

## dw-main-silver.audience.audience_segments
- **Type:** VIEW
- **Use for:** Active campaign-level targeting expressions. This is what the bidder evaluates. Always join here (not bronze) for "what audience is this campaign actually targeting" questions.
- **Columns:** audience_segment_id, audience_id, **campaign_id**, segment_id, expression, expression_type_id, **is_targeted (BOOLEAN)**, update_time, create_time
- **Filter rule:** `expression_type_id = 2 AND is_targeted = TRUE` for actively-targeted audiences. Type=1 (OPM) rows are source representations that get wrapped into type=2 (TPA) form at bidder evaluation; org-wide for retargeting (`objective_id=4`), 0 of 64,202 type=1 rows are `is_targeted=TRUE`.
- **expression_type_id ENUM** (from `audience.expression_types`): 1=opm, 2=tpa, 3=sga.
- **Holdout clause** lives in the type=2 TPA expression JSON: `MD5('{advertiser_id}:{ip}') mod 1000`, buckets 0-99 = holdout (10%). Verified end-to-end TI-837: 0 of 5.43M served retargeting IPs land in holdout buckets.

## bronze.integrationprod.audience_audiences
- **Type:** TABLE
- **Use for:** Named audience objects with targeting expressions.
- **Columns:** audience_id, advertiser_id, name, expression, expression_type_id, user_id, is_test, create_time, update_time

## bronze.integrationprod.audience_data_sources / data_sources
- **Type:** TABLE (both present — audience_data_sources is the audience service version)
- **Use for:** Conversion pixel / data source registry. Joins to conversion_source_id in summarydata.
- **Key columns:** data_source_id, name, display_name, data_source_key, data_source_type_id,
  conversion_type_display_name, is_mobile

---

## bronze.integrationprod (other notable tables)

| Table | PK | Use for |
|-------|----|---------|
| core_budget_types | budget_type_id | Budget type ENUM (Daily, Flight, etc.) |
| core_campaign_statuses | campaign_status_id | Campaign status ENUM |
| core_goal_types | goal_type_id | Goal type ENUM (CPA, ROAS, etc.) |
| core_segment_types | segment_type_id | Audience segment type ENUM |
| beeswax_advertiser_mappings | advertiser_id | MNTN advertiser_id → Beeswax advertiser_id |
| beeswax_campaign_mappings | campaign_group_id | MNTN campaign_group_id → Beeswax campaign ID |
| beeswax_creative_mappings | creative_id | MNTN creative_id → Beeswax creative ID |
| beeswax_line_item_mappings | campaign_id | MNTN campaign_id → Beeswax line item ID |
| beeswax_segment_mappings | mntn_segment_id, advertiser_id | MNTN segment → Beeswax segment |
| dso_campaign_budgets | campaign_id | DSO-managed campaign budget config |
| dso_campaign_group_budgets | campaign_group_id | DSO-managed campaign group budget |
| dso_campaign_group_daily_budgets | campaign_group_id | Daily budget caps (DSO) |
| dso_campaign_group_flight_budgets | campaign_group_id | Flight budget caps (DSO) |
| blocked_ip_addresses | — | IP blocklist for fraud prevention |

---

# bronze.coredw

**Project:** dw-main-bronze | **Dataset:** coredw
Small internal dataset for data usage reporting/auditing.

---

## bronze.coredw.usage_reporting_audits
- **Type:** TABLE
- **Use for:** Monthly data source usage audit — flags anomalous usage changes for billing review.

| Column | Type | Notes |
|--------|------|-------|
| reporting_month | DATE | |
| data_source_id | INTEGER | Join → integrationprod.data_sources |
| name | STRING | Data source name |
| usage | NUMERIC | Current month usage |
| prior_usage | NUMERIC | Prior month usage |
| impressions / prior_impressions | INTEGER | |
| usage_diff_pct | NUMERIC | |
| gate1_usage_diff_pct / gate2_usage_diff_pct_impression_delta / gate3_increase_in_dollar | INTEGER | Gate thresholds |
| final | STRING | Final audit determination |
| override_status | STRING | Manual override |
| explanation | STRING | Audit explanation |
| created_at / updated_at | TIMESTAMP | |

---

## bronze.coredw.usage_reporting_data
- **Type:** TABLE
- **Use for:** Detailed data source usage by day (underlying data for usage_reporting_audits).

| Column | Type | Notes |
|--------|------|-------|
| dt | DATE | |
| data_source_id | INTEGER | |
| data_source_category_id | STRING | |
| segment_name | STRING | |
| tv_cpms | STRING | |
| tv_cpm | NUMERIC | |
| impressions | NUMERIC | |
| usage | NUMERIC | |
| sharethis_id | STRING | |
| domains | RECORD | |
| reporting_month | DATE | |
| status | STRING | |

---

# bronze.external

**Project:** dw-main-bronze | **Dataset:** external
External tables backed by GCS (Parquet/ORC files). Not managed by SQLMesh.

---

## bronze.external.ipdsc__v1
- **Type:** EXTERNAL TABLE (GCS-backed Parquet)
- **GCS path:** `gs://mntn-data-archive-prod/ipdsc/dt=<date>/data_source_id=<id>/`
- **Partition:** `dt` (STRING 'YYYY-MM-DD') and `data_source_id` (INTEGER)
- **No TTL** — historical data is available indefinitely
- **Use for:** IP → audience category_id resolution. The source of truth for which IPs were in a given
  CRM audience segment on a given date. Critical for CRM campaign debugging and audience size analysis.

| Column | Type | Notes |
|--------|------|-------|
| ip | STRING | IP address |
| data_source_category_ids | RECORD | LIST of category_ids this IP is assigned to |
| dt | STRING | Partition date ('YYYY-MM-DD') |
| data_source_id | INTEGER | Data source (e.g. 4 = CRM, 2 = MNTN First Party) |

**Unnest pattern:**
```sql
SELECT DISTINCT ip, dscid.element AS category_id
FROM `dw-main-bronze.external.ipdsc__v1` t
  , UNNEST(t.data_source_category_ids.list) AS dscid
WHERE t.data_source_id = 4
  AND t.dt = '2025-11-25'
  AND dscid.element IN (17077, 17079)  -- audience_upload_ids
```

**Key fact:** `category_id` here = `audience_upload_id` = `data_source_category_id` in integrationprod.audience_uploads.

**⚠ Performance — filter `dt` with a LITERAL, never a subquery.** `WHERE dt = (SELECT MAX(dt) FROM ipdsc__v1)`
does NOT prune partitions — it scanned **164.9B rows / 85,043 slot-sec / 280s wall** for a single-day COUNT
(TI-1026). `WHERE dt = '2026-06-10'` prunes to one partition (~70-105M rows per data source). If you need the
latest date, probe it first (`SELECT DISTINCT dt ... WHERE dt >= recent ORDER BY dt DESC LIMIT 1`), then inline
the literal. Also prefer `APPROX_COUNT_DISTINCT(ip)` over exact `COUNT(DISTINCT ip)` on full-partition scans.

**⚠ Authoritative 3P segment SIZE table (use instead of ipdsc DISTINCT-IP):** `dw-main-bronze.external_ddm.data_source_category_sizes`
(`data_source_id`, `data_source_category_id`, `category_size`, partitioned y/m/d) — this is the per-segment size the **platform UI shows**
(matches buyer-quoted "15M/12M" numbers). **Access-gated** (GCS `gs://mntn-data-monitoring/audience-metrics/data-source-category-sizes/` —
`storage.objects.list` denied to malachi@; request **Storage Object Viewer**). Variants `_unfiltered` / `_dev` are also denied. When granted,
this replaces expensive ipdsc reach scans for sizing. (TI-1053.)

**⚠ `tpa.categories.path_from_root` has TWO formats — do NOT `COALESCE(path_from_root, names, name)` for name-matching.** ~Half of LiveRamp
(DS35) providers store `path_from_root` as a readable `"A > B > C"` string (HCS, Datasys, Clickagy, AtoZ, Start.io); the **other half store it
as an unreadable struct** `{"pathFromRoot":[0,124,...]}` (ZoomInfo, Anteriad/180byTwo, Alliant, LBDigital, OnAudience, NetWise, Skydeo, Audigent...
— i.e. most **premium B2B** providers). `COALESCE` returns the struct first for those → keyword regex matches nothing → **those providers are
silently dropped** (TI-1053: an ICP filter saw 7.7K of the real ~17K+ relevant; missed Edgar's hand-found segments and all premium B2B intent/role
inventory). **Fix:** regex on `CONCAT(IFNULL(path_from_root,''),' ',IFNULL(names,''),' ',IFNULL(name,''))`. Readable path also lives in `names`
(JSON `{"names":["ROOT", <provider>, <path>]}`) and usually in `name`; **provider = `names`[1]**.

**⚠ Wide-window DISTINCT-IP sizing is EXPENSIVE even when partition-pruned.** Sizing ~24 DS35 categories with
`COUNT(DISTINCT ip)` over a **30-day** window (`dt BETWEEN ... AND ...`, `data_source_id=35`, `UNNEST` + `element IN (...)`)
billed **~30.5 TB** (TI-1053). The hive partition (`dt`, `data_source_id`) prunes the day/DS folders, but DISTINCT-IP
across 30 daily partitions of the largest DS still reads enormous Parquet. To size 3P segments cheaply: use a **short
window (3-7 days)** and accept the burstiness undercount, or `APPROX_COUNT_DISTINCT`, or query a single known load-day
per segment. Do **not** run wide DISTINCT-IP scans casually. (A 3-day probe's perf-log showed `0.0 GB` but that was a
parse artifact — verify cost from the job stats, not a possibly-empty perf footer.)

**⚠ 3P (DS35 LiveRamp / bought) delivery into ipdsc is BURSTY — each category refreshes on only ~2-4 days/month**
(TI-1026, confirmed by adversarial validation). The SAME segment delivers millions of IPs on its load day and
**0 on every other day** (e.g. Stirista Fitness cat 1006088981: 2.1M on 2026-06-08, no row 2026-06-06; on any
given day 8-11 of a campaign's 11 segments deliver nothing). The zeros are NOT a partition artifact — DS35 carries
102-107M rows every day from *other* categories that loaded that day.
- **A single-day AND a single-WEEK reach number are both window-luck-dependent.** A 7-day window's 3P reach swung
  **3M → 19M** just by shifting the right edge one day (one mega-batch falls in or out). **Always measure 3P
  (DS35) category reach / exclusion footprint over a ≥30-day window**, and report each category's last-delivery dt.
- **Concrete error this caused (don't repeat):** a single-day query made 7 active LiveRamp income/age *exclusion*
  categories (matching tens of millions of IPs) look "inert/zero" because that day's rotating load didn't include
  them. Same for 6 "include" segments that looked dead but deliver 1.3M-6.7M IPs over 30 days.
- MNTN-internal sources (DS19 MNTN Matched, DS1 Oracle, etc.) behave differently: DS19 is stable daily; DS1/Oracle
  has *zero* ipdsc presence entirely (genuinely inert). Extends TI-999's "most named 3P providers have zero IPDSC volume."

---

## dw-main-silver.geo.* (MaxMind IP geolocation)
- **Type:** VIEWs over the MaxMind feed. Key tables: `maxmind_blocks_ipv4` (IPv4 CIDR block → lat/long),
  `network_locations` (CIDR → city/metro/region + lat/long), `maxmind_isp` (CIDR → ISP).
- **`maxmind_blocks_ipv4` columns:** `network` (CIDR string, e.g. `1.2.3.0/24`), `latitude`, `longitude`,
  `accuracy_radius`, `postal_code`, `is_anonymous_proxy`, `isp_id`, `domain`. ~4.46M US blocks.
- **Geo-fence reach pattern (TI-1026)** — count network blocks within R miles of a set of points (e.g. studio
  locations) via a spatial join (BQ optimizes `ST_DWITHIN`). Cheap (~0.3 GB):
```sql
WITH studios AS (SELECT lat, lon FROM UNNEST(<ARRAY<STRUCT<lat,lon>>>)),
us_blocks AS (
  SELECT network, latitude, longitude,
         POW(2, 32 - CAST(SPLIT(network,'/')[OFFSET(1)] AS INT64)) AS ip_capacity
  FROM `dw-main-silver.geo.maxmind_blocks_ipv4`
  WHERE latitude BETWEEN 18 AND 72 AND longitude BETWEEN -180 AND -65)  -- US incl AK/HI
SELECT COUNT(DISTINCT b.network)
FROM us_blocks b JOIN studios s
  ON ST_DWITHIN(ST_GEOGPOINT(b.longitude,b.latitude), ST_GEOGPOINT(s.lon,s.lat), 11265.4)  -- 7 mi in m
```
  **Block-count** is a decent population-density proxy (blocks cluster in cities); **IP-capacity** (sum of
  `2^(32-prefix)`) over-weights rural ranges, so it understates population coverage. Reference: a 946-studio ×
  7-mi fence covered 49.4% of US blocks but only 24.9% of IP capacity.

---

## bronze.external.TI_835_prospecting_scores
- **Type:** EXTERNAL TABLE (GCS-backed Parquet)
- **GCS path:** `gs://mntn-data-archive-dev/alex.knorr/TI_835_prospecting_scores/*.parquet`
- **Partition:** None (flat Parquet files)
- **Use for:** TI-835 incrementality pre-analysis scoring. Contains per-IP intent group assignments
  and household scores for 10 advertisers across 8 verticals. Created by Alex Knorr from
  Databricks pre-analysis (SteelHouse/databricks_targeting, branch TI-835).

| Column | Type | Notes |
|--------|------|-------|
| company_name | STRING | Advertiser name |
| advertiser_id | INTEGER | FK → core.advertisers |
| campaign_id | INTEGER | FK → core.campaigns |
| ip | STRING | IP address |
| intent_group | STRING | Intent tier assignment (High, Peak, Mid, Max Reach) |
| household_score | INTEGER | Fangorn household score |

**Query tip:** Join on `advertiser_id` + `ip` to link back to impression/visit tables.
Source scoring pipeline: `gs://household-scoring-prod/output/scoring/prospecting_intent/` (daily, 35-day retention).

---

## Production Fangorn score sources (per-IP) — mapped (BQ) vs raw (GCS)
Where the live Fangorn intent scores actually live. **Two scales:** RAW `model_score` ∈ [0,1] (the model output; raw>0.8 ≈ High-Intent threshold) and MAPPED `household_score` ∈ [0,10000] (the bidder scale; 8001+ HI, 6666–8000 PP, 3333–6665 MI, ≤3332 MR).

| Source | Type | Grain / scale | Notes |
|---|---|---|---|
| `bronze.external.household_scoring__prospecting_intent__v1` | EXTERNAL (GCS Parquet) | per-IP × advertiser/campaign; **mapped** `household_score` 0–10000 | Partitions `year`,`month`,`day` are **STRING** — a `year='2026'` filter does **NOT** prune (sweeps the whole year = very slow/expensive). **Don't scan per-advertiser.** Cols: ip, advertiser_id, campaign_group_id, campaign_id, household_score. Sibling: `household_scoring__advertiser_intent__v1` (advertiser-intent variant), `..__prospecting_intent__dniehoff` (dev). |
| `gs://mntn-data-archive-prod/fangorn_14day_lookback_vertical/dt=<YYYY-MM-DD>` | GCS Parquet (Spark) | per-IP × **vertical_id**; **raw** `model_score` 0–1 | The snapshot the **rollout-priority scorer** reads. Read via Spark/Databricks `parquet.\`gs://…/dt=<snap>\``. 14-day lookback. |
| `gs://mntn-data-archive-prod/vertical_categorizations/ip_vertical_associations/dt=<YYYY-MM-DD>` | GCS Parquet (Spark) | IP ↔ `data_source_category_id` (vertical_id) | Which IPs are associated with each vertical (used to compute a vertical's `assoc_median_fangorn_score` / high-mid ratio). |
| `bronze.external.camperbid_prod__intent_score__{intent_score,prospecting_intent,advertiser_intent,…}` | EXTERNAL | per-IP camperbid intent scores | Bidder-side intent-score externals; same family. |
| `bronze.external.fangorn_score_monitor` | EXTERNAL | score-distribution monitor | Daily Fangorn score distribution (TI-849), not per-IP joins. |

**For "if reactivated, where would advertiser X rank?"** the priority scorer's Score-Opportunity / Size-Stability / HHST-relief are computed at the **vertical** level — so read X's vertical row from the per-vertical aggregate, not a per-advertiser IP scan. Canonical scorer + weights: see `data_knowledge.md` §"Canonical rollout-priority scorer". Authoritative Fangorn inclusion/tier is Postgres `tpa.fangorn_advertiser_inclusion` (no BQ mirror).

## silver.archives.household_score_threshold_archives
- **Type:** TABLE (archive / change-history of HHST settings; the time-series counterpart to the current-state `dso.household_score_thresholds`).
- **Use for:** HHST trajectory over time — one row per threshold change. Cols: advertiser_id, campaign_id, campaign_group_id, threshold, update_time. The Fangorn rollout scorer derives each campaign's **lowest *sustained* HHST** (held ≥60 min, `threshold>0`) from this table over an analysis window. An advertiser with no `threshold>0` rows in-window has never run a scored campaign there (e.g. geo-only/Select-Winback advertisers).

---

# bronze.tpa

**Project:** dw-main-bronze | **Dataset:** tpa
Tables related to Third Party Audience (TPA) uploads and management.

---

## bronze.tpa.audience_upload_hashed_emails
- **Type:** TABLE
- **Use for:** Hashed emails (HEMs) uploaded by advertisers for CRM targeting. One row per HEM
  per audience_upload_id. The email is stored in three case variants (UPPERCASE, LOWERCASE, ORIGINAL).

| Column | Type | Notes |
|--------|------|-------|
| audience_upload_id | INTEGER | PK / FK → integrationprod.audience_uploads |
| hashed_email | STRING | SHA256 hashed email |
| pre_hash_case | STRING | 'UPPERCASE', 'LOWERCASE', or 'ORIGINAL' — filter on 'UPPERCASE' to count unique emails |
| update_time | TIMESTAMP | When the row was ingested |

**Query tip:** Always filter `pre_hash_case = 'UPPERCASE'` when counting distinct emails — otherwise
you'll triple-count each email.

**Empty HEM hash:** `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` = SHA256 of empty string.
Exclude this value when counting qualifying HEMs.

---

## bronze.tpa.audience_upload_ips
- **Type:** TABLE
- **Use for:** IPs directly uploaded by advertisers (NOT for email-based CRM uploads).
- **NOTE:** This table is **empty for email-based uploads.** Victor Savitskiy confirmed: for email
  uploads, the HEM → IP resolution happens in the identity graph and lands in `ipdsc__v1`.
  Only populated when an advertiser directly uploads an IP list.

---

# bronze.integrationprod.audience_uploads (addendum)
- **Type:** TABLE (Postgres replica)
- **Use for:** Metadata for CRM upload batches — name, entry count, match rate, data_source_category_id.

| Column | Type | Notes |
|--------|------|-------|
| audience_upload_id | INTEGER | PK |
| advertiser_id | INTEGER | |
| data_source_category_id | INTEGER | = audience_upload_id (same value) |
| name | STRING | Upload name (often includes geographic partition info, e.g. "TX test", "FL control") |
| entry_count | INTEGER | Number of emails in this upload file |
| match_rate | FLOAT | Fraction of emails that resolved to IPs (typically 0.61–0.63) |
| update_time | TIMESTAMP | |

**IP estimate:** `match_rate * entry_count` approximates IP count. Use ipdsc__v1 for exact count.
**Geographic partitions:** Advertisers often split uploads by state group (10 geo partitions common for national campaigns).

---

## bronze.raw.tpa_membership_update_log (full entry)
- **Type:** VIEW (in bronze.raw) → physical table in `bronze.sqlmesh__raw`
- **Partition:** `dt` (STRING 'YYYY-MM-DD') + `hh` (STRING, zero-padded hour, e.g. '00'–'23')
- **Data available from:** 2025-11-21
- **Use for:** Change log of IP segment membership (when IPs enter/leave segments). Complements
  tmul_daily but goes back further and has finer-grained change events.
- **Data sources:** DS 2 and DS 3. DS 4 (CRM) not confirmed to appear here.

| Column | Type | Notes |
|--------|------|-------|
| id / ip | STRING | IP address |
| data_source_id | INTEGER | |
| in_segments | RECORD | Segments IP joined |
| out_segments | RECORD | Segments IP left |
| dt | STRING | Partition date (always filter this!) |
| hh | STRING | Partition hour (zero-padded, e.g. '08') |

**Unnest pattern (DIFFERENT from tmul_daily):**
```sql
-- tpa_membership_update_log: use .segments not .list, and no .element wrapper
UNNEST(td.in_segments.segments) AS isl → isl.segment_id  (direct access)

-- tmul_daily: uses .list and .element wrapper
UNNEST(td.in_segments.list) AS isl → isl.element.segment_id
```

---

# audit (BQ dataset)

## audit.vv_ip_lineage
- **Type:** TABLE (production audit table, TI-650)
- **Partition:** `trace_date` (DATE)
- **Clustering:** `advertiser_id`, `vv_stage`
- **Use for:** IP lineage trace for ALL verified visits (S1/S2/S3) — maps each VV back to its S1 originating bid IP through the full S3→S2→S1 funnel chain. Enables NTB validation and general VV auditability.
- **Architecture:** v12 target — one row per VV. Stage-based column naming (`s3_*`/`s2_*`/`s1_*`). 2-link S1 resolution (`imp_direct` + `imp_visit`). 90-day lookback (120d production default for WGU outlier).
- **Resolution rate:** 99.83% (validated on 20 advertisers / 225,872 VVs and 10 advertisers / 138,557 VVs)

Key column groups (full schema: `tickets/ti_650_stage_3_vv_audit/artifacts/ti_650_column_reference.md`):
- `ad_served_id` — PK. UUID linking clickpass_log, event_log, CIL, ui_visits.
- `vv_stage` — 1/2/3 (from `campaigns.funnel_level`)
- `s3_bid_ip`, `s3_vast_start_ip`, `s3_vast_impression_ip`, `s3_serve_ip`, `s3_win_ip` — S3 impression IPs (NULL for S1/S2 VVs)
- `s2_bid_ip`, `s2_vast_start_ip`, `s2_ad_served_id`, `s2_vv_time` — S2 impression IPs + VV details (NULL for S1 VVs)
- `s1_bid_ip`, `s1_vast_start_ip`, `s1_ad_served_id`, `s1_resolution_method` — S1 impression IPs + resolution method
- `visit_ip`, `impression_ip`, `redirect_ip` — VV visit IPs
- `clickpass_is_new`, `visit_is_new`, `is_cross_device` — classification
- `trace_date` — partition key (`DATE(vv_time)`)

---

# silver.fpa

**Project:** dw-main-silver | **Dataset:** fpa
Tables in this dataset are VIEWs over `bronze.integrationprod.fpa_*` (Datastream CDC from Postgres).

---

## silver.fpa.advertiser_verticals
- **Type:** VIEW → `bronze.integrationprod.fpa_advertiser_verticals`
- **Clustering:** id
- **Rows:** ~39,946 (as of 2026-03-12)
- **Use for:** Mapping advertisers to vertical categories (industry classification)
- **Validated:** TI-737 (2026-03-12) — full parity with CoreDW confirmed

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER | PK (clustered) |
| advertiser_id | INTEGER | FK to advertisers |
| advertiser_name | STRING | **UNRELIABLE — do not use.** Denormalized, write-once, never updated. See gotchas. |
| vertical_id | INTEGER | Vertical category ID |
| vertical_name | STRING | Denormalized vertical name |
| type | INTEGER | 0 = parent vertical, 1 = sub-vertical |
| created_time | TIMESTAMP | Row creation time |
| updated_time | TIMESTAMP | Last update (nearly all NULL) |
| datastream_metadata | RECORD | CDC metadata (uuid, source_timestamp) |

**Key facts:**
- **This is the source of truth for an advertiser's vertical** — the Fangorn rollout scorer joins `type=1` (sub-vertical) as `vertical_id`. `advertisers.advertiser_vertical_id` is frequently NULL even when a vertical exists here, so don't use it (e.g. iMemories 37423: advertiser_vertical_id NULL, but here = sub-vertical 116001 "Gifts & Specialty Stores" / parent 116 "Gifts"). The `type=1` vertical_id also matches the `score.types[].id` (RTC vertical) in the audience expression.
- Every advertiser has exactly 2 rows: type=0 (parent) + type=1 (sub-vertical)
- 185 distinct verticals, 184 distinct names (3 parent/child pairs share names)
- 49 advertiser_ids are orphans (not in advertisers table) — pre-existing source issue
- Join to advertisers: `advertiser_id = advertisers.advertiser_id`

**GOTCHA — `advertiser_name` is unreliable (TI-737, 2026-03-16):**
- Write-once, never updated (only 2 of ~40k rows have ever been updated)
- **Empty name regression:** Starting 2025-12-23, 79–82% of new advertisers inserted with empty string. 4,366 advertisers affected.
- **Stale names:** Even when populated, 1,114 of 16,000 (7%) differ from current `advertisers.company_name` because customers edited their name after the FPA row was created.
- **Always JOIN to `integrationprod.advertisers.company_name`** (or `public_advertisers.company_name`) for the authoritative, current advertiser name.

---

## silver.fpa.categories
- **Type:** VIEW → `bronze.integrationprod.fpa_categories`
- **Use for:** FPA category taxonomy (NOT the verticals lookup — different domain)

| Column | Type |
|--------|------|
| data_source_id | INTEGER |
| data_source_category_id | INTEGER |
| parent_id | INTEGER |
| partner_id | INTEGER |
| name | STRING |
| description | STRING |
| path | STRING |
| names | STRING |
| path_from_root | STRING |
| is_leaf_node | BOOLEAN |
| navigation_only | BOOLEAN |
| advertiser_id | INTEGER |
| deprecated | BOOLEAN |
| public | BOOLEAN |
| sort_order | INTEGER |
| created_date | DATE |
| updated_date | DATE |
| mntn_id | INTEGER |
| mntn_id_type | INTEGER |
| path_from_root_types | STRING |
| datastream_metadata | RECORD |

---

# Greenplum (coreDW) Tables Reference
**Note:** These tables exist in Greenplum/PostgreSQL coreDW, not directly in BigQuery.
**coreDW deprecation date: April 30, 2026.**

| Table | Schema | Purpose | Key Columns |
|-------|--------|---------|-------------|
| `sum_by_campaign_group_by_day` | summarydata | Daily pre-aggregated metrics by campaign group | advertiser_id, campaign_group_id, date, impressions, visits, conversions |
| `v_campaign_group_segment_history` | summarydata | VIEW — segment history per campaign group | campaign_group_id, segment history |
| `valid_campaign_groups` | dso | Active/valid campaign groups for DSO analysis | campaign_group_id |
| `advertiser_verticals` | fpa | Advertiser → vertical mapping | advertiser_id, vertical_id, type (1=primary) |
| `advertiser_settings` | r2 | Advertiser-level reporting settings | advertiser_id, reporting_style ('last_touch', etc.) |
| `campaign_segment_history` | audience | Campaign segment change history (CONTAMINATED — mixes template + targeting objects) | campaign_id, segment history |
| `audience_segment_campaigns` | audience | Maps audience segment → campaign (**1:1 with campaign_id**, NOT campaign_group). Contains audience expression JSON. Filter `expression_type_id = 2 AND is_targeted = TRUE` for actively-targeted audiences (type 1 is OPM source representation; gets wrapped into type 2 at evaluation, never `is_targeted=TRUE` for retargeting — verified empirically TI-837 2026-04-30: 0/64,202 type=1 retargeting rows are targeted). Expression has 4 AND clauses: selects, categories (DS19/CRM/lookbacks), geos, holdout/buckets. Holdout hash: `MD5('{AID}:{IP}')` mod 1000, 0-99 = holdout. | campaign_id, audience_segment_id, expression, expression_type, is_targeted |
| `dim_vertical` | tpa (coredb) | Vertical dimension lookup — PK is `vertical_id`, includes bucket rollup. Created by Ryan Kleck as a convenience replacement for querying `fpa.advertiser_verticals` for vertical/bucket info. | vertical_id, bucket_id, vertical_name, bucket_name, vertical_bucket_name, verticals_in_bucket |
| `membership_updates_logs` | tpa | TPA membership update log (Greenplum version) | ip, segment_id, update_time |
| `advertisers` | public | Advertiser dimension table (Greenplum version of bronze.integrationprod.advertisers) | advertiser_id, name, deleted, is_test |
| `data_sources` | audience | Data source registry | data_source_id, name, data_source_type_id |
| `locations` | geo | Geo location reference | location_id, state/country names |
| `cost_impression_log` | logdata | Won impressions with cost — `model_params` key=value string | ip (TEXT), model_params, impression_id |
| `impression_log` | logdata | All bid attempts (won + lost) | ip (INET), ip_raw, bid_ip, original_ip, campaign_id |
| `ui_visits` | summarydata | Verified visits — `ip` is INET type; use `host(ip)` to strip /32 | ip (INET), impression_time, is_new |
| `ui_conversions` | summarydata | Conversions — use `order_amt`, NOT `order_amt_usd` (which is NULL) | order_amt, advertiser_id |

<!-- slack-extracted: 2026-04-08-full -->
- ### `tpa.dim_vertical` (CoreDB)

A dimension table with a primary key of `vertical_id`, created to simplify lookups that previously required querying `fpa.advertiser_verticals`.

**Schema:**
- `vertical_id` (integer, PK) — advertiser vertical ID (type=1 from `fpa.advertiser_verticals`)
- `bucket_id` (integer) — bucket vertical ID (type=0 from `fpa.advertiser_verticals`)
- `vertical_name` (varchar 512)
- `bucket_name` (varchar 512)
- `vertical_bucket_name` (text) — concatenation: `bucket_name || ' > ' || vertical_name`
- `verticals_in_bucket` (integer) — count of type=1 verticals sharing the same 3-char ID prefix as this bucket
- `created_at`, `updated_at` (timestamp)

**Use for:** Vertical dimension lookups. Prefer over querying `fpa.advertiser_verticals` directly when you need a single row per vertical with bucket context.
- ### `ui.audience_keyword_state` (CoreDB)

Stores the parent and child keyword associations for a given audience ID. This is the preferred table for looking up which keywords are applied to an audience, as an alternative to parsing the JSON expression blob in `audience.audiences`.

**Key columns:**
- `keyword_type` — either `PARENT` (keywords shown in the UI audience builder) or `CHILD` (keywords grouped under a parent)
- `selected` (boolean) — CHILD keywords where `selected = true` are the ones included in the audience expression JSON blob used by DataSource 19

**Use for:** Identifying which keywords (including AI Recommended Attributes) are associated with an audience. (It IS queryable in BigQuery at `dw-main-silver.ui.audience_keyword_state`.)
- More columns: `keyword` (text), `data_source_category_id`, `is_custom` (customer-entered vs system), `is_magic` (LLM/"magic" auto-expansion), `is_hidden`, `expanded_from` (JSON — the PARENT seed this CHILD was expanded from), `model_version`.

**⚠ "I see keywords in the expression I can't find in the UI" — RESOLVED (TI-1026, confirmed by Alex Knorr 2026-06-17).**
The UI audience builder shows **only PARENT keywords** (the customer's seed terms). The **DS19 expression keywords are the
selected CHILD keywords** — MNTN-Matched's auto-expansion of those seeds — and are NOT individually searchable in the UI
(they're grouped/nested under parents). So the expression's DS19 set ≠ what the UI lists.
- Example (OTF, audience 34668): 233 selected PARENT (UI-visible seeds) vs 374 selected CHILD; of the children, **22 are
  `is_magic` (UNTARGETABLE) and 352 are real targetable DS19 keywords**.

**How MNTN Matched (non-BUK) generates keywords (Alex Knorr 2026-06-17, deck slide-3 flow):** LLM scrapes the
advertiser site → describes products/services → describes interested customers → generates **20 search-term keywords
= the PARENT keywords shown in the UI**. Under the hood: LLM generates 10 products/services per parent (≈200) → those
are **embedding-matched into the DS19 universe → N CHILD keywords** that actually target. So: **20 parents → ~200 →
N DS19 children**. The off-target drift comes from the embedding-match step.

**⚠ `is_magic` keywords are UNTARGETABLE (Alex 2026-06-17; confirm exact mechanic with Mike Dolt).** They appear in the
DS19 expression but are a UI construction that exists so the displayed **audience size changes when the customer edits
the audience** — they do NOT target anyone. **Do not count `is_magic` keywords as off-target targeting.** The flashy junk
(Beer Mugs, Compact Suv, Above Ground Pools, Coffee Grinders, Montessori, Butter, Motorcycle Lighting, Arcade) are all
`is_magic` → untargetable. The REAL targetable off-target keywords are the **non-magic** embedding-match drift —
e.g. "Antifreeze" ← "Cold Plunge", "CPUs" ← "Solidcore", "Abrasives" ← "Polar H10" (OTF: 76 real off-target of 352
targetable children).
- To separate: `selected=true AND keyword_type='CHILD' AND is_magic=false` = what actually targets (the DS19 set minus
  magic). `keyword_type='PARENT'` = the 20 UI/seed keywords. Cross-check parents + BUK recs at the shopper-graph
  autopilot endpoint: `https://shopper-graph.in.mountain.com/autopilot?advertiser_id=<id>` (VPN-only, per Alex).
- ### `ui.audience_x_marketing_objective` (CoreDB / intprod)

Mapping table that associates audience segments with a campaign's marketing objective (strategy), formerly called `objective_id` on `campaign_groups`. Used to enforce alignment between audience segment types and the campaign strategy (Retargeting vs. Prospecting).

**Context:** When the Campaign Strategy feature was introduced (~2025), customers were prompted to select a strategy, which locked down audience editing until they complied. Adoption was never forced — legacy audiences that were never updated can still exist on live campaigns with a strategy mismatch. If a customer edits an audience, the strategy is locked to match the campaign's strategy. An audience attached to multiple campaigns can become misaligned with one of them.

<!-- slack-extracted: 2026-04-08-review -->
- ### `archives.advertiser_archives` (CoreDB)

Tracks all database-level changes to advertiser records, including the `is_test` flag and other account-level fields.

**Key columns:**
- `version` — incrementing version number per change
- `update_time` — timestamp of the change
- `user_id` — the internal MNTN user who made the change (populated by the UI layer; may be inaccurate if the change was made directly via database connection, since shared DB credentials are used)
- Various advertiser fields including `is_test`

**Join:** `JOIN users u USING (user_id)` to get `email_addr` of the actor.

**Gotcha:** If a change is made directly to the database (not through the UI), `user_id` will not be reliably set — it will retain the prior value. Only UI-originated changes have accurate `user_id` attribution. Analogous tables exist for other entities (e.g., `archives.campaign_groups_archives`).

<!-- slack-extracted: 2026-04-16 -->
- **`logdata.icloud_ipv4_ips` — CoreDW External Table (Legacy):** A foreign table exists in CoreDW under `logdata.icloud_ipv4_ips` sourcing from a GCS bucket at `pxf://mntn-data-curated-prod/icloud_ips/all_ips/` (Parquet format). The table contains a single column: `ip_address` (text). No active usage was found in the GitHub codebase as of the migration audit.

**Purpose:** Contains Apple iCloud Private Relay IP addresses. Used by the Targeting team to exclude these IPs from bidding so they never enter MembershipDB.

**BQ equivalents:**
- Raw model: `dw-main-bronze.sqlmesh__raw.raw__icloud_ips_raw` (SQLMesh Python model sourcing from the same GCS bucket)
- Migrated table: `dw-main-silver.summarydata.icloud_ipv4` (the migrated version of the related `summarydata.icloud_ipv4` table)

**Note:** `summarydata.icloud_ipv4` in CoreDW has ~120,550 rows; BQ silver has ~120,482 rows — minor row count discrepancy exists. The `ipdsc` job depends on this table and is being updated to point to the BQ version. (via Ryan Kleck, #chapter-data-engineering, 2026-04-01)
- **`geo.locations` / `geo.location_data` — Table Lineage and Migration Status:**
- `geo.locations` on CoreDW is a **view** built on top of `location_data` and `location_counts`. `location_counts` has been deprecated but was not cleaned up from the view definition.
- `geo.locations` in intprod is replicated from CoreDW (row counts match).
- The BQ replacement table is `geo.location_data`. The BQ geo pipeline (originally owned by Sheetal, now Nivas) is **not yet complete**.
- `dw-main-bronze.analytics_curated.geo_location_data` is the current CoreDW copy in BQ — use this for production queries until the native BQ geo pipeline is finished.
- **Do NOT replicate `geo.locations` from intprod to BQ** — this would be circular. The source of truth for geo data is the BQ geo pipeline.
- `geo.locations.location_id` is definitionally unique and can be promoted to a primary key if Datastream replication is needed.

**`tpa.categories` is where 3P / LiveRamp (DS35) segment NAMES live — NOT `silver.fpa.categories`** (verified TI-1037 2026-06-23). `fpa.categories` only carries DS13/14/16/21; querying it for DS35 ids returns empty. Use `dw-main-bronze.tpa.categories` filtered to `data_source_id=35` for LiveRamp segment metadata: `data_source_category_id`, `name`, `path_from_root`, `partner_id`, `public`, `navigation_only`, `is_leaf_node`, `deprecated`, `created_date`/`updated_date`.
- **DS35 = "LiveRamp IP"** (`integrationprod.data_sources`). LiveRamp is an aggregator/marketplace — `partner_id` is the underlying provider (one advertiser's 24-segment set spanned **9** distinct partners: demographic, behavioral, interest brands). So "LiveRamp segments" = the source/pipe, not a single provider.
- **`deprecated=TRUE` does NOT mean "delivers nothing."** Deprecated DS35 segments still delivered 1–51M IPs/30d (same as the TI-1026 Epsilon finding). It means catalog-retired / no longer refreshed — cross-check ipdsc reach before calling a segment dead; a live equivalent usually exists.
- Segment names are NOT in `eval_batch`/ipdsc; this table (or the VPN audience-service catalog) is the resolution path.

**`tpa.categories` (pipeline):** Already exists as a view in bronze (`dw-main-bronze.tpa.categories`). It is sourced from BQ/SQLMesh — not from intprod. Do not add it to Datastream replication from intprod as that would be circular. (via Dustin Niehoff, #data-platform, 2026-04-01)

<!-- slack-extracted: 2026-04-17 -->
- **summarydata.offline_conversions — Migrated to BigQuery**

The `summarydata.offline_conversions` table has been cut over from CoreDW (Postgres) to BigQuery. The CoreDW version is stale (last record: 2026-03-02, last run: 2026-03-04). The active, up-to-date table is in BigQuery (`summarydata.offline_conversions` in BQ), with data current as of mid-April 2026 (last record: 2026-04-14, last run: 2026-04-16). Always query the BigQuery version for offline conversion data. (via Lilit, #reporting_helpdesk_ask_anything, 2026-04-16)
- **dw-main-gold.ddm.audit_45_augmentor_log_summary and audit_128_augmentor_segment_summary — Augmentor Audit Tables**

Two BigQuery audit tables exist for monitoring augmentor pipeline health:
- `dw-main-gold.ddm.audit_45_augmentor_log_summary` — tracks augmentor log counts by status (200, 204, etc.). Confirmed to match `bronze.raw.augmentor_log` row counts exactly.
- `dw-main-gold.ddm.audit_128_augmentor_segment_summary` — tracks segment-level augmentor data.

These tables are used by Mission Control system metrics. Note: The system metrics dashboard was previously pulling from DrMon (which had reliability issues); migration to pull directly from BigQuery was in progress as of April 2026. (via Pratik, #mission-control, 2026-04-16)

<!-- slack-extracted: 2026-04-21 -->
- **fpa.mm_domain_map — Purpose and Known Data Quality Issue:** `fpa.mm_domain_map` is the shopper graph domain table used for "hoteling" — mapping multiple advertiser IDs that share the same domain (e.g., franchise brands like Orange Theory) to a single root/parent advertiser ID. When an advertiser_id is passed with a domain_name that matches an entry in this table, the system uses the root advertiser's autopilot profile rather than generating a new one. **Known data quality issue (2026-04-20):** A query revealed ~561 rows where the domain in `mm_domain_map` does not match the `company_url` in `public.advertisers` for the same advertiser_id — indicating mismatched or incorrect domain mappings. This causes the autopilot profile regeneration UI to fail with an error like "Advertiser ID X does not match root advertiser ID Y for domain Z." Workaround: delete the incorrect row from `mm_domain_map` and re-trigger regeneration with `?override=true`. Diagnostic query: `SELECT dd.advertiser_id, pa.advertiser_id, dd.domain, pa.company_url FROM fpa.mm_domain_map dd JOIN public.advertisers pa ON dd.advertiser_id = pa.advertiser_id AND lower(pa.company_url) NOT LIKE CONCAT('%', lower(dd.domain), '%') ORDER BY dd.advertiser_id DESC;` (via Ryan Kleck, #targeting_helpdesk_ask_anything, 2026-04-20)
- **core.campaign_group_day_parts — Schema and Timezone:** `core.campaign_group_day_parts` is a CoreDW/PRO table (owned by the PRO/application team, not DPLAT). The `exclude_begin_hour` and `exclude_end_hour` columns are stored in **UTC**. The table is populated via Gary (not a formal pipeline). As of 2026-04-20, a replication from IntegrationProd to CoreDW was being set up via Bucardo; this required dropping and recreating the CoreDW table to resolve DDL mismatches between environments. The table had ~3K rows in IntProd and ~2K rows in CoreDW (stale) prior to the fix. (via Tom Manuel, #chapter-data-engineering, 2026-04-20)
- **tpa.categories — Population Source:** `tpa.categories` (in BigQuery, `dw-main-bronze`) is populated via a SQLMesh model at `models/dw-main-bronze/tpa/categories.sql`. It is NOT populated from `tpa.stg_categories` or `lds.ext_tpa_categories` in BigQuery — the `db_repo` `populate_tpa_categories.sql` function is a legacy Postgres path and does not reflect the current BigQuery pipeline. DS42 = MNTN Select is already present in the table. DS9 = "targetable campaigns" (the category used for select campaign targeting). (via Ryan Kleck, #targeting-squad, 2026-04-20)

<!-- slack-extracted: 2026-04-24 -->
- ### ScyllaDB — partner_sync_by_advertiser_v3

- **Table:** `partner_sync_by_advertiser_v3` in ScyllaDB
- **Purpose:** Supports VV (Verified Visit) generation and cross-device GA3 request enrichment in `analytics_request_log`. When this table is unavailable, VV generation is impaired and GA3 volume in `analytics_request_log` drops.
- **Incident note (2026-04-22):** Table was unintentionally truncated between 18:00–19:00 UTC. Hypothesized cause: operator confusion with a similarly named table (`partner_sync`) that had been recommended for truncation on 2026-04-16. ScyllaDB support restored from daily snapshot; restore was throttled to avoid impacting other writes and took approximately one overnight window.
- **Gotcha:** This table is distinct from `partner_sync`. Truncating the wrong one (by_advertiser_v3 vs. base) has significant downstream impact on VV counts and analytics request volume.
- **Note:** Scylla support requires a ticket to perform truncation — unilateral truncation by internal teams should be treated as an incident. (via Sharad, #engineering-team, 2026-04-23)
- ### Bidder serving datastores (non-BQ) — Aerospike + ScyllaDB raw wins (Abbas, 2026-06-09)

Not BigQuery tables — these are the bidder team's serving stores. Cataloged here for lineage. Full architecture: `knowledge/data_knowledge.md` § "Bidder System Design & Caching Architecture". Source: TI-1016 `ti_1016_02_abbas_bidder_sys_design_caching_2026_06_09.txt`.

- **Aerospike — household profile:** the bid-time membership cache. **Primary key = IP address**; value record holds `segments`, intent scores, segment scores, geo version, and holdout IPs. ~300M IP keys, 3–5 TB, single-digit-ms latency, hit on every bid. Populated by the **membership consumer** (GCS→PubSub→RabbitMQ→Aerospike) for household-profile data, and by **Python cache loaders** (CoreDB/BigQuery→Aerospike/Redis) for other data. **Lineage implication:** bid-side BQ tables (`bidder_bid_events`, etc.) do NOT carry segments/scores because those are read from Aerospike at bid time, not logged into the event. Scores' durable system-of-record is **GCS**, not MembershipDB.
- **Aerospike `rtb` namespace — set schemas + access (Confluence "Aerospike Datastore", BP space):** the bidder/PER squad's serving sets. Access via the `aql` CLI (creds in the "camperbid" 1Password group; requires Tailscale/VPN; clusters `prod_east` 10.39.2.5 / `prod_west` 10.41.2.5 / `dev` 10.51.2.5, plus `*_gcp` per-* variants). Ex: `aql --instance prod_west_gcp --timeout=30000`, then `set output json`.
  - **`rtb.household-profile`** — PK = **IP address**. Bins: `segments` (array of segment ids), `holdout_cids` (array of campaign ids this IP is held out from), `geo_version` (epoch string), `hhs:campaign` (map `campaign_id → household_score`), `hhs:advertiser` (map `advertiser_id → score`, often empty), `timestamp` + `hhs:timestamp` (**microseconds**, 16-digit). **Household score is stored per-campaign and per-advertiser as maps, not a single value** — matches the three-score reality (general / per-advertiser). Query: `select * from rtb.household-profile where PK = '73.184.234.107'`.
  - **`rtb.spend`** — PK = `flight_id=<id>` (lifetime) or `flight_id=<id>:<YYYYMMDD>` (daily). Bins: `spend` and `count`, each a map keyed at `campaign_group_id=`, `campaign_id=`, and `campaign_id=…:term_id=` (term/keyword) granularity. `spend` appears to be **micro-dollars** (inferred: a CG with 16,573,741,675 over 2,271,721 wins ≈ $7.30 CPM) — verify before using. Written by the win-aggregator from the spend pipeline; read for IHP pacing.
  - **`rtb.price`** — `avg_cpi` map keyed by ad size `width:height:duration` (e.g. `"1080:1920:15": 709`). The serving copy of `summarydata.publisher_adsize_metrics`.
  - **`rtb.recency`** — per-IP `vast` + `page_view` sub-maps (last-visit epochs by AID).
- **ScyllaDB — raw wins (spend pipeline):** the **Notification service** (HTTP webhook hit by SSPs/Beeswax on win) writes **raw wins** into a ScyllaDB cluster used for **deduplication** (each win processed once → no double-counted spend). Downstream via **Kafka** into 3 aggregators — frequency, spend, and a **logs aggregator that writes to GCS** for downstream teams (the upstream of BQ spend/win tables). Whole pipeline ≈ 1 minute. Aggregators currently write Aerospike (→ Redis soon). Distinct from the `partner_sync_by_advertiser_v3` ScyllaDB use above.
- **Migration note:** the bidder is moving **Aerospike → ScyllaDB** + Redis (cost / support). Treat Aerospike references as current-state, Scylla as future-state.
- ### Bidder price + threshold tables (Confluence BP "Bidder" page, 2026-06-09)

The DW tables the bidder reads to set bid price and evaluate eligibility. Canonical source: [Bidder (BP space)](https://mntn.atlassian.net/wiki/spaces/BP/pages/1860010029/Bidder) → `documentation/docs/bidder_platform_confluence_reference.pdf`. Architecture detail in `knowledge/data_knowledge.md` § "Bidder — CANONICAL reference".

- **`summarydata.publisher_adsize_metrics`** — the bid-price source. Avg **CPI** = avg of win prices over the **last 3 days**, per publisher × ad size (`site, width, height, duration`). Columns: `site, width, height, duration, avg_cpi, min_cpi, max_cpi, viewability_rate, score`. `avg_cpi` drives bid price (vs Publisher Price Threshold); `score` = publisher performance score. No price for an ad size → fall back to avg CPM for that ad size across all publishers.
- **`logdata.publisher_adsize_metrics`** — same column shape, used for the **viewability** check (`viewability_rate` vs Viewability Score Threshold). Distinct from the `summarydata` view (price). Don't conflate the two.
- **`sync.creative_metadata`** — per-creative/campaign bidder config. `pace_multiplier` (numeric, default 1; ×base price → final bid price; updated by DCO). Threshold columns: `recency_threshold`, `recency_floor_threshold`, `household_score_threshold` (HHST), `viewability_score_threshold`, `publisher_price_threshold`. **Null OR zero → threshold not evaluated.**
- **`dso.*` threshold tables:** `dso.recency_score_thresholds`, `dso.household_score_thresholds` (HHST; IP score value source = `tpa`), `dso.viewability_score_thresholds`, `dso.cpm_thresholds` (publisher price), `dso.publisher_performance_thresholds` + `dso.network_performance_threshold`.
- **Recency:** epoch of last visit to the campaign's AID, source `vast_impression` / guidv2 Kafka stream. Eligible iff `recency_floor_threshold < recency_duration < recency_threshold`. Recency Threshold = MAX age; Recency Floor = MIN age. UTC unless stated.
- ### Bidder event logs — GCS buckets + BQ lineage (Confluence BP, 2026-06-09)

- **Auction logs** (fka "augmentor" logs) → `bidder-auction-events-prod-{east,west}` → BQ **`bidder_auction_events`**.
- **Bid logs** (fka "bid price logs" / "BPL") → `bidder-bid-events-prod-{east,west}` → BQ **`bidder_bid_events`** / `bid_events_log` / `bid_attempted_log`.
- **Win notifications** → `bidder-win-notifications-{dev,prod}-central` (NURL via HTTP from SSPs/Beeswax).
- Beeswax-era logs live under `/topics/rtb-bid-events/` and `/topics/rtb-bid-price-events/`; MNTN-Bidder-era logs under `/v2/`. Example: `gs://bidder-bid-events-prod-east/v2/2026-05-11/11/`.
- Wins are written raw to **`rtb.wins` (ScyllaDB)** by the notification service (dedup), then CDC→Kafka→win-aggregator writes win logs to GCS. So GCS win logs are the upstream of any BQ win/spend table.
- ### audience.advertiser_configurations — vertical_data_source column

- **Table:** `audience.advertiser_configurations` (Postgres/coredw, replicated to reportingprod and archive)
- **Column:** `vertical_data_source` (INTEGER, nullable)
- **Purpose:** Specifies a datasource ID to replace DS 13 (MNTNVerticalCategorization) during segment breakdown for a given advertiser. `NULL` = no replacement (default behavior).
- **Added:** 2026-04-23 (DPLAT-969), deployed to QA then prod; replicated to coredw, reportingprod, and archive.
- **Context:** Added to support audience expression customization work (AUD-5301), specifically to allow per-advertiser override of the vertical categorization datasource. (via Jaime Mutale, #data-platform, 2026-04-23)

<!-- slack-extracted: 2026-04-28 -->
- ## graph.visits vs graph.sitevisitors (Reporting Metrics)

- **`graph.visits`**: Event count of Verified Visits — the correct metric for tracking Verified Visit volume.
- **`graph.SiteVisitors`**: Unique users (typically by IP) who had a Verified Visit. Despite the name, this is a unique-user metric, not a visit count. Generally not the right choice when comparing pre/post impression visit volume.
- **Pre-impression visit proxies** (for before/after campaign launch comparisons): `PageViews` (raw page view count) or `RawVisitors` (unique IPs of those page views).
- **Verified Visits timing:** VVs should begin occurring as soon as a campaign launches, provided the advertiser has a pixel set up. The "21 days" threshold sometimes referenced is not a hard system rule — the actual trigger is campaign launch + pixel presence. (via ray, #reporting_helpdesk_ask_anything, 2026-04-28)
- ## summarydata.all_facts — BigQuery Migration ETA

`summarydata.all_facts` is scheduled to be available in BigQuery on **May 5, 2026**. Until then, queries requiring this table must use the CoreDW (Postgres) source. Note: a current pipeline (`airflow-camperbid`) is using `all_facts` and is flagged for eventual refactor to use source tables directly. (via Mike Dolzer, #data-platform, 2026-04-28)
- ## guid_identity_daily.sql — No-Share Filter Required

The model at `models/dw-main-silver/aggregates/guid_identity_daily.sql` in the SQLMesh repo must be updated to join against the `public.advertisers` table and filter out no-share advertisers using the new boolean column (TRUE = cannot use data). This is required as part of the No-Share Advertiser Policy implementation. The column name was pending confirmation as of late April 2026. (via Jack Barbey, #identity_core_dev, 2026-04-28)

<!-- slack-extracted: 2026-05-06 -->
- **`campaign_groups.product_id` — Source of Truth for MNTN Select vs. PTV Identification**

- **Table:** `campaign_groups` (coredb, replicated to `bronze.integrationprod`)
- **Column:** `product_id` — values: `1=PTV`, `2=Select`, `3=QuickFrame` (via `core_products` lookup)
- **Authority:** The UI sets `product_id` when managing Select vs. PTV campaigns. This value flows to reporting and invoices. It is the canonical source of truth across the system.
- **Validation:** Four table variants exist in `bronze.integrationprod` (`campaign_groups`, `campaign_groups_raw`, `public_campaign_groups`, `public_campaign_groups_raw`) — all agree. Immutable across 735K archive row versions. Bidder-side `is_select_cid` agrees 100% but is considered a bespoke derived value, not the source of truth.
- **PMP caveat:** PMP-deal attachment is not a clean proxy — 28 PTV "Pause Ads" groups also use PMP deals.
- **Reporting usage:** `product_id` is the filter element for customer-facing Select reporting and invoices. (via ray, #reporting_helpdesk_ask_anything, 2026-05-05)
- **`audience.advertiser_configurations.enable_taxonomy_block` — DS16 vs. DS2 for MT+ Audiences**

- The field `enable_taxonomy_block` in `audience.advertiser_configurations` controls whether an advertiser uses **DS16** (taxonomy block) or **DS2** for building MT+ (Mid-Touch Plus) audiences.
- If `enable_taxonomy_block = false`, DS2 will be used instead of DS16 — this is expected behavior for those advertisers, not a misconfiguration.
- Only **3 advertiser IDs** currently have `enable_taxonomy_block = false` (including AID 31357).
- The existing audit monitor for DS16 exclusion (audit [40]) does not account for this flag, causing false positives. The DM team has a ticket (DM-4433) to update the audit logic to check `enable_taxonomy_block` before flagging; ETA early June 2026.
- **Campaign group exclusions** in the audit query are likely experiment-related holdouts. (via zach.schoenberger, #production-ops, 2026-05-05)

<!-- slack-extracted: 2026-05-09 -->
- **audience.audience_type_alpha — View Definition and MM Detection Issue**

`audience.audience_type_alpha` is a view in the core (Postgres/coredw) database used to identify Mountain Matched (MM) campaign groups. It detects MM by checking audience expressions for `data_source_id: 13` (DS13) or `data_source_id: 19` (DS19) patterns.

**Schema (abbreviated):**
- `campaign_group_id`
- `audience_id`
- `mntn_matched` (always `true` when row exists)

**Detection logic:** Two UNIONed branches — one matching DS19 expressions, one matching DS13 expressions — both filtered to advertisers present in `fpa.advertiser_verticals`.

**Known issue (Fangorn / DS46 migration):** After Fangorn launch, some campaign groups were migrated to use `data_source_id: 46` (DS46) instead of DS13 in the `audience_segments` table. The `audience_type_alpha` view still only checks for DS13 and DS19, so migrated campaigns will no longer be recognized as MM by this view. This primarily affects **monitoring dashboards and reporting** (e.g., `bi.v_feature_date`) rather than bidder mechanics. Teams that query DS13/DS19 for MM campaign identification (including DM and BAE teams) need to update their queries to include DS46.

**Related tables:**
- `audience.audiences` — contains raw expression JSON
- `audience.audience_x_campaign_groups` — links audiences to campaign groups
- `audience.audience_segments` — mutated by Fangorn overlay; DS13 → DS46 changes applied here
- `campaign_groups` — joined for parent/child campaign group resolution (via Ryan Kleck, #fangorn_launch_day, 2026-05-08)

<!-- slack-extracted: 2026-05-12 -->
- ## coredb: dso.network_performance_thresholds & dso.publisher_performance_thresholds

These two tables in the `dso` schema of coredb (intprod) have not been actively used for more than a year (as of May 2026). Both tables are currently **empty**.

**Datastream replication note:** Datastream will not begin replication of a table from intprod to BigQuery unless the table contains at least one row. These tables are blocked from replication as a result of being empty.

**Status:** The owning squad (per schema ownership, the pacing/spend squad — Swapnil Patil / Tony Chen) confirmed these table references can be removed. No downstream consumers are known. (via Sheetal Ramesh, #data-platform, 2026-05-11)

<!-- slack-extracted: 2026-05-19 -->
- **Databricks → BigQuery Connector — Project/Billing Configuration (mntn-identity-prod):** When using the Spark BigQuery connector to read from `dw-main-silver` while billing jobs to `mntn-identity-prod`, use the `project` and `billingProject` configuration parameters (the older `parentProject` parameter is deprecated per the GoogleCloudDataproc spark-bigquery-connector). Set `billingProject` to `mntn-identity-prod` and `project` to `dw-main-silver`. Materialization temp tables will land in the `dw-silver` project. The `mntn-identity-prod.aggregates` dataset is only needed if billing/materialization is pointed at `mntn-identity-prod`; if billingProject is correctly set, a separate aggregates dataset in the identity project is not required. The service account `databricks-compute@mntn-databricks.iam.gserviceaccount.com` requires `BigQuery Job User` (`roles/bigquery.jobUser`) on `mntn-identity-prod` and `roles/bigquery.dataViewer` + BigQuery Read Session User on relevant datasets. (via Jack Barbey, #identity_core_dev, 2026-05-18)
- **SQLMesh — guid_identity_daily.sql Job Alerting:** As of 2026-05-18, no alerting is configured for SQLMesh job failures, including `guid_identity_daily.sql`. Alerting needs to be set up in the SQLMesh repo. This is a known gap acknowledged by the identity team. (via Weiang Li, #identity_core_dev, 2026-05-18)

<!-- slack-extracted: 2026-05-20 -->
- ## ui.audience_uploads

- **Key column:** `match_rate` — populated for email and phone CRM upload types; always `NULL` for IP-type uploads (`audience_upload_type_id = 3`). This is expected, not a data quality issue.
- **Key column:** `audience_upload_type_id = 3` — denotes IP-type uploads.
- **Match rate source (email/phone):** Calculated by the Spark job at `spark/crm/crm_match_rate_gcp.py`, orchestrated by `dags/crm/crm_match_rate_dag.py` (Airflow). Result written back to this table.
- **Gotcha:** The gary GraphQL API resolves `AudienceUpload.match_rate` directly from this column. For IP uploads, no fallback logic exists — consumers of this field must handle the null case explicitly. (via Macie Kluting, #targeting-squad, 2026-05-19)
- ## core.live_schedule_events

- **Schema change (2026-05-19):** Column `source_event_key` was dropped from this table across QA, DEV, and PROD environments.
- **Ticket:** DPLAT-1069
- **Action required:** Any queries or models referencing `source_event_key` on `core.live_schedule_events` will fail — remove references. (via sai, #data-platform, 2026-05-19)

<!-- slack-extracted: 2026-05-22 -->
- ## silver.logdata.event_log — Video Completion Events

To retrieve video completion events on a per-impression basis, query `dw-main-silver.logdata.event_log` and filter on `event_type_raw = 'vast_complete'`. Key columns for this use case:
- `td_impression_id` — join key to impressions
- `campaign_id`
- `advertiser_id`
- `creative_id`
- `time` — timestamp of the completion event (always filter on this column for performance)

`cost_impression_log` does **not** contain video completion data; `event_log` is the correct source. (via Pratik, #reporting_helpdesk_ask_anything, 2026-05-21)
- ## dw-main-gold.salesforce.v_accounts_log — Known Data Gap (Jan 30 – May 16, 2026)

As of 2026-05-21, `dw-main-gold.salesforce.v_accounts_log` (and its underlying tables in `dw-main-silver.sqlmesh__salesforce`) is missing approximately 3.5 months of historical partitions: data from **2026-01-30 through 2026-05-16 is absent**. Data exists for dates through 2026-01-29 and from 2026-05-17 onward.

**Root cause:** BAE (Business Analytics Engineering) migration/salesforce work is in progress and expected to wrap by **2026-06-02**. Anomalies in this table should be expected until then.

**Usage note:** This view is used for the Scout Book of Business Coverage KR — specifically to pull a frozen Apr 1 snapshot as the OKR denominator. The Apr 1 partition is currently unavailable; downstream users should rely on cached snapshots until the gap is resolved. (via Kaitlin Dickinson, #data-platform, 2026-05-21)

<!-- slack-extracted: 2026-05-23 -->
- **dw-main-bronze.raw — Household ID (HH) Log Tables**

Four raw tables exist for household ID enrichment across event types:
- `dw-main-bronze.raw.ads_clickpass_hh_log`
- `dw-main-bronze.raw.click_hh_log`
- `dw-main-bronze.raw.guid_hh_log`
- `dw-main-bronze.raw.conversion_hh_log`

**Column naming inconsistency (as of 2026-05-22):** `ads_clickpass_hh_log` uses `household_id`, `household_version`, etc. (full names), while `click_hh_log` and `guid_hh_log` use shortened names: `hh_id`, `hh_version`, etc. Standardization to the full `household_id` naming convention has been requested.

**Known data gap:** `conversion_hh_log` currently only contains `hh_resolution_id` — the columns `hh_id`, `hh_confidence_score`, and `hh_ver` are present in the raw Kafka events (confirmed in conversion log source) but are not being ingested into the table. A DPLAT ticket (DPLAT-1100) has been filed to resolve this. The attribution team currently pulls directly from raw Kafka events as a workaround.

**Expected schema for HH log tables:**
- `hh_id` (or `household_id`) — household identifier
- `hh_confidence_score` — confidence score for household resolution
- `hh_ver` (or `hh_version`) — graph version used for resolution
- `hh_resolution_id` — resolution event identifier (via Jack Barbey, #identity_core, 2026-05-22)

<!-- slack-extracted: 2026-05-30 -->
- **`dw-main-bronze.raw.guid_log` — Size and Performance Characteristics**

- **Total logical data:** ~200 TB
- **Partition:** DAY-partitioned on `time` (TIMESTAMP). Matches the coreDW source table which was also daily-partitioned on `time`.
- **Query behavior:** Filtering by `time` date range works correctly and limits partition scans. A secondary filter on `advertiser_id` does NOT further reduce partition scans — BQ must scan all rows within the time-partitioned range and then filter by `advertiser_id` in memory. A 3-month range query (March–May 2026) processed ~393 GB.
- **Performance gotcha:** Ad-hoc queries run against a small slot reservation. Apparent slow queries (2+ minutes wall clock) are frequently caused by slot contention, not actual compute time. The underlying query may execute in ~4 seconds once slots are available. Check the BigQuery Admin monitoring console to distinguish slot-wait time from actual execution time.
- **Slot reservation note:** The ad-hoc query reservation is intentionally limited for cost savings and will not be increased until end of quarter. (via Dustin Niehoff, #data-platform, 2026-05-28)
- **`dso.campaign_group_daily_budgets` and `archives.campaign_group_daily_budget_archives` — Daily Budget Source**

Real-time campaign budget data is sourced by unioning `dso.campaign_group_daily_budgets` (current) with `archives.campaign_group_daily_budget_archives` (historical). The union is then deduplicated using `DISTINCT ON (advertiser_id, campaign_group_id, hour(update_time))` to get one budget record per advertiser/campaign-group/hour. Rows where `billing_type_id = 2` are excluded (these represent a specific billing type that should not be included in budget reporting). This pattern is used in at least one real-time Mode report for monitoring budget vs. spend. (via Benny, #production-ops, 2026-05-29)
- **Geo Location Mapping Discrepancy — `geo.v_location_data` (coreDW) vs. `dw-main-bronze.geo.v_location_data_lat_long` (BigQuery)**

A confirmed data inconsistency exists in the `parent_location_id` field between the coreDW and BigQuery versions of the location data table. Example: `location_id = 657177` (postal code 14527, Feura Bush) has `parent_location_id = 143675` in coreDW but `parent_location_id = 93463` in BigQuery. This discrepancy affects geo-targeting audit logic (specifically audit 22) and caused IP-to-location mapping mismatches for certain IPs (e.g., `67.241.244.44`). The fix was expected to be deployed by Nivas on 2026-06-01, after which the audit would no longer trigger on this data. The active geo version at the time of discovery was `1777852800`. (via Benny, #mission-control, 2026-05-29)

<!-- slack-extracted: 2026-06-02 -->
- **`dw-main-silver.summarydata.all_facts` — spend/impression query pattern:** To retrieve daily spend and impression data for a campaign group, query `all_facts` filtering on `hour` (TIMESTAMP) and joining on `advertiser_id` and `campaign_group_id`. Spend fields include `media_spend`, `data_spend`, and `platform_spend`. Impression fields include `display_impressions` and `ctv_impressions`. Note that `hour` is in UTC. Example: `SELECT CAST(hour AS DATE) AS day, SUM(media_spend+data_spend+platform_spend) AS spend, SUM(display_impressions+ctv_impressions) AS win_impressions FROM dw-main-silver.summarydata.all_facts WHERE hour >= '2026-05-20' AND hour < '2026-06-02' AND advertiser_id=31460 AND campaign_group_id=117407 GROUP BY 1 ORDER BY 1` (via Pratik, #production-ops, 2026-05-27)
- **`audience.audience_segment_campaigns` — Segment Expression Schema:** This table stores audience targeting segment configurations per campaign. Key columns include: `audience_segment_id`, `audience_id`, `expression`, `expression_type_id`, `segment_id`, `is_active`, `update_time`, `is_targeted`, `objective_id`. Join to `public.advertisers` on `advertiser_id`, to `public.campaign_groups` on `campaign_group_id`, and to `public.campaigns` on `campaign_id` for full context. `expression_type_id = 2` can be used to filter for a specific expression type. The `funnel_level` column on `public.campaigns` indicates campaign stage (e.g., `funnel_level = 1` for Stage 1). (via Weiang Li, #identity_core, 2026-06-01)
- **CoreDB / `audience.data_sources` table — Write Access:** To create entries in the `audience.data_sources` table, use the integrations service account. This should be done directly rather than raising a request to the data platform team. The data platform team maintains service accounts only, plus a data platform account for object creation. No user-scoped CoreDB accounts exist beyond service accounts. (via mohan, #data-platform, 2026-06-01)

<!-- slack-extracted: 2026-06-03 -->
- ## core.flight_billing_types

- **Source of truth** for billing type associated with a flight. This is the current/official table for flight-level billing type (supersedes the older `billing_type_id` column on the `advertisers` table, which is legacy).
- **Key column:** `flight_billing_type` (also referenced as `billing_type_id` in join context)
- **Current billing type IDs:**
  - `2` = PTV Fixed CPM (also currently used for MNTN SELECT campaigns — to be separated)
  - `3` = Proposed new ID for MNTN SELECT / native impression cap campaigns (not yet live as of 2026-06-02)
- **Derivation logic:** Gary (gary-ql service) derives and upserts `flight_billing_types` after budget processing. The SELECT/Premier UI does not pass billing type directly on flight creation — Gary maps it from the campaign group's `product_id`. See `gary-ql/src/gql/types/Budget/UiToCore.ts#L1026-L1037` and `gary-ql/src/data/Budget.ts#L1813-L1905`.
- **Reporting note:** The reporting team (BAE/ray) stopped using `advertisers.billing_type_id` in spend logic — `core.flight_billing_types` is the authoritative source.
- **Related table:** `core.billing_types` — the lookup/reference table for billing type definitions. New billing type IDs are added here.
- **Ticket:** PER-6526 tracks the work to introduce billing_type_id = 3 for SELECT. (via Tony Chen, #reporting_helpdesk_ask_anything, 2026-06-02)

<!-- slack-extracted: 2026-06-06 -->
- **augmentor_identity_daily and guid_identity_daily — Identity Graph Aggregation Models**

- **Location:** `dw-main-silver/aggregates/augmentor_identity_daily.sql` and `dw-main-silver/aggregates/guid_identity_daily.sql` (SQLMesh models)
- **Owning team:** Identity (ID team, led by Jack Barbey / Weiang Li / Alexander Jerneck)
- **Purpose:** Aggregates daily bidder auction logs to extract device-to-device connections used as core input to the MNTN identity graph (household graph).
- **Downstream consumer:** Spark jobs running on Databricks — no downstream SQLMesh dependencies, which is why they appear as leaf nodes in the SQLMesh DAG.
- **Current status (June 2026):** Running ~6 hours/day and timing out due to bidder data volume explosion (+10x since May 28). Alerts have been added to both jobs.
- **Performance gotcha:** These models are already running on a large BQ compute reservation. The bottleneck is raw data volume, not compute configuration. The path forward is architectural (Spark/GCS direct access or shared aggregation table), not query tuning. (via Jack Barbey, Weiang Li, scotty, #data-platform, 2026-06-05)

<!-- slack-extracted: 2026-06-09 -->
- **silver.logdata.cost_impression_log — `ad_served_id` filter behavior**

The filter `ad_served_id IS NOT NULL` in queries against `cost_impression_log` is intended to keep only valid/won impressions by requiring `ad_served_id` to be present. This filter is most relevant when `unlinked = TRUE` (i.e., no `impression_id` was found in `impression_log`, so the row is 'unlinked'). When filtering on `unlinked = FALSE`, adding `ad_served_id IS NOT NULL` is redundant but harmless — empirically, nearly all `unlinked = FALSE` rows have a non-null `ad_served_id` (spot check for 2026-06-01 through 2026-06-07 showed at most 8 rows per day with `unlinked = FALSE` and null `ad_served_id` out of 54–60M daily impressions). All `unlinked = TRUE` rows have a null `ad_served_id`. (via Sonali, #reporting_helpdesk_ask_anything, 2026-06-08)
- **bidder_win_notifications — data quality issue: empty `geo_version` and null `device_ip` for STICKYADS rows (2026-06-08)**

On 2026-06-08, rows arriving via the STICKYADS inventory source had an empty `geo_version` and a null `device_ip`. This caused two downstream model failures:
- `cil__impression_info` errored on a hard `geo_version::INT64` cast ("Bad int64 value").
- `impression_facts` errored because a null `device_ip` produced a NULL element in its `uniques_arr` array.

**Root cause:** Empty `geo_version` is a product of World Cup targeting requirements (skipping the normal closed-loop geo resolution). Null `device_ip` is expected for IPv6 traffic — for IPv6 auctions, the IP is in `device_ipv6` rather than `device_ip`.

**Fix (PR #1033):** Three model-level defensive changes: (1) `NULLIF(geo_version, '') → NULL`, (2) `IGNORE NULLS` on the `uniques_arr` array agg, (3) `SAFE_CAST` on the `geo_version` cast. Affected rows were patched directly down the live chain (`raw → spend_log → cil__impression_info → cost_impression_log`) to restore pipeline. Remaining work: decide whether to populate `device_ip`/`geo_version` earlier in the bidder ingest.

**Spend impact:** None — geo_version and device_ip are not required to charge a customer. The only hard requirement for charging is that the impression appears in both the source (spend_log/win_log) and impression_log. (via scotty, #data-platform, 2026-06-08)
- **augmentor_identity_daily — new Spark/GCS pipeline architecture**

The `augmentor_identity_daily` pipeline has been redesigned from a monolithic job to a parallelized Airflow DAG running hourly Dataproc batches:
- **Processing:** Each hourly batch handles one hour of auction log data. 6 batches run in parallel across 4 waves.
- **Merge step:** A daily merge step at the end aggregates all hourly outputs into the daily output.
- **Idempotency:** Each batch safely overwrites its output, so failed hours can be rerun independently.
- **Runtime:** ~40–50 minutes end-to-end for a full day.
- **Output location:** `gs://mntn-data-archive-dev/identity/augmentor_identity_daily/YYYY-MM-DD/` (Parquet)
- **Airflow DAG:** `airflow-ti` repo, branch `augmentor_daily`, file `dags/attribution/augmentor_daily_gcs.py`
- **Hourly processing job:** `spark/auction_log_augmentor_process_gcs.py`
- **Daily merge job:** `spark/auction_log_augmentor_merge_gcs.py`

Reads directly from GCS parquet sources (not BigQuery tables) to avoid BQ timeout issues. The previous BigQuery-based `augmentor_identity_daily` SQLMesh model was timing out and has been disabled. (via Weiang Li, #identity_core, 2026-06-08)
- **bid_events_agg and auction_events_agg — BigQuery aggregation performance benchmarks**

Benchmarks from a June 2026 SQLMesh test run (1 hour of data, 2-hour lookback):
- **bid_events_agg:** 62.7M rows, 5.6 TiB scanned, completed in ~963 seconds (~16 min). ~140× compression vs. raw.
- **auction_events_agg:** 580M rows, 13.2 TiB scanned, completed in ~1,875 seconds (~31 min). ~9× compression vs. raw. The high slot time is driven by the identity grain (IP columns) plus distinct timestamp aggregation required by the Identity team.

A 2-hour lookback is required to capture late-arriving data. At these scan volumes, running both jobs in BigQuery is likely too slow for production use; Spark-based processing from GCS parquet sources is under evaluation as an alternative. See PR #1037 for the BQ SQLMesh attempt. (via Jane Brooks, #data-platform, 2026-06-08)

<!-- slack-extracted: 2026-06-10 -->
- ## dw-main-silver.salesforce.accounts_log — Downstream Dependencies

When `dw-main-silver.salesforce.accounts_log` is restated, it triggers backfill of **3 models and 19 views** downstream. Key affected models/views include:

**Gold models (full refresh):**
- `dw-main-gold.bae.advertiser_attributes`
- `dw-main-gold.bae.advertiser_monthly_performance_rating`
- `dw-main-gold.bae.advertiser_monthly_spend_changes`

**Gold views (recreate):**
- `dw-main-gold.bae.v_campaign_product_adoption`
- `dw-main-gold.bae.v_cs_retention_growth_adoption`
- `dw-main-gold.bae.v_customer_journey`
- `dw-main-gold.bae.v_fin_revenue_team_override`
- `dw-main-gold.bae.v_go_live_snapshot_monthly` / `_weekly`
- `dw-main-gold.bae.v_spend_after_marketing_touchpoint`
- `dw-main-gold.bae.v_trial_cohort_conversion_rate` / `_cs`
- `dw-main-gold.bae_finance.v_billable_spend`
- `dw-main-gold.bae_finance.v_credit_program_invoiced_spend`
- `dw-main-gold.salesforce.v_accounts_log`
- `dw-main-gold.tableau.v_advertiser_monthly_spend_changes`
- `dw-main-gold.tableau.v_client_count_spend_changes`
- `dw-main-gold.tableau.v_cohort_advertisers_wow_spend`
- `dw-main-gold.tableau.v_cs_growth_pipeline`
- `dw-main-gold.tableau.v_marketing_funnel_by_account`
- `dw-main-gold.tableau.v_sum_by_advertiser_by_day`
- `dw-main-gold.tableau.v_sum_by_parent_campaign_group_by_day_simple`

**Restate tip:** Coordinate with data platform (DPLAT) before running restate. Command: `sqlmesh plan prod --restate-model "dw-main-silver.salesforce.accounts_log" --start "<date>"` (via Sheetal Ramesh, #data-platform, 2026-06-09)

## bronze.raw.bid_price_log — Beeswax bidder price/ghost-bid log (TI-1044, 2026-06-23)
- **What:** Beeswax-bidder bid decisions (the `gs://bidder-price-events-prod-east/.../rtb-bid-price-events`
  stream, ingested to BQ). Row per bid decision. **This is where Beeswax ghost bids are queryable** —
  `bidder_bid_events` (silver) is MNTN-bidder only and has NO ghost rows; the GCS bucket is access-gated.
- **Partition:** HOUR on `time`. **Clustered on `ip`** (NOT advertiser_id → advertiser filters scan full
  partitions; narrow the date window to cut cost). **TTL ~10 days.** ~72B rows/day (all advertisers).
- **Ghost-bid (holdout) flag:** `threshold_failure_reasons = 'ghostBid'` (Beeswax camelCase). Empty/NULL
  reason = bid placed (targeted, entered auction). Other values = dropped (missingIntentScore, cappedOrPaced,
  etc.). Ghost logging live since **2026-05-27** (Ryan Kleck); no backfill. **No ghost-WIN logging** → win
  estimated via win-rate. (MNTN-bidder equivalent: `bid_dropped_reason='ghost-bid'`.)
- **Key cols:** `advertiser_id`, `campaign_id`, `campaign_group_id`, `beeswax_campaign_group_id`,
  `device_ip`/`ip`, `device_ipv6`, `is_ctv` (BOOL), `bid_price`, `threshold_failure_reasons`,
  `advertiser_intent_score`/`campaign_intent_score`/`segment_intent_score`, `intent_score_threshold`,
  `household_score_threshold`, `advertiser_household_score`, `conquest_score_threshold` (RTC),
  `campaign_frequency_cap`, `campaign_impressions`. (`bidder_price_events` has `bid_placed` BOOL but is EMPTY.)
- **Ghost-ad lift recipe (TI-837):** control = ghostBid IPs; treated = served (`cost_impression_log`,
  advertiser-clustered, cheap) for ATT, or empty-reason IPs for clean ITT (pre-auction, no win-selection).
  Outcomes from clickpass_log (visits) / conversion_log (convs) by ip. See `ti_1044_ghost_lift*.sql`.
- **`threshold_failure_reasons` vocabulary (obj=1 prospecting, INCR bias register 2026-06-23):**
  `missingIntentScore` 57.5%, `invalidCampaignIntentScore` 33.9%, `''` (submitted/placed) 6.0%,
  `ghostBid` 2.1%, `bidPriceBelowImpressionBidFloor` 0.4%. **`is_submitted = (threshold_failure_reasons = '')`**
  — submitted bids carry empty-string, NOT NULL (column is never NULL), so treatment is not undercounted.
  **No fcap tokens appear** → frequency-capping is config-OFF for these prospecting advertisers.
- **Ghost-lift selection gotcha (INCR, PROVEN 2026-06-23):** distinct-IP `ghost_frac` inflates above the
  true 0.10 hash (to 0.13–0.47) via **bid-multiplicity** — holdout IPs never win→never exit the pool→re-bid
  repeatedly→over-weight high-frequency IPs→spurious negative lift. Single-qual-bid IPs split at exactly
  0.0988. De-bias by gating to `ghost_frac ∈ [.095,.11]`. See experimentation.md "Ghost-bid lift — bias register."

## integrationprod.advertiser_configurations — block/lookback settings + STALE-IN-BQ warning (TI-1044, 2026-06-24)
- Holds advertiser-level exclusion settings: `block_conversion`, `block_prospecting`, `block_first_party`,
  `conversion_lookback_window`, `page_view_lookback_window`, `enable_taxonomy_block`. Keyed `advertiser_id`.
  Booleans are BOOL (use `WHERE block_prospecting` / `= TRUE`, not `='true'`).
- **⚠ ABSENCE ≠ OFF (Zach Schoenberger, 2026-06-24):** a row is written ~only when an advertiser **CHANGES from
  defaults**; the **defaults are block_conversion/block_prospecting ON at 30/30 days.** So a MISSING advertiser
  is using the defaults (blocks ON), NOT off. BUT the table also contains many default-looking 30/30 rows, so
  its semantics are inconsistent — **you cannot reliably infer block status from presence/absence here.** To
  verify whether an advertiser is actually being blocked, you need another source (bidder effective-config);
  the per-campaign `audience_audience_segments` expression is also unreliable (~96% carry no pageview clause).
  Tracked in **TI-1061**. (This corrected a false "ElevenLabs blocks off" read in TI-1044.)
- **⚠ TWO TABLES — use the right one (verified 2026-06-24):**
  - `integrationprod.advertiser_configurations` (no prefix) = **STALE, frozen 2026-01-12** (broken sync). Do NOT use.
  - **`silver.audience.advertiser_configurations`** = **`integrationprod.audience_advertiser_configurations`** =
    **FRESH (updated daily)** — current source for block/lookback settings (with the absence-≠-off caveat above).
    Keyed advertiser_id; 14,582 advertisers as of 2026-06-26.
  - Block is applied **at the advertiser level** (bidder reads this config and suppresses globally); ~96% of
    prospecting campaigns have NO per-campaign pageview clause in `audience_audience_segments`, so the config
    table — not the per-campaign expression — is authoritative. (Archive `archives_advertiser_configuration_archives`
    is also fresh for change history.)
- **Per-campaign exclusion clause in `audience_audience_segments`** (`is_targeted=false`) can look like
  `UserLastVisitTime >= N,day and UserNumPageViews >= K` (lookback + threshold) — but this is NOT the
  authoritative block (block_prospecting is enforced advertiser-level by the bidder; ~96% of campaigns have no
  such clause). A `>= 0` per-campaign clause does NOT mean blocks are off. The full pageview block excludes
  **ANY guid pageview** (organic / other-marketer), not just MNTN-attributed VVs (VVs = a subset).
- 10% holdout encoded in the expression: `md5(<advertiser_id>:<ip>) bucket 0–99 of 1000`.

### silver.archives.household_score_threshold_archives (HHST gate change history)
CDC archive of the per-campaign Household Score Threshold (the intent gate the bidder enforces). Cols: `household_score_threshold_archives_id`, `advertiser_id`, `campaign_group_id`, `campaign_id`, `threshold` (INT; 0 or negative = NO gate / serve anyone; 10000 = HI-only), `transaction_id`, `create_time`, `update_time`, `datastream_metadata`. One row per write (not per change) → collapse with LAG(threshold) OVER (PARTITION BY campaign_id ORDER BY update_time) keeping threshold!=prev. Current live values: `silver.dso.household_score_thresholds`. Join campaigns via `bronze.integrationprod.campaigns` (campaign_id PK; objective_id=1 & funnel_level=1 = Stage-1 prospecting). Use for daily gate event-studies (AUDI-1070: gate flips drive overnight delivery-composition inversions).

### silver.archives.audience_segment_archives (audience-expression change history)
CDC / type-2 archive of the audience targeting expression (archive of live `audience_audience_segments`). One row per version; linked by `campaign_id`; `expression` is nested JSON; filter `expression_type_id=2 AND is_targeted=TRUE`. Use it to build a **per-campaign data-source add/remove timeline**: extract DS ids by regex — `REGEXP_EXTRACT_ALL(expression, r'"data_source_id":([0-9]+)')`. Gotcha: `version` is NON-MONOTONIC — order by `create_time`/`update_time`, not `version` (AUDI-1070). Analysis note: HI substrate = vertical(DS13)∩keyword(DS19); changes to OTHER sources (add CRM DS4, drop DS35 LiveRamp, add DS16/DS21/DS34) are real audience changes that do NOT touch the HI-defining layers. The RTC directive (`score_type=rtc`, id=vertical_id) can live in the expression from ~Jul 2025 yet only fire in delivery from 2026 — 'in expression' ≠ 'firing.'

### household_score_threshold_archives — complete `threshold` value map (AUDI-1070)
Consolidated value semantics for the `threshold` column (the earlier entries listed only 0/negative and 10000): `-100`/`-1`/`0` = NO gate (max-reach / serve-anyone); `3333`/`3334` = Mid floor (excludes MaxReach 1–3332); `6666` = HI+PP floor; `8000` = PP floor; `10000` = HI-only. **The gate binds on `household_score`, NOT `advertiser_household_score`** (the two diverge ~10%; AHS logs ~3500 for ~10% of genuine-HI impressions). On the gated path ~99.99% of impressions carry `household_score` exactly equal to the gate value; residual ~0.01–0.02% is ~1-day flip-day propagation lag (unscored, not PP/Mid). Always reason about binding on `household_score`, and exclude the RTC path (RTC bypasses the gate). **Short-flight gotcha:** flights <72h auto-set `threshold`=0 for deliverability — a `threshold`=0 row is not necessarily an intentional gate-off; cross-check flight length first.

### Historical-reconstruction gotchas — vertical sizes and by-tier visit rates NOT recoverable in BQ (AUDI-1070)
- `dw-main-bronze.external_ddm.data_source_category_sizes` is **3P-ONLY** — does NOT contain DS13 (vertical) or DS19 (keyword) sizes. To size a vertical's HI supply historically, route to the Measurement/scoring team's GCS pull (`ip_vertical_associations` / `prospecting_intent`) — not this table, not BQ.
- `clickpass_log` is purged for older periods → historical **visit-rate-BY-TIER** queries on 2025 return 0 retained visit rows; reconstructing 2025 by-tier VR needs a Measurement-team pull.
- `bronze.external.household_scoring__prospecting_intent__v1` retains ~35 days active (10-day in BQ; deeper via raw GCS) — can't reconstruct >35d pool history. `TI_835_prospecting_scores` GCS files were deleted; `ddp_vertical_classification_api` is API logs, not a membership table.

### silver.core.flights (authoritative flight schedule — start/end times)
The real flight table (Tofer): `flight_id`, `campaign_group_id` (a flight is per client-campaign/GROUP), `start_time`, `end_time`, `budget`, `budget_type_id`, `status_id` (3=active/completed, 8=superseded), `ui_flight_id`. Compute flight length as `TIMESTAMP_DIFF(end_time,start_time,HOUR)`; join advertiser via `campaign_groups.advertiser_id`. Use this for the **short-flight (<72h → manual HHST=0) check** — do NOT infer flight length from consecutive active-days (merges flights). Each budget/schedule EDIT spawns a new flight row, so a <72h "flight" can be a mid-schedule tweak, not a fresh launch. Companion: `silver.dso.campaign_group_flight` (adds local-tz start/end + name).
