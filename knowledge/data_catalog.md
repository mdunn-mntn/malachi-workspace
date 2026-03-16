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
- [audit](#audit-bq-dataset) — stage3_vv_ip_lineage
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
**Retention:** Earliest data is 2025-01-01 for most tables (event_log, impression_log). viewability_log starts 2025-04-08. No BQ layer (silver or bronze.raw) has data before 2025-01-01. Pre-2025 data only in Greenplum coreDW.

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
- **Partition:** None at view level. **No TTL** — confirmed 2026-03-03 (expirationTime: none). Use `DATE(time)` for date filters.
- **Use for:** Verified visit log — one row per verified visit redirect. "clickpass" is the old term for verified visit (VV). Contains ALL VV types: CTV and display. **Not** click-only; not CTV-only. (Confirmed by Zach: "vv can happen for display as well and would be here.") `ui_visits` is the superset that adds display clicks and non-VV traffic.
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
| ip | STRING | **VAST playback IP** — IP of the CTV device during VAST ad playback. ≠ bid_ip ~3.5% of the time. |
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
- **Use for:** GUID (cookie) creation and attribute events — user identity tracking

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
- **Type:** VIEW → `sqlmesh__logdata.logdata__cost_impression_log__2498930125` (**TABLE** — physical)
- **Partition:** DAY on `time`
- **Clustering:** advertiser_id, impression_id
- **Use for:** Impression-level spend enriched with geo, device, segment data. 90-day rolling.

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
| viewability_type_id | INTEGER | |
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
- **Key columns include:** All columns from visit_facts + conversion_facts + spend_facts, plus
  display_impressions, ctv_impressions, media_cost, fee_cost, vast_* video metrics,
  uniques (BYTES — HLL), *_arr serialized arrays, probattr_* columns, competing_* columns.

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
- **Partition filter REQUIRED** (requirePartitionFilter: true)
- **Use for:** Raw augmentor service events — pre-bid request enrichment. Raw source for v_augmentor_log.

| Column | Type | Notes |
|--------|------|-------|
| time | TIMESTAMP | Partition key (**required in filter**) |
| ip | STRING | Clustering key |
| epoch | INTEGER | |
| domain / app_bundle / site_name | STRING | |
| placement_type / environment_type / inventory_source | STRING | |
| device_type / video_placement | STRING | |
| os / user_agent / ifa / network / isp | STRING | |
| geo | STRING | Raw geo string |
| mntn_segments | RECORD | LIST of segment IDs |
| pmp | RECORD | LIST of PMP deals |
| iab_categories / categories | RECORD | LIST |
| is_blocked | BOOLEAN | |
| blocking_site | STRING | |
| ipv6 | STRING | |
| page / referrer | STRING | |
| _source_file / _batch_id | STRING | |

- **Warning:** Very short TTL (10 days). Partition filter required — always include `time` in WHERE.

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
- **Use for:** Advertiser-level audience segment definitions (targeting expressions).
- **Columns:** advertiser_id, expression (STRING), expression_type_id, segment_id, create_time, update_time

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

## audit.stage3_vv_ip_lineage
- **Type:** TABLE (production audit table, created TI-650)
- **Partition:** `trace_date` (DATE)
- **Clustering:** `advertiser_id`
- **Use for:** IP lineage trace for Stage 3 verified visits — maps VV back to its originating
  bid IP. Enables NTB validation and general VV auditability.
- **Requires:** 30-day event_log lookback for full coverage

Key columns (see audit_trace_queries.sql in mm_44_ipdsc_hh_discrepancy/queries/ for full CREATE):
- `ad_served_id` — join key (win_log → CIL → event_log → clickpass_log → ui_visits)
- `win_ip`, `cil_ip`, `el_ip`, `cp_ip`, `visit_ip` — IP at each pipeline checkpoint
- `ip_mutated` — boolean: win_ip ≠ visit_ip
- `cross_device` — from ui_visits
- `trace_date` — partition

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
| `audience_segment_campaigns` | audience | Maps active audience segment → campaign_group | campaign_group_id, expression_type_id |
| `membership_updates_logs` | tpa | TPA membership update log (Greenplum version) | ip, segment_id, update_time |
| `advertisers` | public | Advertiser dimension table (Greenplum version of bronze.integrationprod.advertisers) | advertiser_id, name, deleted, is_test |
| `data_sources` | audience | Data source registry | data_source_id, name, data_source_type_id |
| `locations` | geo | Geo location reference | location_id, state/country names |
| `cost_impression_log` | logdata | Won impressions with cost — `model_params` key=value string | ip (TEXT), model_params, impression_id |
| `impression_log` | logdata | All bid attempts (won + lost) | ip (INET), ip_raw, bid_ip, original_ip, campaign_id |
| `ui_visits` | summarydata | Verified visits — `ip` is INET type; use `host(ip)` to strip /32 | ip (INET), impression_time, is_new |
| `ui_conversions` | summarydata | Conversions — use `order_amt`, NOT `order_amt_usd` (which is NULL) | order_amt, advertiser_id |
