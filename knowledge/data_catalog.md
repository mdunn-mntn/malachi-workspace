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
Freshness gotcha: `<dataset>.__TABLES__` shows these clean views as 0 rows / 0 bytes (not materialized) —
to check "did the job run?" query the physical `sqlmesh__<dataset>.__TABLES__` timestamp, not the alias.

## Datastream Replication Pattern
Most `bronze.integrationprod` tables include a `datastream_metadata RECORD` with:
- `uuid` — Datastream replication event UUID
- `source_timestamp` — Epoch of the source Postgres change event
This confirms `bronze.integrationprod` is a Postgres replica via GCP Datastream (CDC).

---

# silver.logdata

**Project:** dw-main-silver | **Dataset:** logdata
All tables in this dataset are VIEWs pointing to `sqlmesh__logdata`.
**Retention:** Earliest data is 2025-01-01 for most tables (event_log, impression_log). viewability_log starts 2025-04-08. **Exceptions (verified 2026-07-08): `cost_impression_log` back to 2023-10-01 (fixed floor, window grows) and silver `conversion_log` to ~2024-01 — the old "no BQ data before 2025-01-01" blanket claim is false for these.** Pre-2025 data only in Greenplum coreDW (deprecated April 30, 2026). Per-physical-table retention details in `data_knowledge.md` under "Partition Filter Best Practice".

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
- **Type:** VIEW → `sqlmesh__logdata.logdata__conversion_log__143007104` (re-versioned; verified 2026-07-08) → **UNION ALL** of `dw-main-bronze.sqlmesh__raw.raw__conversion_log__3603699041` (rows ≥ 2026-01-01; DAY-partitioned on `time`, 546-day partition expiration ≈18 mo, 11.9 TB) + `dw-main-bronze.history.conversion_log_physical` (rows ≤ 2025-12-31; DAY-partitioned on `time`, no expiration, 13.2 TB). Neither branch clustered. A plain `time >=` predicate **prunes partitions through both branches** (dry-run verified, including computed/`CURRENT_DATE()`-derived bounds).
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

**Caveat — silver NULLs corrupt order_amt, rows retained** (TI-832, 2026-05-06; mechanism corrected 2026-07-08). Bronze (`dw-main-bronze.raw.conversion_log`) contains thousands of rows from 4 advertisers (34957 Harley, 33903 Bioharvest, 32023 Tarte, 63746 Networking Today) with corrupt `order_amt` in the $1B–$7.4T range — likely timestamp leakage / unit-conversion bugs at the pixel layer. The silver view wraps `order_amt` (and `order_amt_usd`) in `CASE WHEN abs(...) >= 100000000 THEN NULL` — **amounts ≥ $100M are NULLed but the ROWS are kept** (verified: Harley May–Jun 2026 bronze 8,619 rows = silver 8,619 rows, all silver `order_amt` NULL). So silver/bronze row counts match; only amounts differ. Consequences: `COUNT(*)` fire-volume analyses are safe on silver; `SUM(order_amt)` on silver is blind to any single ≥$100M fire (it shows as an amount-coverage drop, not a sum spike). **For amount-integrity investigations, query `bronze.raw.conversion_log`** (≈9-month retention). Pixel ops (Ashley Pineda Varela) flagged via TI-832 outlier sheet.

**Column facts (verified 2026-07-08, TI-1037 module-13 audit):** `ip_raw` is literally `ip AS ip_raw` in the view — identical by construction (the ui_visits ip-vs-ip_raw gotcha does NOT apply here). `order_amt_usd` IS populated (unlike ui_conversions where it's NULL) with coverage identical to `order_amt` in sampled months. `guid` is a user/cookie ID, NOT a row ID (~0.76× rows) — `COUNT(DISTINCT guid)` silently undercounts fires ~24%. No dedup needed for fire counts: exact-identity collisions ≈0.04% (raw `COUNT(*)` is the correct fire-volume signal; refires are part of the signal).

**Payload → column mapping (WGU-REV, 2026-07-08):** the `query` JSON holds the raw pixel GET params; `shoamt` → `order_amt` (ingest **digit-extracts** the string, pre-bronze; no digits → NULL — bronze `order_amt` is already final, silver casting is NOT the parse point), `shoid` → `order_id` (often a page slug, not a transaction ID), `type` → `conversion_type` (absent → NULL). `order_curr` defaults to `'USD'` even when no amount is sent. See data_knowledge.md "Conversion pixel payload anatomy".

**Retention (verified 2026-07-08):** bronze.raw.conversion_log ≈ **9 months** (not 10–90d); silver floor ≈ **2024-01** (no earlier partitions). Scan envelope (post-re-version, 2026-07-08): one-advertiser 18-month window ≈ **831 GB** dry-run / ~774 GB billed / ~35 s; unbounded full history ≈ 938 GB (the older "≈564 GB" figure predates the UNION-ALL re-version).

**ui_conversions gotcha:** NULL `conversion_type` is rendered as string sentinel **`'-101'`** in `summarydata.ui_conversions` — never filter `conversion_type IS NULL` there. Attribution layer also drops extreme amounts (empirically between $590K kept and $5.7M dropped; plausibly a ~$1M cap).

**ui_conversions.impression_time = the FIRST qualifying impression of the attribution, NOT the VV-triggering one (PS-8572, 2026-08-06):** on the SAME `ad_served_id`, `ui_conversions.impression_time` differs from `clickpass_log.impression_time` (observed deltas +46s to +34.5d, 10/10 sampled). Client-facing matchback exports surface the ui_conversions value, so chains look anchored to much older impressions than the fresh one that actually triggered the VV — reconstruct the true chain from `clickpass_log`. Also: matchback export timestamps are EDT (UTC-4), not UTC.

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
- **Join key / total-visit unit (AUDI-1173, 2026-07-28):** the join key for attribution-independent total-visit work = **`(advertiser_id, ip)`** (`advertiser_id` INT; `ip`/`ip_raw`/`original_ip` STRING, CIDR-stripped). The **total-site-visit unit = distinct visit-days per `(advertiser_id, ip, date)`** — dedup raw page-views to visit-days; guid_log rows are page-views, NOT visits (see data_knowledge fcap/total-visit gotcha).
- **Query discipline (MANDATORY):** 107 B rows / 366 TB → **always partition-prune on `time` AND cohort-restrict** (advertiser set + date bound), or read via **Databricks / GCS Spark** on the archive (`gs://mntn-data-archive-prod/guid_log/`). **Never full-scan** — a naive scan bills the whole 366 TB.
- **Use for:** site-traffic / page-view-level analysis; cause-agnostic "did this IP hit the advertiser site?" signal at IP-day granularity (after dedup).
- **DS23 svs feeder (AUDI-1091, `spark/fpa/dsid23_guid_log_processing.py`):** reads `guid_log`, left-anti-joins pixel-isolation blocked advertisers, uses **URL = `product_referer`**, and returns distinct rows into `site_visit_signal/data_source_id=23`.

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
- **⚠️ Join to CIL / win_logs on the AUCTION id:** `spend_log.auction_id` = `cost_impression_log.impression_id` = `win_logs.auction_id` (= `win_logs.request_id`) — the `<micros>.<rand>.<n>.steelhouse` id. `spend_log.impression_id` is a SEPARATE column (a UUID, sometimes literal `'1'`); do NOT join it to CIL (0 matches, verified 2026-07-30). spend_log carries `campaign_group_id`/`campaign_id` natively, so it's the recovery path for a CIL row whose `campaign_id` resolved to `-3` (INC-001: 110,732 of 110,750 `-3` rows recovered CG 131563 / campaign 648323 via this join).

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

## silver.enriched.lift__ghost_bid_* + gold.reporting.lift__ghost_bid_* (ghost-bid incrementality, Matt Brorby)
- **Purpose:** Live ghost-bid holdout lift measurement (BER-2250/INCR line). SQLMesh views; the silver ones now **materialize daily and accumulate (no TTL)** — logging live 2026-05-27, so a true ≥30-day window arrives ~late-July.
- **silver `enriched.lift__ghost_bid_visits`** — the outcomes table. Grain: `dt × advertiser_id × campaign_group_id × campaign_id × ip × arm`. `arm` ∈ {`ghost`=holdout, `submitted`=treatment}; `visited`/`converted` bools over a **7-day-from-first-bid** window; also `bid_count`, `first_bid_time`, `won` (EXPOSURE: did the treatment-eligible IP win an impression — compliance source), `first_win_time`, `first_visit_time`, `first_conv_time`, `household_score`, `eff_score` (gated/effective intent score), `is_new`, `objective_id`, `partner_id`. `enriched.lift__ghost_bid_audiences` = exposure only (real-vs-ghost bids per IP/day). Identical dev copies in `enriched__dev_matthewbrorby.*` and `enriched__dev_mbrorby_incr66.*`.
  - **Not a custom-arm estimand table (AUDI-1173, 2026-07-28):** the `visited`/`converted` window is **hard-coded 7-day-from-first-bid** with fixed arms (ghost/submitted). It is a **platform 7-day sanity/reference** for total-visit lift — NOT usable to size a ≥30-day, custom-arm (e.g. cap-8 / cap-3) frequency estimand. For a longer or custom-arm design, build the total-visit outcome directly from `guid_log` (visit-days per `(advertiser_id, ip, date)`).
- **Single-campaign read + holdout is FIXED (AUDI-1148, 2026-07-22):** slice one campaign by filtering `campaign_group_id` (entry-cohort + drop the left-censored first day, same as pooled). The **holdout is a fixed ~10% platform-wide — not a per-test tunable**, so power scales only with campaign SIZE and POOLING; a small low-VR campaign can't resolve a few-percent lift (Gruns CGID 126905, 0.19% VR → ~19 holdout visits/3wk → +15%, CI [−32%,+63%], p=0.53). The table **accumulates (no TTL)** from ~2026-06-22 (raw `bronze.raw.bid_price_log` has a ~10-day TTL, so pre-06-22 is unrecoverable); the newest usable entry day is ~7 days behind the data edge (7-day visit window must mature). Full method: `experimentation.md` §"Ghost-bid lift".
- **gold `reporting.lift__ghost_bid_results`** (per campaign×stratum, with `ghost_frac`/compliance flags) and **`reporting.lift__ghost_bid_rollup`** (per advertiser & campaign_group: `abs_itt`, `se`, CI, `z`, MH stratified estimator, conversions).
- **`lift__ghost_bid_rollup.level`** ∈ {`campaign_group`, `advertiser`} (verified AUDI-1172 2026-07-30). `level='advertiser'` (`entity_id`=`advertiser_id`) is the advertiser total **POOLED ACROSS BOTH PRODUCTS** (adv-level `n_treatment` = SUM of that advertiser's CG-level rows, 0 mismatches over 1,189 advertisers) — use it for an advertiser's OVERALL incrementality. **Gotcha: advertiser-level rows are split by `partner_id` (8, 79), NOT product** — 27 advertisers have 2 rows; sum the ≤2 partner rows (counts add; IVW-combine `abs_itt`/`se`). **To classify an advertiser's product mix, join CG-level rows to `campaign_groups.product_id` (2=Select/1=PTV) — never infer product from partner** (partner 8 carries 44 Select CGs, partner 79 carries 4 non-Select CGs). Advertiser-level lift is a cheap rollup-only pull for all ~1,189 advertisers (no `all_facts` needed).
- **CRITICAL gotchas** (full method in `experimentation.md` "Ghost-bid lift is queryable in BQ" + "the staged gate + two-instrument gotcha"): (1) **BOTH bidder legs are now in `_results`/`_rollup` (corrected AUDI-1172, 2026-07-29, superseding "Rust not folded in yet"):** `partner_id=8` = Beeswax/JVM leg, `partner_id=79` = MNTN Rust leg. **The leg maps to PRODUCT:** MNTN Select (product_id=2) delivers ~97% on the Rust leg (partner 79); PTV/non-Select (product_id=1) is ~100% Beeswax (partner 8). **No campaign_group is split across legs (Matt Brorby confirmed, 2026-07-29): the leg is 1:1 with product** — a CG is entirely Rust (Select) or entirely Beeswax (PTV), never mixed — so `all_facts` spend maps cleanly to a CG's single leg and there is NO spend-leg to restrict/gate for a CPIV denominator. The residual caveat is **measurement-basis only**: Select is measured on the clean Rust leg (symmetric fcap-cache exit) while non-Select is on Beeswax (residual `ghost_frac` multiplicity bias), so a small part of a Select>non-Select rel-lift gap could be measurement-leg rather than product — but it is NOT a within-group confound and needs no partner_id split of spend. (2) **Exclude the first window day** (`dt`=window min, e.g. 2026-06-22) — it's a left-censored stock that inflates `ghost_frac` and manufactures a spurious NEGATIVE; use the **entry-cohort anchor** (`ROW_NUMBER() OVER (PARTITION BY advertiser_id, campaign_id, ip ORDER BY dt)`=1). On the clean set `ghost_frac`→0.10 and pooled visit lift = **+5%**. (3) **The gold `ghost_bid_rollup` / `ghost_bid_results` are NOW correctly time-boxed (verified AUDI-1148, 2026-07-22)** — they apply the entry-cohort + drop-left-censored logic and reproduce the silver calc over the same full available window to the digit (Matt-confirmed for the `WHERE entity_id=` one-liner). **But the rollup is ALL-TIME only — NO `period` and NO `dt` column (verified live 2026-08-03) — so it cannot reproduce an arbitrary pre/post window; for a windowed / experiment dashboard, compute from silver `enriched.lift__ghost_bid_visits` yourself.** Simplest path is a one-liner: `SELECT * FROM dw-main-gold.reporting.lift__ghost_bid_rollup WHERE entity_id=<campaign_group_id>` (aggregate: `rel_itt`/`abs_itt`/`se`/CI/`z`/`significant_95`/`compliance_wt`/conv_*), or `…lift__ghost_bid_results WHERE campaign_group_id=<id>` for per-stratum rows (`stratum_type` ∈ overall/score_band/bid_count; score_band ∈ High/Mid/no_score — lets you confirm the intent-band composition of the audience). *(SUPERSEDED: the earlier "rollup reads spuriously −1.8% all-time" caveat was pre-time-boxing.)* (4) Result is **bid-grain ITT** (diluted by win rate; scale by win rate for served-user ATT); z is N-inflated → rank by relative magnitude, treat significance as a floor. **`incremental_visits` field = `abs_itt × n_treatment`** (raw COUNT of incremental visits over the treated arm, verified to the digit; `conv_abs_itt × n_treatment` = incremental conversions, no dedicated field). This raw count is the **sanctioned denominator for a cost-per-incremental-visit (CPIV) metric** (Matt Brorby 2026-07-29). Careful: SUM of `incremental_visits` = **volume-weighted** (emphasizes high-spend campaigns) and answers a different question than IVW-pooling `abs_itt` (= "average campaign-level lift"); on the AUDI-1172 cohort raw implies ~+100% lift vs IVW ~+22% — same data, cost-per uses the raw count, campaign-avg uses IVW. **Spend basis for CPIV (Matt Brorby, 2026-07-29):** Matt's pipeline tracks NO spend and its "visit" = pixel-fire within 7d of an IP's first bid (not a Reporting Verified Visit), so raw `all_facts` spend ÷ his `incremental_visits` over-counts. `ip_compliance` (in `_results`) = fraction of bid IPs that win an impression (~50%), so **households reached ≈ `ip_compliance × n_treatment`** — use that for cohort-matched delivery/spend instead of full-campaign spend. **To express CPIV on the CLIENT (Reporting Verified-Visit) basis** (AUDI-1172, verified 2026-07-29): do NOT scale by a pipeline→VV factor (an earlier attempt using `first_day..seventh_day_visits` was wrong). Use Matt's method — **incremental_VV = Reporting_VV × rel_lift/(1+rel_lift)**, where `Reporting_VV = SUM(clicks+views+competing_views)` (obj=1, cohort CGs, window) and `rel_lift` = the volume-weighted (raw-count) pooled pipeline lift `(Σvis_treatment/Σn_treatment)/(Σvis_holdout/Σn_holdout)−1`. Matt's ghost-bid pipeline UNDERCOUNTS Reporting visits (non-Select: 223K pipeline incr vs 6.5M reported VV; ~2.9x for non-Select/PTV vs ~1.1x for Select — empirical, both products are CTV so NOT a display-vs-CTV effect), so the pipeline-basis CPIV overstates cost 3-4x for non-Select; the VV basis is the client-facing number. Result: CPIV Select $5.23 / non-Select $8.23 (1.6x); CPIA $84 / $256 (3.0x) — vs pipeline-basis 5.1x/9.8x. (5) **Cross-campaign band aggregation MUST be inverse-variance-weighted** (`SUM(abs_itt/se²)/SUM(1/se²)`), NOT a naive count pool (`SUM(vis)/SUM(n)` gave a Simpson-confounded no_score +29%; IVW → ~0). Standard clean gate = `has_valid_holdout AND meets_min_n AND meets_min_compliance AND NOT ghost_frac_inflated AND NOT arm_imbalance_suspect AND se>0`. Refreshed 2026-07-24 gradient (rel lift): Mid +9.2% · MaxReach +6.6% · PP +1.8% · High +1.7% · no_score +0.2% (dead). **⚠️ SUPERSEDED 2026-08-19 (AUDI-1209): those magnitudes and that ordering come from dividing an IVW absolute effect by an IVW base rate, and that denominator collapses for low-baseline bands.** Re-estimating the SAME clean-gated gold strata as an inverse-variance pool on the **log risk ratio** gives no_score +23.8% (z=95.6) · PP +12.1% · High +11.5% · MaxReach +3.9% · Mid +2.6% — near the reverse order. Pool relative lift on log(p_t/p_h) with variance (1-p_t)/(p_t·n_t)+(1-p_h)/(p_h·n_h); helper `tickets/incr_75_eligible_advertisers/artifacts/incr_75_lift_stats.py`. Both readings kept: `experimentation.md` §"CONTRADICTION — the band gradient reverses".
- **(6) Do NOT hand-band `eff_score` — use the gold `score_band` strata (AUDI-1209, 2026-08-19).** `eff_score` is NOT `household_score`: on one day's partner-8 rows they agree on 59%, `eff_score` is NULL for 27% where `household_score` is never NULL (it uses -1), and 43% of `eff_score` values sit at exactly 10000. Banding `eff_score` by the documented household-score cutpoints (High ≥8001 · PP 6666-8000 · Mid 3333-6665 · MaxReach 1-3332) and joining per campaign×band against gold `lift__ghost_bid_results` matches only **51%** of cells even with the entry-cohort dedup applied (3.7% without it — omitting the dedup is the bigger error and was mine first).
- **(7) The entry-cohort estimator SELF-POISONS past ~15 days (AUDI-1209, 2026-08-19).** The table now accumulates (2026-06-22 floor → 58 days by 2026-08-19, 4.22B rows, 1,498 advertisers) but the usable window did NOT grow with it. A held-out IP never wins, so it never leaves the prospecting pool and is anchored at its first bid almost immediately, while treated IPs churn and are replaced — later entry cohorts are progressively treatment-only. Observed `ghost_frac` decays **0.1054 (06-23) → 0.0836 (08-11)** against a fixed 10% platform holdout and measured lift inflates in lockstep (**+2.8% → +16-26%**, peak +94% on 07-16); pooling the whole window reads a false **+18.6%**. Valid only while observed `ghost_frac` sits in the clean 0.09-0.11 band → **through 2026-07-07**. Audit both ends with `tickets/incr_75_eligible_advertisers/queries/incr_75_entry_cohort_byday_window.sql`. Clean-window pooled read (log-RR, 1,054 advertisers): visits **+4.66%** CI [+4.35%, +4.96%], conversions **+3.33%** CI [+1.77%, +4.91%].
- **(8) `partner_id` 79 is in silver `lift__ghost_bid_visits` from the week of 2026-07-05 and is NOT usable (AUDI-1209, 2026-08-19).** Its observed `ghost_frac` runs 0.066-0.083 from its first week and it reads **+128% to +290%** relative lift. Filter `WHERE partner_id = 8` for any silver entry-cohort read until Matt Brorby confirms its holdout write path.
- **Product split (Select vs PTV) + rollup grain (AUDI-1172, 2026-07-28):** `reporting.lift__ghost_bid_rollup` has `level` ∈ {`advertiser`, `campaign_group`} — filter `level='campaign_group'` for the per-CG estimate; `entity_id` = campaign_group_id. **Join `entity_id → dw-main-silver.public.campaign_groups.campaign_group_id`** (the PK column is `campaign_group_id`, NOT `id`) to pick up `product_id` (2=Select / 1=PTV / 3=QF). Select is **sparse** in these views — ~44 advertisers / 112 CGs vs PTV ~1,173 / 2,114 — because the queryable leg is Beeswax-only. All rows are `objective_id=1` (prospecting): the holdout is prospecting-only by construction (held-out IPs never win → never leave the pool). To compare Select vs non-Select per advertiser, IVW-pool CGs → advertiser×product (weight `1/se²`) on `se>0 AND NOT low_coverage`. Data floor for the queryable window ≈ **2026-06-22** (no backfill; live physical table currently `sqlmesh__enriched.enriched__lift__ghost_bid_visits__2999749496`).
- **Verified live 2026-08-03 (Slack Q, Nick Martin building a generalized incrementality dashboard):** `reporting.lift__ghost_bid_rollup` is a **VIEW** over `dw-main-gold.sqlmesh__reporting.reporting__lift__ghost_bid_rollup__4089669024` (physical table refreshed **daily**; 2026-08-03 = 3,519 rows). **Read freshness off the physical `sqlmesh__` table, NOT the view** — every `lift__`/`ghost` object in `reporting`+`enriched` is a VIEW, so its `lastModifiedTime` (2026-07-26) is DDL-author time, not data freshness. **NO `dt` and NO `period` column** — grain is one row per `level × entity_id × partner_id` (35 cols); it is an all-time grand-total cross-check only, useless for a pre/post window. `level` counts: advertiser 1,232 rows / 1,202 distinct (30 advertisers appear under BOTH partners 8 & 79 → re-aggregate the ≤2 partner rows), campaign_group 2,287 rows / 2,287 distinct (1 per CG, no split). **Clean gate is NOT fully baked into the rollup:** IVW pooling + campaign-inclusion (`n_campaigns_incl` vs `n_campaigns_total`) are, but you still filter `se>0 AND NOT low_coverage` yourself (and `ghost_frac`/compliance on `_results`). Silver `enriched.lift__ghost_bid_visits`: MIN(dt)=**2026-06-22** (floor, no backfill), MAX(dt)=**2026-08-02** (1 day behind), **42 contiguous daily partitions, 3.30B rows**; `arm` exactly {`submitted`=2.98B treatment, `ghost`=322M holdout, **~9.7% share**}. **7-day-from-first-bid visit window → effective analysis edge = MAX(dt)−7d = 2026-07-26; end any window ≥7 days before the data edge or lift is right-censored / falsely negative** (Matt's ElevenLabs query encodes this via `analysis_end = DATE_SUB(MAX(dt), INTERVAL 7 DAY)`). Only ~6 weeks exist → a matured 30-day window only since late July; no long pre/post horizon yet. Always partition-prune on `dt`.
- **Separate NEWER holdout-lift lineage (discovered 2026-08-03, redeployed 2026-08-02 — distinct methodology, NOT a v2 of ghost-bid):** alongside the ghost-bid views, `dw-main-gold.reporting` + `dw-main-silver.enriched` carry a **holdout-based (observational) lift** family redeployed 2026-08-02 (~1 wk after the ghost-bid views' 2026-07-26 deploy): reporting `lift__conversions`, `lift__holdout_advertisers`, `lift__holdout_conversions(_export)`, `lift__holdout_results_step1/step2`, `lift__results_by_month_raw`, `lift__stg_holdout_results_by_day`, `v_lift__conversions`, `v_lift__results_by_month(_review)`; enriched `lift__holdout_advertisers/_audiences/_campaign_groups/_households/_visits`. This is holdout-based lift, **not** ghost-bid ITT and **not** a rename/supersession of `lift__ghost_bid_rollup`. **Open Q — which is canonical for a generalized incrementality dashboard: ask Matt Brorby.** (Naming: all incrementality reporting is under the `lift__` prefix; zero objects contain `incr`.) See memory `reference_holdout_lift_lineage`.
- **Holdout-lineage grain + arm semantics (verified live AUDI-1215, 2026-08-21):** gold `reporting.v_lift__results_by_month` = one row per MONTHLY run per advertiser×objective; latest run `begin_date` = 2026-07-01 as of 2026-08-21 (**NO August run**; physical table last modified 2026-08-11), so any post-window read caps at 07-31 until the next run lands. **Control arm is advertiser-level only, never campaign_group-attributed** (`campaign_group_id` NULL on control rows); treated rows ARE CG-attributed. `multiplier` = `users_reached/control_users`. Window splits run in **America/New_York**. `v_lift__conversions` carries both arms with conversion timestamps and attributes treated conversions via a **43-day (3,715,200s) impression lookback**, so a post-change window is contaminated by pre-change impressions (AUDI-1215: 27.8% of post conversions attached to pre-period impressions, 14.5% to the blackout); the carryover flatters POST, so a measured post decline is a LOWER bound. Split windows on the impression date. Fixed MD5 membership = no entry-cohort depletion, structurally right for a pre/post read (AUDI-1215 used it as the powered conversion instrument); only the RATIO of its attributed-style multiplier over time is meaningful, never the level. Full detail: memory `reference_holdout_lift_lineage`.
- **`enriched.lift__ghost_bid_audiences` has NO audience_id column despite the name (verified live AUDI-1215, 2026-08-21).** Cols: `dt, advertiser_id, campaign_group_id, campaign_id, objective_id, partner_id, ip, arm, bid_count, first_bid_time, household_score, eff_score, household_score_threshold`. 4.24B rows / 468 GB, dt-partitioned, NOT clustered. An audience SWITCH cannot be evidenced from the lift tables; recover targeting history from `silver.archives` (`audience_x_campaign_group_archives` + `audience_segment_archives` + `audiences_archives`, see §"Config-change AUDIT tables"). Same date: `lift__ghost_bid_visits` MAX(dt) = 2026-08-20 (accumulating); a full-history CGID-filtered scan bills ~277 GB (no clustering, so `campaign_group_id` filters prune rows, not bytes), meaning the 5 GB dry-run cap is unachievable on any full-history read; it runs on the us-central1 flat-rate reservation.

---

## frequency_caps config tables (fcap knob) — which are populated (AUDI-1173, 2026-07-28)
- **`advertiser_frequency_caps` = EMPTY (0 rows)** — the advertiser-level fcap table exists but is unused; there is no advertiser-scoped cap in prod. (Confirms the "no advertiser rollup" fcap defect from the counter side.)
- **`integrationprod.campaign_group_frequency_caps`** and **`integrationprod.dso_frequency_caps` = POPULATED** — the real cap config lives at campaign_group + DSO scope. These sync to the bidder cache (`do_fcap`, Redis counters keyed on IP). See data_knowledge "IP Frequency Capping (fcap)" for mechanics.

---

## silver.logdata.cost_impression_log
- **⚠️ CIL can re-stamp real impressions to `campaign_id = -3` (unresolved-campaign sentinel) — a campaign-resolution regression (PROVEN 2026-07-29, INC-001):** CIL = `spend_log` (wins/spend, source of truth) + `win_logs` (Beeswax wins), and resolves `campaign_id` via a dim join. A build/reprocess can regress a resolved partition: the rows keep their impression but get `campaign_id = -3`, so the real id reads **0** while the count sits under `-3`. Verified: Bombora campaigns (CG 131563) `dt=2026-07-27` = 0 under 648318-648323 but **110,750 under `-3`** (spend_log 110,792 / $904 billed / 100% rendered; win_logs 110,862); time-travel showed 109,530 correctly attributed 47h ago → 0 now. Cascaded `enriched_impressions=0` (can't map `-3` to a segment). **The rows are NOT lost — check `campaign_id=-3` and reconcile vs `spend_log` before trusting a CIL per-campaign zero.** `-3` = unresolved campaign; real ids are positive. **The re-stamp blanks `campaign_id`, `group_id`, AND `creative_id` together — only `advertiser_id` (and `partner_id`) keep real values, so a `-3` row is identifiable/backfillable ONLY by `advertiser_id` (verified 2026-07-30: adv 30506 07-27 `-3` slice = 110,750; the `group_id`/`creative_id` on those rows are also `-3`). Spend breakdown (`media_spend`/`data_spend`/`platform_spend`) is NULL on `-3` rows, though `media_cost` stays populated.** **Time-travel to confirm a regression: the VIEW ignores `FOR SYSTEM_TIME` ("Snapshot time ignored ... because it is a view") — query the PHYSICAL `dw-main-silver.sqlmesh__logdata.logdata__cost_impression_log__2498930125 FOR SYSTEM_TIME AS OF ...` (worked at 26-47h back; ~48h horizon).**
- **⚠️ JOIN KEY — recover the real campaign (or any dim) for a CIL row, incl. a `-3` one: `cost_impression_log.impression_id` = `spend_log.auction_id` = `win_logs.auction_id` (= `win_logs.request_id`) — the `<micros>.<rand>.<n>.steelhouse` id.** spend_log's OWN `impression_id` is a DIFFERENT column (a UUID, sometimes literal `'1'`), so `CIL.impression_id = spend_log.impression_id` returns **0 matches** (verified 2026-07-30 — the naive join fails). Join `CIL.impression_id → spend_log.auction_id → spend_log.campaign_group_id`/`campaign_id` (spend_log carries both natively). Row-level proof of the INC-001 `-3`: of 110,750 `-3` rows (adv 30506, 07-27), **110,735 matched spend_log by auction_id and 110,732 carried CG 131563 / campaign 648323 (Bombora)** — so the `-3` rows ARE Bombora, and spend_log (CIL's input) had the correct campaign, meaning the break is in the CIL build's campaign resolution, not the input.
- **Retention: NOT 90 days — floor is 2023-10-01 (fixed, so the window GROWS)**: the live table (`sqlmesh__logdata.logdata__cost_impression_log__2498930125`) has 1,012 contiguous daily partitions 20231001→today, verified with row counts (2023-10-15 = 53.6M rows; 2024-09-15 = 92M) on 2026-07-08 (TI-1037); re-verified 2026-08-11 = **1,047 partitions, 77.6B rows**, floor still 20231001. ~33 months of history today, +1 month per month. Supersedes both the old "90d TTL" note and the 2026-07-07 "floor ≈ 2025-01-01" estimate. `household_score` is NULL on ALL pre-2025-06 rows (verified same check) — IP reach is computable to Oct 2023, HI/score analysis only from Jun 2025.
- **Type:** VIEW → `sqlmesh__logdata.logdata__cost_impression_log__2498930125` (**TABLE** — physical, 71 B rows / 56 TB)
- **Partition:** DAY on `time`
- **Clustering:** advertiser_id, impression_id
- **GCS archive:** **None — BigQuery-only dataset.** Stream from BQ via Spark BigQuery connector (efficient with the table-only mode; SQLMesh physical name resolved at runtime). (via Victor Savitskiy 2026-04-28, TI-837)
- **Use for:** Impression-level spend enriched with geo, device, segment data.
- **⚠️ RETENTION CORRECTION (AUDI-1070, verified 2026-06-30):** NOT 90-day rolling — CIL retains **multi-year history**. Empirically probed back to **2024 and earlier** (82.5M rows on 2024-06-15; 84.7M on 2025-02-01; agent MIN(time)≈2023-10). A Jan-2024→present per-impression analysis IS feasible from CIL. Cost-control: always partition-prune on `time` (one day ≈ 0.68 GB) + exploit `advertiser_id` clustering; a 4-month × 3-AID score scan billed ~12 GB. **MCP `bigquery` tool historical failure FIXED 2026-07-16** (it ran with `--location US`; dataset isn't US — `.mcp.json` now sets `us-central1`); `bq`/`bq_run.sh` remain the default path.
- **⚠️ SCORE COLUMNS (AUDI-1070):** `advertiser_household_score` (MM-tuned per-advertiser) and `household_score` (general) are INT 0–10000 (−1/NULL = unscored). **BOTH columns are 100% NULL before 2025-06-01 and ~0% NULL from 2025-06-01 onward — a sharp, platform-wide, single-week cutover (verified AUDI-1070 2026-06-30: 100% NULL wk of 2025-05-25 → 0% NULL wk of 2025-06-01).** This is a **CIL LOGGING change** (the score columns began being written into cost_impression_log on 2025-06-01), **NOT** a scoring-pipeline onset — the bidder scored households before this date (scores lived upstream/in the bidder), CIL just didn't carry the columns. **Consequence: the TYPED COLUMNS cannot answer "score distribution / % under 8000 / scored-fraction over time" before 2025-06-01.** BUT scores are RECOVERABLE one cutover earlier — they first appear in the `model_params` STRING on **2025-05-06** (another clean 0%→100% overnight cutover). So the recoverable CIL score floor is **2025-05-06**, not 2025-06-01: `COALESCE(advertiser_household_score, SAFE_CAST(REGEXP_EXTRACT(model_params, r'advertiser_household_score=(-?\d+)') AS INT64))` (same pattern for household_score). **No CIL score history of any kind exists before 2025-05-06.** Do not read the NULL→populated transition as a performance event. Unscored encoding: **HS = −1; AHS = NULL/−1** (the two diverge — retargeting rows have HS=−1 but AHS=10000). Where populated, AHS scored impressions are nearly all at/near max (~9,900) → AHS is effectively **binary (scored vs unscored)**; the meaningful signal is the **scored fraction**, not the level. **RTC caveat: `realtime_conquest_score=…` is logged on ~100% of rows regardless of whether RTC fired — do NOT exclude rows merely containing that token.** Genuine RTC = `realtime_conquest_score=10000`; value −1 = RTC not active (the case for Caraway/Avon/HexClad). CIL partition-prunes on `DATE(time)`, clusters on `advertiser_id`. **Beeswax-leg caveat (observed AUDI-1148, 2026-07-22, not fully explained):** for Beeswax-bidder campaigns (campaign names prefixed "Beeswax …"), CIL `household_score` can read **−1 (unscored) across ALL impressions** even for a scored prospecting campaign — every Gruns CGID 126905 impression (Jun–Jul 2026) had HS=−1 despite the campaign targeting scored mid/PP intent. Consistent with Beeswax CIL enrichment being sparse (cf. `sh_device` often NULL for Beeswax). For Beeswax-leg intent-score analysis, use the ghost-bid lift table's `eff_score`/`household_score`, not CIL HS; verify before relying on CIL HS for a Beeswax advertiser.

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
| hour | DATETIME | Partition key |
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
| first_day_visits through seventh_day_visits | INTEGER | **LAST-TOUCH visits bucketed by day-since-impression (days 1-7; `visits_tail`=day 8+).** Pairs with `competing_first_day_views..competing_seventh_day_views` (the first-touch side). **NOT the Verified Visit** — it omits `clicks`, first-touch, and CTV view paths (for Select/CTV these day-buckets are ~0 while VV is large). The **Verified Visit = `clicks+views+competing_views`** (see note below). Corrected AUDI-1172 2026-07-29 (an earlier note here wrongly called this the 7d VV). |
| *_arr | STRING | HyperLogLog++ serialized arrays for unique counts |

---

## silver.summarydata.conversion_facts
- **Type:** VIEW → `sqlmesh__summarydata.summarydata__conversion_facts__3549666587` (**TABLE** — physical)
- **Partition:** DAY on `hour`
- **Clustering:** advertiser_id, campaign_id
- **Use for:** Pre-aggregated conversion metrics by campaign/geo/device/hour.

| Column | Type | Notes |
|--------|------|-------|
| hour | DATETIME | |
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
| hour | DATETIME | |
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
| hour | DATETIME | |
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
- **STALENESS GOTCHA (verified 2026-05-01):** This rollup view is currently lagging by ~17 days (max=2026-04-14 when current date is 2026-05-01). Same lag affects `sum_by_campaign_group_by_day` and `sum_by_advertiser_by_day`. Verify max(day) before using for recent-window analysis. The downstream `silver.aggregates.agg__daily_sum_by_campaign` was empty since 2026-03-31 and has since been **deleted entirely** (2026-08-19). **For fresh data, query the underlying fact tables directly: `silver.summarydata.{impression_facts, visit_facts, conversion_facts, spend_facts}`** — these are at hour grain and stay fresh through current day.

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
- All go back to 2024-01-01. **2024-01-01 is a platform-wide silver floor, not a per-table quirk (verified 2026-08-20, AUDI-1213):** `sum_by_advertiser_by_day` and `sum_by_campaign_by_day` return the identical `MIN(day)` and the identical advertiser counts (6,232 advertisers with impressions since then; 1,863 delivering within 30d, 4,369 lapsed). An advertiser whose last delivery predates 2024-01-01 is **absent entirely**, not merely truncated.
- **To reach further back, `summarydata.all_facts` starts 2020-10-01** — and it is the only option. Measured 2026-08-20: 2,281 advertisers delivered before 2024-01-01, of which **1,410 never delivered again** and so are invisible in every silver day-rollup ($149.03M pre-2024 `media_spend`, 953 of them above $10k). Cost of that reach: an advertiser-grain last-active scan over `all_facts` is **2,683 GB** against **0.161 GB** on `sum_by_advertiser_by_day`. `all_facts` also carries `media_spend` only (no data/platform legs), so its totals are NOT on the advertiser-facing basis. It has `require_partition_filter: false`, so always bound `hour` explicitly or it full-scans.

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

## silver.aggregates.agg__daily_sum_by_campaign — DELETED
- **🚫 THE VIEW NO LONGER EXISTS (verified `bq ls` 2026-08-19, AUDI-1209).** The whole `agg__*` family is gone from `silver.aggregates`; the dataset now holds only the `campaign_group_log_*`, `tpa_*`, `win_rate_*` and identity views. Any query naming it fails outright with "Table ... was not found in location us-central1" — it does NOT return zero rows, so the break is loud. **Replacement: `summarydata.sum_by_advertiser_by_day`** (already advertiser grain, fresh through the current day, same `media_spend`/`data_spend`/`platform_spend` columns) or `sum_by_campaign_by_day` for campaign grain. Everything below is retained for history only.
- **Type (historical):** VIEW → `sqlmesh__aggregates.aggregates__agg__daily_sum_by_campaign__11365516`
- **Partition:** DAY on `day`
- **⚠️ FROZEN — do NOT use for any current or historical window (verified 2026-08-11):** 242 partitions spanning **2025-09-01 → 2026-04-30** only, unchanged for 3+ months. A trailing-12mo or lapsed-advertiser window returns **zero rows silently**. Use `summarydata.sum_by_advertiser_by_day` (advertiser × day, 2024-01-01→current) or `sum_by_campaign_by_day` (campaign × day, 2024-01-01→current) instead. Also note its entire reach/site-visitor family (`uniques`, `site_visitors`, `*_users_reached`) is empty across all history.
- **Use for:** nothing current. Historical campaign rollup inside Sep 2025 – Apr 2026 only.

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

## Identity Graph — MNTN ID / household resolution (ID-327, epic AUDI-1049)

The `idg` identity graph (Experian backbone + first/third-party edges, shared-IP disambiguation
built in — ~80% of IPs map cleanly to one household) is published to the DW as two objects. This
is the **source of truth for IP → MNTN ID (household) mapping**; do not build IP→household bridges
by hand. Used by the Fangorn-on-MNTN-ID feature store (AUDI-1049/1055/1056/1100).

### bronze.raw.identity_graph_history
- **Type:** TABLE (physical) — daily **as-of history** of the graph.
- **Use for:** Point-in-time joins (training, backfill). Join per-IP daily aggregates as-of on
  `as_of_date` within the `start_time`/`end_time` interval.
- **Key columns:** `id`, `id_type`, `household_id`, `is_shared`, `confidence_score`,
  `start_time`, `end_time`, `as_of_date`, `graph_version`.
- **Join key:** filter **`id_type = 30` (IPV4)** → direct IP→household mapping. Same table carries other id
  types keyed as `(id, id_type)`: **`id_type = 42` = GUID** (confirmed Sean Yang 2026-07-29; `guid_log`
  carries `guid`), IPv6 is its own id_type, plus hashed-email/hashed-phone. **`guid_log` has NO IPv6** → IPv6
  only matters once `augmentor_log` enters training (excluded from the Fangorn v1). Open (Sept-4): whether to
  add GUID (42) as a 2nd resolution identifier alongside IPv4 (30).
- **Shared IPs:** `is_shared = TRUE` → weight/threshold on `confidence_score`; out-of-graph IPs go to a
  fallback bucket (excluded from MVP training AND serving per 15-Jul decision).
- **Retention:** RFD states **60-day**; the AUDI-1057 decision log claims graph TTL "90+ days,
  sufficient for training." **Unresolved contradiction** — AUDI-1101 exists to confirm mapping-history
  depth vs the ~14-day-label backtest window. Verify before assuming backfill depth.
- **Dropped columns:** history table omits `IDStability` and household geo (present in underlying graph).

### silver.public.identity_graph
- **Type:** VIEW pinned to the **latest** graph version.
- **Use for:** current-graph translation (HHDSC, TPA export) — NOT point-in-time. Use the `_history`
  table for any training/backtest join.

### household_graph_parquet (deep as-of snapshot, GCS parquet)
- **Type:** GCS parquet — the graph team's full graph output; the **deep-history** source behind the
  ~60-day BQ `identity_graph_history` (use this when the backtest window exceeds BQ retention). **~600 GB.**
- **Partitions:** `as_of_date` **AND** `as_of_date_revision_number` (usually `0`; occasionally `1` when the
  graph is rewritten/revised). A `graph_version` column exists **but is NOT part of the partitioning**.
- **As-of join pattern (partition elimination):** for a lookback day D (e.g. one day of `guid_log`), take
  `max(as_of_date) < D`, then within it `max(as_of_date_revision_number)`, then look up — so you join against
  the graph *as it was*, not the current graph. A partition never changes once written (the 07-20 graph stays
  the 07-20 graph). **TTL unconfirmed** (AUDI-1049 action item — must be retained for historical training).
- **Cost note:** materializing a daily subset snapshot (AUDI-1166 L1 mirror) is a **~7-min job**; joining the
  full 600 GB parquet every run is the alternative Sean is cost-testing. Source: Ryan Kleck design sync 2026-07-28.
- **Resolution rule:** a row with multiple ids (IPv4/IPv6/GUID/HEM) → join per-id, take **max
  `confidence_score`**, one household per row (matches the id-service `resolveHouseholdId` endpoint). See
  `data_knowledge.md` § "MNTN ID (household) re-keying of the feature store".

### silver.identity.graph_translation_signal (graph-vendor crediting log)
- **Type:** TABLE (dev version being built by Weiang Li / ID team, 2026-07-29). The **crediting log** for graph
  usage under MNTN ID — modeled on today's `hashed_email_signal` table.
- **Use for:** record **every event where an ID is translated into a household_id** in the feature store, so
  graph vendors (whose licensed data is in the graph) can be credited. **Required even when the FS sources only
  internal logs (guid/augmentor)** — the crediting is for the graph, not the source log.
- **Producer:** AUDI feature-store code (AUDI-1167 resolution path). The ID team is shipping a **pyspark graph
  interface** (current-graph selection + translation logging) ~early Aug that Sean Yang drops into the FS code.
- **Related:** DDP-vendor crediting (non-graph vendors, e.g. DS13/19 usage) may also change under MNTN ID —
  separate, ~mid-October, see `data_knowledge.md` DDP billing + `reference_ddp_billing_logic`.

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
  - Date level: `dt=YYYY-MM-DD/`, then hour level `hh=HH/` (the svs feeders read `region={east,west}/dt=$dt/hh=$hh`)
  - Full path example: `gs://mntn-data-archive-prod/augmentor_log/region=east/dt=2026-04-23/`
  - Earliest partition: ~`2026-03-30` (archive history is ~30 days; not infinite as the "no TTL" framing suggests)
  - For a complete daily scan you must read BOTH `region=east` and `region=west`. Spark loads both if you read the parent path with `.filter("dt = '2026-04-23'")`, but explicit per-region paths give better partition pruning.

- **`placement_type` composition (AUDI-1091, 1-hr sample 2026-07-20):** ~75% of rows are `VIDEO` (CTV/video, and **99.6% carry no `page`/`referrer` URL** — not site visits), ~25% `BANNER` (~100% URL-bearing, ~9M IPs/hr), and a tiny `BANNER_AND_VIDEO` sliver. The svs DS30 feeder (`spark/fpa/dsid30_augmentor_log_processing.py`) **keeps only BANNER + BANNER_AND_VIDEO**. BQ scan cost: a 1-hr projection of placement_type/page/referrer/ip ≈ **67 GB**; a full day ≈ **1.54 TB** → use Spark on the GCS archive for full scans, not BQ.
- **DS30 svs feeder mechanics (AUDI-1091, `spark/fpa/dsid30_augmentor_log_processing.py`):** builds site-visit rows from **BOTH the `page` and `referrer` columns** (the referrer-derived visit is timestamped 1s earlier than the page visit), normalizes URLs by prepending `http://`, requires non-empty `ip`, and first-touch dedups per `(ip, url)`.
- **`aug_log_ip` feature-store substitute (TI-933):** the airflow-ti feature-store output `aug_log_ip` can substitute for raw `bronze.raw.augmentor_log` as the biddability filter in Spark lift runs — much smaller, same biddability filter.
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

## dw-main-gold.bae.v_daily_goal_by_campaign_group + v_campaign_feature_date — daily goal & feature snapshots (goal-attainment spine, discovered 2026-07-21)
- **Owner:** `bae` gold dataset. These are the tables the live Mode report **"Campaign Groups Hitting Goal Percentage"** is built on (report token `30fb4d3f8447`; see data_knowledge.md "Hitting-goal %"). Ready-made source for "did the customer hit their goal?".
- **`v_daily_goal_by_campaign_group`** — authoritative **as-of-day goal** per campaign group. Grain: one row per `campaign_group_id` × `day`. Cols: `day` DATE, `advertiser_id` INT, `campaign_group_id` INT, `goal_type_id` INT (decode via `silver.core.goal_types`), `goal_type_name` STRING, `goal_value` NUMERIC, `current_record` INT (1 = currently-effective goal). **Prefer over live `campaign_groups.goal_value`** when you need the goal as it stood on a date. Join to performance on `campaign_group_id` + `day`.
- **`v_campaign_feature_date`** — maps each campaign group to the **targeting feature** active over a date interval. Cols: `advertiser_id`, `campaign_group_id`, `feature_enabled_date`, `start_date`, `end_date` (report treats NULL as open via `COALESCE(end_date, DATE '2100-01-01')`), `feature_name`, `feature_type`. `feature_type` domain (2026-07-20) = **`KW`** (BUK keywords), **`PP`** (Peak Performance), **`KW+PP`** (both). Join where `start_date <= day < COALESCE(end_date,'2100-01-01')`. Lets you cut goal attainment by AUDI feature.
- **Goal-type coverage (2026-07-20, 94,906 cgs-with-a-goal):** CostPerVisit(13) 31.9k · CostPerCompletedView(14) 22.7k · ROAS(1) 18.4k · CPA(16) 16.7k = **94.5%**; remainder (Efficiency(9) 5.2k = reach/awareness, no pass/fail number) unscored.
- **Performance source the report trusts:** `dw-main-gold.summarydata.sum_by_campaign_by_day` (GOLD copy; silver equivalent also exists) joined to `dw-main-silver.public.{campaigns,campaign_groups,campaign_groups_raw}`. Advertiser spend **tier** from `dw-main-gold.summarydata.sum_by_advertiser_by_day`: SMB <$25k/mo, Mid Market $25–65k, Upper Mid Market ≥$65k.

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
- **Use for:** **Authoritative `data_source_id` → name registry — decode the `data_source_id` (DS) leaves in
  audience expressions.** Also the conversion-pixel registry (joins to conversion_source_id in summarydata).
- **Key columns:** data_source_id, name, display_name, description, data_source_key, data_source_type_id,
  conversion_type_display_name, is_mobile. (`datastream_metadata` is a RECORD — breaks CSV export; select scalar cols.)
- **Query:** `SELECT data_source_id, name, display_name, description FROM audience_data_sources ORDER BY data_source_id`
- **DS id → name (verified TI-1037 2026-07-06; these are the AUTHORITATIVE names — supersede informal labels):**
  `-1`=MNTN Pixel · `2`=MNTN First Party (1P) · `3`=MNTN Third Party · `4`=CRM · `8`=IP List · `11`=LiveRamp ·
  `13`=MNTN Vertical Categorization · `14`=**MNTN Global Data** (the `DS14[1]` availability gate; NOT "Beeswax Bidder"
  — that's the category name) · `16`=**MNTN Taxonomy Data** (its per-advertiser categories = the advertiser's own
  **funnel tags** Impressions/Wins/PageViews/Conversions/VV › stage › CampaignGroupID › CampaignID — this is what the
  "net-new gate" targets; catalog in `bronze.tpa.categories` WHERE data_source_id=16 AND advertiser_id=<AID>) ·
  `17`=ShareThis (3P; decoded TI-1037 2026-07-08) · `18`=Dstillery (3P) ·
  `19`=**MNTN Matched** (this is "MM") · `21`=MNTN Conversion · `34`=MNTN Pageview · `35`=**LiveRamp IP** (the "3P"
  segments) · `46`=ML Audience Intent Scoring Model (RTC scoring) · `47`=CRM Identity Graph Generated · `42`=MNTN Select ·
  `51`=Bombora · `25`=5x5. Interest sources = DS13/19 (MM) + DS35/17/18 (3P); DS16 = funnel gate; DS2/21/34/47 = exclusion/suppression.
- **DS13/19/46 co-occurrence gotcha (TI-1037 2026-07-08):** at segment level DS46 NEVER co-occurs with DS13 (the
  `onFangorn` flip swaps 13→46); DS19 survives the flip. "MM = has DS19" undercounts the MM-scored layer by ~7.6% of
  prospecting spend (DS46-only + DS13-only cells) — full 8-cell table in data_knowledge.md § `"MM = has DS19" is an undercount`.
- **Official product names for the MM components (Matt Brorby 2026-07-08):** DS19 = **"MM Core"** (Keyword-Only) ·
  DS13 vertical anchor = **"Peak Performance"** · DS46 = **"Peak Performance v2"** (Fangorn) · DS13-with-bucket-ids =
  "Expanded Peak Performance" (named, unshipped — zero live campaigns carry bucket ids). Live DS13/DS46 leaves hold ONLY
  the 6-digit vertical id (= the RTC id); config space = 2×3 grid (DS19 y/n × anchor none/13/46).

## bronze.integrationprod.core_advertiser_conversion_types
- **Type:** TABLE — **auto-registered conversion-type registry** (one row per advertiser × conversion_type × conversion_source_id)
- **Use for:** dating client-side pixel/tag changes. `create_time` = the FIRST occurrence of that conversion_type in conversion_log (verified to the second: WGU `app_submitted` registry 2025-09-30 22:57:04 == MIN(time) in the log). A new row here means the advertiser's tag started sending a new `type` param — a client-side change, not an MNTN config action.
- **Columns:** advertiser_conversion_type_id, advertiser_id, conversion_type, conversion_source_id, create_time. No user_id, no deleted.
- **Sentinels — SIX exist, not two (verified 2026-07-08, TI-1037 module-13 audit):** `-100` (source `-1`, legacy MNTN Pixel unnamed conversion), `-101` (source `23`, since the platform-wide 2025-01-10 NULL→23 source migration), **`-102`** (source `31` offline events; 21 advertisers, 2025-08-13..2026-02-23), **`-105`/`-106`/`-107`** (sources 328494/328496/328497; first registered 2025-01-15, -105/-107 continue through late 2025). Each code binds to a single source_id across unrelated advertisers = platform pseudo-types. A registry-wide `REGEXP_CONTAINS(conversion_type, r'^-[0-9]+$')` matches exactly these six and NO genuine client or pentest-injection type — use the regex, not a hard-coded pair, when excluding sentinels. `ui_conversions` renders untyped conversions with the `-101` string.
- **Hygiene (full-scan verified 2026-07-08):** 141,431 rows; zero duplicate (advertiser, type, source) triples; zero NULL conversion_type. Re-registration of an existing type under a new source_id is negligible (26 pairs platform-wide). The two mass backfills (2023-11, 2024-09) are ~100% `-100` sentinel rows.
- **⚠️ 50M+ ID namespace (since 2026-03-31):** an automated flow registers exactly one `'Purchase'` type (source 23) per sequential advertiser_id in a **50M+ namespace that does NOT exist in `integrationprod.advertisers`** (dim max ID ≈3.5M) — ~2,100–2,600 regs/month. These are not platform advertisers; DISTINCT-advertiser counts on the registry are inflated ~10–30× from Apr 2026. Real new advertisers (classic IDs) almost never get a first-month Purchase registration (0.19%). Separately, the Apr/May 2026 registry ROW explosion (12.6K/8.9K non-sentinel regs) is ~76–79% classic-namespace rows concentrated on ~90 advertisers — a fuzzing/pentest-shaped many-types-per-advertiser pattern.
- **Gotcha:** because registration is automatic from the log, junk types pollute it — WGU has ~75 SQL-injection/XSS payload rows from a 2026-02-07 pentest (Burp/oastify.com); other garbage at trace scale (`[object Object]`, `undefined`, `true`, epoch-millis strings).
- **conversion_source_id decode** (via `data_sources`): `-1`='MNTN Pixel', `23`='guid_log' (display 'MNTN Pixel'/'Website Event', created 2024-10-04 — the post-2025-01-10 standard for website pixel events), `21`='MNTN Conversion'.
- **Related pixel-config tables (no separate `conversion_sources` table exists):** `core_pixel_integrations`(+`_types`) (e-commerce integrations), `ui_advertiser_pixel_infos` (pixel notes/URLs), `attr_advertiser_waypoints_event_mapping` + `attr_advertiser_selective_performance_config` (event classification), `advertisers` pixel flags (`populate_order_on_conversion`, `conv_pixel_opt_out`, `pixel_isolation`, `allow_duplicate_orders`).

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
- **Scope (verified 2026-07-10, AUDI-1089 q0):** meters ALL CPM-billed DDPs, not just MM site-visit —
  MM (24/28/33/36/40 @ $0.50), interests (17 ShareThis @ $0.95; 35 LiveRamp IP variable_cpm, implied
  $1.19–1.32), CRM (29 deepsync @ $0.50). Flat-fee vendors (25/26/39) never appear.
- **Meter math:** `usage = impressions × (registry fixed_cpm / 1000)` — exact per month for every
  fixed-CPM source Jan–Jun 2026. **REGIME CHANGE at reporting_month 2026-05 (AUDI-1092 residue
  analysis, 2026-07-13):** Jan–Apr 2026 `impressions` are ~100% FRACTIONAL (clean 1/N fractions =
  credit split across contributing vendors); May 2026+ are 100% INTEGER (single-vendor credit).
  Never mix the regimes in savings arithmetic.
- **Join to registry:** `data_source_id` here is INTEGER; `tpa.direct_data_partners.data_source_id`
  is STRING — cast one side. Use `reporting_month` for month rollups; closed months have `status='Complete'`.
- **`dt` = MONTH-END SNAPSHOT ONLY** (last day of reporting_month) — mid-month dt filters silently return
  zero rows (AUDI-1089, 2026-07-10). `domains` RECORD (`domains.list[].element`) = the billed-credit
  domains, populated ONLY for MM site-visit CPM vendors (24/28/33/36/40); ~50% of 28/33/40 imps sit on
  unattributed aggregate rows (Justuno 80% / Cybba 86% attributed). `data_source_category_id` is NULL for
  ALL MM-vendor rows — no DS13/DS19 split in the meter (that split lives in `external.targeted_signal`,
  which IS BQ-queryable — see the DDP-pipeline input-tables section below; corrected 2026-07-20).
- **Credit semantics (current, May 2026+):** single-vendor credit per used (ip,url,DATE) —
  first-reporter (AP-3779) or cheapest/free-priority (winner rule unconfirmed; dbt
  `targeted_signal_ds_13/19`); paid only if used (DS13 OR DS19 path). **Free logs do NOT preempt
  paid credit (Sean Yang 2026-07-13, AUDI-1093).** Jan–Apr 2026 was fractional-split. See
  data_knowledge § Site Visit Signal. **NUANCE (2026-07-17, AUDI-1115 §4f): the upstream gold
  winners table shows CROSS-PATH fractional splitting is alive in June** — an impression matching
  both a 3P segment path (e.g. DS17) and the MM path carries `impression_cnt=0.5` on the MM row;
  the "integer May+" reading applies to this table's `impressions` field, not the allocation upstream.
  Also: NO simple aggregation of the winners table reproduces this meter exactly (±7–42% by vendor,
  directions vary) — the exact BAE downstream allocation is a 2026-07-20 billing-sync question.

## dw-main-gold.reporting.ddp_* — BAE billing table family (BQ-migrated; Alyson pointer 2026-07-17)
- **Monthly series since ~2025-09/10** + unsuffixed current + `_w_select` variants:
  - `ddp_all_matches_cpm[_YYYYMM]` — per (ad_served_id × data_source_id × data_source_category_id ×
    and_seq/or_seq): ALL matched billable paths incl. 3P segments; columns `time, ip, tv_cpm,
    segment_name, mm_dsids` (matched MM dsids). **`tv_cpm` = the path's billing rate: DS17 ShareThis
    segments $0.95, MM $0.50** — per-impression rate visibility.
  - `ddp_mm_winners_imp[_YYYYMM]` — the MM slice: per impression-row `mm_dsids_winner ARRAY<INT64>`
    (co-winning svs sources incl. free logs 23/30), `impression_cnt FLOAT` (~90% =1.0; fractional =
    cross-PATH split, e.g. 0.5 when a 3P segment path also matched — NOT 1/n_winners), `tv_cpm`
    (**=0 on 100% of free-only-winner rows — free logs never bill; =$0.50 on 91.7% of mixed
    free+paid rows — the AUDI-1093 preemption gap, 291.1M imps in 202606**). June: 530.7M rows.
    **`mm_dsid_count` != `ARRAY_LENGTH(mm_dsids_winner)` — DO NOT recompute the denominator
    (BAE-4923, measured 2026-08-05):** the native `mm_dsid_count` column is the array length MINUS
    ONE *exactly* when both **DS28 (33Across) and DS40 (33Across API)** are in the winner array —
    the pipeline dedupes them to ONE vendor for the credit split. Measured with zero exceptions on
    202606: both-present → diff 1 (181,514,444 rows, **34.2%**); otherwise → diff 0 (349,175,512
    rows). Recomputing `ARRAY_LENGTH` as the 1/N denominator (as BAE-4923's original query did)
    both inflates N and counts 33Across twice in the numerator, **overstating any per-vendor credit
    share by ~15-19%/mo**. Always use the native column. Note the two 33Across dsids also bill as
    one vendor (~$598K/yr combined, AUDI-1089).
    Mixed free+paid **metered** imps must exclude flat-fee-only rows: winners whose sole non-free
    dsid is 25/26/39 carry no metered credit (268.9M in 202606 vs 291.1M under the loose
    free-vs-anything definition). Full winner-array roster is exactly 10 dsids —
    23/24/25/26/28/30/33/36/39/40 — no DS17 or DS35 appears here.
  - `ddp_mm_winners_domains[_YYYYMM]` — domain-grain winners.
- **Gotchas:** `data_source_id` here = the CONSUMER (13/19 in winners_imp; 17/35/etc. in
  all_matches), NOT the vendor — vendors live in the arrays; winner-array order is effectively
  uniform (first-element sums == equal-split sums); same ad_served_id appears on multiple
  slot/category rows. Canonical recon query: AUDI-1115 `audi_1115_l0b_bae_winners_recon.sql`.
- **Sibling `bronze.coredw.usage_reporting_audits`:** monthly anomaly-gate table — per vendor-month:
  usage, prior_usage, usage_diff_pct, gate1/2/3 flags, final pass/override. Its `impressions` column
  is the PLATFORM total (identical across vendor rows in a month) — a gate stat, NOT per-vendor credit.
- **Kafka-path svs vendors have BQ landing tables:** `fpa_dsid{24,33,39,40}_kafka_log` (per
  `gcp_pixel_page_view_signal_*_backfill_workflow.py` DAGs) — queryable raw landings for
  Justuno/Sovrn/Klickly/33A API before GCS.

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

## DDP usage-reporting pipeline — input tables (billing-team doc 2026-07-20; schemas BQ-VERIFIED 2026-07-20)
The upstream inputs to the DDP metering pipeline (source: `audi_1089_ddp_steps.xlsx`; full step map in
`data_knowledge.md` § "Canonical DDP usage-reporting pipeline" + AUDI-1089 summary §4f). All confirmed via
`bq show` on 2026-07-20 except `enriched_impressions` (access resolved 2026-08-17 via the `bq-read` PAM entitlement — see below).
- **`dw-main-bronze.external.targeted_signal`** — ⭐ **the row-level "used-signal" table, and it's queryable
  in BigQuery (was long believed Athena-only — corrected).** BQ external table over
  `gs://mntn-data-archive-prod/signals/targeted_signal/*.parquet`, **hive-partitioned (CUSTOM) on
  `data_source_id` (CONSUMER: 4=CRM, 13=MM verticals, 19=MM product-cats), `dt` (STRING, daily,
  2025-07-31 → current), `source_data_source_id` (ORIGINATING vendor: 21/22/23/26/29 CRM+free,
  24/25/26/28/33/36/39/40 DDPs, 23/30 free logs)**. Cols: `uid, ip, data_source_category_id, source_time,
  time, signal_type_id, ip_to_dscid_link_number, data_source_id, dt, source_data_source_id`. **A GROUP BY on
  the partition columns bills $0** (reads parquet metadata only; 1-day probe ~110s wall, 0 GB). Prune on `dt`.
  **⚠ grain caveat:** rows = raw used-signal events (uid×ip×dscid×time — 33Across ~591M/day), **NOT billed
  impressions** (~70M/mo) and not deduped to billing grain; use for the DS13/DS19 × vendor *decomposition*,
  then apply first-reporter/credit-split to get $. Companion **`external.targeted_signal_domain`** (`uid,
  domain, dt`; partitioned on `dt`; join on `uid`). Probe query + snapshot in AUDI-1089 `queries/`+`outputs/`.
- **`dw-main-silver.summarydata.v_campaign_group_segment_history`** — SCD **VIEW** over
  `audience.audience_segments`. Cols: `campaign_group_id, audience_id, start_time, end_time, data_source_id,
  data_source_category_id` (**REPEATED** array), `category_info`. Keyed on campaign_group_id+audience_id; the
  audience-target side of step-3's impression match (holds the INCLUDE dsid/dscid effective per window).
- **`dw-main-bronze.integrationprod.direct_data_partners`** — DDP reference registry (16 cols, 23 rows; **`data_source_id`
  is STRING** here); the raw table behind the `dw-main-silver.tpa.direct_data_partners` view (fee structures +
  per-partner reporting requirements). Full column list under that view's entry in `data_knowledge.md`.
- **DDP taxonomy (segment names + variable CPM):** `dw-main-bronze.tpa.categories` (VIEW, 18 cols),
  `dw-main-bronze.tpa.liveramp_categories` (719K-row TABLE — **carries `digital_cpm` + `tv_cpm` NUMERIC**, the
  LiveRamp variable-CPM source), `dw-main-bronze.external.sharethis_categories` (CSV EXTERNAL, categories only —
  **no CPM column**; ShareThis's $0.95 comes from the registry, not here).
- **`website_crawl_verticals` (wcv)** — GCS parquet `gs://mntn-data-archive-prod/vertical_categorizations/website_crawl_verticals/*.parquet`, ~1.42M domains. Cols: **`domain_name → vertical_id, vertical_name` (e.g. 104008 "B2B - Information Technology & Engineering"), `bucket_id`, `is_manual_override`**. This is the **DS13 domain→vertical** map (cached, refreshed ~every few months). Used as the "usable domain" gate (any domain in wcv, minus a webmail blocklist) AND as the vertical grain for category-level coverage (q3f). `product_categorization` (pc, DS19): `composite_key (URL) → product_category` + `data_source_category_id.list[].element` (keyword cat ids **≥900000** = the DS19 keyword categories; ids <900000 are other taxonomy). Both queried as external tables via the runbook svs setup.
- **DS13 vertical taxonomy = 148 DISTINCT verticals across 37 buckets** (corrected AUDI-1208, 2026-08-18 — the earlier "152" counted ROWS over a roster with 4 duplicate `vertical_id`s; reconciliation in the next bullet) (AUDI-431, 2026-08-10; `SELECT DISTINCT vertical_id, vertical_name, bucket_id` off the wcv parquet — wcv's own distinct set is the operative roster when proposing wcv edits). `vertical_id` encodes the bucket: `1BBSSS` where the leading 3 digits = `bucket_id` (101 Apparel & Accessories → 101000 "Apparel & Accessories", 101011 "Footwear"). **Prod contains a typo that must be reproduced verbatim when matching: `104xxx` "Learning & Eduction Technology"** (missing the 'a'). Any tool proposing a vertical must copy names character-for-character from this roster — free-text category names do not join. Companion dims: `tpa.dim_vertical` (Ryan-built, adds `bucket_name`/`verticals_in_bucket`), `silver.fpa.advertiser_verticals` (type0 parent / type1 sub). **The full roster is saved to git** at `tickets/audi_431_blocklist_whitelist/outputs/audi_431_vertical_taxonomy.csv` (`vertical_id, vertical_name, bucket_id`, 152 rows) — read that instead of re-querying the 1.42M-row parquet.
- **"152 vs 148 verticals" SETTLED (AUDI-1208, 2026-08-18) — it is 148.** The AUDI-431 roster CSV holds **152 rows but only 148 distinct `vertical_id`** (and 152 distinct `vertical_name`), because **4 ids carry two names each**: `105000` = **"MNTN Matched Audience"** AND "Building Materials" (an alias, not a vertical — this is exactly the row Ryan Kleck's `vertical_size_monitor.py` filters via `vertical_name != 'MNTN Matched Audience'`, and because the id keeps a real name the filter drops **no** vertical, so the monitor also reports 148); plus three apostrophe variants, `126002` Mens/Men's Health & Personal Care, `126006` Womens/Women's Health & Personal Care, `135004` RVs/RV's & RV Accessories. Three independent confirmations of 148 on 2026-08-18: `integrationprod.fpa_categories` `data_source_id=13` → 1 root + 37 buckets (101–137) + **148** verticals (101000–137001); `SELECT DISTINCT vertical_id` on `integrationprod.fpa_advertiser_verticals` → 37 (`type=0`) / **148** (`type=1`); a full day of `ip_vertical_associations` → exactly **148** six-digit + 37 three-digit ids, **zero-row symmetric diff** vs the AUDI-431 distinct ids. **Count and join on `vertical_id`, never `vertical_name`** — same failure class as the AUDI-431 "Learning & Eduction Technology" typo warning.
- **`integrationprod.fpa_advertiser_verticals` is one row per ADVERTISER, not a vertical dimension** (AUDI-1208, 2026-08-18): **30,863 rows for EACH of `type=0` and `type=1`**, resolving to only 37 / 148 distinct `vertical_id`. Joining it raw to a per-vertical size table fans the sizes out by ~200x. **Always `SELECT DISTINCT vertical_id, vertical_name` first** — this is why the monitor's JDBC subquery uses `SELECT DISTINCT`. For a clean vertical dim prefer `tpa.dim_vertical` or `integrationprod.fpa_categories` (`data_source_id=13`).
- **`missing_domains`** (AUDI-431, 2026-08-10) — GCS parquet `gs://mntn-data-archive-prod/vertical_categorizations/missing_domains/dt=YYYY-MM-DD/`, daily since 2025-11-02, ~2.5 MiB/day. Cols in-file: `domain, count` (`dt` is dir-only — re-derive from path). = svs domains (DS23 excluded, tldextract eTLD+1) **net of blocklist + whitelist + wcv** (all three anti-joins live in prod `SteelHouse/dbt ml_squad/models/vertical_categorization/missing_domains.py`; verified overlap 0/0/0). Each run reads 2 days and dynamic-overwrites dt=D and D-1, so partition dt=D is finalized by the D+1 run. THE candidate source for list refreshes — 28d ≈ 87 MB local, no BQ scan needed.
- **`ddp_url_verticals`** (AUDI-431, 2026-08-10) — GCS parquet `gs://mntn-data-archive-prod/vertical_categorizations/ddp_url_verticals/dt=…/`, live daily, ~113 GB/day. Cols: `ip, domain, uid, time, vertical_id, bucket_id, vertical_name, is_ecommerce, is_in_vertical_mapping, data_source_id, input_timestamp, url, ecommerce_score, is_whitelist` (+hive `dt`). Prod ecommerce-model scores for EVERY svs URL (MLflow `prod.ml.ecommerce_classifier@champion`, threshold 0.4) — `is_in_vertical_mapping` = wcv membership flag, so wcv joins are avoidable in-scan. **Consumes NO blocklist** (blocklisted domains like aol.com still scored daily, 47M urls/day) and whitelist is a LEFT-join flag, not a filter. Daily run overwrites dt=today AND yesterday — query a closed window ending dt ≤ D-2. BQ external table w/ `hivePartitioningOptions: AUTO` def-file works; 7d scan of domain+score+ip cols ≈ 109 s on the us-central1 reservation.
- **`dw-main-bronze.coredw.usage_reporting_audits`** — audit/anomaly-gate table (documented above; 20 cols, 99 rows).
- ✅ **`mntn-analytics-prod-01.analytics_curated.enriched_impressions`** — the persisted intermediate the meter
  consumes (F1 impression ⋈ targeted segments ⋈ IPDSC; produced by the UI Audience Segment Reporting pipeline).
  **Access resolved 2026-08-17 (was Access Denied since 2026-07-20): PAM entitlement `bq-read` on project
  `mntn-analytics-prod-01`** (`roles/bigquery.dataViewer` + `jobUser` + `connectionUser` + `storage.objectViewer`,
  4h max, DevOps approve via Slack). Request:
  `gcloud pam grants create --entitlement=bq-read --project=mntn-analytics-prod-01 --location=global --requested-duration=14400s --justification="..."`.
  **It is an EXTERNAL BigLake table** over `gs://mntn-analytics-curated/...` via connection
  `mntn-analytics-prod-01.us-central1.gcs_biglake`, hive-partitioned on `dt`, `metadata_cache_mode=AUTOMATIC`.
  Two consequences: **`--dry_run` reports "lower bound of 0 bytes" and is useless for cost-gating**, and
  **`INFORMATION_SCHEMA.PARTITIONS` returns zero rows** — neither means the table is empty. Real cost with a
  `dt` filter is **~2.4 GB/day**; always filter `dt`.
  **Verified schema (2026-08-17, 34 cols):** `time, hour, data_source_id, data_source_category_id
  STRUCT<list ARRAY<STRUCT<element INT64>>>, advertiser_id, campaign_group_id, campaign_id, group_id,
  creative_id, country, metro_id, region, city, domain, ip, guid, impression_id, ad_served_id, media_cost,
  data_cost, fee_cost, partner_cost, media_spend, platform_spend, data_spend, publisher_type_id, unlinked,
  objective_id, channel_id, category_info STRING, device_type, audience_id, dt DATE, hh STRING`.
  Note it carries `objective_id` and `channel_id` **but not `funnel_level`** — that still needs the
  `campaigns` join, which is why the billing script joins it.
  **Builder (found 2026-07-29):** `SteelHouse/data-pipeline/pyspark_pipelines/impression_enrichment.py`, prod config
  `conf/impression_enrichment/prod/config.yaml` — `lookback=2` (impression days), `ipdsc_lookback=35`,
  `dsid_block_list=[2,14,42]`, inputs all `dw-main-silver` (`logdata.cost_impression_log`, `public.campaigns/advertisers`,
  `summarydata.v_campaign_group_segment_history`, `ber_stg.category_facts__domain_x_publisher_types`) + ipdsc from
  `gs://mntn-data-archive-prod/ipdsc`; writes bucketed by `ad_served_id` (600),
  partitioned `dt,hh`, **dynamic overwrite** on a rolling 2-day window. The `data_source_id` tag = what the campaign
  **targeted** (segment history); the ipdsc join is a **35-day BACKWARD** window (`ipdsc_dt BETWEEN to_date(time)-35d AND time`).
  **Key fact (verified 2026-07-29):** on healthy days enriched DS_x count ≈ that DS's campaigns' **served** impressions
  in CIL, ~1:1 (DS51/Bombora CG 131563: 07-26 enriched 108,744 = CIL 108,744 = spend_log 108,744). **enriched inherits any
  CIL campaign-mis-resolution:** DS51 07-27 read 0 because CIL re-stamped those 110,750 impressions to `campaign_id = -3`
  (unresolved sentinel) on a reprocess — so the enrichment (campaign → segment → data_source_id) can't tag them, DS51→0. The
  impressions are real (spend_log 110,792, $904 billed) and PRESENT in CIL as `-3`, not lost; NOT the ipdsc skip, serving,
  or enrichment. **Reconcile a per-campaign enriched/CIL zero against spend_log AND check `campaign_id=-3`.** See
  `data_knowledge.md` § IPDSC + on-call INC-001.
  **Scope note (2026-07-29):** enriched `data_source_id=51` = the Bombora test campaign CG 131563 essentially 1:1 (07-26
  enriched 108,744 = CG 131563 CIL 108,744), NOT the broader DS51-targeted population (Sonali's CIL `campaign→segment`
  join shows ~141K/day DS51-targeted, i.e. other campaign groups too). On 07-27 the total `source_row_count` was 63.2M
  (normal, matching 07-26/28) with DS51=0 — the partition is fully built, so a DS51 zero is an ATTRIBUTION failure, not a
  lag/missing-partition.
- Scripts: `SteelHouse/bae-sql-utility/ddp/`.

### DDP file-drop batch ingestion → fpa_vendor_log + site_visit_signal (AUDI-1089)
- **`ENABLED_DSIDS = [23, 25, 26, 28, 30, 36]`** — the batch-ingest DAG's file-drop vendor set (streaming/pixel DDPs like DS33 Sovrn are NOT here; their off-switch is vendor-side).
- **Per-vendor ingest (DS26 Predactiv example):** reads hourly drops `gs://mntn-data-partners/partners/predactiv/dt=YYYYMMDDHH/*.parquet`, then (stage 1) writes the **FULL payload** to `gs://mntn-data-archive-prod/fpa_vendor_log/data_source_id=26/`, and (stage 2) a **thin `ip`/`url`/`time` projection** to `site_visit_signal/data_source_id=26` (`user_agent`/`query_parameters`/`advertiser_id` nulled in the svs projection).

---

# bronze.external

**Project:** dw-main-bronze | **Dataset:** external
External tables backed by GCS (Parquet/ORC files). Not managed by SQLMesh.

---

## bronze.external.tpa__mntn_matched_taxonomy__v2
- **Type:** EXTERNAL TABLE — the DS19 ("MM Core") keyword-name dimension (verified 2026-07-15, AUDI-1089)
- **Columns:** `data_source_category_id` (INTEGER, the >=900000 DS19 keyword space), `name` (STRING,
  human-readable product-category keyword, e.g. 900000='Electrolyte Supplements'), `parent_id`, `partner_id`,
  `description`
- **Use for:** joining DS19 `data_source_category_id`s (from `product_categorization` GCS parquet or
  `targeted_signal`) to keyword names. NOTE: DS19 ids share the `integrationprod.categories` id space with
  DS16 — do NOT resolve DS19 names through `categories`; use this table.

## bronze.external.ipdsc__v1
- **Type:** EXTERNAL TABLE (GCS-backed Parquet)
- **GCS path:** `gs://mntn-data-archive-prod/ipdsc/dt=<date>/data_source_id=<id>/`
- **Partition:** `dt` (STRING 'YYYY-MM-DD') and `data_source_id` (INTEGER)
- **No TTL** — historical data is available indefinitely
- **Use for:** IP → audience category_id resolution. The source of truth for which IPs were in a given
  CRM audience segment on a given date. Critical for CRM campaign debugging and audience size analysis.

**Producer / freshness / on-call.** Partitions are written by the `tpa_ipdsc_export` DAG (`airflow-ti`,
team TPA_EXPORT, schedule `35 2 * * *` UTC): one `ipdsc_ds_<id>` builder per source, plus registry-driven
`ipdsc_<partner>` builders for 3P audience partners (`dags/ipdsc_third_party_audience_builders.json`).
`dt=D`'s partition lands ~04:58 UTC on **D+1**. A separate `ipdsc_monitor` DAG (`5 0 * * *`) polls each
`data_source_id=<id>/_SUCCESS` with an **18h hard-fail GCSObjectExistenceSensor**. **DS51 (Bombora) is an
`optional: true` partner** (`source_date_offset_days: 1`, source `gs://mntn-data-partners/partners/bombora/segments/<D-1>/`):
on days Bombora doesn't deliver source files, the producer silently skips it (partition absent, export ships
`{"data_source_id":51,"cats":[]}`) and `ipdsc_monitor`'s `precondition_bombora` sensor pages an 18h
timeout — **this alert is EXPECTED/benign on Bombora-skip days** (Bombora's feed is intermittent). Mandatory
sources (DS4, DS17, …) are never tolerated — a missing mandatory partition hard-fails `tpa_export`. Full
on-call protocol + diagnosis commands: `on-call/oncall_runbook.md` (INC-001).

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

**⚠ Parquet predicate pushdown makes IP-list membership checks cheap (PS-8572, 2026-08-06):** an
`ip IN UNNEST([...])` filter with ~2,154 literal IPs cut a ~91GB DS47 single-partition scan to ~9.3GB actual
(10x). For "are these specific IPs members?" questions, push the IP list into the WHERE clause instead of
joining/scanning the whole partition.

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
Source scoring pipeline: `gs://household-scoring-prod/output/scoring/prospecting_intent/` (daily, 35-day retention). **DAG: `audience_intent` in airflow-ti (`dags/audience_intent/audience_intent.py`), daily batch ~3–7 AM UTC (Ryan Kleck's page).** It writes TWO products, and we consume the **prospecting** one:
- **prospecting** (per `ip, advertiser_id, campaign_group_id, campaign_id`; `…/scoring/prospecting_intent`): **HI 10K = in Vertical (DS13) AND in Keywords (DS19)**; PP 8K = in vertical, no keyword; MI 3333–6665 = in bucket, not vertical; Unscored (prev Max Reach) = outside bucket/vertical but inside keywords.
- **advertiser** (per `ip, advertiser_id`; `…/scoring/advertiser_intent`; sibling `household_scoring__advertiser_intent__v1`): a pre-batch fallback so a new campaign has scores before the batch runs. **HI 10K = in Vertical only (NO keyword split); PP = N/A.** We don't really use this one.

**⚠ COST TIP (TI-1027):** the full all-IP MM scoring universe `household_scoring.prospecting_intent_daily` is **~19.4 TB/day to scan** — do NOT scan it for realized-score lookups. Use the **delivered `household_score` in `cost_impression_log`** (e.g. 7-day window) as the cheap realized-score substitute when joining vendor/site-visit IPs to the MM score they actually got served with.

---

## Production Fangorn score sources (per-IP) — mapped (BQ) vs raw (GCS)
Where the live Fangorn intent scores actually live. **Two scales:** RAW `model_score` ∈ [0,1] (the model output; raw>0.8 ≈ High-Intent threshold) and MAPPED `household_score` ∈ [0,10000] (the bidder scale; 8001+ HI, 6666–8000 PP, 3333–6665 MI, ≤3332 MR).

| Source | Type | Grain / scale | Notes |
|---|---|---|---|
| `bronze.external.household_scoring__prospecting_intent__v1` | EXTERNAL (GCS Parquet) | per-IP × advertiser/campaign; **mapped** `household_score` 0–10000 | **⚠ A 0-row result can be transient — re-run before concluding it has no data (see the note below this table).** Partitions `year`,`month`,`day` are **STRING** — a `year='2026'` filter does **NOT** prune (sweeps the whole year = very slow/expensive). **Don't scan per-advertiser.** Cols: ip, advertiser_id, campaign_group_id, campaign_id, household_score. Sibling: `household_scoring__advertiser_intent__v1` (advertiser-intent variant), `..__prospecting_intent__dniehoff` (dev). |
| `gs://mntn-data-archive-prod/fangorn_14day_lookback_vertical/dt=<YYYY-MM-DD>` | GCS Parquet (Spark) | per-IP × **vertical_id**; **raw** `model_score` 0–1 | The snapshot the **rollout-priority scorer** reads. Read via Spark/Databricks `parquet.\`gs://…/dt=<snap>\``. 14-day lookback. |
| `gs://mntn-data-archive-prod/vertical_categorizations/ip_vertical_associations/dt=<YYYY-MM-DD>` | GCS Parquet (Spark) | IP ↔ `data_source_category_id` (vertical_id) | Which IPs are associated with each vertical. **SOURCE OF TRUTH for vertical SIZE** (AUDI-1208) — what Ryan Kleck's `airflow-ti models/monitoring/vertical_size_monitor.py` reads for its daily `MNTN GCS Vertical Sizes - PROD` email. Size = `COUNT(DISTINCT ip)` per `data_source_category_id`; **6-digit id = vertical, 3-digit = its bucket parent** (the monitor's own rule is `LENGTH(...) > 3`). `data_source_category_id` is a **FLOAT** here — `CAST(... AS INT64)` before matching or string-length tests. Readable from BQ with an inline `--external_table_definition` over one `dt` dir. 2026-08-17 integrity: 2,375,803,803 rows / 214,079,274 distinct IPs / exactly 185 categories (148+37) / 0 null `ip` / 0 null category id. **`ipdsc__v1` `data_source_id=13` is the downstream copy and agrees to a median +1.9% at one day's lag** — either answers a sizing question. Also used for a vertical's `assoc_median_fangorn_score` / high-mid ratio. |
| `bronze.external.camperbid_prod__intent_score__{intent_score,prospecting_intent,advertiser_intent,…}` | EXTERNAL | per-IP camperbid intent scores | Bidder-side intent-score externals; same family. |
| `bronze.external.fangorn_score_monitor` | EXTERNAL | score-distribution monitor | Daily Fangorn score distribution (TI-849), not per-IP joins. |

**⚠ RETRACTED 2026-08-18 — the registered `bronze.external.household_scoring__prospecting_intent__v1` is NOT broken.** An earlier note here (same day) claimed it could not see partitions after mid-July and blamed its hive `sourceUriPrefix`. **That was wrong and is withdrawn.** What actually happened, in order:
- ~09:35 PT: `WHERE year='2026' AND month='08' AND day IN ('16','17')` returned **0 rows** (exit 0, 7,302 slot-sec); `WHERE year='2026' AND month='08'` returned **0 rows** (46,537 slot-sec); a bare `LIMIT 5` returned `2026/07/13`. All logged in `bq_perf_log.jsonl`.
- ~11:15 PT: the **identical** query returned `2026-08-16 → 247,392,860,754` and `2026-08-17 → 251,588,309,448` rows. A wider probe returned every July and August day tested.
- The day-17 count from the registered table **matches the independent inline-external read exactly** (251,588,309,448), so the data is consistent and AUDI-1208's numbers are unaffected.

**Root cause NOT established — treat it as transient, not structural.** The DDL (`sqlmesh/dataform/external_tables/definitions/v1/household_scoring__prospecting_intent__v1.sqlx`, `hive_partition_uri_prefix = 'gs://household-scoring-prod/output/scoring/prospecting_intent'`) is the correct DDL form; the "`sourceUriPrefix` needs `{key:TYPE}`" theory was a guess that the retest disproves. Most likely an external-table file-listing/metadata visibility lag, but that is a hypothesis, not a finding.
- **Operationally:** a 0-row result from a federated external table is not proof of missing data. **Re-run before concluding**, and cross-check GCS with `gcloud storage ls '<day prefix>/**' | grep '\.parquet$'` (a plain `ls` of the day prefix shows only the dir and `_SUCCESS`, which reads as empty).
- Reading a single day inline still works and is a fine way to bound cost: `--external_table_definition="pi::PARQUET=gs://household-scoring-prod/output/scoring/prospecting_intent/year=2026/month=08/day=17/*.parquet"`. **Any inline-external GCS query MUST pass `--location=us-central1`** or it bills on-demand in the US multi-region (the AUDI-1089 ~$875 footgun). One day is ~251.6B rows, so aggregate with `APPROX_COUNT_DISTINCT`.
- **Do not confuse two tables.** `dw-main-bronze.household_scoring.prospecting_intent_daily` is a NATIVE partitioned table the `audience_intent` DAG rebuilds each run (`export_intent` task group: `create_daily_* → export_snapshot_* → delete_stale_partitions_*`, the last `DELETE`ing every partition except the run date) — it holds **ONE day only**. The `external.*__v1` table is the GCS-backed view of the full retained archive. Different objects, different retention.

**For "if reactivated, where would advertiser X rank?"** the priority scorer's Score-Opportunity / Size-Stability / HHST-relief are computed at the **vertical** level — so read X's vertical row from the per-vertical aggregate, not a per-advertiser IP scan. Canonical scorer + weights: see `data_knowledge.md` §"Canonical rollout-priority scorer". Authoritative Fangorn inclusion/tier is Postgres `tpa.fangorn_advertiser_inclusion`.

**CORRECTION (verified 2026-07-22, AUDI-1083): a BQ mirror DOES exist and is queryable/populated** — `dw-main-bronze.integrationprod.tpa_fangorn_advertiser_inclusion` (CDC from Postgres). Cols: `advertiser_id`, `vertical_id`, `is_express`, **`fangorn_rollout_tier_num`** (the tier num — NOT `tier`), `fangorn_advertiser_inclusion_date`, `created_at`, `updated_at`. Prior "no BQ mirror" notes here + at the DS46-overlay proxy entries above are superseded — use this table directly for advertiser→tier in BQ (still lags Postgres slightly on very-recent flips). Camperbid live per-campaign addressable-pool signal: `bronze.external.camperbid_prod__hhst_v3__campaign_bucket_population` (population by intent band, hourly; v2 `campaign_qualified_rate` is DEAD since 2025-11).

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
**No match-rate HISTORY exists anywhere in BQ (PS-8572, 2026-08-06):** searched integrationprod `archives_*`, `history`, `coredw`, `reportingdbprod`, and sqlmesh datasets — only the CURRENT `match_rate` is observable. `integrationprod.ui_audience_uploads` is a 1:1 current-state mirror, not history. Claims like "match rate climbed N pts since upload" are unverifiable in BQ.

---

## bronze.raw.tpa_membership_update_log (full entry)
- **Type:** VIEW (in bronze.raw) → physical `bronze.sqlmesh__raw.raw__tpa_membership_update_log__546164626`
- **Partition (CORRECTED 2026-08-25, AUDI-1016):** physical table is a native TABLE, **DAY-partitioned on `time` (TIMESTAMP) with 90-day partition expiration**. `dt`/`hh` are plain STRING columns (no pruning; no clustering) — filter `DATE(time)`, not `dt`, for partition pruning.
- **Data available:** rolling last ~90 days (expiration), ~273B rows / ~110 TiB total, **~3.0B rows / ~1.23 TiB per day**.
- **Use for (CORRECTED 2026-08-25):** BQ landing of the membership-db GCS segment dump — but it receives **ONLY the daily 08:00 UTC sweep (1 of the 6 four-hourly sweeps; every row has `hh='08'`, `source_version='v2'`, `delta=false`)**. NOT a change log despite the name: with no stored producer state, `in_segments` re-emits the FULL current membership every sweep. Composition of a day (2026-08-23 exact + 9-day sample, AUDI-1016): **~57% rows have in_segments AND out_segments both empty** (duplicate-empty noise), ~17% out_segments only (just-became-empty transitions), ~26% carry segments. The consumer-side 24h load (10.6B records, 92.9% empty per Eric Salinger's doc) is NOT reproducible here — 5 of 6 sweeps + the Kafka feed never land in BQ.
- **Data sources:** DS 2 and DS 3. DS 4 (CRM) not confirmed to appear here.

| Column | Type | Notes |
|--------|------|-------|
| id / ip | STRING | IP address |
| time / activity_time | TIMESTAMP | `time` = partition column |
| data_source_id | INTEGER | |
| in_segments | RECORD | `.segments[]`: advertiser_id, campaign_id, segment_id, version, score, tags — FULL current set per sweep |
| out_segments | RECORD | same shape; segments removed since prior sweep |
| metadata_info | RECORD | key/value |
| scores | RECORD | key/value scores map — **never populated** (0 of 2.7M sampled rows, 2026-08-25) |
| delta | BOOLEAN | always false in landed data |
| dt / hh | STRING | plain columns, NOT partitions; hh always '08' |
| source_version | STRING | 'v2' uniformly |

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
- **This is the BQ-side source of truth for reading an advertiser's vertical** — the Fangorn rollout scorer joins `type=1` (sub-vertical) as `vertical_id`. `advertisers.advertiser_vertical_id` is frequently NULL even when a vertical exists here, so don't use it (e.g. iMemories 37423: advertiser_vertical_id NULL, but here = sub-vertical 116001 "Gifts & Specialty Stores" / parent 116 "Gifts"). The `type=1` vertical_id also matches the `score.types[].id` (RTC vertical) in the audience expression.
- **How to CORRECT a mis-tagged advertiser vertical (AUDI-owned, verified 2026-07-27):** the operational source of truth is **CoreDB** (Postgres); the vertical is written by **an API call to the Shopper Graph service**, NOT by editing BQ (BQ is a read-only Datastream CDC mirror — you cannot and should not patch it directly). Contact/owner for the change: **Alyson Lefkowitz**. Path: Shopper Graph API → CoreDB `fpa_advertiser_verticals` → Datastream CDC → `bronze.integrationprod.fpa_advertiser_verticals` → this view → Fangorn scorer (keys on the `type=1` id, so a wrong sub-vertical is functional, not cosmetic). **Propagation is not instant** — after the source change the BQ mirror lags (CDC batch); re-query later to confirm both the type=0 parent and type=1 sub row flipped. Worked example: AID 69864 "Lake Erie Heritage Foundation" was mis-tagged B2B (parent 104 "B2B Software & Services" / sub 104012 "B2B - Sales & Marketing"), corrected to Travel (parent 135 "Travel" / sub 135006 "Travel Destination Promotion"). Both parent+sub rows must move together.
- Every advertiser has exactly 2 rows: type=0 (parent) + type=1 (sub-vertical)
- 185 distinct verticals, 184 distinct names (3 parent/child pairs share names)
- 49 advertiser_ids are orphans (not in advertisers table) — pre-existing source issue
- Join to advertisers: `advertiser_id = advertisers.advertiser_id`

**GOTCHA — `advertiser_name` is unreliable (TI-737, 2026-03-16):**
- Write-once, never updated (only 2 of ~40k rows have ever been updated)
- **Empty name regression:** Starting 2025-12-23, 79–82% of new advertisers inserted with empty string. 4,366 advertisers affected.
- **Stale names:** Even when populated, 1,114 of 16,000 (7%) differ from current `advertisers.company_name` because customers edited their name after the FPA row was created.
- **Always JOIN to `integrationprod.advertisers.company_name`** (or `public_advertisers.company_name`) for the authoritative, current advertiser name.
- **There is no `dw-main-silver.core.advertisers`** — it errors `Not found ... in location us-central1`. The advertiser dim is `dw-main-bronze.integrationprod.advertisers` (filter `deleted=FALSE AND is_test=FALSE`; ~37.7k rows). Verified 2026-08-20 (AUDI-1141 name refresh).

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
| `ui_visits` | summarydata | Visit events fanned out PER ATTRIBUTION MODEL (15+ ids; dedup by (advertiser_id, guid, epoch) preferring type 0→1 last-touch, lowest model id). Deduped ≈ `clickpass_log` 1:1 (+0.5%, verified week 2026-07-02..10: 9.81M vs 9.76M); is_pa adds only ~14K/wk — clickpass IS the visit source of truth at event grain. sum_by_* views are campaign/advertiser-day aggregates (no IP grain). | ad_served_id, ip/ip_raw |

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
- The BQ replacement table is `geo.location_data`. **Ownership (Nivas, 2026-07-29): the geo pipeline is now owned by the Measurement team** (former BER+ATTR). Originally Sheetal → Nivas; Nivas has since moved teams and no longer has deploy perms. **Sonali Vengurlekar owns the `location_data` logic.** (Superseded the "owned by Nivas" note.)
- **`dw-main-bronze.geo.location_data` is a live sqlmesh model** (not a copy) built from the maxmind tables (`geo.geo_location_data_stage`, `raw.geo_maxmind_network_locations_base`, `raw.geo_maxmind_versions`, `geo.archive_location_data`, `analytics_curated.geo_metros`, `external.maxmind_*`). It builds the parent `hierarchy`. It is exported to `gs://mntn-analytics-curated/geo/location_data/*.parquet` by airflow-reporting DAG `dags/geo/geo_copy_ch_coredb.py`; TPA export reads that GCS copy. (via Benny/Nivas/Sean, #mission-control aud22 thread, 2026-07-02)
- **`dw-main-bronze.analytics_curated.geo_location_data` is the OLD CoreDW export — deprecated, do NOT use** ("we shouldn't be using it anymore", Nivas 2026-07-29). Superseded the earlier "use this for production queries" note. Query `dw-main-bronze.geo.location_data` (or the silver views) instead.
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
- **fpa.mm_domain_map — Purpose and Known Data Quality Issue:** `fpa.mm_domain_map` is the shopper graph domain table used for "hoteling" — mapping multiple advertiser IDs that share the same domain (e.g., franchise brands like Orange Theory) to a single root/parent advertiser ID. When an advertiser_id is passed with a domain_name that matches an entry in this table, the system uses the root advertiser's autopilot profile rather than generating a new one. **Known data quality issue (2026-04-20):** A query revealed ~561 rows where the domain in `mm_domain_map` does not match the `company_url` in `public.advertisers` for the same advertiser_id — indicating mismatched or incorrect domain mappings. This causes the autopilot profile regeneration UI to fail with an error like "Advertiser ID X does not match root advertiser ID Y for domain Z." Workaround: delete the incorrect row from `mm_domain_map` and re-trigger regeneration with `?override=true`. Diagnostic query: `SELECT dd.advertiser_id, pa.advertiser_id, dd.domain, pa.company_url FROM fpa.mm_domain_map dd JOIN public.advertisers pa ON dd.advertiser_id = pa.advertiser_id AND lower(pa.company_url) NOT LIKE CONCAT('%', lower(dd.domain), '%') ORDER BY dd.advertiser_id DESC;` (via Ryan Kleck, #targeting_helpdesk_ask_anything, 2026-04-20) **NOT mirrored to BigQuery (verified 2026-08-24, AUDI-1142):** a region-wide INFORMATION_SCHEMA sweep (us-central1 + US; dw-main-bronze/silver/gold) found zero hits — only `fpa_advertiser_verticals` and `fpa_categories` are mirrored from the fpa Postgres schema. The diagnostic query and the ~561 count are Postgres-side only; requantifying needs coredb access (DS/targeting). BQ-side proxy: `dw-main-gold.bae.v_aid_flagged_dup_domain`, BAE's curated dup-domain flag view (823 AIDs / 312 domains as of 2026-08-24).
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

**Column naming inconsistency (as of 2026-05-22):** `ads_clickpass_hh_log` uses `household_id`, `household_version`, etc. (full names), while `click_hh_log` and `guid_hh_log` use shortened names: `hh_id`, `hh_version`, etc. Standardization to the full `household_id` naming convention has been requested. **CONFIRMED 2026-07-29 (Jack Barbey):** the standard going forward is **`household_id`** (= "mountain_id" = MNTN ID — all the same thing; use `household_id`, it's more literal). Use it for the MNTN-ID re-key work (AUDI-1049).

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

## BQ job location & slot-reservation routing (MUST-KNOW, verified 2026-07-16, AUDI-1089 incident)

- **The org slot reservation is `dw-main-bronze:us-central1.background-jobs` and it only covers jobs in `us-central1`.** Assignments are per (project, location) — a job created in the **US multi-region gets NO reservation and bills on-demand at $6.25/TiB**, even in the same project. It does not matter which project the tables live in; what matters is the *job's* location.
- **All MNTN datasets are us-central1** (verified: silver logdata/summarydata/aggregates/core/enriched; bronze raw/integrationprod/coredw), so ordinary table queries auto-infer us-central1 and route correctly.
- **The leak: queries with NO dataset reference default to the US multi-region.** This bites (a) inline `--external_table_definition` GCS-parquet queries (DDP runbook svs/wcv/pc — `referenced_tables` is empty, so BQ can't infer a location) and (b) trivial dataset-less queries (`SELECT 1`, date-math tests). July 9–15 2026 the DDP runbook re-run billed **~140 TiB ≈ $875 on-demand** this way (flagged by Alek Piasecki; ~$720/wk from his lens).
- **Fix (three layers, all applied 2026-07-16):** `~/.bigqueryrc` has `location = us-central1` (covers plain `bq`); `.claude/scripts/bq_run.sh` injects `--location=us-central1` unless the caller passes one; workspace `.mcp.json` bigquery server switched from `--location US` → `us-central1` (this was also why the MCP tool used to fail on MNTN datasets — now fixed).
- **`.bigqueryrc` precedence gotcha (verified 2026-07-16):** `location` in the rc file IS honored, but `project_id` is NOT when a gcloud default project is set — `gcloud config` (`core/project`, currently `mntn-coredw-prod`) wins, and we lack `bigquery.jobs.create` there, so plain `bq query` without `--project_id` fails with Access Denied. Always pass `--project_id=dw-main-silver` explicitly; don't rely on an rc `project_id` line.
- **GCS compatibility:** `gs://mntn-data-archive-prod` and `gs://mntn-data-tpa-prod` are US-CENTRAL1 buckets — a us-central1 job reads them natively. (`gs://mntn-data-partners` is US multi-region; a multi-region bucket is still readable from a job in a region it contains, but test before a big scan.)
- **Proof pattern:** identical `svs` external-table COUNT ran with `--location=us-central1` → `reservation_id = dw-main-bronze:us-central1.background-jobs`, `total_bytes_billed = 0` (slot-billed). Without the flag → job in `US`, `reservation_id = NULL`, full on-demand bytes billed.
- **Audit your own routing** (note: querying `region-us` INFORMATION_SCHEMA requires an explicit `--location=US` override now that the rc default exists):
  ```sql
  SELECT DATE(creation_time) day, IFNULL(reservation_id,'ON_DEMAND') res, COUNT(*) jobs,
         ROUND(SUM(total_bytes_billed)/POW(2,40),2) tib
  FROM `dw-main-silver`.`region-us`.INFORMATION_SCHEMA.JOBS_BY_USER
  WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY) AND job_type='QUERY'
  GROUP BY 1,2 ORDER BY 1 DESC
  ```
  Anything `ON_DEMAND` with real `tib` in `region-us` is a routing leak.
- **Trade-off of correct routing (Alek Piasecki, 2026-07-16):** reservation jobs share the intentionally-small org slot pool — heavy scans that ran fast on-demand may queue/throttle when slots are maxed. Expected and acceptable; stagger the biggest scans (see the existing "never run large queries simultaneously" rule) and escalate to Alek if queueing becomes genuinely problematic. Never revert to US/on-demand as a workaround.
- **`dso.campaign_group_daily_budgets` and `archives.campaign_group_daily_budget_archives` — Daily Budget Source**

Real-time campaign budget data is sourced by unioning `dso.campaign_group_daily_budgets` (current) with `archives.campaign_group_daily_budget_archives` (historical). The union is then deduplicated using `DISTINCT ON (advertiser_id, campaign_group_id, hour(update_time))` to get one budget record per advertiser/campaign-group/hour. Rows where `billing_type_id = 2` are excluded (these represent a specific billing type that should not be included in budget reporting). This pattern is used in at least one real-time Mode report for monitoring budget vs. spend. (via Benny, #production-ops, 2026-05-29)
- **Geo Location Mapping Discrepancy — `geo.v_location_data` (coreDW) vs. `dw-main-bronze.geo.v_location_data_lat_long` (BigQuery)**

A confirmed data inconsistency exists in the `parent_location_id` field between the coreDW and BigQuery versions of the location data table. Example: `location_id = 657177` (postal code 14527, Feura Bush) has `parent_location_id = 143675` in coreDW but `parent_location_id = 93463` in BigQuery. This discrepancy affects geo-targeting audit logic (specifically audit 22) and caused IP-to-location mapping mismatches for certain IPs (e.g., `67.241.244.44`). The fix was expected to be deployed by Nivas on 2026-06-01, after which the audit would no longer trigger on this data. The active geo version at the time of discovery was `1777852800`. (via Benny, #mission-control, 2026-05-29)

**Follow-up — AUDI-1072 recurrence, RE-VERIFIED (2026-07-30):** aud22 re-triggered on non-World-Cup CGs. Sonali (owns `location_data`) challenged the diagnosis in-thread, tracing the fired example (campaign 311522, IP 161.38.248.34, ZIP 45814) and ZIP 43221 and finding both internally consistent. A strict same-geo_version, per-row re-verification settled it three ways: **(1) the `location_data` defect is REAL** — at the current geo_version `1785110400` exactly 5 ZIP rows have a scalar `metro_id` disagreeing with the metro in their own `hierarchy`: 01223 field=543 Springfield-Holyoke-MA vs hierarchy=532 Albany-NY; 17371 566 Harrisburg vs 508 Pittsburgh; 66214 616 Kansas-City vs 603 Joplin; 92545 & 95245 862 Sacramento vs 803 Los-Angeles. Same 5 as at `1783900800`, so they persist; the ORIGINAL 43221/45814 examples are clean, so the affected set **changes across geo versions** (not "rotates every rebuild"). This class is what PR #1147 fixes. **(2) Sonali is right about 43221/45814** — the earlier "43221 metro_id=535 but hierarchy→638" phrasing was imprecise; 43221 spans two internally-consistent geoname rows (Columbus 535, Arlington/Toledo 547). Do NOT cite 43221/45814 as the defect. **(3) The 45814/161.38.248.34 flag is NOT a `location_data` issue** — `network_locations` and `location_data` both resolve that IP to Toledo (547/638) and the campaign targeted Columbus (517), so it's a serve-side geo-build skew, not a geo-table defect. **Refined 2026-07-31 (Benny's direct memdb read + BQ):** memdb's `geo_version` field read Jul-13 (1783900800) but its mapping for the covering `/24` still returned 535 Columbus, while the table holds 547 Toledo at Jul-13. A single network block covers the IP (no ambiguous/overlapping records), so it is memdb's per-network mapping not refreshing when its version pointer advanced — NOT a version-pointer lag. See the "Geo store versioning" note below. **Two workstreams stand:** deploy PR #1147 (a GENERAL `metro_id` COALESCE for type-6/7, still OPEN/DRAFT — blocked on DEV-8264 dw-main-gold access + a >24h `sqlmesh plan` vs 8h PAM windows; needs forward-only) for the `location_data` class; and Nate's new `network_locations` investigation for the network/serve-side cases (e.g. 43221's Toledo-tagged network slice). AUDI-1072 was marked Done because DM (Data Monitoring) believed they'd fixed the audit-logic side (an **unapproved audience leaking past the exclusion filter**, DM tightened validation Jul-21) — a separate candidate root cause; Benny confirmed last week's false positive is distinct from this week's live occurrence. **Per-row diagnostic (geo_version-pinned):** among `location_type_id=7` rows, join each `hierarchy` ancestor to `location_type_id=3` metros WITHIN the same geo_version, flag where the ancestor metro number != the row's scalar `metro_id`. A cross-version join fabricates false flags — pin geo_version. Not a per-CGID redeploy.

**Geo store versioning — network_locations vs location_data (2026-07-30):** `dw-main-bronze.geo.network_locations` retains ~3 recent geo_versions (partitioned by `geo_version`, ~9.3-9.4M rows each; observed 1782691200 / 1783900800 / 1785110400, ~bi-weekly), while `dw-main-bronze.geo.location_data` retains ONLY the latest geo_version (`GROUP BY geo_version` returns one row = 1785110400). **Query cost:** only `geo_version` prunes — filtering on `network` / `geoname_id` / `postal_code` triggers ~7-10 GB full scans; always add a `geo_version =` filter (one version + `network LIKE` prefix ~0.35 GB, all 3 versions ~1 GB). **A network's `metro_id` (DMA) can be REASSIGNED across versions** for the same geoname_id (verified: `161.38.248.0/24` geoname 5145962 = 535 Columbus at 1782691200, then 547 Toledo at 1783900800 and 1785110400). So resolve an impression's geo at the **served** `geo_version`, and beware a downstream consumer (memdb) whose per-network MAPPING is stale under a CURRENT version stamp: for `161.38.248.0/24` memdb returned metro 535 Columbus while its version field read 1783900800 (Jul-13), which holds 547 Toledo in the table (the 535->547 reassignment landed at Jul-13 but did not refresh in memdb). A single network block covers that IP, so it is not ambiguous records — it is memdb's mapping not refreshing when its version pointer advanced (aud22 mechanism (c)). Version activation dates + active flag live in `dw-main-bronze.raw.geo_maxmind_versions` (date, geo_version, active). See [[reference_aud22_geo_reporting_sync]].

**Geo table schema quick-ref (2026-07-31):** `location_data.location_type_id` encodes the hierarchy level — **3** = metro/DMA number (the `location` STRING *is* the DMA code, e.g. "535"), **4** = metro/DMA display name ("Columbus, OH"), **7** = ZIP (the `location` string is the postal code, `sub_location1` = city). `hierarchy` (REPEATED INT64) = the row's ancestor `location_id`s and contains BOTH the type-3 (DMA number) and type-4 (DMA name) metro ancestors, so a ZIP row carries its metro two ways (join type-3 by number, or read the scalar `metro_id`). `network_locations` has TWO DMA columns — `metro_id` (raw MaxMind) and `metro_id_cil` (CIL-specific); they usually agree (identical at all 3 versions for 161.38.248.0/24). `raw.geo_maxmind_versions` (date, geo_version, active) is the version calendar — exactly one `active=true` at a time, new build ~bi-weekly on Mondays; the ACTIVE build can differ from the build CIL served on and from what a consumer (memdb) has loaded (three-way skew is possible). **`network_locations` prefix columns (all STRINGs):** `network_first_8_bits` = first octet ("161"), `network_first_16_bits` = first two octets ("161.38"), `bits` = prefix length ("24"). **Filter `network_first_16_bits = "a.b"` to prune a /16 to ~1 GB** (vs 7-10 GB for a `network`/`geoname_id`/`postal_code` scan — those aren't clustered). **Recipe — all blocks covering IP a.b.c.d at a geo_version:** `WHERE geo_version=V AND network_first_16_bits="a.b" AND (network LIKE "a.b.c.%" OR SAFE_CAST(bits AS INT64)<=23)`, then eyeball which contain the IP; overlapping blocks can carry different `metro_id`, and CIL takes the most-specific (highest `bits`).

- **`dw-main-bronze.external_ddm.audit_22_geo_inclusion_exclusion_logs` — the FA022 audit output.** BigQuery **external table over `gs://mntn-data-monitoring/feature-audits/audit_22/logs/*.parquet`**. `malachi@mountain.com` lacks `storage.objects.list` on `mntn-data-monitoring` → **querying it fails** ("Permission denied while globbing file pattern"); request bucket read or get the flagged-IP list from DM/Compass. Schema: `run_date` (DATE, partition-like filter), `audit_id`, `campaign_group_id`, `campaign_id`, `ip`, `geo_version` (INT), `first_impression_time`/`last_impression_time` (TS), `location_ids` (repeated STRING — the geo the IP resolved to), `geo_includes`/`geo_excludes` (repeated STRING — the audience's targeted geo), `matched_excludes` (repeated STRING), `comment`, `impressions` (INT), `total_spend` (FLOAT). The audit reconciles each impression's resolved geo vs the audience expression; a row = an IP flagged out-of-geo.

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
- **⚠ `env` column = WHICH LIVE BIDDER DEPLOYMENT served the bid, NOT test-vs-real (AUDI-1223, 2026-08-25):** `rtb-bidder-service` (Kotlin/Beeswax) runs TWICE on the prod clusters — namespace `bidder` logs `env='prod'`, namespace `bidder-burnin` logs `env='burnin'` (designed as the release soak/canary stage; QA is a separate third env). Burnin bids are real auctions and real spend. Campaigns are routed to the burnin deployment by `burnin_bidding_enabled` in `integrationprod.beeswax_{campaign,campaign_group,advertiser}_sync_config` (coalesced campaign → group → advertiser → FALSE; group table also carries `purpose` + `bidding_strategy`). ThirdLove CG 115424: `true / PER-6332 / MNTN_BURNIN_BIDDING_STRATEGY` (verified in BQ 2026-08-25) — a May-2026 ops override, unrelated to `is_test`. **Consequence:** the SQLMesh model behind `enriched.lift__ghost_bid_visits` filters `env='prod'` and silently drops burnin-routed LIVE traffic (142 of 1,215 INCR-75-eligible advertisers invisible to ghost-bid lift as of 2026-08-25). Correct fix is `env IN ('prod','burnin')` PLUS an `is_test=TRUE` exclusion — a bare swap to burnin double-counts the `lift__ghost_bid_audiences_test_campaigns` sibling's population. Env can flip when the flag is toggled (Gruns CG 126905 measurable June-July, burnin 2026-08-24) — check freshly, don't cache.
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
    table — not the per-campaign expression — is authoritative. (Archive =
    `dw-main-bronze.integrationprod.archives_advertiser_configuration_archives` — note NO `audience_` prefix
    (no `archives_audience_advertiser_configuration_archives` exists) and NO `update_time` column: order by
    `create_time` + `version`. Fresh for change history. PS-8572, 2026-08-06.)
- **`conversion_lookback_window` / `page_view_lookback_window` = the BLOCK lookbacks** (the horizon
  `block_conversion` / `block_first_party` suppress over) — a different knob from the VV windows
  (`advertisers.clickpass_acquisition_ttl` / `clickpass_click_ttl`) and from `conversion_window`; a THIRD knob
  (`lookback_window`) lives INSIDE DS21/DS34 clauses of the `audience_segments` expression. Lovepop 58797:
  14d PRO VV / 7d RT VV / 30d conversion window but 90d block lookback (90→180 on 2026-08-04) + DS21 180d /
  DS34 90d clause lookbacks. Name the specific knob. (PS-8572, 2026-08-06)
- **Per-campaign exclusion clause in `audience_audience_segments`** (`is_targeted=false`) can look like
  `UserLastVisitTime >= N,day and UserNumPageViews >= K` (lookback + threshold) — but this is NOT the
  authoritative block (block_prospecting is enforced advertiser-level by the bidder; ~96% of campaigns have no
  such clause). A `>= 0` per-campaign clause does NOT mean blocks are off. The full pageview block excludes
  **ANY guid pageview** (organic / other-marketer), not just MNTN-attributed VVs (VVs = a subset).
- 10% holdout encoded in the expression: `md5(<advertiser_id>:<ip>) bucket 0–99 of 1000`.

### silver.archives.household_score_threshold_archives (HHST gate change history)
CDC archive of the per-campaign Household Score Threshold (the intent gate the bidder enforces). Cols: `household_score_threshold_archives_id`, `advertiser_id`, `campaign_group_id`, `campaign_id`, `threshold` (INT; 0 or negative = NO gate / serve anyone; 10000 = HI-only), `transaction_id`, `create_time`, `update_time`, `datastream_metadata`. One row per write (not per change) → collapse with LAG(threshold) OVER (PARTITION BY campaign_id ORDER BY update_time) keeping threshold!=prev. Current live values: `silver.dso.household_score_thresholds`. Join campaigns via `bronze.integrationprod.campaigns` (campaign_id PK; objective_id=1 & funnel_level=1 = Stage-1 prospecting). Use for daily gate event-studies (AUDI-1070: gate flips drive overnight delivery-composition inversions).

### silver.archives.audience_segment_archives (audience-expression change history)
CDC / type-2 archive of the audience targeting expression (archive of live `audience_audience_segments`). One row per version; linked by `campaign_id`; `expression` is nested JSON; filter `expression_type_id=2 AND is_targeted=TRUE`. Use it to build a **per-campaign data-source add/remove timeline**: extract DS ids by regex — `REGEXP_EXTRACT_ALL(expression, r'"data_source_id":([0-9]+)')`. Gotcha: `version` is NON-MONOTONIC across segment generations — order by `update_time`, not `version` (AUDI-1070). **Row semantics (verified AUDI-1215, 2026-08-21): `update_time` = when the version became EFFECTIVE, `create_time` = when it was SUPERSEDED** (the earlier "order by create_time" phrasing here ordered by supersession, not effect; corrected in place). Analysis note: HI substrate = vertical(DS13)∩keyword(DS19); changes to OTHER sources (add CRM DS4, drop DS35 LiveRamp, add DS16/DS21/DS34) are real audience changes that do NOT touch the HI-defining layers. The RTC directive (`score_type=rtc`, id=vertical_id) can live in the expression from ~Jul 2025 yet only fire in delivery from 2026 — 'in expression' ≠ 'firing.'

**Full audience-history recovery trio (AUDI-1215, 2026-08-21):** `silver.archives.audience_x_campaign_group_archives` (audience↔campaign_group mapping versions: which audience was live on a CG and when), `audience_segment_archives` (per-campaign bidder-evaluated expression versions, `expression_type_id=2`), `audiences_archives` (audience entity metadata). Nothing is lost to CDC current-state: a complete targeting timeline (audience swaps, DS adds/drops, geo, lookbacks) is reconstructable. Worked example: `tickets/audi_1215_elevenlabs_lift_post_audience_change/queries/audi_1215_audience_change_timeline.sql`.

### household_score_threshold_archives — complete `threshold` value map (AUDI-1070)
Consolidated value semantics for the `threshold` column (the earlier entries listed only 0/negative and 10000): `-100`/`-1`/`0` = NO gate (max-reach / serve-anyone); `3333`/`3334` = Mid floor (excludes MaxReach 1–3332); `6666` = HI+PP floor; `8000` = PP floor; `10000` = HI-only. **The gate binds on `household_score`, NOT `advertiser_household_score`** (the two diverge ~10%; AHS logs ~3500 for ~10% of genuine-HI impressions). On the gated path ~99.99% of impressions carry `household_score` exactly equal to the gate value; residual ~0.01–0.02% is ~1-day flip-day propagation lag (unscored, not PP/Mid). Always reason about binding on `household_score`, and exclude the RTC path (RTC bypasses the gate). **Short-flight gotcha:** flights <72h auto-set `threshold`=0 for deliverability — a `threshold`=0 row is not necessarily an intentional gate-off; cross-check flight length first.

### Historical-reconstruction gotchas — vertical sizes and by-tier visit rates NOT recoverable in BQ (AUDI-1070)
- `dw-main-bronze.external_ddm.data_source_category_sizes` is **3P-ONLY** — does NOT contain DS13 (vertical) or DS19 (keyword) sizes. To size a vertical's HI supply historically, route to the Measurement/scoring team's GCS pull (`ip_vertical_associations` / `prospecting_intent`) — not this table, not BQ.
- `clickpass_log` is purged for older periods → historical **visit-rate-BY-TIER** queries on 2025 return 0 retained visit rows; reconstructing 2025 by-tier VR needs a Measurement-team pull.
- `bronze.external.household_scoring__prospecting_intent__v1` retains ~35 days active (10-day in BQ; deeper via raw GCS) — can't reconstruct >35d pool history. **A 0-row result here is not proof of missing data (AUDI-1208, 2026-08-18): the same query returned 0 rows at 09:35 and full counts at 11:15. Re-run before concluding.** See the RETRACTED note under "Production Fangorn score sources". `TI_835_prospecting_scores` GCS files were deleted; `ddp_vertical_classification_api` is API logs, not a membership table.

### silver.core.flights (authoritative flight schedule — start/end times)
The real flight table (Tofer): `flight_id`, `campaign_group_id` (a flight is per client-campaign/GROUP), `start_time`, `end_time`, `budget`, `budget_type_id`, `status_id` (3=active/completed, 8=superseded), `ui_flight_id`. Compute flight length as `TIMESTAMP_DIFF(end_time,start_time,HOUR)`; join advertiser via `campaign_groups.advertiser_id`. Use this for the **short-flight (<72h → manual HHST=0) check** — do NOT infer flight length from consecutive active-days (merges flights). Each budget/schedule EDIT spawns a new flight row, so a <72h "flight" can be a mid-schedule tweak, not a fresh launch. Companion: `silver.dso.campaign_group_flight` (adds local-tz start/end + name).

**Public API / MCP id mapping (verified 2026-08-19, IMP-047):** MNTN's customer-facing Public API (and the Public MCP at `mcp.ex.mountain.com`) calls a **campaign** what we call a **`campaign_group`**. Checked on 10 WGU campaign ids: all 10 resolve in `public.campaign_groups`, **none** in `public.campaigns`. Its model is campaign owns flights with **no line item**, so our internal per-stage `campaign_id` layer has no counterpart and there is **no funnel-stage dimension** in its reporting catalog — MCP campaign 24081 spans `objective_id` 1/5/6/7 and `funnel_level` 1-4, all flattened to the single strategy "Prospecting". Its `objective` field is therefore correct at group grain; the "`objective_id` is unreliable" caveat applies to the internal row, not to what the API returns. Its `flight` is our `flight_id`. Detail: `knowledge/memory/reference_mntn_public_mcp.md`.

### Config-change AUDIT tables — "what changed for this advertiser, and when" (AUDI-1070 cheat-sheet)
To reconstruct why an advertiser's delivery/composition/performance changed (HHST, flights, audience, attribution, campaign structure) — the tables Tofer/Measurement pointed to. All are archive/CDC or source tables; filter by advertiser (some via campaign_id/campaign_group_id join to `bronze.integrationprod.campaigns`/`campaign_groups`, `deleted=FALSE`).

| What changed | Table | Key cols / how |
|---|---|---|
| **HHST intent gate** (0/-1=no gate, 6666=HI+PP, 10000=HI-only) | `silver.archives.household_score_threshold_archives` | campaign_id, threshold, update_time. Collapse to change-events with LAG(threshold) OVER(PARTITION BY campaign_id ORDER BY update_time). Live value: `silver.dso.household_score_thresholds`. |
| **Flight schedule** (short-flight <72h → manual HHST=0) | `silver.core.flights` | campaign_group_id, start_time, end_time, budget, status_id (3=active/completed, 8=superseded). Duration = TIMESTAMP_DIFF(end,start,HOUR). Each budget/schedule edit = new row. Companion: `silver.dso.campaign_group_flight`. |
| **Audience / data-source (DS) targeting** | `silver.archives.audience_segment_archives` | campaign_id, expression (nested JSON), expression_type_id=2 & is_targeted=TRUE. Extract DS ids: REGEXP_EXTRACT_ALL(expression, r'"data_source_id":([0-9]+)'). **ORDER BY update_time, NOT version (non-monotonic); update_time = effective, create_time = superseded (AUDI-1215).** Live: `bronze.integrationprod.audience_segments`. |
| **Audience ↔ campaign-group assignment** (which audience was live on a CG, when) | `silver.archives.audience_x_campaign_group_archives` (+ `audiences_archives` for audience metadata) | campaign_group_id, audience_id. update_time = when effective, create_time = when superseded; order by update_time (AUDI-1215). |
| **Attribution / reporting_style** (last_touch vs industry_standard) | `silver.archives.advertiser_setting_archives` | advertiser_id, reporting_style, update_time. LAG to find flips. Live: `bronze.integrationprod.r2_advertiser_settings`. |
| **Lookback windows** | `silver.audience.advertiser_configurations` | conversion_lookback, page_view_lookback = BLOCK lookbacks, not VV windows (PS-8572). FRESH; not the stale integrationprod one. |
| **Campaign / group launches & pauses** | `bronze.integrationprod.campaigns` + `campaign_groups` (+ `sum_by_campaign_by_day` for first/last delivery day) | campaign_group_id=client campaign; campaign_id=internal stage (obj 1 S1 / 5,6 MT / 7 Ego / 4 Retgt). Group NAMES encode intent (Scale-Up / General-Interest / DMA). campaign_groups is full of test/archived junk. |
| **Scoring engine (Fangorn migration)** | `logdata.cost_impression_log` household_score | continuous 8001-9999 = Fangorn; exactly 10000/8000 = bucketed. Per-advertiser rolling migration. DS46 in the audience expression = Fangorn. |
| **Delivery composition / VR / craters** | `summarydata.sum_by_campaign_by_day` + `logdata.cost_impression_log` | daily VR (views+clicks)/impressions to spot tracking outages (VR→~0 at normal spend = data gap, NOT audience). |

Reusable one-command runner over most of these: `documentation/docs/advertiser_yoy_diagnostic/queries/run_diagnostic.sh <AID> <win_start> <win_end> <p1s> <p1e> <p2s> <p2e>`.

## bq CLI — SQL positional arg must not START with a `--` comment (TI-1037, 2026-07-08)

`bq query ... '<sql>'` treats a leading `--` in the SQL string as a command-line flag →
`FATAL Flags parsing error: Unknown command line flag`. Strip leading comment-only lines (or start the
string at the first SQL keyword) when passing templated .sql files as a shell argument; `--` comments
inside the body are fine.

**DECLARE contaminates CSV stdout (AUDI-1089, 2026-07-10):** a `DECLARE` turns the query into a
multi-statement script and bq then echoes the statement text (ending `-- at [N:1]`) to stdout BEFORE the
result rows — any `--format=csv > file.csv` redirect captures the SQL too. Keep queries meant for CSV
capture single-statement; inline parameters as literals with a `-- PARAM` marker for runner substitution.

### identity translation signals — the graph vendor-crediting inputs (AUDI-694, verified 2026-08-17)

`dw-main-silver.identity.graph_translation_signal` and `...auction_translation_signal` are the two tables the
graph crediting leg reads. Both are **VIEWS** that `SELECT *` from a single sqlmesh physical table
(`identity__{graph,auction}_translation_crm__*`) — **there are no UNION ALL branches**, despite the view
description promising them ("add UNION ALL branches here as new sources are onboarded"). Anything written by
`mntn_graph.log_translation` to `gs://mntn-data-archive-{env}/identity_resources/{graph,auction}_logs/` is
therefore **not readable through these views** until someone adds the branch.

**Columns (both, 9 each) — the timestamp column is `translation_timestamp`, NOT `translation_date`:**
- graph: `translation_id, data_source_category_id, data_source_id, targeted_id, targeted_id_type, household_id, graph_version, graph_data_sources ARRAY<INT64>, translation_timestamp`
- auction: same but `output_id, output_id_type` in place of `targeted_id, targeted_id_type`

`translation_date` has **never** existed — the pre-refactor dev tables from 2026-08-02/03 also carry
`translation_timestamp`. Any SQL referencing `translation_date` cannot compile.
**`targeted_id_data_sources` (the design doc's Vendor List 1 column) does not exist in this schema.**

**Physical shape:** `PARTITION BY DATE(translation_timestamp)`, `CLUSTER BY data_source_id,
data_source_category_id, {output,targeted}_id_type`, `partition_expiration_days=60`.
**Each daily partition is a FULL SNAPSHOT of the live population, not that day's events** — row counts are
near-identical day over day and creep upward: ~2.19B rows / 335 GB per day (auction), ~2.37B / 437 GB (graph),
so ~20-26 TB retained each. A "30-day lookback" over these unions ~30 copies of the same population; scanning
one column across 7 days is ~245 GB. **Use `INFORMATION_SCHEMA.PARTITIONS` (free) for counts, and prefer the
already-materialised gold outputs (`dw-main-gold.reporting.ddp_crm_graph_cpm`) over re-scanning these.**

`data_source_category_id` on these = the CRM `audience_upload_id`. `output_id_type = 30` (IPv4) is the only
value produced today — the upstream `auction_translation_crm` model filters `g.id_type = 30`. Note
`IdTypeFamily.IPV4` in `identity-graph-interface` covers **{30, 32}**, so a literal `= 30` filter downstream
becomes lossy the moment an IP_DAY (32) source is unioned in.

### dw-main-gold.reporting.ddp_crm_graph_cpm — the DS63 crediting output (2026-08-13 build)

One row per DS63 impression with the credited graph vendors already assembled. Columns:
`advertiser_id, campaign_id, time, ip, ad_served_id, data_source_id, data_source_category_id, and_seq, or_seq,
dt, cpm, segment_name, graph_dsids ARRAY<INT64>, leg1_graph_dsids ARRAY<INT64>, leg2_graph_dsids ARRAY<INT64>`.
Leg 1 = auction translation (household ⟶ IP at bid time), leg 2 = graph translation (segment ID ⟶ household).
44 MB / 216,409 rows for dt 2026-08-06..08-12. **This is the cheap table to answer graph-crediting questions
from** — it holds the impression join already, so most questions need no `enriched_impressions` access.
Sibling `ddp_crm_graph_matches_cpm` adds `type` and `auction_signal_timestamp`. Its `graph_dsids` **retain**
free/internal sources (23, 30, 58) and non-externally-reported partners (22), unlike `bae-sql-utility#24`.
