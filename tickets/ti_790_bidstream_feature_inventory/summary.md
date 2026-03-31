# TI-790: Feature Inventory & Quality Assessment

**Jira:** https://mntn.atlassian.net/browse/TI-790
**Epic:** [TI-789](https://mntn.atlassian.net/browse/TI-789) — Bidstream Feature Extraction & Audience Augmentation
**Status:** In Progress
**Date Started:** 2026-03-30
**Date Completed:**
**Assignee:** Malachi

---

## 1. Introduction

Part of the Feature Store initiative (TI-789) with Alex Knorr and Ryan Kleck. The goal: catalog every IP-level feature across MNTN's log tables, assess quality, and determine which are most promising for improving targeting performance (predicting Visits / IVR).

**Scope expanded** from just bidstream tables to all 25 log tables in the system. We systematically profiled every table, identified truly unique columns per table, and built snapshot queries for the 6 highest-value sources.

## 2. The Problem

Fangorn's feature store needs more signals to improve targeting predictions. We have rich data across dozens of log tables that we're not fully leveraging. Before we can test features against IVR (Matt's XGBoost methodology), we need to know: **what data exists, where is it, what's unique to each table, and what's the quality?**

## 3. What We Found

### 3.1 Scale of the Data

| Table | Rows/Day | Distinct IPs/Day | Cost/Day | TTL |
|-------|----------|------------------|----------|-----|
| guid_log | ~340M | 31.2M | ~75 GB | Long |
| augmentor_log | ~29B | 23.6M (1hr) | ~241 GB | 10d BQ / 30d parquet |
| bidder_auction_events | ~2.8B | — | ~17 GB/hr | 90d |
| win_logs | ~68M | 11.7M | ~13 GB | 90d |
| cost_impression_log | ~68M | 11.7M | ~7 GB | 90d |
| conversion_log | ~63M | 10.7M | ~10 GB | Long |

Daily: **11.7M IPs get impressions, 563K visit (4.8% base IVR).** The feature store's job is to help identify which of those 11.7M IPs are most likely to be in that 563K.

### 3.2 The 6 Tables That Matter (and Why)

We analyzed all 25 log tables. Most share common columns (ip, time, advertiser_id, device_type, etc.). Only 6 tables have **substantial unique signals** not available anywhere else:

---

#### Table 1: guid_log — "What people do on advertiser sites"
**15 unique columns.** This is the demand-side signal — pixel fires on advertiser websites.

| Feature | What It Tells Us | Why It Matters |
|---------|-----------------|----------------|
| `product` (JSON) | Product category, brand, name, amount, SKU | **Purchase intent.** An IP browsing $200 shoes behaves differently than one browsing $5 socks. |
| `cart` (JSON) | Cart contents, value | **Cart = high intent.** Product views are interest; cart is commitment. |
| `ga_utm_source/medium/campaign` | How they got to the site (Google, Facebook, email, organic) | **Traffic source = intent quality.** Organic search > social scroll. |
| `ga_gclid` | Google Ads click ID | Paid search visitor — specific intent signal. |
| `user_agent.ua_advanced` (JSON) | DeviceBrand (799 values), DeviceName (9,984), DeviceCpu (21) | **Device fingerprinting** — Matt's prototype already uses this. Richer than basic device_type. |
| `is_cookied` | Has a tracking cookie | Cookied users = more data, better attribution. |

**Matt's prototype** already builds a daily snapshot from this table: device flags, OS flags, browser flags, diversity counts, percentages. Our snapshot extends this with product/cart/UTM features.

---

#### Table 2: augmentor_log — "What content they consume (supply side)"
**7 unique columns.** Enriched bidstream data for auctions we participated in.

| Feature | Fill % | What It Tells Us | Why It Matters |
|---------|--------|-----------------|----------------|
| `iab_categories` | 30% | IAB content taxonomy (IAB1=Arts, IAB12=News, IAB17=Sports...) | **Direct vertical mapping.** An IP consuming IAB8 (Food & Drink) content is relevant to food advertisers. Key for Alex's vertical classification work. |
| `inventory_source` | 100% | Which SSP (40 sources: Index Exchange, StickyAds, PubMatic, Tremor, Rubicon...) | **Inventory quality signal.** Premium SSPs correlate with higher-value audiences. |
| `mntn_segments` | 86% | MNTN segments already assigned to this IP | **Incrementality baseline.** How many segments does this IP already have? Needed for DS13 augmentation. |
| `iab_categories` + `categories` | 30% + 13% | Content taxonomy | **Bronze only** — silver view drops these. Must query `bronze.raw.augmentor_log`. |

**Key finding:** `iab_categories` is the most directly useful field for vertical classification, but it's only in bronze (not silver). Alex and Ryan need to work from `bronze.raw` or parquet.

**CTV-specific signals** not in guid_log: `device_type` includes CONNECTED_TV and SET_TOP_BOX. OS includes Roku, Tizen, SmartCast, webOS. These are CTV-specific behavioral indicators.

---

#### Table 3: bidder_auction_events — "What content they watch (broader view)"
**15 unique columns.** Auctions we saw but didn't bid on — broader behavioral window.

| Feature | Fill % | What It Tells Us | Why It Matters |
|---------|--------|-----------------|----------------|
| **`content_genre`** | **87%** | What content they're watching: entertainment, news, drama, comedy, sports, reality, documentary... | **THE breakout feature.** Not in ANY other table. 87% fill. Maps directly to advertiser verticals. After normalization, ~50-100 clean genres. |
| **`device_make`** | **90%** | Physical device: Roku, Samsung, LG, Vizio, Amazon, Apple, Sony... | **Demographic proxy.** Device ownership correlates with income/age. 457 distinct values. |
| `content_series` | 37% | Specific show name (Hawaii Five-0, NCIS, newscasts...) | Granular content signal. Needs cleanup (hashed values, templates). |
| `content_channel` | 36% | Channel name | Content affinity. |
| `content_network` | 38% | Network name (structured) | Premium vs long-tail. |
| `publisher_name` | 100% | Publisher identity (301 values) | Cleaner than augmentor_log.network. |

**Data quality issues:**
- `content_genre`: Case-inconsistent ("Entertainment" vs "entertainment" vs "GENRE_COMEDY"), comma-delimited multi-genres ("sitcom,comedy"), whitespace (" "). Needs LOWER() + SPLIT + strip prefix.
- `device_make`: Case-inconsistent ("SAMSUNG" vs "Samsung"). Needs UPPER().
- `content_series`: 37% fill but includes MD5 hashes and `{{CONTENT_SERIES}}` templates. Filter by length ≠ 32 and no `{{`.

**Real signal example from test data:**
- IP `68.33.19.80`: Samsung, 99.8% entertainment genre — heavy entertainment viewer
- IP `38.62.136.32`: Roku+Samsung, 99.8% news genre — news viewer
- These IPs have completely different content profiles and would respond to different advertisers.

---

#### Table 4: win_logs — "How they engage with our ads"
**66 unique columns (richest table).** One row per auction won.

| Feature | What It Tells Us | Why It Matters |
|---------|-----------------|----------------|
| `video_completes/plays/skips` | Video completion rate (VCR), skip rate | **Ad engagement.** IPs that complete videos are more engaged than skippers. |
| `video_mutes/pauses/fullscreens` | Granular interaction | Muted = watching but audio-off. Fullscreen = high engagement. |
| `in_view/in_view_time_ms` | Viewability metrics | **Ad attention signal.** High viewability = actually seeing the ad. |
| `invalid_impression/invalid_automated_browser/invalid_data_center_traffic` | IVT (fraud) flags | **Bot detection.** Filter invalid traffic before modeling. |
| `platform_device_make/model/screen_size` | Device hardware details | Screen size, model specificity. |
| `content_language/content_rating` | Content metadata | Language and maturity rating. |
| `clearing_price_micros_usd` | Actual price paid | **Inventory quality.** Higher CPMs = premium inventory. |
| `clicks` | Click behavior | Direct response signal. |

**Test finding:** VCR is very high (close to 1.0) for most IPs — CTV viewers generally watch to completion. The signal will be in the variance: IPs that *don't* complete stand out. Skip rate is near 0 across the board (CTV doesn't have skip buttons like YouTube).

---

#### Table 5: cost_impression_log — "Impression-level enrichment"
**20 unique columns.** Enriched impression data with scoring and cost.

| Feature | What It Tells Us | Why It Matters |
|---------|-----------------|----------------|
| `recency_elapsed_time` | Time since last impression to this IP | **Frequency signal.** How recently and frequently we're showing ads. |
| `household_score` | Fangorn household score | Existing model output — use carefully (potentially circular). `-1` = unscored. |
| `advertiser_household_score` | Advertiser-specific score | `10000` = RTC conquest. Other values = Fangorn score. |
| `media_cost/media_spend/data_spend/platform_spend` | Cost breakdown | **CPM efficiency.** What we're paying per impression. |
| `ott_device` | OTT device classification | Unique device typing not in other tables. |
| `partner_ad_format` | VIDEO vs BANNER (authoritative) | Authoritative format flag. |
| `supply_vendor` | Supply-side vendor | Which vendor sourced this impression. |

---

#### Table 6: conversion_log — "What they actually buy"
**3 unique columns + rich `query` JSON field.** Conversion events (purchases, signups).

| Feature | What It Tells Us | Why It Matters |
|---------|-----------------|----------------|
| `order_amt` | Dollar value of purchase | **Purchase value.** $200 converters behave differently than $5 converters. |
| `conversion_type` | Type of conversion (purchase, signup, call...) | **Conversion quality.** Purchases > page views. |
| `conversion_source_id` | Which conversion path (5 values) | Attribution path. |
| `query.shoamt` (75% prevalence) | Order amount from pixel | Dollar value from query string. |
| `query.shpt` (74%) | Product type purchased | What category they bought. |
| `query.ga_client_id` (67%) | Google Analytics cross-session ID | **Cross-session identity.** Links visits across time. |
| `query.email_data` (2.3%) | Hashed email | Identity resolution signal. |
| `query.androidId/idfa/adid` (~3%) | Device advertising IDs | Cross-device identity. |

**Note from Matt/Ryan:** Don't use conversion_log for device/browser features (those overlap with guid_log). Use it **only** for the net-new fields: order data, conversion type, and identity signals from the query JSON.

### 3.3 Tables We Analyzed and Deprioritized

| Table | Why Skip |
|-------|----------|
| **bid_logs** | 90% redundant with win_logs. Same device_make/model, same pricing. |
| **spend_log** | Intent scores are Fangorn *outputs* — using them as inputs would be circular. |
| **conversion_signal_log** | Rich CallRail data (call duration, customer city/state) but only 193 rows in 6 days. Too sparse. |
| **bid_price_log** | viewability_score and publisher_performance are unique but 10-day TTL limits use. |
| **event_log** | Video events better captured via win_logs aggregates. |
| **click_log** | Only unique field is `landing_page`. |
| **clickpass_log** | This is our *outcome variable* (IVR), not a feature. |
| **tpa_membership_update_log** | `scores` field is empty. `metadata_info` is null. No signal currently. |
| **kochava_log / singular_log** | Mobile attribution — niche, not core CTV targeting. |
| **auction_log** | Subset of augmentor_log fields. |
| All others | Redundant columns or too low fill/volume. |

Full cross-table unique column analysis: [ti_790_cross_table_unique_columns.md](artifacts/ti_790_cross_table_unique_columns.md)

### 3.4 Combined Feature Count

| Table | # Unique Features in Snapshot | Signal Category |
|-------|------------------------------|-----------------|
| guid_log | 35 | Demand-side: device, browser, OS, product, cart, UTM |
| augmentor_log | 25 | Supply-side: CTV device, SSP, IAB categories, segments |
| bidder_auction_events | 20 | Content: genre, device make, series, publisher |
| win_logs | 30 | Engagement: VCR, viewability, IVT, pricing |
| cost_impression_log | 17 | Enrichment: recency, scoring, cost, format |
| conversion_log | 15 | Conversion: order value, type, identity signals |
| **Total** | **~142** | |

After dedup and removing overlapping fields: **~60 unique features** in the combined training dataset.

## 4. What We Built

### Snapshot Queries (all tested and working)

| Query | Cost | Wall Time |
|-------|------|-----------|
| [guid_log_daily_snapshot.sql](queries/ti_790_guid_log_daily_snapshot.sql) | ~75 GB/day | ~4 min |
| [augmentor_log_daily_snapshot.sql](queries/ti_790_augmentor_log_daily_snapshot.sql) | ~117 GB/hr | ~66s |
| [bidder_auction_events_daily_snapshot.sql](queries/ti_790_bidder_auction_events_daily_snapshot.sql) | ~7 GB/hr | ~92s |
| [win_logs_daily_snapshot.sql](queries/ti_790_win_logs_daily_snapshot.sql) | ~13 GB/day | ~19s |
| [cost_impression_log_daily_snapshot.sql](queries/ti_790_cost_impression_log_daily_snapshot.sql) | ~7 GB/day | ~21s |
| [conversion_log_daily_snapshot.sql](queries/ti_790_conversion_log_daily_snapshot.sql) | ~10 GB/day | ~32s |

### Combined Training Dataset

[training_dataset.sql](queries/ti_790_training_dataset.sql) — Joins all 6 snapshots on IP with IVR label from clickpass_log.
- Base: IPs served impressions (win_logs), 10% deterministic sample (~1.2M IPs)
- Label: visited yes/no from clickpass_log
- Features: LEFT JOIN all 6 snapshot CTEs
- Tested with 1% sample: 117K IPs, 17s, ~65 GB
- Filters proxy/CDN IPs (>10K wins, 0.0.0.0, 127.0.0.1)

### Supporting Artifacts

| File | What It Is |
|------|-----------|
| [ti_790_feature_inventory.md](artifacts/ti_790_feature_inventory.md) | Detailed field profiling for augmentor_log + bidder_auction_events |
| [ti_790_exhaustive_feature_sources.md](artifacts/ti_790_exhaustive_feature_sources.md) | All 8 candidate tables with unique signals and aggregatable features |
| [ti_790_cross_table_unique_columns.md](artifacts/ti_790_cross_table_unique_columns.md) | Programmatic cross-table dedup of all 25 log tables |
| [ti_790_project_plan.md](artifacts/ti_790_project_plan.md) | Full project plan with phases and timeline |
| [ti_790_xgboost_split_analysis.py](artifacts/ti_790_xgboost_split_analysis.py) | XGBoost feature importance script (pre-visit vs feedback split) |
| [ti_790_importance_pre_visit.csv](outputs/ti_790_importance_pre_visit.csv) | Pre-visit feature importance rankings |
| [ti_790_shap_pre_visit.png](outputs/ti_790_shap_pre_visit.png) | SHAP summary plot for pre-visit features |

## 5. Key Takeaways for Alex & Ryan

### For Alex (TI-791 — Vertical Classification):
1. **`iab_categories` in augmentor_log (bronze only)** is the most direct vertical signal — IAB taxonomy codes at 30% fill. Top categories: IAB1 (Arts & Entertainment), IAB12 (News), IAB17 (Sports), IAB8 (Food & Drink).
2. **`content_genre` in bidder_auction_events** is the breakout feature — 87% fill, ~50-100 clean genres after normalization. Maps directly to advertiser verticals.
3. Both need heavy normalization (case, commas, prefixes). I have the normalization patterns documented.
4. **Must use `bronze.raw.augmentor_log`** for iab_categories — the silver view drops this field.

### For Ryan (TI-792 — OpenRTB Spec & Exchange Reference):
1. **`inventory_source`** has 40 values in augmentor_log but only 3 in bidder_auction_events — different coverage.
2. Still need the **exchange reference table** to map inventory_source strings to metadata. `core.exchanges` doesn't exist where expected.
3. **`content_genre/series/channel/network`** in bidder_auction_events align with OpenRTB `content` object fields. Check if they map to the Magnite spec.
4. **`device_make`** (90% fill, 457 values) maps to OpenRTB `device.make`.

### XGBoost Results — What Actually Predicts Visits?

We split features into **pre-visit** (available at bid time) and **feedback** (available after site visit):

| Model | Features | AUC | Use Case |
|-------|----------|-----|----------|
| **Pre-visit only** | 47 (bidstream + impression) | **0.896** | Targeting decisions |
| Feedback only | 19 (guid_log + conversion_log) | 0.999 | Post-visit enrichment, retraining |
| All combined | 66 | 0.999 | Full picture (leaky for targeting) |

**Top 10 pre-visit features by SHAP (for targeting):**

| Rank | Feature | Source | SHAP | What It Means |
|------|---------|--------|------|---------------|
| 1 | `al_avg_segments` | augmentor_log | 0.986 | More existing MNTN segments → more likely to visit |
| 2 | `ci_pct_new` | cost_impression_log | 0.670 | New IPs visit less |
| 3 | `ci_pct_rtc` | cost_impression_log | 0.392 | RTC-targeted IPs visit more |
| 4 | `ci_total_cost` | cost_impression_log | 0.363 | More spend → more exposure → more visits |
| 5 | `wl_avg_price` | win_logs | 0.231 | Premium inventory → better audiences |
| 6 | `al_n_auctions` | augmentor_log | 0.228 | More active IP → more likely to visit |
| 7 | `wl_n_models` | win_logs | 0.205 | Multi-device households visit more |
| 8 | `n_win_adv` | base | 0.175 | More advertisers targeting → popular IP |
| 9 | `ci_hh_score` | cost_impression_log | 0.152 | Existing Fangorn score (already predictive) |
| 10 | `al_pct_pmp` | augmentor_log | 0.105 | Premium inventory signal |

**Source table ranking (pre-visit):**
1. **cost_impression_log** — best avg rank (13.9), dominates with recency, scoring, cost features
2. **augmentor_log** — segment density and auction volume
3. **bidder_auction_events** — content_genre, device_make (mid-tier for IVR, high-value for vertical segmentation)
4. **win_logs** — video engagement, device details, pricing

**Iterative paring:** AUC holds at ~0.896 even with only 5-11 features. The top 5 features carry most of the signal.

**Key insight:** Bidstream content features (content_genre, device_make) are mid-tier for raw IVR prediction but high-value for **vertical classification** — different use case than visit prediction. Both are valuable for the feature store.

### For Everyone — Next Steps:
1. **Wednesday sync**: Walk through these results with Alex and Ryan
2. **Vertical classification model**: Use content_genre + iab_categories to map IPs to advertiser verticals (Alex's focus)
3. **Cold-start analysis**: Test bidstream features specifically on IPs with NO existing Fangorn scores
4. **Production integration planning**: Top 5-10 features → Fangorn feature store

## 6. Questions Answered

- **Q:** Is augmentor_log the only bidstream table?
  **A:** No. `bidder_auction_events` has 15 unique fields not in augmentor_log (content_genre, device_make, content_series, content_channel, content_network). Both are needed.

- **Q:** Which tables have truly unique data for targeting?
  **A:** 6 of 25 log tables. The rest are redundant. See Section 3.2.

- **Q:** What's the single most valuable new feature?
  **A:** `content_genre` from bidder_auction_events. 87% fill, directly maps to advertiser verticals, not in any other table, shows strong IP-level differentiation (99%+ concentration in single genres for residential IPs).

- **Q:** Should we use conversion_log despite double-counting concerns?
  **A:** Yes, but only for its unique fields (order_amt, conversion_type, query string identity signals). Skip the device/browser columns that overlap with guid_log.

## 7. Data Documentation Updates

- Documented augmentor_log schema (bronze vs silver differences) in feature inventory
- Documented bidder_auction_events schema, partition strategy (_PARTITIONTIME), and content fields
- Documented conversion_log query JSON structure (key_value array)
- Documented cost_impression_log recency_elapsed_time as INTERVAL type, household_score semantics (-1 = unscored, 10000 = RTC)

## 8. Open Items / Follow-ups

- [ ] Export 10% training dataset to CSV for Python XGBoost modeling (TI-793)
- [ ] Ryan: find exchange reference table for inventory_source mapping
- [ ] Build genre normalization mapping (~37K raw values → ~50-100 clean categories)
- [ ] Consider bid_price_log features (viewability_score, publisher_performance) if 10-day TTL permits
- [ ] Investigate recency_elapsed_time population — was NULL on 3/30, may need different dates
- [ ] Wednesday 4/2 sync: walk through findings with Alex and Ryan
