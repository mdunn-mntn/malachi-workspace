# TI-810: Ryan Review Meeting Prep

**PR:** https://github.com/SteelHouse/airflow-ti/pull/962
**Branch:** `feature/ti-810-bidstream-ip-features`
**Date:** 2026-04-08

---

## Status Summary

| Item | Status |
|------|--------|
| Layer 1 models written | 7/7 complete |
| CI (model-upload-dryrun) | Green |
| Ryan's feedback applied | HLL sketches, sum+count not avg, Spark config placement, removed timeout overrides |
| Dev backfill (30 days) | Complete — ~530 Dataproc Serverless jobs, zero errors |
| Backfill current through | dt=2026-04-02 (paused pending review) |
| DAG changes | Included in PR |
| model_task_config.json | Regenerated and committed |

---

## What I Need From Ryan

### 1. PR Review + Approval
- PR #962 is draft — should I mark ready-for-review?
- 10 files changed, +1,281 / -4 lines
- All 7 models follow FileStorageBaseModel pattern + naming conventions doc

### 2. OK to Copy Dev → Prod
```bash
gsutil -m cp -r \
  gs://mntn-data-archive-dev/feature_store/feature_group_1_source/{model}_feature_ti_810_*/ \
  gs://mntn-data-archive-prod/feature_store/feature_group_1_source/{model}/
```
- New folders only — `win_logs_ip/`, `bae_ip/`, `cil_ip/`, `guid_log_ip/`, `conv_log_ip/`, `aug_log_ip/`, `aug_log_ip_hourly/`
- No overwrite risk — these folders don't exist in prod yet

### 3. DAG Wiring Confirmation
- `feature_store_hourly.py`: add `aug_log_ip_hourly` task
- `feature_store_setup_model.py`: add 6 daily tasks, dependency `aug_log_ip_hourly >> aug_log_ip`
- Any other dependencies or ordering constraints?

### 4. Compute Cost Sanity Check
- **Daily ongoing:** 12 hourly jobs (aug_log_ip_hourly, 2hr chunks) + 6 daily jobs = 18 Dataproc Serverless jobs/day
- **Backfill was:** ~530 jobs over ~30 days, all completed without issues
- Is this within expected cost envelope? Zach watches GCP spend.

---

## Open Technical Questions

### Q1: isNotNull() for Parquet LIST Fields
Parquet legacy LIST fields (pmp, iab_categories, mntn_segments in augmentor_log) have schema `struct<list: array<struct<element: T>>>`. Direct `F.size(F.col("pmp.list"))` fails because Spark interprets `.list` as a map subscript.

**Current workaround:** `F.col("pmp").isNotNull()` instead of checking array size.

Is this acceptable long-term, or is there a better pattern? (e.g., explicit schema casting, `F.col("pmp")["list"]` syntax)

### Q2: Layer 2 Template + Timeline
- You offered `guid_log_derived_ip_vertical_id.py` as a template for the Layer 2 derived model
- Layer 2 would join all 7 Layer 1 IP rollups, compute 7d/14d/30d rolling windows, ratios
- What's the right timing? Should Layer 1 be fully in prod first?

### Q3: Mark PR Ready for Review?
PR is currently draft. All code compiles, CI green, backfill validated. Should I mark it ready?

---

## Schema Spot-Check: Sample Output

### win_logs_ip (dt=2026-04-02)
- **Rows:** 10,883,368 (8 parquet parts)
- **Grain:** 1 row per IP per day

| Column | Type | Description |
|--------|------|-------------|
| dt | string | Event date (partition key) |
| ip | string | IP address |
| win_count | int64 | Total auction wins |
| advertiser_id_hll | binary | HLL sketch — distinct advertisers |
| device_model_hll | binary | HLL sketch — distinct device models |
| device_make_hll | binary | HLL sketch — distinct device manufacturers |
| clearing_price_usd_sum | double | Sum of clearing prices (for avg in L2) |
| clearing_price_count | int64 | Count of clearing prices (for avg in L2) |
| min_clearing_price_usd | double | Min clearing price |
| max_clearing_price_usd | double | Max clearing price |
| video_play_count | int64 | Video plays |
| video_complete_count | int64 | Video completions |
| video_mute_count | int64 | Video mutes |
| video_pause_count | int64 | Video pauses |
| click_count | int64 | Clicks |
| viewable_count | int64 | Viewable impressions |
| measurable_count | int64 | Measurable impressions |

**Sample rows:**
```
IP: 100.1.10.11 → 3 wins, $0.032 total clearing price, 3 plays, 3 completes, 3 viewable
IP: 100.1.139.209 → 1 win, $0.009 clearing price, 1 play, 1 complete, 1 viewable
```

### bae_ip (dt=2026-04-02)
- **Rows:** 37,614,632 (8 parquet parts)
- **Grain:** 1 row per IP per day

| Column | Type | Description |
|--------|------|-------------|
| dt | string | Event date (partition key) |
| ip | string | IP address |
| auction_count | int64 | Total dropped auctions |
| genre_present_count | int64 | Auctions with genre data |
| genre_entertainment_count | int64 | Entertainment genre |
| genre_news_count | int64 | News genre |
| genre_drama_count | int64 | Drama genre |
| genre_comedy_count | int64 | Comedy genre |
| genre_sports_count | int64 | Sports genre |
| genre_hll | binary | HLL sketch — distinct genres |
| publisher_hll | binary | HLL sketch — distinct publishers |
| device_make_hll | binary | HLL sketch — distinct device manufacturers |
| roku_count | int64 | Roku device auctions |
| samsung_count | int64 | Samsung Smart TV auctions |
| lg_count | int64 | LG Smart TV auctions |

**Sample rows:**
```
IP: 100.0.187.223 → 15 auctions, 13 with genre (all news), 0 roku/samsung/lg
IP: 100.0.213.161 → 115 auctions, 93 with genre (10 entertainment), 1 roku, 109 samsung
```

---

## All 7 Models Summary

| Model | Source | Read Method | Key Features | Daily Rows (approx) |
|-------|--------|-------------|--------------|---------------------|
| aug_log_ip_hourly | augmentor_log | Parquet | Auction counts, CTV/video/PMP/IAB/domain/SSP/network | ~40M |
| aug_log_ip | hourly output | read_model | Daily rollup of above | ~40M |
| win_logs_ip | win_logs | Parquet | Device diversity, clearing price, video engagement, viewability | ~11M |
| bae_ip | bidder_auction_events | Parquet | Genre counts, publisher/device diversity, Roku/Samsung/LG | ~38M |
| cil_ip | cost_impression_log | BQ (bigquery_data.BQ) | Impression counts, HH scores, video/banner format, RTC | ~8M |
| guid_log_ip | guid_log | Parquet | Device/OS/browser diversity, product views, UTM, IP stability | ~5M |
| conv_log_ip | conversion_log | Parquet | Order amounts, conversion types/sources | ~1M |

---

## Backfill Results (30 Days, Zero Errors)

| Model | Days | Jobs | Errors |
|-------|------|------|--------|
| win_logs_ip | 31 | 31 | 0 |
| bae_ip | 31 | 31 | 0 |
| cil_ip | 31 | 31 | 0 |
| guid_log_ip | 31 | 31 | 0 |
| conv_log_ip | 31 | 31 | 0 |
| aug_log_ip_hourly | 31 | 372 | 0 |
| aug_log_ip (daily) | 30 | 30 | 0 |

**Total: ~530 Dataproc Serverless jobs, zero failures.**
