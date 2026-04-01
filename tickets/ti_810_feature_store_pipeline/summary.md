# TI-810: Adapt Feature Store Pipeline for New Bidstream Features

**Jira:** https://mntn.atlassian.net/browse/TI-810
**Epic:** [TI-789](https://mntn.atlassian.net/browse/TI-789) — Bidstream Feature Extraction & Audience Augmentation
**Status:** In Progress
**Date Started:** 2026-04-01
**Date Completed:**
**Assignee:** Malachi

---

## 1. Introduction

Add new IP-level features to MNTN's feature store pipeline to improve the Fangorn targeting model. Features were identified and ranked by SHAP importance in [TI-790](https://mntn.atlassian.net/browse/TI-790) (AUC 0.831 all-features, 0.777 NEW-only).

The feature store is a three-layer PySpark pipeline running on GCP Dataproc via Airflow:
- **Layer 1 (source):** Hourly/daily rollups from raw log parquet archives → `feature_group_1_source/`
- **Layer 2 (derived):** 7/14/30-day windowed aggregations, joins, scoring → `feature_group_2_derived/`
- **Layer 3 (pivoted):** Wide-format pivot (one row per IP) → `feature_group_3_pivoted/`

All output goes to `gs://mntn-data-archive-prod/feature_store/`. Code lives in `SteelHouse/airflow-ti` under `models/feature_store/`.

**Key scope clarification:** We do NOT need to distinguish pre-visit vs feedback features for this work. The purpose is to gather features to **train** the Fangorn model — not for real-time scoring. Even guid_log and conversion_log features are valuable training signals. EXISTING features not directly connected to scoring/targeting (like device mix, browser families) are also fine to include.

## 2. The Problem

TI-790 identified 46 pre-visit + 17 feedback features across 6 source tables that predict site visits. Ryan's existing pipeline only extracts IP × vertical_id rollups from augmentor_log. We need Layer 1 models for each source table to extract these features at IP level, then a Layer 2 model to join them with rolling windows.

## 3. Plan of Action

1. ~~Inventory parquet archives~~ → Confirmed: augmentor_log, guid_log, conversion_log all have parquet at `gs://mntn-data-archive-prod/`
2. Check parquet archives for: win_logs, bidder_auction_events, cost_impression_log
3. Write Layer 1 models (one per source table, following `FileStorageBaseModel` pattern):
   - `aug_log_ip_features_hourly.py` → hourly IP rollup from augmentor_log parquet
   - `aug_log_ip_features.py` → daily rollup from hourly (reads upstream via `read_model`)
   - `guid_log_ip_features.py` → daily IP rollup from guid_log parquet
   - `conv_log_ip_features.py` → daily IP rollup from conversion_log parquet
   - `win_logs_ip_features.py` → daily IP rollup (parquet or BQ TBD)
   - `bae_ip_features.py` → daily IP rollup (parquet or BQ TBD)
   - `cil_ip_features.py` → daily IP rollup (parquet or BQ TBD)
4. Write Layer 2 derived model: join all Layer 1 IP rollups, compute rolling windows (7d/14d/30d)
5. Update DAGs (`feature_store_hourly.py`, `feature_store_setup_model.py`) to include new tasks
6. Test locally via `model_run.py` → Astro dev
7. Submit PR to `SteelHouse/airflow-ti` for Ryan's review

## 4. Investigation & Findings

### Feature Store Architecture (from Ryan walkthrough + repo review)

**Repo:** `SteelHouse/airflow-ti`
**Project:** `dw-main-bronze`
**Bucket:** `gs://mntn-data-archive-prod/feature_store/`

**Three-layer pattern:**
```
Layer 1 (feature_group_1_source/)
  ├── aug_log_ip_vertical_id_hourly  ← reads gs://mntn-data-archive-prod/augmentor_log/
  ├── aug_log_ip_vertical_id         ← reads hourly output via read_model()
  ├── guid_log_advertiser_id_dsc_id  ← reads gs://mntn-data-archive-prod/guid_log/
  ├── guid_log_ip_advertiser_id      ← reads guid_log parquet
  ├── conversion_log_advertiser_id_dsc_id ← reads gs://mntn-data-archive-prod/conversion_log/
  ├── site_visit_signal_advertiser_id_dsc_id
  ├── core_advertiser_id             ← reads from Postgres via JDBC
  ├── core_campaign_id
  ├── core_campaign_group_id
  ├── salesforce_advertiser_id
  ├── sentiment_advertiser_id
  ├── summary_advertiser_id          ← reads from BQ via Spark connector
  ├── summary_campaign_id
  └── summary_campaign_group_id

Layer 2 (feature_group_2_derived/)
  ├── guid_and_conv_log_derived_advertiser_id_dsc_id  ← 30d window, lift scoring
  ├── guid_log_derived_ip_vertical_id                 ← 7/14/30d visit windows
  ├── guid_log_generic_penalty_derived                ← category popularity penalty
  ├── site_visit_signal_derived
  ├── core_derived_advertiser_id                      ← rolling windows + HLL merge
  ├── core_derived_campaign_id
  └── core_derived_campaign_group_id

Layer 3 (feature_group_3_pivoted/)
  └── guid_log_pivot_ip_vertical_id  ← wide format: one row per IP
```

**Base class pattern:**
```python
from utils_model.base_model import FileStorageBaseModel, model_config, compute

@compute.dataproc_batch(runtime_properties={...}, labels={...})
@model_config(
    location_root="gs://mntn-data-archive-prod/feature_store/feature_group_1_source",
    location_root_dev="gs://mntn-data-archive-dev/feature_store/feature_group_1_source",
    file_format="parquet",
)
class MyModel(FileStorageBaseModel):
    def model(self, args_run_date: str):
        # Read upstream: self.read_model("module.ClassName").load(paths)
        # Read parquet: self.spark.read.parquet(path)
        # Read Postgres: self.read_model("core_db_data.CoreDb").dbtable(query)
        # Read BQ: self.read_model("bq_data.BqData").query(sql)
        # Write: self.df_write(df).mode("overwrite").save("/dt={date}")
```

**DAGs:**
- `feature_store_hourly` — runs at :15 past each hour, currently only `aug_log_ip_vertical_id_hourly`
- `feature_store_setup_model` — daily at 01:03 UTC, runs all Layer 1 → Layer 2 → Layer 3 with deps
- `feature_store_snapshot` — monthly snapshots on specific days of month

**Key patterns from existing code:**
- `read_model("module.ClassName")` resolves upstream dependencies — Victor's framework knows to read from prod if upstream unchanged, dev if changed
- `model_upload.py` compiles + uploads code artifacts to GCS
- `model_run.py` runs a single model locally for testing
- HLL sketches (`hll_sketch_agg`) used for distinct count features (e.g., distinct IPs)
- Ryan's advice: distinctness is "tricky" — use HLL sketches for count distinct, or defer
- Parquet archives at `gs://mntn-data-archive-prod/{table_name}/` partitioned by `dt=YYYY-MM-DD/hh=HH`
- Group by IP in Layer 1 with `CASE WHEN` flags for binary features, `COUNT(*)` for volume

### Confirmed Parquet Archives
| Table | Parquet Path | Partition | Status |
|-------|-------------|-----------|--------|
| augmentor_log | `gs://mntn-data-archive-prod/augmentor_log/` | `region={east,west}/dt=YYYY-MM-DD/hh=HH` | Confirmed (Ryan's pipeline) |
| guid_log | `gs://mntn-data-archive-prod/guid_log/` | `dt=YYYY-MM-DD/hh=HH` | Confirmed (existing pipeline) |
| conversion_log | `gs://mntn-data-archive-prod/conversion_log/` | `dt=YYYY-MM-DD` | Confirmed (existing pipeline) |
| win_logs | `gs://mntn-data-archive-prod/win_logs/` | `dt=YYYY-MM-DD/hh=HH` | Confirmed (gsutil ls) |
| bidder_auction_events | `gs://mntn-data-archive-prod/bidder_auction_events/` | `region={east,west}/dt=YYYY-MM-DD` | Confirmed (gsutil ls) |
| cost_impression_log | **NO PARQUET ARCHIVE** | — | Not in bucket. Must read from BQ via Spark connector or skip. |

### Scope Update: All Features for Model Training

**Original assumption (TI-790):** Pre-visit features for targeting, feedback features for retraining — separate pipelines, separate output locations.

**Updated understanding (post-meeting):** The goal is to gather the best features to **train the Fangorn model**, not to serve features in real-time. This means:
- Pre-visit vs feedback distinction **doesn't matter** — all features go into the same training dataset
- guid_log features (device mix, browser families, product views, visit patterns) are valuable training signals
- conversion_log features (order amounts, conversion types) are valuable training signals
- EXISTING features not directly tied to scoring/targeting are fine to include
- Features still need to be computed per source table (separate Layer 1 models), but output goes to the same feature store location
- No IPv6 fallback — skip IPs where IP is blank

### Feature Selection (Full — All Sources)

**From augmentor_log (hourly → daily):**

| Feature | SHAP (NEW-only) | Description | Layer 1 Computation |
|---------|-----------------|-------------|-------------------|
| al_n_auctions | 0.189 | # auctions | COUNT(*) |
| al_has_ctv | 0.010 | Has CTV device | MAX(CASE WHEN device_type IN ('connected_tv','set_top_box') THEN 1 ELSE 0 END) |
| al_pct_ctv | 0.171 | % CTV device type | COUNTIF(CTV) / COUNT(*) |
| al_pct_video | 0.253 | % VIDEO placement | COUNTIF(placement_type='VIDEO') / COUNT(*) |
| al_n_ssps | 0.082 | # distinct SSPs | COUNT(DISTINCT inventory_source) — use HLL sketch |
| al_n_networks | 0.113 | # distinct networks | COUNT(DISTINCT network) — use HLL sketch |
| al_pct_iab | 0.185 | % with IAB data | COUNTIF(iab_categories not empty) / COUNT(*) |
| al_avg_segments | n/a (EXISTING) | Avg MNTN segments | AVG(ARRAY_LENGTH(mntn_segments.list)) |
| al_pct_pmp | 0.219 | % with PMP deals | COUNTIF(pmp not empty) / COUNT(*) |
| al_n_domains | 0.320 | # distinct domains | COUNT(DISTINCT domain) — use HLL sketch |

**From win_logs (daily):**

| Feature | SHAP (NEW-only) | Description |
|---------|-----------------|-------------|
| wl_n_models | 0.413 | # distinct device models (household size proxy) |
| wl_avg_price | 0.245 | Avg clearing price (USD) |
| wl_n_wins | 0.237 | Total auction wins |
| wl_n_adv | 0.155 | # distinct advertisers |
| wl_plays | 0.066 | # video plays |
| wl_completes | 0.079 | # video completions |
| wl_vcr | 0.059 | Video completion rate |
| wl_viewable | 0.066 | # viewable impressions |
| wl_n_makes | 0.052 | # distinct device manufacturers |
| wl_mutes | 0.009 | # video mutes |
| wl_pauses | 0.004 | # video pauses |
| wl_clicks | 0.003 | # clicks |
| wl_measurable | 0.019 | # measurable impressions |

**From bidder_auction_events (daily):**

| Feature | SHAP (NEW-only) | Description |
|---------|-----------------|-------------|
| bae_pct_ent | 0.238 | % entertainment genre |
| bae_pct_comedy | 0.142 | % comedy genre |
| bae_pct_news | 0.111 | % news genre |
| bae_pct_drama | 0.099 | % drama genre |
| bae_pct_sports | 0.073 | % sports genre |
| bae_pct_genre | 0.178 | % with any genre data |
| bae_n_genres | 0.125 | # distinct genres |
| bae_n_pubs | 0.158 | # distinct publishers |
| bae_n_makes | 0.105 | # device manufacturers |
| bae_n_auctions | 0.124 | # dropped auctions |
| bae_samsung | 0.066 | Has Samsung Smart TV |
| bae_roku | 0.025 | Has Roku device |
| bae_lg | 0.022 | Has LG Smart TV |

**From cost_impression_log (daily):**

| Feature | SHAP (NEW-only) | Description |
|---------|-----------------|-------------|
| ci_pct_video | 0.341 | % VIDEO format impressions |
| ci_n_vendors | 0.069 | # distinct supply vendors |
| ci_pct_new | n/a (EXISTING) | % impressions where IP is "new" |
| ci_hh_score | n/a (EXISTING) | Fangorn household score |
| ci_adv_hh_score | n/a (EXISTING) | Fangorn advertiser score |
| ci_pct_rtc | n/a (EXISTING) | % RTC conquest impressions |
| ci_total_cost | n/a (EXISTING) | Total media $ spent |
| ci_n_imp | n/a (EXISTING) | # impressions served |

**From guid_log (daily):**

| Feature | Description |
|---------|-------------|
| gl_n_events | Total pixel fires |
| gl_n_adv | # distinct advertisers visited |
| gl_has_desktop | Has desktop device |
| gl_has_mobile | Has mobile device |
| gl_has_tablet | Has tablet device |
| gl_pct_mobile | % mobile events |
| gl_n_os_families | OS diversity |
| gl_n_browser_families | Browser diversity |
| gl_n_product_views | Product page views |
| gl_n_utm_events | Events with UTM tracking |
| gl_has_new_visit | New visitor flag |
| gl_pct_new | % new visits |
| gl_pct_ip_stable | IP stability (ip == original_ip) |

**From conversion_log (daily):**

| Feature | Description |
|---------|-------------|
| cv_n_conv | # conversions |
| cv_total_amt | Total order amount |
| cv_avg_amt | Avg order amount |
| cv_n_orders | # distinct orders |
| cv_n_types | # conversion types |
| cv_n_adv | # advertisers converted |

### Ryan's Technical Guidance

From walkthrough:
- **Group by IP** in Layer 1. One row per IP per day.
- **CASE WHEN flags** for binary features (has_ctv, has_pmp, etc.)
- **COUNT(*)** for volume features
- **Distinctness is tricky** — use HLL sketches for count distinct (already used in guid_log pipeline). Ryan: "maybe skip distinct stuff for now... we do it with sketches"
- **Percentages** compute in Layer 2 (Layer 1 stores raw counts, Layer 2 divides). Ryan: "Layer 1 you do count by placement_type, Layer 2 you say what percent were video"
- **Rolling windows** in Layer 2: 7d, 14d, 30d (Matt's standard)
- **Compute settings** scale with data size: augmentor_log needs 50-200 executors (huge), guid_log IP rollup only needs 5-20
- For tables with hourly parquet: hourly model → daily model (aug_log pattern). For daily-only parquet: single daily model.

### Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Pre-visit vs feedback separation | **No separation** | Training, not real-time. All features valuable. |
| IPv6 fallback | **No** | Skip IPs where IP is blank |
| Pipeline structure | **One model per source table** | Follows existing pattern, isolates failures |
| Output location | `feature_group_1_source/` for Layer 1 | Same bucket as existing models |
| Distinct counts | **HLL sketches** | Ryan's recommendation, already used in codebase |

## 5. Solution

_(To be filled as pipeline code is written)_

## 6. Questions Answered

- **Q:** Do we need separate output locations for pre-visit vs feedback features?
  **A:** No. The feature store's purpose is to gather features for model training, not real-time serving. All features go to the same location.

- **Q:** Should we use IPv6 when IP is blank?
  **A:** No. Skip those rows.

- **Q:** One combined pipeline or per source table?
  **A:** Per source table — one Layer 1 model per table, following the existing pattern. But the output is all IP-level features that get joined in Layer 2.

- **Q:** Can we include EXISTING features (like guid_log, ci_hh_score)?
  **A:** Yes — as long as they're not directly connected to scoring/targeting. Device mix from guid_log, browser families, product views, conversion amounts — all fine for training.

## 7. Data Documentation Updates

_(To be filled)_

## 8. Open Items / Follow-ups

- [x] ~~Check parquet archives~~ → win_logs ✅, bidder_auction_events ✅, cost_impression_log ❌ (no parquet — must use BQ Spark connector or skip)
- [ ] Confirm naming convention with Ryan (e.g., `aug_log_ip_features` vs `aug_log_ip_bidstream_features`)
- [ ] HLL sketch implementation for distinct counts — review existing usage in guid_log pipeline
- [ ] Determine Layer 2 rolling window sizes (7d/14d/30d — follow Matt's convention)
- [ ] Set up Astro dev locally for testing
- [ ] PMP deal name enrichment via `private_marketplace_deals` table (stretch goal)
- [x] ~~IPv6 handling~~ → No IPv6 fallback
- [x] ~~Pre-visit vs feedback separation~~ → No separation needed

## Files

| File | Purpose |
|------|---------|
| [aug_log_ip_vertical_id_hourly.py](../ti_790_bidstream_feature_inventory/artifacts/aug_log_ip_vertical_id_hourly.py) | Ryan's template pipeline (local copy) |
| [ti_790_presentation.md](../ti_790_bidstream_feature_inventory/artifacts/ti_790_presentation.md) | Full feature rankings from TI-790 |
| [ti_790_training_dataset_v2.sql](../ti_790_bidstream_feature_inventory/queries/ti_790_training_dataset_v2.sql) | BQ feature extraction queries (reference for column names/logic) |

## Reference: Repo Structure

```
SteelHouse/airflow-ti/
├── model_upload.py          ← compiles + uploads code to GCS
├── model_run.py             ← local execution (test single model)
├── models/feature_store/
│   ├── feature_group_1_source/   ← ADD NEW FILES HERE
│   ├── feature_group_2_derived/
│   └── feature_group_3_pivoted/
└── dags/models/
    ├── feature_store_hourly.py       ← update if adding hourly models
    └── feature_store_setup_model.py  ← update to add new daily tasks
```
