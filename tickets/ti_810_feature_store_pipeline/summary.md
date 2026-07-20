---
doc_type: ticket
title: "TI-810: Adapt Feature Store Pipeline for New Bidstream Features"
status: done
date: 2026-04-17
summary: "Add IP-level bidstream features to the feature-store pipeline for Fangorn training"
result: "7 Layer-1 PySpark models live in prod daily since 2026-04-09 (PR #962); Layer 2 next"
---

# TI-810: Adapt Feature Store Pipeline for New Bidstream Features

**Jira:** https://mntn.atlassian.net/browse/TI-810
**Epic:** [TI-789](https://mntn.atlassian.net/browse/TI-789) — Bidstream Feature Extraction & Audience Augmentation
**Status:** Complete — Layer 1 in prod, running daily
**Date Started:** 2026-04-01
**Date Completed:** 2026-04-08
**Assignee:** Malachi

---

## Current Status (2026-04-17)

**Layer 1 COMPLETE. All 7 models running in prod daily since 2026-04-09. Layer 2 next (with Ryan, next week).**

### What's Done
- 7 Layer 1 PySpark models written, tested, compiled (PR #962 CI green)
- Applied Ryan's feedback: HLL sketches, sum+count not avg, Spark config placement, removed timeout overrides
- Fixed 2 parquet schema bugs: guid_log product STRUCT, aug_log nested LIST fields (pmp/iab/segments)
- All 7 models backfilled 30 days in dev (~530 Dataproc Serverless jobs, zero errors)
- Ryan approved PR #962 ("Looks good!") — merged 2026-04-08
- Prod DAGs running daily since merge — all 7 models current through dt=2026-04-16
- Ryan confirmed: isNotNull() workaround for parquet LIST fields is acceptable
- Ryan: Layer 2 derived model next week, using `guid_log_derived_ip_vertical_id.py` as template
- Ryan: "whenever you do analysis and something is important, turn it into a feature"
- DAG changes included in PR
- model_task_config.json regenerated

### Backfill Results
| Model | Days | Jobs | Errors |
|-------|------|------|--------|
| win_logs_ip | 31 | 31 | 0 |
| bae_ip | 31 | 31 | 0 |
| cil_ip | 31 | 31 | 0 |
| guid_log_ip | 31 | 31 | 0 |
| conv_log_ip | 31 | 31 | 0 |
| aug_log_ip_hourly | 31 | 372 | 0 |
| aug_log_ip (daily) | 30 | 30 | 0 |

### Dev Output Paths
All at `gs://mntn-data-archive-dev/feature_store/feature_group_1_source/{model}_feature_ti_810_bidstream_ip_features/dt=YYYY-MM-DD/`

### What's Next
1. ~~Meet with Ryan~~ — Done 2026-04-08, approved PR
2. ~~gsutil cp dev→prod~~ — Prod DAGs populated automatically after merge
3. ~~Ryan approves + merges PR~~ — Merged 2026-04-08
4. ~~Monitor first prod runs~~ — Running daily since 2026-04-09, all current through dt=2026-04-16
5. **Layer 2 derived model** — next ticket, Ryan offered to walk through template next week

### Key Links
- **PR:** https://github.com/SteelHouse/airflow-ti/pull/962
- **Branch:** `feature/ti-810-bidstream-ip-features`
- **Repo local:** `~/Developer/work/mntn/airflow-ti`
- **Feature store naming doc:** `airflow-ti/docs/feature_store_naming_standards.md`

### Open Questions for Ryan
- isNotNull() workaround for parquet LIST fields — good enough or better pattern?
- Layer 2 template — he offered `guid_log_derived_ip_vertical_id.py` as reference
- Compute cost review — ~530 Dataproc Serverless jobs for backfill, ongoing daily cost

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

1. ~~Inventory parquet archives~~ → All confirmed (win_logs, BAE have parquet; CIL uses BQ read_model)
2. ~~Read naming conventions + RFD docs~~ → Naming pattern: `{source}_{dimensions}`, Layer 1 uses `dt`, Layer 2 uses `effective_date`
3. ~~Write Layer 1 models~~ → 7 models written, compile passes. PR #962 open (draft).
4. Write Layer 2 derived model: `bidstream_derived_ip.py` — join all Layer 1 IP rollups, compute 7d/14d/30d rolling windows, ratios, transformations
5. ~~Submit PR~~ → Draft PR #962: https://github.com/SteelHouse/airflow-ti/pull/962

### Deployment Plan (aligned with Ryan's process, 2026-04-02)

Ryan's recommended sequence:
1. Test code on 1 day to make sure it works, inspect the data
2. Once that works, backfill for ALL of augmentor_log (will take a while)
3. Once done, copy data from dev to prod
4. Get PR approved (keep backfill up to date while PR is being approved, copy new data to prod)
5. Once approved, monitor a few prod runs

---

**What I'm planning to do, step by step:**

**Step 1: Upload compiled models to dev GCS**

I will run `model_upload.py` from my feature branch. This compiles all models (existing + my 7 new ones) and uploads the code artifacts to the dev GCS bucket. It does NOT deploy to prod — it only stages code in `gs://mntn-data-archive-dev/`.

```bash
cd ~/Developer/work/mntn/airflow-ti   # on branch feature/ti-810-bidstream-ip-features
uv run python model_upload.py
```

**What this does:** Compiles Python model files + utils into deployable artifacts and uploads them to `gs://mntn-data-archive-dev/ti_resources/` (or similar dev path). This is what `model_run.py` picks up when it submits Dataproc jobs.

**What this does NOT do:** Touch prod. Touch DAGs. Change anything in Airflow.

---

**Step 2: Test ONE model on ONE day — inspect output**

I will run `win_logs_ip` for a single day (2026-03-31) to verify it works end-to-end on Dataproc Serverless.

```bash
uv run python model_run.py win_logs_ip -a '{"run_date": "2026-03-31"}'
```

**What this does:** Submits a PySpark job to **Dataproc Serverless** (cloud, not local). The job:
- Reads from `gs://mntn-data-archive-prod/win_logs/dt=2026-03-31/hh=*` (prod parquet — read only)
- Aggregates by IP: win count, device model diversity, clearing price, video engagement, viewability
- Writes output to `gs://mntn-data-archive-dev/feature_store/feature_group_1_source/win_logs_ip/dt=2026-03-31/` (dev only)

**What I'll verify:**
- Job completes without errors
- Output parquet exists at expected path
- Schema looks right: `dt, ip, win_count, distinct_device_model_count, avg_clearing_price_usd, ...`
- Row count is reasonable (should be millions of unique IPs per day)
- Spot-check a few IPs to make sure values are sane

**What this does NOT do:** Write to prod. Affect any existing pipeline.

---

**Step 3: If Step 2 passes, test ALL 7 models on ONE day**

Run each model for the same day (2026-03-31) to verify they all work:

```bash
uv run python model_run.py aug_log_ip_hourly -a '{"run_date": "2026-03-31 12:00:00"}'
uv run python model_run.py aug_log_ip -a '{"run_date": "2026-03-31"}'
uv run python model_run.py win_logs_ip -a '{"run_date": "2026-03-31"}'
uv run python model_run.py bae_ip -a '{"run_date": "2026-03-31"}'
uv run python model_run.py cil_ip -a '{"run_date": "2026-03-31"}'
uv run python model_run.py guid_log_ip -a '{"run_date": "2026-03-31"}'
uv run python model_run.py conv_log_ip -a '{"run_date": "2026-03-31"}'
```

**Same verification as Step 2** for each model. All output goes to dev.

---

**Step 4: Backfill — run all 7 models for the full augmentor_log parquet range**

Augmentor_log parquet goes back ~30 days. I'll backfill all 7 models for the available date range. This will take a while since each model × each date = one Dataproc job.

```bash
# For each date in the backfill range:
for DATE in 2026-03-{03..31} 2026-04-01; do
  uv run python model_run.py aug_log_ip_hourly -a "{\"run_date\": \"$DATE 12:00:00\"}"
  uv run python model_run.py aug_log_ip -a "{\"run_date\": \"$DATE\"}"
  uv run python model_run.py win_logs_ip -a "{\"run_date\": \"$DATE\"}"
  uv run python model_run.py bae_ip -a "{\"run_date\": \"$DATE\"}"
  uv run python model_run.py cil_ip -a "{\"run_date\": \"$DATE\"}"
  uv run python model_run.py guid_log_ip -a "{\"run_date\": \"$DATE\"}"
  uv run python model_run.py conv_log_ip -a "{\"run_date\": \"$DATE\"}"
done
```

**Note on aug_log_ip_hourly:** The hourly model processes 2 hours per run (same pattern as Ryan's `aug_log_ip_vertical_id_hourly`). To backfill a full day, I may need to run it for each hour — OR just run the daily `aug_log_ip` which reads from the hourly output. **Need to confirm with Ryan: should I backfill hourly or just daily?**

**All output goes to dev bucket only.**

---

**Step 5: Verify backfilled data in dev**

```bash
# Check all models have partitions
for MODEL in aug_log_ip_hourly aug_log_ip win_logs_ip bae_ip cil_ip guid_log_ip conv_log_ip; do
  echo "=== $MODEL ==="
  gsutil ls gs://mntn-data-archive-dev/feature_store/feature_group_1_source/${MODEL}*/
done

# Spot-check row counts
gsutil cat gs://mntn-data-archive-dev/feature_store/feature_group_1_source/win_logs_ip*/dt=2026-03-31/*.parquet | wc -c
```

---

**Step 6: Copy backfilled data from dev to prod**

After verifying dev data looks good, copy to prod:

```bash
for MODEL in aug_log_ip_hourly aug_log_ip win_logs_ip bae_ip cil_ip guid_log_ip conv_log_ip; do
  gsutil -m cp -r \
    gs://mntn-data-archive-dev/feature_store/feature_group_1_source/${MODEL}*/ \
    gs://mntn-data-archive-prod/feature_store/feature_group_1_source/${MODEL}/
done
```

**This is the scariest step — writing to prod GCS.** The data goes into new folders that don't exist yet (e.g., `win_logs_ip/`), so it won't overwrite anything. But I want Ryan's explicit OK before doing this.

---

**Step 7: Get PR #962 approved**

PR: https://github.com/SteelHouse/airflow-ti/pull/962

While PR is in review, keep backfill up to date — run new days as they become available and copy to prod.

---

**Step 8: Ryan wires DAG dependencies + merge**

After PR approval, Ryan adds tasks to the DAG files:
- `feature_store_hourly.py`: add `aug_log_ip_hourly` task
- `feature_store_setup_model.py`: add all 6 daily model tasks, set dependency `aug_log_ip_hourly >> aug_log_ip`

Merge triggers GitHub Actions deploy. Daily DAG picks up new models on next run (01:03 UTC).

---

**Step 9: Monitor first few prod runs**

Watch the first 2-3 prod runs to confirm:
- All 7 models complete successfully
- Output data matches what we saw in dev
- No impact on existing pipeline tasks
- Dataproc costs are within expectations

---

### Questions for Ryan before I start

- [ ] Does this plan look right?
- [ ] For the backfill range: should I go back as far as the parquet archives allow (~30d for augmentor_log, ~90d for win_logs/BAE), or just 30 days for all?
- [ ] For `aug_log_ip_hourly` backfill: do I need to run every hour, or can I just backfill the daily `aug_log_ip` directly from raw parquet?
- [ ] The `gsutil cp` from dev→prod for Step 6 — is that the right approach, or is there a different process?
- [ ] Any concerns about Dataproc compute costs for the backfill (~7 models × ~30 days = ~210 Dataproc jobs)?
- [ ] Should I add the DAG changes to the PR or leave that to you?

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

**Naming Conventions (from TAR-Feature Store Naming Conventions doc):**

Table pattern: `{source_dataset}_{suffix}_{dimensions}`
- Layer 1 (source): no suffix. E.g. `aug_log_ip`, `win_logs_ip`
- Layer 1 hourly: `_hourly` suffix. E.g. `aug_log_ip_hourly`
- Layer 2 (derived): `_derived_`. E.g. `bidstream_derived_ip`
- Layer 3 (pivoted): `_pivot_`. E.g. `bidstream_pivot_ip`
- Multi-source Layer 2: combine source names. E.g. `guid_and_conv_log_derived_advertiser_id_dsc_id`

Official source prefixes: `aug_log`, `guid_log`, `conversion_log`/`conv_log`, `site_visit_signal`, `ipdsc`
New prefixes needed for: `win_logs`, `bae` (bidder_auction_events), `cil` (cost_impression_log)

Column naming:
- snake_case, metric names include lookback: `visit_count_7d`, `distinct_site_count_30d`
- Dimensions use `_id` suffix: `advertiser_id`, `vertical_id`
- Outcome variables include `_outcome`: `visits_forward_7d_outcome`

Partitioning:
- Layer 1: `dt=YYYY-MM-DD` (event date), optionally `dt=YYYY-MM-DD/hh=HH`
- Layer 2/3: `effective_date=YYYY-MM-DD` (midnight after lookback window)

GCS paths:
- Prod: `gs://mntn-data-archive-prod/feature_store/feature_group_{N}_{type}/`
- Dev: `gs://mntn-data-archive-dev/feature_store/feature_group_{N}_{type}/`

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
