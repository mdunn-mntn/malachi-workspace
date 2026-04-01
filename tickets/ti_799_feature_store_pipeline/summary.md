# TI-799: Adapt Feature Store Pipeline for New Bidstream Features

**Jira:** https://mntn.atlassian.net/browse/TI-799
**Epic:** [TI-789](https://mntn.atlassian.net/browse/TI-789) — Bidstream Feature Extraction & Audience Augmentation
**Status:** In Progress
**Date Started:** 2026-04-01
**Date Completed:**
**Assignee:** Malachi

---

## 1. Introduction

Adapt Ryan Kleck's existing feature store pipeline (`aug_log_ip_vertical_id_hourly.py`) to extract the top NEW features identified in [TI-790](https://mntn.atlassian.net/browse/TI-790). The pipeline reads from parquet archives (not BQ) because augmentor_log has only a 10-day BQ TTL but ~30 days of parquet history.

Two separate output locations:
- **Pre-visit features** (targeting) — available at bid time, used by DS13/DS19 via site_visits signal table
- **Feedback features** (retraining) — post-visit signals from guid_log + conversion_log, used to enrich the feature store over time

## 2. The Problem

TI-790 identified 37 genuinely new IP-level features that predict site visits (AUC 0.777 NEW-only). These features exist in raw log tables but aren't yet flowing into the feature store pipeline. Ryan's existing pipeline only extracts IP × vertical_id rollups from augmentor_log. We need to expand it to capture the top features across all source tables.

## 3. Plan of Action

1. Inventory which source tables have parquet archives at `gs://mntn-data-archive-prod/`
2. Design pipeline architecture — one pipeline per source table or combined
3. Write pre-visit feature pipeline(s):
   - **augmentor_log** (parquet): al_n_domains, al_pct_video, al_pct_ctv, al_pct_pmp, al_pct_iab, al_n_auctions, al_n_networks, al_n_ssps, al_has_ctv
   - **win_logs** (parquet TBD): wl_n_models, wl_avg_price, wl_n_wins, wl_n_adv, wl_plays, wl_completes, wl_vcr, wl_viewable, wl_n_makes
   - **bidder_auction_events** (parquet TBD): bae_pct_ent, bae_pct_comedy, bae_pct_news, bae_pct_drama, bae_pct_sports, bae_n_genres, bae_n_pubs, bae_n_makes, bae_n_auctions, bae_pct_genre
   - **cost_impression_log** (parquet TBD): ci_pct_video, ci_n_vendors
4. Write feedback feature pipeline(s):
   - **guid_log**: device mix, browser/OS families, product views, UTM source diversity, visit patterns
   - **conversion_log**: order amounts, conversion types, identity signal counts
5. Define output schemas and partition strategy (dt/hh matching Ryan's pattern)
6. Test locally with a single hour of data
7. Submit for code review (Ryan + Alex)

## 4. Investigation & Findings

### Source Table Parquet Archives
_(To be filled — checking gs://mntn-data-archive-prod/ for each table)_

### Feature Selection

**Pre-visit features (37 total, top 10 NEW-only by SHAP):**

| # | Feature | Source | SHAP (NEW-only) | Description |
|---|---------|--------|-----------------|-------------|
| 1 | wl_n_models | win_logs | 0.413 | Device model diversity — household size proxy |
| 2 | ci_pct_video | cost_impression_log | 0.341 | % VIDEO format impressions |
| 3 | al_n_domains | augmentor_log | 0.320 | Content consumption breadth |
| 4 | al_pct_video | augmentor_log | 0.253 | % VIDEO placement in auctions |
| 5 | wl_avg_price | win_logs | 0.245 | Clearing price per auction (USD) |
| 6 | bae_pct_ent | bidder_auction_events | 0.238 | % entertainment genre |
| 7 | wl_n_wins | win_logs | 0.237 | Total auction wins |
| 8 | al_pct_pmp | augmentor_log | 0.219 | PMP deal rate |
| 9 | al_n_auctions | augmentor_log | 0.189 | Market activity |
| 10 | al_pct_iab | augmentor_log | 0.185 | IAB category data availability |

**Feedback features (from TI-790 Model B — AUC 0.999 tautological but useful for retraining):**

| Feature | Source | Description |
|---------|--------|-------------|
| gl_n_events | guid_log | Total pixel fires |
| gl_n_adv | guid_log | # distinct advertisers visited |
| gl_has_desktop/mobile/tablet | guid_log | Device type flags |
| gl_pct_mobile | guid_log | % mobile events |
| gl_n_os_families | guid_log | OS diversity |
| gl_n_browser_families | guid_log | Browser diversity |
| gl_n_product_views | guid_log | Product page views |
| gl_n_utm_events | guid_log | Events with UTM tracking |
| gl_has_new_visit | guid_log | New visitor flag |
| gl_pct_new | guid_log | % new visits |
| gl_pct_ip_stable | guid_log | IP stability signal |
| cv_n_conv | conversion_log | # conversions |
| cv_total_amt | conversion_log | Total order amount |
| cv_avg_amt | conversion_log | Avg order amount |
| cv_n_orders | conversion_log | # distinct orders |
| cv_n_types | conversion_log | # conversion types |
| cv_n_adv | conversion_log | # advertisers converted |

### Meeting Context (2026-04-01 Sync)

Key technical decisions from the meeting:
- Ryan's pipeline reads from **parquet** (not BQ) — augmentor_log has 10d BQ TTL but ~30d parquet
- Page URL already added to augmentor_log for banner placements (Matt's feature store)
- IPv6 often populated when IP field is blank in augmentor_log
- `private_marketplace_deals` table has PMP deal names/IDs; DS42 converts string→integer category IDs
- New features flow to DS13/DS19 via site_visits signal table
- OpenRTB spec is standardized — MNTN Bidder will have same fields (contact: Rogus)
- Alex: CTV vs non-CTV is primary delimiter; domain parsing > IAB categories alone

### Pipeline Architecture

Two output locations:
```
gs://mntn-data-archive-prod/feature_store/
├── pre_visit_features/     ← targeting (DS13/DS19)
│   ├── /dt=YYYY-MM-DD/hh=HH/   (augmentor_log features)
│   └── ...
└── feedback_features/      ← retraining/enrichment
    └── /dt=YYYY-MM-DD/hh=HH/   (guid_log + conversion_log)
```

## 5. Solution

_(To be filled as pipeline code is written)_

## 6. Questions Answered

_(To be filled)_

## 7. Data Documentation Updates

_(To be filled)_

## 8. Open Items / Follow-ups

- [ ] Confirm parquet archive paths for win_logs, bidder_auction_events, cost_impression_log, guid_log, conversion_log
- [ ] Confirm output GCS paths with Ryan
- [ ] Determine if feedback features should be hourly or daily (guid_log fires are sparse per hour)
- [ ] IPv6 handling — should we use IPv6 when IP is blank?
- [ ] PMP deal name enrichment via private_marketplace_deals table

## Files

| File | Purpose |
|------|---------|
| [aug_log_ip_vertical_id_hourly.py](../ti_790_bidstream_feature_inventory/artifacts/aug_log_ip_vertical_id_hourly.py) | Ryan's template pipeline |
| [ti_790_presentation.md](../ti_790_bidstream_feature_inventory/artifacts/ti_790_presentation.md) | Full feature rankings from TI-790 |
| [ti_790_training_dataset_v2.sql](../ti_790_bidstream_feature_inventory/queries/ti_790_training_dataset_v2.sql) | BQ feature extraction queries (reference) |
