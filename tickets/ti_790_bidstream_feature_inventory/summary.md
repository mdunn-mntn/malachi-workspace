# TI-790: Feature Inventory & Quality Assessment

**Jira:** https://mntn.atlassian.net/browse/TI-790
**Epic:** [TI-789](https://mntn.atlassian.net/browse/TI-789) — Bidstream Feature Extraction & Audience Augmentation
**Status:** In Progress
**Date Started:** 2026-03-30
**Assignee:** Malachi

---

## 1. Introduction

Catalog every IP-level feature across MNTN's log tables, assess quality, and determine which are most valuable for improving targeting performance. Part of the Feature Store initiative (TI-789) with Alex Knorr and Ryan Kleck.

## 2. The Problem

Fangorn's feature store needs more signals. We have 25+ log tables but don't know which features are unique per table, which are redundant, and which actually predict visits.

## 3. What We Did

1. Scanned all 25 log tables. Identified unique columns per table programmatically.
2. 6 tables have unique signal — built and tested daily snapshot queries for each.
3. Joined into a training dataset (117K IPs, 2026-03-29) with IVR labels from clickpass_log.
4. Trained XGBoost. Measured importance via gain, weight, cover, composite rank, and SHAP.
5. Split pre-visit (targeting) vs feedback (retraining) features to avoid leakage.
6. Investigated 1P vs 3P segment composition — 97% of mntn_segments are our own outputs.

## 4. Key Findings

- **66 features identified** across 6 tables. 9 are our own system outputs (EXISTING), 35 are genuinely new (NEW), 17 are post-visit feedback, 5 have zero importance.
- **Pre-visit model AUC: 0.896.** EXISTING features dominate raw rankings — validates that Fangorn/RTC works.
- **Top 3 genuinely new features:** clearing price (win_logs), device model diversity (win_logs), auction activity (augmentor_log).
- **Content genre features** rank mid-tier for general IVR but are highest value for vertical classification (Alex's TI-791).
- **guid_log/conversion_log features** are near-perfect predictors (AUC 0.999) but are leaky — post-visit only. Use for retraining.

Full ranked table of all 66 features with methodology: [ti_790_presentation.md](artifacts/ti_790_presentation.md)

## 5. Deliverables

| File | Purpose |
|------|---------|
| [ti_790_presentation.md](artifacts/ti_790_presentation.md) | **The shareable doc.** All 66 features ranked, takeaways, next steps, methodology. |
| [ti_790_project_plan.md](artifacts/ti_790_project_plan.md) | Phased execution plan for the TI-789 epic |
| [ti_790_cross_table_unique_columns.md](artifacts/ti_790_cross_table_unique_columns.md) | Supporting: programmatic unique-column analysis of all 25 tables |
| [ti_790_xgboost_split_analysis.py](artifacts/ti_790_xgboost_split_analysis.py) | Python script that produced all results |
| [queries/](queries/) | 6 daily snapshot queries + combined training dataset query |
| [ti_790_shap_pre_visit.png](outputs/ti_790_shap_pre_visit.png) | SHAP summary plot |
| [ti_790_all_features_ranked.csv](outputs/ti_790_all_features_ranked.csv) | Raw feature rankings CSV |

## 6. For Alex & Ryan

**Alex (TI-791):** `iab_categories` (augmentor_log, bronze only, 30% fill) and `content_genre` (bidder_auction_events, 87% fill) are the vertical classification signals. Both need normalization (case, commas, prefixes). Genre ranked mid-tier for general IVR but should rank high for per-advertiser prediction.

**Ryan (TI-792):** `inventory_source` has 40 values in augmentor_log vs 3 in bidder_auction_events. Still need the exchange reference table. Content fields map to OpenRTB `content` object. `device_make` maps to `device.make`.

## 7. Documentation Updates

- `knowledge/data_knowledge.md` — Added Feature Store section: pre-visit vs feedback leakage, bronze-only fields, content_genre normalization, cost_impression_log gotchas, scale reference
- `knowledge/experimentation.md` — Added Feature Importance Methodology Lessons section

## 8. Open Items

- [ ] Vertical classification model — test genre features for per-advertiser IVR (Alex)
- [ ] Cold-start analysis — test new features on IPs with no Fangorn score
- [ ] 1P vs 3P segment split — isolate DS3 interest segments as a genuinely-new feature
- [ ] Ryan: find exchange reference table
- [ ] Features not yet modeled: IAB category percentages, content_series, parsed identity signals from conversion_log query string
- [ ] Wednesday 4/2 sync: present findings to Alex and Ryan
