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
3. Joined into a training dataset — 372K (IP, advertiser) pairs. Features from 2026-03-28, labels from 2026-03-29. No temporal leakage.
4. Trained XGBoost. Ranked features by SHAP (mean absolute Shapley value).
5. Split pre-visit (targeting) vs feedback (retraining) features to avoid leakage.
6. Ran two models: all features (EXISTING + NEW) and NEW-only (EXISTING removed).
7. Investigated 1P vs 3P segment composition — 97% of mntn_segments are our own outputs.
8. Rebuilt presentation using Presentation Playbook framework — restructured from report to persuasion format.

## 4. Key Findings

- **46 pre-visit features identified** across 6 tables. 9 are our own system outputs (EXISTING), 37 are genuinely new (NEW). 17 additional post-visit feedback features ranked separately.
- **Temporally-correct model AUC: 0.831** (all features) / **0.777** (NEW features only). Label = visited THIS advertiser. 372K (IP, advertiser) pairs, 0.84% visit rate. Features from day N-1, labels from day N.
- **Lift at actionable thresholds:** Top 1% of IPs = 8.2% visit rate = 10x lift. NEW-only: top 1% = 5.7% = 7x lift. Note: all IPs were pre-selected by Fangorn targeting, so lift is *within the Fangorn-selected pool*, not vs random population.
- **Top NEW features (NEW-only model):** device model diversity (win_logs, SHAP 0.413 — household size proxy), video format (ci, 0.341), content domain breadth (augmentor_log, 0.320), video placement (augmentor_log, 0.253), clearing price (win_logs, 0.245).
- **Content genre features rose significantly** with full-day BAE data — `bae_pct_ent` jumped from #26 to #8. Entertainment-heavy IPs visit less (↓ direction). Comedy, news, drama, sports all carry signal.
- **Temporal leakage was real but small:** V1 (same-day) AUC 0.842 → V2 (day N-1/N) AUC 0.831. Rankings stable across correction.
- **guid_log/conversion_log features** produce AUC 0.999 but this is tautological — guid_log only fires on site visits. Use for retraining, not prediction.
- **Known limitations:** features are IP-level not (IP, advertiser)-level, 4-hour augmentor_log sample, single day with no CIs, ci_pct_new may be data-availability confound. See presentation for full details.

Full ranked table with methodology: [ti_790_presentation.md](artifacts/ti_790_presentation.md)
Presentation-format version: [ti_790_presentation_new.md](artifacts/ti_790_presentation_new.md)

## 5. Deliverables

| File | Purpose |
|------|---------|
| [ti_790_presentation.md](artifacts/ti_790_presentation.md) | **Reference doc.** All 46 pre-visit features ranked, methodology, glossary, known limitations. |
| [ti_790_presentation_new.md](artifacts/ti_790_presentation_new.md) | **The shareable presentation.** Playbook-structured: Power Line, lift table, top 10 features, story, call to action. Full tables in appendix. |
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
- `knowledge/experimentation.md` — Added Feature Importance Methodology Lessons section. Updated 2026-04-01: temporal leakage lesson, sample selection bias in targeting-system evaluation, content genre sampling sensitivity, fillna(0) vs NaN for ratios, corrected AUC numbers to v2.

## 8. Open Items

- [ ] Vertical classification model — test genre features for per-advertiser IVR (Alex)
- [ ] Cold-start analysis — test new features on IPs with no Fangorn score
- [ ] 1P vs 3P segment split — isolate DS3 interest segments as a genuinely-new feature
- [ ] Ryan: find exchange reference table
- [ ] Features not yet modeled: IAB category percentages, content_series, parsed identity signals from conversion_log query string
- [ ] Wednesday 4/2 sync: present findings to Alex and Ryan
