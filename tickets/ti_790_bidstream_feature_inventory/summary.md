---
doc_type: ticket
title: "TI-790: Bidstream Feature Inventory & Quality Assessment"
status: done
date: 2026-05-06
summary: "Catalog + rank IP-level log-table features for Fangorn feature store by SHAP"
result: "46 pre-visit features cataloged; XGBoost AUC 0.831, top 1% of IPs = 10x visit lift"
keywords: [ti_790, bidstream, feature store, feature inventory, shap, xgboost, visit prediction, fangorn, win_logs, augmentor_log, pre-visit features, ti_789]
---

## TL;DR

**Q:** TI-790: inventory + quality-rank every IP-level log-table feature for the Fangorn feature store (TI-789 epic).

**A:** 46 pre-visit features cataloged across 6 of 25 log tables; temporally-correct XGBoost AUC 0.831 (0.777 NEW-only), top 1% of IPs = 8.2% visit rate ≈ 10x lift *within the Fangorn-selected pool*. 37 of the 46 are genuinely new signals; top NEW = device-model diversity (household-size proxy), video format, content-domain breadth.

**How:** Scanned 25 log tables for unique columns; 6 carried unique signal → 6 daily snapshot queries joined into a 372K (IP,advertiser) training set (features day N-1, labels day N, 0.84% visit rate, no temporal leakage). XGBoost ranked by mean-abs SHAP; pre-visit (targeting) split from feedback (retraining) features. Ran all-features vs NEW-only models.

**Tables:** win_logs · augmentor_log · ci · guid_log · conversion_log

**Learned:**
- Temporally-correct AUC 0.831 (all) / 0.777 (NEW-only); lift is within the Fangorn-selected pool, not vs random population.
- Top NEW features: device model diversity (win_logs, SHAP 0.413), video format (ci, 0.341), content domain breadth (augmentor_log, 0.320).
- Temporal leakage real but small: same-day AUC 0.842 → day N-1/N 0.831, rankings stable. guid_log/conversion_log give AUC 0.999 but tautological (fire on visit) — retraining only, not prediction.
- 97% of mntn_segments are MNTN's own outputs (1P, not 3P).

**Reuse when:** ranking log-table features for the feature store · which bidstream/log signals predict visits · SHAP importance with pre-visit vs feedback leakage split · TI-789 bidstream epic.

---

# TI-790: Feature Inventory & Quality Assessment

**Jira:** https://mntn.atlassian.net/browse/TI-790
**Epic:** [TI-789](https://mntn.atlassian.net/browse/TI-789) — Bidstream Feature Extraction & Audience Augmentation
**Status:** Done
**Date Started:** 2026-03-30
**Date Completed:** 2026-04-01
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
| [ti_790_xgboost_scoped.py](artifacts/ti_790_xgboost_scoped.py) | Scoped XGBoost variant |
| [ti_790_nba_fast_food_pmp_analysis.py](artifacts/ti_790_nba_fast_food_pmp_analysis.py) | Ryan's PMP deal overlap analysis (NBA vs HGTV vs fast food) |
| [ti_791_bidstream_investigation_alex.py](artifacts/ti_791_bidstream_investigation_alex.py) | Alex Knorr's bidstream domain/IAB investigation script |
| [ti_797_customer_audience_tracking.xlsx](artifacts/ti_797_customer_audience_tracking.xlsx) | Alex's BUK customer audience tracking spreadsheet |
| [aug_log_ip_vertical_id_hourly.py](artifacts/aug_log_ip_vertical_id_hourly.py) | Ryan's template pipeline (local reference copy) |
| [queries/](queries/) | 6 daily snapshot queries + combined training dataset query |
| [ti_790_shap_pre_visit.png](outputs/ti_790_shap_pre_visit.png) | SHAP summary plot |
| [ti_790_all_features_ranked.csv](outputs/ti_790_all_features_ranked.csv) | Raw feature rankings CSV |
| [feature_store_naming_conventions.pdf](../../documentation/docs/feature_store_naming_conventions.pdf) | Team-wide naming standards (moved to documentation/) |
| [feature_store_rfd.pdf](../../documentation/docs/feature_store_rfd.pdf) | Feature store RFD — architecture decision doc (moved to documentation/) |
| [dataproc_local_astronomer_guide.pdf](../../documentation/docs/dataproc_local_astronomer_guide.pdf) | Dataproc + Astronomer local setup guide (moved to documentation/) |

## 6. Meeting Outcomes (2026-04-01 Sync)

**Attendees:** Malachi, Alex Knorr, Ryan Kleck

**Alex's findings (TI-791):**
- IAB categories are too generic alone ("arts and entertainment, television" dominate). Insufficient semantic info for embeddings.
- Domain field has more signal — mix of cleaned site name + app bundle. Can parse for genre/vertical clues.
- CTV vs non-CTV is the primary delimiter — classification logic should split on device_type first.
- Approach: parse domains, build simple rules mapping to verticals rather than embeddings.

**Ryan's contributions:**
- Shared existing feature store pipeline: `aug_log_ip_vertical_id_hourly.py` — reads augmentor_log hourly from parquet, maps domains to vertical IDs via tldextract. Already in production.
- Page URL (full URL) already added to augmentor_log for banner placements — implemented in Matt's feature store.
- PMP deal analysis: NBA (3K IPs) vs HGTV (55K IPs) overlap with fast food — 6.5% vs 5.8%. Small sample, directional.
- `private_marketplace_deals` table has PMP deal names/IDs. DS42 converts string PMP IDs to integer category IDs.
- IPv6 often populated when IP field is blank in augmentor_log → can link to household ID.
- OpenRTB spec is standardized — MNTN Bidder will have same fields. Rogus is the contact.
- New features can flow to DS13/DS19 via site_visits signal table.

**Action items:**
- Malachi: Adapt Ryan's pipeline for new features ([TI-810](https://mntn.atlassian.net/browse/TI-810))
- Malachi: Multi-day validation ([TI-809](https://mntn.atlassian.net/browse/TI-809))
- Malachi: Add advertiser-side features to model ([TI-811](https://mntn.atlassian.net/browse/TI-811))
- Alex: Continue vertical classification via domain parsing (TI-791)
- Ryan: Engineering lift assessment for pipeline capture
- Send presentation PDF to Kale

## 7. Documentation Updates

- `knowledge/data_knowledge.md` — Added Feature Store section: pre-visit vs feedback leakage, bronze-only fields, content_genre normalization, cost_impression_log gotchas, scale reference
- `knowledge/experimentation.md` — Added Feature Importance Methodology Lessons section. Updated 2026-04-01: temporal leakage lesson, sample selection bias, content genre sampling sensitivity, fillna(0) vs NaN.
- `knowledge/data_knowledge.md` — Added: augmentor_log 10d BQ TTL / ~30d parquet, private_marketplace_deals reference table, IPv6 when IP blank, DS42 PMP integer conversion

## 8. Follow-up Tickets

| Ticket | Summary | Owner | Status |
|--------|---------|-------|--------|
| [TI-791](https://mntn.atlassian.net/browse/TI-791) | Vertical classification from bidstream | Alex | In Progress |
| [TI-794](https://mntn.atlassian.net/browse/TI-794) | DS13 audience augmentation validation | TBD | Backlog |
| [TI-795](https://mntn.atlassian.net/browse/TI-795) | Holdout experiment for augmented audiences | TBD | Backlog |
| [TI-796](https://mntn.atlassian.net/browse/TI-796) | Integrate into DS13/DS19 staging + RTC | TBD | Backlog |
| [TI-799](https://mntn.atlassian.net/browse/TI-799) | Adapt feature store pipeline for new features | Malachi | Next |
| [TI-800](https://mntn.atlassian.net/browse/TI-800) | Multi-day validation of feature rankings | Malachi | Next |
| [TI-801](https://mntn.atlassian.net/browse/TI-801) | Add advertiser-side features to model | Malachi | Next |
