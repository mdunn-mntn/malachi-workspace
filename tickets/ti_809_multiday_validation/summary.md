# TI-809: Multi-Day Validation of Feature Rankings

**Jira:** https://mntn.atlassian.net/browse/TI-809
**Previously referenced as:** TI-800 (in TI-790 summary and meeting notes)
**Epic:** [TI-789](https://mntn.atlassian.net/browse/TI-789) — Bidstream Feature Extraction & Audience Augmentation
**Status:** In Progress
**Date Started:** 2026-04-01
**Date Completed:**
**Assignee:** Malachi

---

## 1. Introduction

Run the XGBoost model from TI-790 on 7 date pairs to compute confidence intervals on SHAP rankings. Confirm the top 10 NEW features are stable across days before committing to pipeline features in TI-810.

## 2. The Problem

TI-790 used a single day (features 2026-03-28, labels 2026-03-29). Rankings are directional but have no CIs. We need to validate that the top features are stable across different days (including weekday/weekend variation) before investing engineering effort in the pipeline.

## 3. Plan of Action

1. [x] Create parameterized version of ti_790_training_dataset_v2.sql
2. [x] Pick 7 date pairs covering Mon-Sun
3. [ ] Run BQ training query for each date pair (~65 GB, ~5 min each)
4. [ ] Run XGBoost + SHAP for each day (both all-features and NEW-only models)
5. [ ] Compute rank stability: mean rank, std, 95% CI, rank CV per feature
6. [ ] Compute Spearman rank correlation matrix across days
7. [ ] Determine which top 10 NEW features are stable vs unstable

## 4. Investigation & Findings

### Date Pairs Selected

| Pair | Feature Date | Label Date | Day of Week | Status |
|------|-------------|------------|-------------|--------|
| 1 | 2026-03-22 (Sun) | 2026-03-23 (Mon) | Weekend→Weekday | Pending |
| 2 | 2026-03-24 (Mon) | 2026-03-25 (Tue) | Weekday | Running |
| 3 | 2026-03-25 (Tue) | 2026-03-26 (Wed) | Weekday | Pending |
| 4 | 2026-03-27 (Thu) | 2026-03-28 (Fri) | Weekday | Pending |
| 5 | 2026-03-28 (Fri) | 2026-03-29 (Sat) | Original TI-790 | Pending |
| 6 | 2026-03-29 (Sat) | 2026-03-30 (Sun) | Weekend | Pending |
| 7 | 2026-03-30 (Sun) | 2026-03-31 (Mon) | Weekend→Weekday | Pending |

All dates within augmentor_log's ~10d BQ TTL window. Other tables (win_logs, BAE, CIL) have 90d retention.

### Query Cost

Dry run showed ~12.4 TB upper bound, but actual cost with partition pruning is ~65 GB per query (confirmed from TI-790 perf log). Total: 7 × 65 GB ≈ 455 GB.

### Results

_(To be filled after all queries complete and Python script runs)_

## 5. Solution

_(To be filled)_

## 6. Questions Answered

_(To be filled)_

## 7. Data Documentation Updates

_(To be filled)_

## 8. Open Items / Follow-ups

- [x] Choose date pairs — 7 pairs covering Mon-Sun
- [ ] Determine if augmentor_log 4-hour sample limitation varies by day
- [ ] Consider weekday vs weekend effects on visit rate and feature rankings
- [ ] If rankings are stable: proceed with TI-810 pipeline for top 10 features
- [ ] If rankings are unstable: investigate which features are day-dependent

## Files

| File | Purpose |
|------|---------|
| [ti_809_training_dataset_parameterized.sql](queries/ti_809_training_dataset_parameterized.sql) | Parameterized training query (replace FEATURE_DATE/LABEL_DATE) |
| [ti_809_run_all_queries.sh](queries/ti_809_run_all_queries.sh) | Shell script to run all 7 query pairs |
| [ti_809_multiday_validation.py](artifacts/ti_809_multiday_validation.py) | Python script: XGBoost + SHAP + rank stability analysis |
