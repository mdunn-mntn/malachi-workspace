# TI-800: Multi-Day Validation of Feature Rankings

**Jira:** https://mntn.atlassian.net/browse/TI-800
**Epic:** [TI-789](https://mntn.atlassian.net/browse/TI-789) — Bidstream Feature Extraction & Audience Augmentation
**Status:** Not Started
**Date Started:**
**Date Completed:**
**Assignee:** Malachi

---

## 1. Introduction

Run the XGBoost model from TI-790 on 3+ additional days to compute confidence intervals on SHAP rankings. Confirm the top 10 NEW features are stable across days.

## 2. The Problem

TI-790 used a single day (features 2026-03-28, labels 2026-03-29). Rankings are directional but have no CIs. Need to validate stability before committing to pipeline features.

## 3. Plan of Action

1. Reuse `ti_790_training_dataset_v2.sql` with different date pairs
2. Run XGBoost + SHAP for each day
3. Compute mean, std, and 95% CI for each feature's SHAP rank
4. Test rank correlation (Spearman) across days
5. Flag any features that are unstable (rank variance > threshold)

## 4. Investigation & Findings

_(To be filled)_

## 5. Solution

_(To be filled)_

## 6. Questions Answered

_(To be filled)_

## 7. Data Documentation Updates

_(To be filled)_

## 8. Open Items / Follow-ups

- [ ] Choose date pairs (need at least 3, ideally 5-7 covering different days of week)
- [ ] Determine if augmentor_log 4-hour sample limitation varies by day
- [ ] Consider weekday vs weekend effects
