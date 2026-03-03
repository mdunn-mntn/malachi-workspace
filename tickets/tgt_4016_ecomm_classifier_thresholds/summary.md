# TGT-4016: Ecommerce Classifier Thresholds

**Jira:** https://mntn.atlassian.net/browse/TGT-4016
**Status:** Complete
**Date Started:** ~2025 (estimate)
**Date Completed:** ~2025 (estimate)
**Assignee:** Malachi

---

## 1. Introduction

Analysis and tuning of the ecommerce domain classifier thresholds. The classifier assigns domains to ecommerce vs. non-ecommerce categories; this ticket involved evaluating threshold choices and their impact on classification quality.

---

## 2. The Problem

The ecommerce classifier's threshold settings needed evaluation — too permissive would include non-ecommerce sites, too strict would exclude legitimate ecommerce domains. Needed empirical threshold analysis.

---

## 3. Plan of Action

1. Load product lookup data
2. Run classifier threshold analysis across candidate values
3. Evaluate precision/recall tradeoffs
4. Recommend threshold setting

---

## 4. Investigation & Findings

- Product lookup CSV used as ground truth: `data/product_lookup.csv`
- Analysis implemented in both Python script and Jupyter notebook
- See `artifacts/ecomm_classifier_thresholds.ipynb` for full analysis
- Companion Python script: `artifacts/ecomm_classifier_thresholds.py`

---

## 5. Solution

Delivered threshold analysis with recommended value(s). See notebook for full results.

---

## 6. Questions Answered

- **Q:** What threshold maximizes classifier quality for ecommerce domains?
  **A:** See notebook results.

---

## 7. Data Documentation Updates

None (classifier-specific, not BQ table knowledge).

---

## 8. Open Items / Follow-ups

- See TI-200 for downstream whitelist/blocklist work using these thresholds.
- See `documentation/docs/ecommerce_threshold_writeup.md` for written summary.

---

## Drive Files

- (None found in Drive for TGT-4016)
