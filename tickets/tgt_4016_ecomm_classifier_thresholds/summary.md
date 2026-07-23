---
doc_type: ticket
title: "TGT-4016: Ecommerce Classifier Thresholds"
status: done
date: 2026-05-06
summary: "Tune and evaluate ecommerce domain classifier thresholds for precision/recall"
result: "Delivered threshold analysis with a recommended value; full results in the notebook"
keywords: [ecommerce classifier, threshold, precision recall, domain classification, tgt-4016, product lookup]
---

## TL;DR

**Q:** What ecommerce classifier threshold was chosen and how was it evaluated (TGT-4016)?

**A:** TGT-4016 evaluated and tuned the ecommerce domain classifier's thresholds, trading off precision (too permissive includes non-ecommerce sites) against recall (too strict excludes legitimate ecommerce domains). A product lookup CSV served as ground truth. The analysis ran candidate thresholds and assessed precision/recall tradeoffs, delivering a recommended threshold value. The summary does not state the specific recommended value in text — it defers to the notebook (artifacts/tgt_4016_thresholds.ipynb) and companion script (artifacts/tgt_4016_thresholds_script.py) for full results. Status: complete. Downstream whitelist/blocklist work using these thresholds is tracked in TI-200.

**How:** Loaded product lookup CSV as ground truth, ran classifier threshold analysis across candidate values (in both a Python script and Jupyter notebook), evaluated precision/recall tradeoffs, and recommended a threshold. Numeric results live only in the notebook, not the summary text.

**Learned:**
- Classifier-specific work; the summary explicitly records no BQ table knowledge to document.
- The companion Python file was renamed from .py to avoid a Databricks Repos collision with the .ipynb of the same base name.
- The recommended threshold value is not written in the summary text; it lives in the notebook.

**Reuse when:**
- tuning or evaluating the ecommerce domain classifier
- precision/recall threshold analysis for a domain classifier
- downstream whitelist/blocklist work (TI-200) referencing these thresholds

---

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

- Product lookup CSV used as ground truth: `data/tgt_4016_product_lookup.csv`
- Analysis implemented in both Python script and Jupyter notebook
- See `artifacts/tgt_4016_thresholds.ipynb` for full analysis
- Companion Python script: `artifacts/tgt_4016_thresholds_script.py` (renamed from `.py` to avoid a Databricks Repos collision with the `.ipynb` of the same base name)

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
