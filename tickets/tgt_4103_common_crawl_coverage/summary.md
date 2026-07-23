---
doc_type: ticket
title: "TGT-4103: Common Crawl Coverage Analysis"
status: done
date: 2026-05-06
summary: "Common Crawl data coverage analysis for MNTN's domain classification pipeline."
result: "Coverage analysis delivered; full results in tgt_4103_coverage.ipynb notebook."
keywords: [common crawl, coverage, domain classification, ecommerce classifier, domain vertical, tgt-4103]
---

## TL;DR

**Q:** What is the Common Crawl coverage of MNTN's domains (TGT-4103), and what was found?

**A:** TGT-4103 analyzed Common Crawl data coverage for MNTN's domain-classification pipeline. Common Crawl is used as a data source for training/enriching MNTN's domain-vertical and ecommerce classifiers. Per summary.md, a coverage analysis was delivered and the full results are deferred to the notebook (artifacts/tgt_4103_coverage.ipynb) — the summary's Findings, Solution, and Questions Answered sections state only "see notebook" and record no numbers. Status: done.

**How:** Per summary.md Section 3 (Plan of Action), the work was: (1) load the Common Crawl dataset, (2) measure coverage against MNTN's domain universe, (3) identify gaps and patterns. Sections 4-6 defer all results to artifacts/tgt_4103_coverage.ipynb; the summary itself reports no coverage figures, crawl identifier, or domain lists.

**Learned:**
- summary.md is a thin retrospective stub: its Findings, Solution, and Questions Answered sections defer entirely to the notebook, and no numeric results live in the summary prose (all in artifacts/tgt_4103_coverage.ipynb).
- Common Crawl is used as a data source for training/enriching MNTN's domain-vertical and ecommerce classifiers.
- Front-matter summary and result fields are both present and populated — no fix needed.

**Reuse when:**
- Estimating what fraction of MNTN's domain universe an external corpus (Common Crawl) covers
- Assessing coverage gaps for domain-vertical or ecommerce classifier training data
- Locating the TGT-4103 Common Crawl coverage notebook for the actual figures

---

# TGT-4103: Common Crawl Coverage Analysis

**Jira:** https://mntn.atlassian.net/browse/TGT-4103
**Status:** Complete
**Date Started:** ~2025 (estimate)
**Date Completed:** ~2025 (estimate)
**Assignee:** Malachi

---

## 1. Introduction

Analysis of Common Crawl data coverage for MNTN's domain classification pipeline. Common Crawl is used as a data source for training/enriching domain verticals and ecommerce classifiers.

---

## 2. The Problem

Needed to understand coverage gaps in Common Crawl data — which domains are covered, which are missing, and what the implications are for classifier quality.

---

## 3. Plan of Action

1. Load Common Crawl dataset
2. Measure coverage against MNTN's domain universe
3. Identify gaps and patterns

---

## 4. Investigation & Findings

Full analysis in `artifacts/tgt_4103_coverage.ipynb`.

---

## 5. Solution

Delivered coverage analysis. See notebook for full results.

---

## 6. Questions Answered

- **Q:** What % of MNTN-relevant domains are covered by Common Crawl?
  **A:** See notebook.

---

## 7. Data Documentation Updates

None.

---

## 8. Open Items / Follow-ups

None known.

---

## Drive Files

- (None found in Drive for TGT-4103)
