---
doc_type: ticket
title: "TI-542: Max Reach Causal Impact Analysis"
status: done
date: 2026-05-06
summary: "Causal-impact analysis of the Max Reach targeting-expansion feature's incremental lift"
result: "Causal impact analysis delivered; Max Reach incremental performance quantified"
keywords: [max reach, causal impact, ti-542, incremental lift, targeting expansion, bayesian]
---

## TL;DR

**Q:** What did TI-542 (Max Reach Causal Impact Analysis) do and find?

**A:** TI-542 ran a Bayesian causal-impact analysis of the Max Reach feature (a campaign setting that expands IP targeting beyond the core scored audience to maximize reach) to measure whether it adds incremental lift or dilutes performance by adding lower-quality IPs. Status: Complete. The analysis was delivered and Max Reach incremental performance was quantified, but the summary itself does not state the numeric result: the causal-impact results live only in the local notebook (artifacts/ti_542_causal_impact_analysis.ipynb) and a gitignored performance report (artifacts/ti_542_mullet_performance_report.pdf). The summary's Questions Answered section defers the "does Max Reach add incremental lift?" answer to the notebook. No BQ table documentation updates and no open follow-ups.

**How:** Bayesian causal impact methodology applied to Max Reach vs non-Max Reach campaigns/periods, measuring incremental visits and conversions; work captured in a Jupyter notebook plus a PDF performance report. The summary is a thin stub and does not report the numeric outcome.

**Learned:**
- TI-542 is a completed but thin-stub ticket: it records that a Max Reach causal-impact analysis was delivered but keeps all numeric results in the local notebook/PDF, not in the summary.
- Max Reach is framed here as a campaign setting that expands IP targeting beyond the core scored audience to maximize reach; the question was incremental lift vs quality cannibalization.
- Deliverables are ti_542_causal_impact_analysis.ipynb and a gitignored ti_542_mullet_performance_report.pdf; no separate Drive folder exists (Drive's TI-541 folder conflates both tickets).

**Reuse when:**
- Asked about Max Reach incremental lift or its causal-impact history
- Looking for prior causal-impact analyses of targeting-expansion features
- Cross-referencing TI-541/TI-542 Drive folders

# TI-542: Max Reach Causal Impact Analysis

**Jira:** https://mntn.atlassian.net/browse/TI-542
**Status:** Complete
**Date Started:** ~2025 (estimate)
**Date Completed:** ~2025 (estimate)
**Assignee:** Malachi

---

## 1. Introduction

Causal impact analysis of the Max Reach feature — a campaign setting that expands IP targeting beyond the core scored audience to maximize reach. Measured whether Max Reach adds incremental lift or cannibalizes quality.

---

## 2. The Problem

Max Reach was available to advertisers but its incremental impact was unknown. Does expanding beyond the highest-scored IPs increase total visits/conversions, or does it dilute performance by adding lower-quality IPs?

---

## 3. Plan of Action

1. Define Max Reach vs. non-Max Reach campaigns or periods
2. Apply Bayesian causal impact methodology
3. Measure incremental visits and conversions attributable to Max Reach
4. Produce performance report

---

## 4. Investigation & Findings

**Local artifacts:**
- `artifacts/ti_542_causal_impact_analysis.ipynb` — main causal impact notebook
- `artifacts/ti_542_mullet_performance_report.pdf` — performance report (gitignored)

---

## 5. Solution

Causal impact analysis delivered. Max Reach performance quantified.

---

## 6. Questions Answered

- **Q:** Does Max Reach add incremental lift?
  **A:** See causal impact notebook for results.

---

## 7. Data Documentation Updates

None specific to BQ tables.

---

## 8. Open Items / Follow-ups

- None known.

---

## Drive Files

- (No separate Drive folder found for TI-542; Drive's TI-541 folder appears to conflate both tickets)
