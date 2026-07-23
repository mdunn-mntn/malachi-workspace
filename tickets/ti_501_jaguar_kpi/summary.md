---
doc_type: ticket
title: "TI-501: Jaguar KPI Analysis"
status: done
date: 2026-05-06
summary: "KPI + causal-impact analysis of the Jaguar IP scoring system by score tier"
result: "Jaguar improves IVR/conversion; score-tier breakdown shows which tiers drive most lift"
keywords: [jaguar, score tier, causal impact, ivr, conversion, ds13, audience intent, model_params, cost_impression_log]
---

## TL;DR

**Q:** Produce the TL;DR card for TI-501 Jaguar KPI Analysis (summary.md), plus delta_facts and any front-matter fix.

**A:** TI-501 delivered a KPI + causal-impact analysis of the Jaguar IP scoring system, segmented by Jaguar score tier. Per the summary: Jaguar score improves IVR/conversion rate ("Yes — see causal impact notebook for quantified lift"), and a score-tier breakdown shows which tiers drive the most incremental lift (specific tier values live in the notebooks, not stated in the summary). Status: Complete. Work delivered as three notebooks (causal impact via Bayesian structural time series, KPI-by-score, score-distribution analysis) plus one IP-level KPI SQL query; written analysis and score-usage reference live on Drive.

**How:** Pulled impression + visit data segmented by Jaguar score tier, ran causal impact analysis (Bayesian structural time series), segmented KPIs by score bucket. Jaguar score lives in cost_impression_log.model_params (same field as RTC score); IP-level scoring from the DS13 Audience Intent pipeline. Quantified lift figures are in the notebooks, not restated in the summary.

**Tables:** cost_impression_log

**Learned:**
- Jaguar score lives in cost_impression_log.model_params (same field as RTC score)
- IP-level Jaguar scoring is from the DS13 Audience Intent pipeline
- Jaguar improves IVR/conversion per the causal impact analysis, with a score-tier breakdown identifying the highest-lift tiers

**Reuse when:**
- Jaguar score-tier KPI analysis
- causal impact of an IP scoring system
- DS13 Audience Intent scoring performance
- mapping model_params score to campaign KPI lift

---


# TI-501: Jaguar KPI Analysis

**Jira:** https://mntn.atlassian.net/browse/TI-501
**Status:** Complete
**Date Started:** ~2025 (estimate)
**Date Completed:** ~2025 (estimate)
**Assignee:** Malachi

---

## 1. Introduction

Deep KPI analysis of the Jaguar IP scoring system. Jaguar scores IPs by predicted visit likelihood. This ticket measured KPIs by Jaguar score tier, used causal impact methodology, and produced a comprehensive performance assessment.

---

## 2. The Problem

Beyond the simple pre/post (TI-270), needed a rigorous analysis of Jaguar's causal impact on campaign KPIs. How much lift does Jaguar generate? Which score tiers perform best?

---

## 3. Plan of Action

1. Pull impression + visit data segmented by Jaguar score tier
2. Run causal impact analysis (Bayesian structural time series)
3. Segment KPIs by score bucket
4. Deliver findings to stakeholders

---

## 4. Investigation & Findings

**Local artifacts:**
- `artifacts/ti_501_causal_impact.ipynb` — causal impact analysis notebook
- `artifacts/ti_501_kpi_by_score.ipynb` — KPI breakdown by score tier
- `artifacts/ti_501_score_analysis.ipynb` — score distribution analysis
- `queries/ti_501_kpis_ip_level.sql` — SQL for IP-level KPIs

**Drive:**
- `TI-452 Jaguar Analysis.gdoc` — written analysis (note: labeled TI-452 but stored in TI-501 Drive folder)
- `TI-502 How We Use Scores.gsheet` — score usage reference (stored in TI-501 folder, belongs to TI-502)

---

## 5. Solution

Delivered full Jaguar KPI analysis with causal impact modeling. Score-tier breakdown shows which Jaguar scores drive the most incremental lift.

---

## 6. Questions Answered

- **Q:** Does Jaguar score improve IVR/conversion rate?
  **A:** Yes — see causal impact notebook for quantified lift.

- **Q:** Which score tiers are most valuable?
  **A:** See `ti_501_kpi_by_score.ipynb`.

---

## 7. Data Documentation Updates

- Jaguar score lives in `cost_impression_log.model_params` (same field as RTC score).
- IP-level scoring is from the DS13 Audience Intent pipeline.

---

## 8. Open Items / Follow-ups

- TI-502: how scores are used in bidding — reference doc on Drive.
- TI-541: IP scoring pipeline architecture.

---

## Drive Files

📁 `Tickets/TI-501 Jaguar Analysis/`
- `TI-452 Jaguar Analysis.gdoc` — written analysis (mislabeled as TI-452)
- `TI-502 How We Use Scores.gsheet` — score usage reference (belongs to TI-502)
