# BER-2250: Incrementality Overhaul

**Jira:** https://mntn.atlassian.net/browse/BER-2250
**Status:** Not Started
**Date Started:** 2026-04-06
**Date Completed:**
**Assignee:** Malachi

---

## 1. Introduction

Initiative to prove whether MNTN's intent tier targeting generates **incremental** lift, or whether we're buying audiences who would have converted anyway. This is the single highest-leverage initiative for Q2 2026.

MNTN's high-intent targeting concentrates spend on the same audience segments targeted by Meta and Google. If our marginal contribution to conversion is low, we're charging customers for outcomes they would have achieved without us — a retention and differentiation risk.

**Product Brief:** [Intent Score Shuffling — Confluence](https://mntn.atlassian.net/wiki/external/NTM1ZmViMzc1YzczNDQ0YjgzZDVlMjdkNTk2ZGY4NmY)

## 2. The Problem

We currently measure incrementality against a counterfactual, but we have **never tested whether the intent scoring methodology itself drives incremental lift**. Without that test, we cannot determine whether the scores we use to allocate spend are optimizing for incrementality or simply for conversion correlation.

- Advertisers increasingly demand proof of incremental lift, not just attribution
- If competitors (Meta, Google) can claim the same conversions on the same audiences, MNTN's value proposition narrows to reach and format, not performance
- The inability to demonstrate incrementality becomes a churn driver as advertisers mature in their measurement sophistication

## 3. Plan of Action

1. TI-835: Design control group and measurement methodology (shuffle %, cohort selection, ITT framework)
2. TI-831: Build audience deciles for advertiser experimentation (even/odd targeting)
3. TI-837: Create implementation plan (shuffling mechanism, system changes, rollback, RX coordination)
4. Run the experiment for the defined window
5. TI-839: Measure results using ITT methodology, determine follow-up
6. TI-842: Present results to leadership and broader audience

## 4. Investigation & Findings

*Work not yet started. Findings will be added as the experiment progresses.*

## 5. Solution

*Pending experiment results.*

## 6. Questions Answered

- **Q:** Is our intent targeting generating incrementality, or are we buying audiences who would have converted anyway?
  **A:** *Pending — this is the core question the experiment will answer.*

## 7. Data Documentation Updates

- Added Intent Score Shuffling section to `knowledge/experimentation.md` (ITT methodology, design, parameters)
- Added incrementality initiative context to `knowledge/mntn_business.md`
- Created `knowledge/strategic_north_star.md` with Q2 OKR leverage framework

## 8. Open Items / Follow-ups

- [ ] TI-835: Control group design — shuffle %, cohort selection, duration
- [ ] TI-831: Audience deciles — even/odd targeting implementation
- [ ] TI-837: Implementation plan — which systems change, how scores are logged, rollback
- [ ] Coordinate with RX squad on ITT reporting requirements
- [ ] Determine minimum experiment duration for statistical power

## Child Tickets

| Ticket | Summary | Folder | Status | SP |
|--------|---------|--------|--------|----|
| [TI-831](https://mntn.atlassian.net/browse/TI-831) | Audience Deciles for Advertiser Experimentation | `ti_831_audience_deciles/` | Not Started | 5 |
| [TI-835](https://mntn.atlassian.net/browse/TI-835) | Control group design and measurement methodology | `ti_835_control_group_design/` | Backlog | 3 |
| [TI-837](https://mntn.atlassian.net/browse/TI-837) | Implementation plan for intent score shuffling | `ti_837_implementation_plan/` | Backlog | 5 |
| [TI-839](https://mntn.atlassian.net/browse/TI-839) | Measure incrementality results | `ti_839_measure_results/` | Backlog | 5 |
| [TI-842](https://mntn.atlassian.net/browse/TI-842) | Present results to broader audience | `ti_842_present_results/` | Backlog | 3 |
