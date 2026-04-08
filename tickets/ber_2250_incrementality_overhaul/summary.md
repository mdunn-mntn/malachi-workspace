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

## 3. Three Workstreams Under BER-2250 (Alex Bloore, 2026-04-08)

**Workstream 1: Intent Score Shuffling Experiment (THE PRIORITY — product brief)**
- Discovery work — "needs to happen FIRST" (Alex Bloore)
- TI squad: shuffle IPs between HI/MI tiers, log original scores
- RX squad (or TI with RX consultation): ITT measurement
- Tickets: TI-837 (design + implement), TI-839 (measure results), TI-842 (present)

**Workstream 2: Population Split / Deciles (separate from experiment)**
- Random 10-group split of all US IPs for customer A/B testing (like Trade Desk)
- Customers select even/odd groups, layer their segments on top
- Ticket: TI-831

**Workstream 3: Observational Analysis (our initiative, not in brief)**
- Use existing 10% holdout to measure baseline incrementality NOW
- Ticket: TI-835
- **Initial results (2026-04-08):** guid_log shows ~0 lift across 10 advertisers. clickpass_log analysis pending.

### Key Insight: Control Group Already Exists
Every campaign has a **10% holdout group** — `MD5('{AID}:{IP}') mod 1000`, bucket 0-99 = holdout. Per-advertiser per-IP. No shuffling needed for observational analysis.

### Key Tension: Performance vs Incrementality
Optimizing for incrementality and visit rate are partially opposed (Matt Brorby). Need leadership direction on balance.

### HHST Reality (2026-04-08)
All scored IPs get flat HHST=10000 (HI). Per-tier analysis not possible until continuous scoring rolls out. PP at 8000 is planned but not active. Aggregate analysis only for now.

## 4. Investigation & Findings

### Matt Brorby Sync (2026-04-07)
- 10% holdout exists on all campaigns — use this as control (no shuffling needed for baseline)
- ITT methodology: compare ALL IPs in 90% targeted group vs 10% holdout, regardless of actual impression delivery
- Nick has the holdout identification query
- Kristen may already be doing related work (#chapter-data-analytics)
- Phase 2 idea: train a model on lift directly, using impression receipt as a feature
- Alex Bohr is the product lead on incrementality (identity team)
- Performance vs incrementality trade-off is a real tension — need leadership direction

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
| [TI-835](https://mntn.atlassian.net/browse/TI-835) | Observational incrementality analysis (10% holdout) | `ti_835_control_group_design/` | Backlog | 3 |
| [TI-837](https://mntn.atlassian.net/browse/TI-837) | Shuffling experiment design (contingent on TI-835) | `ti_837_implementation_plan/` | Backlog | 5 |
| [TI-839](https://mntn.atlassian.net/browse/TI-839) | Measure incrementality results | `ti_839_measure_results/` | Backlog | 5 |
| [TI-842](https://mntn.atlassian.net/browse/TI-842) | Present results to broader audience | `ti_842_present_results/` | Backlog | 3 |
