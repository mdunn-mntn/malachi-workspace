# BER-2250: Incrementality Overhaul

**Jira:** https://mntn.atlassian.net/browse/BER-2250
**Status:** In Progress
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
- **COMPLETE (2026-04-08):** "The Two Stories" finding — guid_log ~0% lift, clickpass_log 2-8x lift. See TI-835 summary.

### Key Insight: Control Group Already Exists
Every campaign has a **10% holdout group** — `MD5('{AID}:{IP}') mod 1000`, bucket 0-99 = holdout. Per-advertiser per-IP. No shuffling needed for observational analysis.

### Kale's Strategic Direction (2026-04-08)
**"The most valuable thing right now is getting this incrementality thing out. Solving this would be HUGE and dramatically change growth and retention."**

- **Incremental ROAS** is the top metric — not visits, not impressions
- MNTN almost certainly looks bad on external incrementality (LiftLab/Kochava) because everything is optimized toward the visit
- **TI-835 confirms this:** internal attributed-visits metric (clickpass_log) shows 2-8x lift, but total site traffic (guid_log) shows ~0% lift. External vendors measure closer to guid_log.
- **Strategic shift:** shutter internal incrementality dashboards → move to approved third-party vendors
- OKR: **run 5 experiments with external vendors**
- Customer-driven: ask advertisers what they want (reach, performance, incrementality) → tailor experience
- Need a dedicated LiftLab liaison/DS
- CPM pricing → incrementality changes don't directly hit profit, but IVR metrics will suffer
- If we adjust targeting for incrementality, IVR performance will look worse — but it's the right thing to do

### Key Tension: Performance vs Incrementality
Optimizing for incrementality and visit rate are partially opposed (Matt Brorby, confirmed by Kale). Kale's direction: incrementality wins. If we start adjusting for incrementality, IVR performance will suffer, and TI will appear to perform worse on current metrics. But incremental ROAS is what matters.

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

- [x] TI-835: Observational analysis — **COMPLETE** (two stories finding)
- [ ] TI-856: Research LiftLab methodology — prerequisite for external experiments
- [ ] TI-857: Plan and scope 5 external vendor experiments (Q2 OKR)
- [ ] TI-858: Identify which targeting audiences are incremental vs not
- [ ] TI-859: Expand holdout bucketing infrastructure (Zach + Jordan)
- [ ] TI-837: Implementation plan — which systems change, how scores are logged, rollback
- [ ] TI-831: Audience deciles — even/odd targeting (separate customer-facing A/B tool)
- [ ] Coordinate with RX squad on ITT reporting requirements
- [ ] Determine minimum experiment duration for statistical power
- [ ] Establish LiftLab liaison/DS relationship

## Jira Structure (updated 2026-04-08)

```
BER-2250: Incrementality Overhaul (Initiative)
├── TI-831: Audience Deciles for Advertiser Experimentation (separate workstream)
└── TI-855: Incrementality Experimentation & External Vendor Validation (EPIC) ← NEW
    ├── TI-835: Observational incrementality analysis (10% holdout) — COMPLETE
    ├── TI-837: Design and implement intent score shuffling experiment — Backlog
    ├── TI-839: Measure incrementality results — Backlog
    ├── TI-842: Present results to broader audience — Backlog
    ├── TI-856: Research LiftLab methodology — NEW
    ├── TI-857: Plan 5 external vendor experiments (Q2 OKR) — NEW
    ├── TI-858: Identify incremental vs non-incremental audiences — NEW
    └── TI-859: Expand holdout bucketing infrastructure (Zach + Jordan) — NEW
```

## Child Tickets

### TI-855: Incrementality Experimentation & External Vendor Validation (EPIC)

| Ticket | Summary | Status | SP |
|--------|---------|--------|----|
| [TI-835](https://mntn.atlassian.net/browse/TI-835) | Observational incrementality analysis (10% holdout) | **Complete** | 3 |
| [TI-837](https://mntn.atlassian.net/browse/TI-837) | Design and implement intent score shuffling experiment | Backlog | 5 |
| [TI-839](https://mntn.atlassian.net/browse/TI-839) | Measure incrementality results | Backlog | 5 |
| [TI-842](https://mntn.atlassian.net/browse/TI-842) | Present results to broader audience | Backlog | 3 |
| [TI-856](https://mntn.atlassian.net/browse/TI-856) | Research LiftLab methodology | Not Started | 3 |
| [TI-857](https://mntn.atlassian.net/browse/TI-857) | Plan 5 external vendor experiments (Q2 OKR) | Not Started | 5 |
| [TI-858](https://mntn.atlassian.net/browse/TI-858) | Identify incremental vs non-incremental audiences | Not Started | 5 |
| [TI-859](https://mntn.atlassian.net/browse/TI-859) | Expand holdout bucketing infrastructure | Not Started | 5 |

### TI-831: Audience Deciles (separate workstream)

| Ticket | Summary | Status | SP |
|--------|---------|--------|----|
| [TI-831](https://mntn.atlassian.net/browse/TI-831) | Audience Deciles for Advertiser Experimentation | Not Started | 5 |
