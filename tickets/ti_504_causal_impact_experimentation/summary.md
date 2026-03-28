# TI-504: Create Causal Impact Analysis for Experimentation Team

**Jira:** https://mntn.atlassian.net/browse/TI-504
**Status:** In Progress
**Date Started:** 2026-03-27
**Date Completed:**
**Assignee:** Malachi
**Story Points:** 3 (1-2 days)
**Related:** TI-457 (Audience Intent Scoring Phase 2), TI-748 (Media Plan Causal Impact — methodology source)

---

## 1. Introduction

Build a causal impact analysis framework for the experimentation team, starting with Audience Intent Scoring (AIS). TI-748 established the methodology (per-advertiser CausalImpact, BIC-optimized covariates, ramp-up exclusion, panel data model). This ticket adapts that framework for AIS and future experiment use cases.

Meeting with Matt on 2026-03-31 to discuss scope and requirements.

## 2. The Problem

The experimentation team needs a rigorous causal inference framework to measure the effect of AIS on campaign performance. Key questions:
- What is the treatment? (AIS model enabled vs not)
- What metrics matter? (IVR, VVR, conversion rate, ROAS?)
- What's the experiment design? (randomized, staggered rollout, pre/post?)
- How do we handle confounds? (campaign maturity, seasonality, advertiser heterogeneity)

## 3. Plan of Action

1. ⬜ Meet with Matt (2026-03-31) — understand AIS experiment design and requirements
2. ⬜ Review TI-457 (AIS Phase 2) for current state and what's being measured
3. ⬜ Identify data sources for AIS treatment/control comparison
4. ⬜ Adapt TI-748 methodology for AIS context
5. ⬜ Build reusable analysis script/framework
6. ⬜ Run initial analysis
7. ⬜ Document methodology and results

## 4. Investigation & Findings

TBD — pending Matt meeting.

## 5. Solution

TBD

## 6. Questions Answered

TBD

## 7. Data Documentation Updates

TBD

## 8. Open Items / Follow-ups

- Matt meeting 2026-03-31: understand AIS experiment design, treatment definition, metrics, data sources
