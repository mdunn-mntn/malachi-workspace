# TI-849: Monitor Fangorn score lift and visit rate improvements

**Jira:** https://mntn.atlassian.net/browse/TI-849
**Status:** In Progress (passive this sprint — monitoring methodology only)
**Date Started:** 2026-04-20
**Date Completed:**
**Assignee:** Malachi
**Story Points:** 5
**Priority:** P1 - Critical
**Parent:** TI-457

---

## 1. Introduction

Set up monitoring and reporting to track the impact of Fangorn scores on visit rates,
aiming to validate the expected ~10% lift. Deliver a Mode dashboard that tracks lift
across verticals and advertiser tiers as the Fangorn rollout proceeds.

The Fangorn rollout is handled by Ryan, Sean, and Matt (TI-862, TI-863, TI-727, TI-864).
This ticket is the lift-monitoring layer that runs after each rollout tier goes live.

## 2. The Problem

Without structured lift monitoring, a successful or failing Fangorn rollout is
invisible to leadership until someone runs an ad-hoc analysis. We need:
- A pre-rollout baseline so lift is a comparison, not a guess
- Per-tier monitoring so we catch tier-specific regressions early
- Dashboards that are self-serve for stakeholders (no weekly pull from Malachi)

## 3. Plan of Action

1. Wait for Ryan/Sean/Matt tier-1 rollout to complete (TI-862, TI-863, TI-727, TI-864).
2. Define lift methodology:
   - Baseline option A: 2025 YTD numbers (easy, but potentially stale)
   - Baseline option B: pre-rollout period per advertiser (cleanest, but harder)
   - Baseline option C: causal impact analysis (CausalImpact / synthetic control)
   - Decision pending — preliminary preference: per-advertiser pre-rollout period
     for tiers 1 and 2, with CausalImpact as a secondary read.
3. Aggregations: advertiser, vertical, tier, campaign objective, funnel level.
4. Build Mode dashboard; validate against TI-835's "two stories" finding (guid_log
   vs clickpass_log VV differences).
5. Establish an alerting cadence — weekly rollup to Ryan/Sean/Matt + a leadership
   summary on a biweekly cadence.

## 4. Investigation & Findings

_(Populated once rollout tickets close and monitoring begins.)_

## 5. Solution

_(Populated at completion — Mode dashboard URL, methodology doc, stakeholder contacts.)_

## 6. Questions Answered

- **Q:** How is "lift" determined?
  **A:** TBD — baseline option pending decision with Sean/Ryan.
- **Q:** What aggregations?
  **A:** advertiser, vertical, tier, funnel_level, objective_id. Funnel_level is
  authoritative for stage (not objective_id) per the 2026-03-11 Ray finding.

## 7. Data Documentation Updates

_(Populated as new knowledge emerges.)_

## 8. Open Items / Follow-ups

- Baseline methodology decision — needs Sean + Ryan input.
- Monitoring cadence — weekly vs biweekly rollups to leadership.
- Active work starts once rollout tickets (TI-862/863/727/864) close — this sprint
  is scaffolding + methodology definition only.
