# TI-780: Campaign Ramp-Up Research — How Long Until New Campaigns Reach Steady State?

**Jira:** https://mntn.atlassian.net/browse/TI-780
**Status:** In Progress
**Date Started:** 2026-03-26
**Date Completed:**
**Assignee:** Malachi
**Story Points:** 3 (1-2 days)
**Related:** TI-748 (Media Plan Causal Impact — this research unblocks the within-advertiser comparison)

---

## 1. Introduction

When a new prospecting campaign launches, it doesn't perform at steady-state immediately. The bidder needs time to learn which IPs convert, frequency hasn't built up, and the campaign is still exploring its delivery footprint. This "ramp-up period" means new campaigns always underperform mature ones initially.

This matters for ANY experiment that compares new campaigns to existing ones (like TI-748's within-advertiser comparison of recommended vs non-recommended media plan campaigns). Without knowing how long ramp-up takes, we can't separate "media plan effect" from "new campaign effect."

## 2. The Problem

**Core question:** How many weeks does a new prospecting campaign take to reach steady-state IVR?

**Sub-questions:**
- Does ramp-up duration vary by advertiser size (spend)?
- Does it vary by channel (CTV vs display)?
- Does it vary by vertical?
- What's the right definition of "steady state" — when week-over-week IVR change drops below some threshold?

**Impact:** Until we answer this, the TI-748 within-advertiser comparison is confounded and the CausalImpact post-period may include ramp-up noise that distorts the treatment effect.

## 3. Plan of Action

1. ⬜ Ask Kirsa/product team if there's an existing benchmark for campaign maturity
2. ⬜ Pull cohorts of new prospecting campaigns (2024-2025) and track weekly IVR from launch
3. ⬜ Compute IVR stabilization curves — when does week-over-week change drop below 5%?
4. ⬜ Segment by advertiser spend tier, channel (CTV/display), and vertical
5. ⬜ Produce a recommended exclusion window (first N weeks) for future experiments
6. ⬜ Apply finding to TI-748 — re-run CausalImpact excluding ramp-up period
7. ⬜ Update experimentation.md with ramp-up knowledge

**Also planned (from TI-748 roadmap):**
- Build panel data model (pooled regression with advertiser fixed effects) as a methodological upgrade alongside the ramp-up adjustment

## 4. Investigation & Findings

TBD

## 5. Solution

TBD

## 6. Questions Answered

TBD

## 7. Data Documentation Updates

TBD

## 8. Open Items / Follow-ups

- First step: ask Kirsa if product team has an existing definition of campaign maturity
