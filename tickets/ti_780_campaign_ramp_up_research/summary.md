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

### IVR Stabilization Curve (N = 6,917 prospecting campaigns, $10K+ spend, since June 2024)

| Week | Median IVR | WoW Change | % of Steady State |
|---|---|---|---|
| 0 | 0.0041 | — | 38% |
| 1 | 0.0072 | +75% | 67% |
| 2 | 0.0090 | +25% | 84% |
| 3 | 0.0095 | +6% | 88% |
| **4** | **0.0095** | **+0.3%** | **89%** |
| 5 | 0.0098 | +3% | 91% |
| 6 | 0.0099 | +1% | 92% |
| 7 | 0.0102 | +2% | 94% |
| 8+ | 0.0103-0.0113 | <5% | 96-105% (steady state) |

**Steady-state IVR (weeks 8-20 average): 0.0108**

**Week 4 is the inflection point:** First week where WoW change drops below 5% AND IVR reaches >85% of steady state. By week 4, the rapid ramp-up phase is over and the campaign oscillates around its steady-state level.

### Segmentation by Spend Tier (CTV only — most campaigns are CTV)

| Week | High (>$100K) | Mid ($30-100K) | Low ($10-30K) |
|---|---|---|---|
| 0 | 52% of steady | 41% | 43% |
| 2 | 90% | 81% | 73% |
| 4 | 87% | 97% | 85% |
| 7 | 98% | 100% | 97% |

**All spend tiers converge by week 4-5.** High-spend campaigns ramp slightly faster (more data for the bidder to learn from), but the 4-week window works universally.

### Why Campaigns Ramp Up

Based on data patterns and platform knowledge:
1. **Bidder learning:** The delivery system needs time to learn which IPs/households respond to this advertiser's ads. More impressions → better targeting → higher visit rate.
2. **Frequency buildup:** First impressions are less effective than repeated exposures. It takes time to build frequency across a household.
3. **Delivery footprint exploration:** New campaigns start broad, then narrow to high-performing placements/networks.

### Recommendation

**Exclude first 4 weeks of any new campaign from causal impact analysis.**

This applies to:
- TI-748: CausalImpact post-period should start 4 weeks after the first media plan campaign begins delivering
- TI-748: Within-advertiser comparison should only include campaign groups with 4+ weeks of delivery
- Any future experiment comparing new vs existing campaigns

## 5. Solution

TBD

## 6. Questions Answered

TBD

## 7. Data Documentation Updates

TBD

## 8. Open Items / Follow-ups

- First step: ask Kirsa if product team has an existing definition of campaign maturity
