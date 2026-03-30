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

The experimentation team needs a rigorous causal inference framework to measure the effect of AIS (Fangorn) on campaign performance. Key questions:
- What is the treatment? Fangorn AIS model enabled (new intent groupings) vs standard targeting
- What metrics matter? IVR is the primary metric (conversions)
- What's the experiment design? Full randomized A/B test — cloned prospecting campaigns into control/treatment arms
- How do we handle confounds? Synthetic control via CausalImpact, BIC-optimized covariates, IP hashing for audience size equalization

## 3. Plan of Action

1. ✅ Meet with Matt (2026-03-30) — understand AIS experiment design and requirements
2. ⬜ Review TI-457 (AIS Phase 2) for current state and what's being measured
3. ⬜ Get campaign/advertiser list from Nick's spreadsheet (campaign_group_ids for all 5 advertisers)
4. ⬜ Pull performance data for control vs treatment campaigns
5. ⬜ Run causal impact analysis to validate RCT results
6. ⬜ Design clustering methodology for systematic Fangorn rollout
7. ⬜ Document methodology and results

## 4. Investigation & Findings

### Matt Meeting (2026-03-30) — Key Takeaways

**Matt's causal impact framework status:**
- Working framework, fully based on GCP/BigQuery — no DataGrip dependency
- Uses Bayesian structural time series (synthetic control) with BIC-optimized covariate selection
- Portable — anyone with Google creds can clone repo and run it
- Matt expects to have initial results by 2026-03-31

**Fangorn experiment design (set up by Nick):**
- 5 advertisers: Collector Store, Edward Martin, G-Shock, Reads, Izumba
- Each advertiser: one prospecting campaign cloned into control + treatment arms
- Each arm has 4 intent groupings (8 campaign groups per advertiser):
  - Peak Performance — in the vertical
  - High Intent — in the vertical + matches selected keywords from DS 19
  - Mid Intent — in the bucket (Venn diagram overlap)
  - Mid Intent + Peak Performance — combined group
- IP hashing used for holdout group selection to equalize audience sizes between control/treatment
- All campaigns started fresh (cloned from existing, not mid-flight modifications)
- Nick has a spreadsheet with all campaign_group_ids

**Goal for causal impact:**
1. **Immediate:** Validate the RCT results — use causal impact to see if we measure the same treatment effect the actual experiment showed
2. **Near-term (few weeks):** When Fangorn rolls out to broader set of advertisers/verticals, use causal impact as the primary measurement tool (like Jaguar rollout pattern)

**Feature selection methodology (for clustering & future model work):**
- XGBoost feature importance: train on small random sample, get ranked importances via 3 methods (information gain, frequency, weighted), create composite score
- Iterative paring: start with all features → pare down to top 50 → retrain → repeat while maintaining eval metrics
- Simple group-by / linear regression for categorical features (e.g., iPhone vs Android → group by, check if metric differs significantly)
- Variance decomposition analysis: quantify variance at different feature levels (advertiser, vertical, time) to identify which levels matter
- SHAP values: use at the end for fine-tuning, more expensive to compute
- BIC (Bayesian Information Criterion): balances model fit vs complexity, minimizes covariates

**Fangorn model architecture notes:**
- Current Fangorn: trained intermittently (not daily). Bottoms-up keywords retrained daily.
- Future neural net version: goal is real-time inference with features updated as fast as possible (not necessarily continuous learning, but real-time feature ingestion)

**Rollout strategy discussion:**
- Cluster advertisers to systematically choose a broad range for rollout (one per cluster)
- Some verticals are poorly targeted (Fangorn helps a lot), some are well-targeted (Fangorn adds little)
- Estimated impact by vertical should guide rollout priority

## 5. Solution

TBD — awaiting Matt's initial causal impact results and campaign data from Nick's spreadsheet.

## 6. Questions Answered

| Question | Answer |
|----------|--------|
| What's the experiment design? | Full RCT — 5 advertisers, cloned prospecting campaigns, control/treatment with 4 intent groupings each |
| Who set up the experiment? | Nick |
| How were audiences equalized? | IP hashing for holdout group bucket ranges |
| Were campaigns modified mid-flight? | No — all started fresh as new campaigns |
| What's the primary metric? | IVR (conversions) |
| What's the immediate goal? | Validate RCT results with causal impact, then use framework for broader Fangorn rollout |
| How to select features for clustering? | XGBoost importance → iterative paring → group-by for categoricals → SHAP for fine-tuning |

## 7. Data Documentation Updates

TBD

## 8. Open Items / Follow-ups

- ⬜ Get Nick's spreadsheet with campaign_group_ids for all 5 advertisers and their control/treatment arms
- ⬜ Matt delivering initial causal impact results ~2026-03-31
- ⬜ Review TI-457 for current AIS Phase 2 state
- ⬜ When Fangorn rollout begins (few weeks): have causal impact framework ready for broader measurement
- ⬜ Clustering methodology for systematic advertiser selection during rollout
