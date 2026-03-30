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

**Malachi's causal impact framework status:**
- Working framework, fully based on GCP/BigQuery — no DataGrip dependency
- Uses Bayesian structural time series (synthetic control) with BIC-optimized covariate selection
- Portable — anyone with Google creds can clone repo and run it
- Malachi expects to have initial results by 2026-03-31

**Fangorn experiment design (described by Matt, set up by Nick):**
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

**Feature selection methodology (Matt's guidance for clustering & future model work):**
- XGBoost feature importance: train on small random sample, get ranked importances via 3 methods (information gain, frequency, weighted), create composite score
- Iterative paring: start with all features → pare down to top 50 → retrain → repeat while maintaining eval metrics
- Simple group-by / linear regression for categorical features (e.g., iPhone vs Android → group by, check if metric differs significantly)
- Variance decomposition analysis: quantify variance at different feature levels (advertiser, vertical, time) to identify which levels matter
- SHAP values: use at the end for fine-tuning, more expensive to compute
- BIC (Bayesian Information Criterion): Malachi already uses this — balances model fit vs complexity, minimizes covariates

**Fangorn model architecture notes:**
- Current Fangorn: trained intermittently (not daily). Bottoms-up keywords retrained daily.
- Future neural net version: goal is real-time inference with features updated as fast as possible (not necessarily continuous learning, but real-time feature ingestion)

**Rollout strategy discussion:**
- Cluster advertisers to systematically choose a broad range for rollout (one per cluster)
- Some verticals are poorly targeted (Fangorn helps a lot), some are well-targeted (Fangorn adds little)
- Estimated impact by vertical should guide rollout priority

### RCT Analysis Results (2026-03-30)

**Data:** All 40 experiment campaigns have data in `cost_impression_log` (not in summary tables due to `is_test = true`). Run period: 2026-03-04 to 2026-03-24 (21 days). Total: 3.2M impressions, 38.6K VVs. VVs from `clickpass_log`.

**Track 1: Direct RCT — Intent-Group Level (head-to-head, primary analysis)**

| Advertiser | Intent | Control IVR | Treatment IVR | Lift | p-value | Sig? |
|---|---|---|---|---|---|---|
| Edward Martin | PP | 0.005176 | 0.011544 | +123.0% | 0.0000 | YES |
| Edward Martin | MI_PP | 0.005424 | 0.009207 | +69.8% | 0.0021 | YES |
| Collector Store | PP | 0.003987 | 0.006360 | +59.5% | 0.0134 | YES |
| Collector Store | MI | 0.003930 | 0.005631 | +43.3% | 0.0252 | YES |
| Edward Martin | HI | 0.013173 | 0.016705 | +26.8% | 0.1068 | no |
| Reedsy | PP | 0.004739 | 0.005946 | +25.5% | 0.0903 | no |
| Collector Store | HI | 0.006006 | 0.008173 | +36.1% | 0.0841 | no |
| G-Shock | PP | 0.014968 | 0.018433 | +23.2% | 0.1382 | no |
| Reedsy | MI | 0.005760 | 0.007038 | +22.2% | 0.1311 | no |
| Zumba | MI | 0.010197 | 0.011312 | +10.9% | 0.4175 | no |
| G-Shock | HI | 0.034458 | 0.034301 | -0.5% | 0.9742 | no |
| G-Shock | MI_PP | 0.014226 | 0.013955 | -1.9% | 0.9002 | no |
| Zumba | MI_PP | 0.007806 | 0.007256 | -7.1% | 0.6202 | no |
| Zumba | HI | 0.022569 | 0.019876 | -11.9% | 0.3531 | no |
| G-Shock | MI | 0.029283 | 0.025826 | -11.8% | 0.3748 | no |
| Zumba | PP | 0.013209 | 0.011275 | -14.6% | 0.1918 | no |
| Reedsy | MI_PP | 0.003959 | 0.003297 | -16.7% | 0.1485 | no |
| Collector Store | MI_PP | 0.003644 | 0.004493 | +23.3% | 0.1961 | no |
| Edward Martin | MI | 0.011485 | 0.012326 | +7.3% | 0.6100 | no |
| Zumba | MI_PP | 0.007806 | 0.007256 | -7.1% | 0.6202 | no |

Summary: **4/20 significant** (all positive). 12/20 show positive lift.

**Track 1: Advertiser-Level Summary**

| Advertiser | Control IVR | Treatment IVR | Lift | p-value | Sig? |
|---|---|---|---|---|---|
| Edward Martin | 0.008813 | 0.012446 | +41.2% | 0.0153 | YES |
| Collector Store | 0.004392 | 0.006164 | +40.3% | 0.0272 | YES |
| Reedsy | 0.006096 | 0.006492 | +6.5% | 0.6027 | no |
| G-Shock | 0.023239 | 0.023141 | -0.4% | 0.9766 | no |
| Zumba | 0.013421 | 0.012416 | -7.5% | 0.5548 | no |

**Pooled (all advertisers):** Overall +3.4% lift, p=0.78 — not significant.

**Track 2: Synthetic Control (CausalImpact) — Secondary validation**

Used parent campaign pre-period IVR as baseline, compared to treatment arm IVR during experiment.

| Advertiser | Pre-Weeks | Predicted IVR | Actual IVR | Effect | p-value |
|---|---|---|---|---|---|
| Collector Store | 32 | 0.042594 | 0.006724 | -84.2% | 0.005 |
| Reedsy | 41 | 0.031328 | 0.007228 | -76.9% | 0.001 |
| G-Shock | 21 | 0.123730 | 0.024269 | -80.4% | 0.093 |
| Edward Martin | 19 | 0.035262 | 0.012494 | -64.6% | 0.026 |
| Zumba | 41 | 0.039938 | 0.014177 | -64.5% | 0.052 |

**IMPORTANT caveat:** These massive negative effects are NOT a treatment signal — they reflect that experiment campaigns target IP bucket subsets (600-999) vs parent campaigns targeting the full audience. The IVR drop is expected from audience fragmentation + fresh campaigns without optimization history. Synthetic control is not valid for this comparison because the experiment campaigns are structurally different from parent campaigns.

**Key takeaways:**
1. The RCT is the valid analysis here — treatment vs control with matched audiences
2. Fangorn shows strong, significant improvement for Edward Martin (+41%) and Collector Store (+40%)
3. No effect for G-Shock (-0.4%), Reedsy (+6.5%), or Zumba (-7.5%)
4. This aligns with Matt's observation: some verticals are well-targeted already (Fangorn adds little), while others benefit significantly
5. The PP (Peak Performance) intent group shows the most consistent treatment effect across advertisers

## 5. Solution

Analysis complete — see Track 1 results above. Script: `artifacts/ti_504_fangorn_rct_analysis.py`. Data: `outputs/ti_504_experiment_daily_metrics.csv`.

## 6. Questions Answered

| Question | Answer |
|----------|--------|
| What's the experiment design? | Full RCT — 5 advertisers, cloned prospecting campaigns, control/treatment with 4 intent groupings each |
| Who set up the experiment? | Nick |
| How were audiences equalized? | IP hashing for holdout group bucket ranges |
| Were campaigns modified mid-flight? | No — all started fresh as new campaigns |
| What's the primary metric? | IVR (impression-to-visit rate) |
| Does Fangorn improve IVR? | Mixed — strong improvement for Edward Martin (+41%) and Collector Store (+40%), no effect for G-Shock/Reedsy/Zumba |
| Which intent group benefits most? | PP (Peak Performance) shows the most consistent treatment effect |
| Are test campaigns in summary tables? | No — `is_test = true` excludes them. Must query `cost_impression_log` and `clickpass_log` directly |
| When did campaigns run? | 2026-03-04 to 2026-03-24 (21 days) |
| Is synthetic control valid here? | Not for this comparison — audience fragmentation makes parent vs experiment comparison invalid |

## 7. Data Documentation Updates

- Test campaigns (`is_test = true`) are excluded from all silver summary/aggregate tables. Must query log-level tables directly.

## 8. Open Items / Follow-ups

- ✅ Get Nick's spreadsheet with campaign_group_ids
- ✅ Run causal impact / RCT analysis
- ⬜ Review results with Matt — discuss why some advertisers show strong effect and others don't
- ⬜ Add conversion/revenue metrics (CVR, ROAS) if needed — currently IVR only
- ⬜ Review TI-457 for current AIS Phase 2 state
- ⬜ When Fangorn rollout begins: have causal impact framework ready for broader measurement
- ⬜ Clustering methodology for systematic advertiser selection during rollout
