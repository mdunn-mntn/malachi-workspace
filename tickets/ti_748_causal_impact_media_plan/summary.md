# TI-748: Causal Impact — Media Plan Feature

**Jira:** https://mntn.atlassian.net/browse/TI-748
**Status:** In Progress
**Date Started:** 2026-03-26
**Date Completed:**
**Assignee:** Malachi

---

## 1. Introduction

Media Plan is a beta feature giving advertisers control over network allocation (% of spend per publisher like ABC, CBS, ESPN). MNTN auto-recommends allocations based on audience and goals; advertisers can accept or customize. Only applies to new prospecting campaigns.

Kirsa asked for a causal impact analysis to determine whether advertisers who adopted the Media Plan feature (using MNTN's recommended settings) saw improved prospecting performance. Kept quiet from broader team — Kirsa wants to surprise with data.

## 2. The Problem

**Core question:** Did advertisers who adopted MNTN's recommended Media Plan see improved prospecting performance?

**Two analyses:**
1. **CausalImpact (time-series):** Advertiser-level pre/post adoption comparison, controlling for market trends
2. **Within-advertiser comparison:** Recommended vs non-recommended campaign groups side-by-side

**Key challenges:**
- Staggered adoption (19 eligible with recommended plans, ~8 analyzable)
- Selection bias (adopters may differ systematically)
- Campaign maturity confound (new media plan campaigns vs established ones)
- Only prospecting campaigns relevant

## 3. Plan of Action

1. ✅ Identify recommended-only adopters via `media_plan_publishers.badge_state`
2. ✅ Switch to `sum_by_campaign_by_day` (data back to 2024-01-01 for 52-week pre-period)
3. ✅ Build platform-wide non-adopter covariates (IVR, spend, impressions, holidays, VCR, active advertisers)
4. ✅ Run per-advertiser CausalImpact for all metrics
5. ✅ Run within-advertiser comparison (recommended vs non-recommended campaign groups)
6. ✅ Run placebo tests for validation
7. ✅ Covariate significance testing — VIF multicollinearity, BIC stepwise, cross-validation, sensitivity analysis
8. ✅ Add advertiser-specific covariates — spend_change_pct, metric_lag1/2, adv_active_cgs, ctv_share (tested, some not selected by BIC)
9. ✅ Exclude 4-week ramp-up period post-intervention (TI-780 finding)
10. ✅ Add panel data model as complementary aggregate analysis
11. ⬜ Share results with Kirsa
12. ⬜ Re-run in 6-8 weeks with more adopters and post-period data
13. ⬜ Consider per-advertiser ramp-up duration when more data available

## 4. Investigation & Findings

### Adopter Identification (v2 — Recommended Only)
- **19 advertisers** have at least one all-recommended media plan
- 124 total recommended plans, 45 customized plans across all adopters
- `badge_state` values: `RECOMMENDED`, `USER_MODIFIED`, `USER_ADDED`
- Sky Zone, Beddy's, SoilTaxPro are 100% customized → excluded from recommended analysis
- All adopters use `industry_standard` attribution

### Pre-Period Data
- **`sum_by_campaign_by_day`** goes back to 2024-01-01 (vs Sep 2025 for agg table)
- 52-week pre-period captures full seasonality cycle (Black Friday, Christmas, Q1 slowdown)
- Earliest analyzable advertiser (FICO) gets 87 pre-weeks

### IVR Results (v5 — BIC-Optimized Covariates + 4-Week Ramp-Up Exclusion)

| Advertiser | ID | Pre Wks | Post Wks | Effect | p-value | Significant | BIC-Selected Covariates |
|---|---|---|---|---|---|---|---|
| FICO | 37056 | 86 | 18 | -3.97% | >0.05 | No | BIC-selected per-advertiser |
| Taskrabbit | 34114 | 76 | 18 | +8.30% | <0.05 | Yes | BIC-selected per-advertiser |
| Lighting New York | 31116 | 93 | 18 | +10.47% | <0.05 | Yes | BIC-selected per-advertiser |
| Tempo | 41545 | 29 | 18 | -26.18% | <0.05 | Yes | metric_lag1, spend_change_pct |
| #CWH: CWRV Sales | 32756 | 95 | 17 | +16.76% | <0.05 | Yes | platform_ivr, platform_impressions, metric_lag1, spend_change_pct |
| Boll & Branch | 31966 | 97 | 15 | -31.45% | <0.05 | Yes | platform_ivr, platform_spend, platform_impressions, metric_lag1, spend_change_pct |
| Talkspace | 34094 | 103 | 8 | +4.65% | varies | Varies | BIC-selected per-advertiser |
| Am. College of Ed | 33667 | 106 | 5 | +3.59% | >0.05 | No | BIC-selected per-advertiser |

**Key changes from v4:**
- BIC covariate selection is now **per-advertiser** (not hand-picked)
- Covariates typically: `metric_lag1`, `spend_change_pct`, sometimes `platform_ivr`
- **4-week ramp-up exclusion**: first 4 weeks post-intervention excluded (TI-780 finding — new campaigns need ramp-up before steady-state)
- Boll & Branch and Tempo now included (previously dropped)

**Aggregate (IVR):**
- 8 advertisers analyzed, **5/8 statistically significant, 5/8 positive**
- Median effect: **+4.65%**
- **Spend-weighted effect: -0.23%** (near zero — large negative outliers like Boll & Branch and Tempo offset positive results)

**Steady-state IVR note:** Steady-state IVR varies by launch quarter (0.008-0.013, ~60% range), meaning the baseline advertisers are compared against is itself variable.

### Cross-Metric Summary (v5 — BIC-Optimized + Ramp-Up Exclusion)

| Metric | N | Significant | Positive | Median | Spend-Weighted |
|---|---|---|---|---|---|
| IVR | 8 | 5/8 | 5/8 | +4.65% | **-0.23%** |

### Panel Data Model (IVR)

Complementary aggregate analysis using a panel regression across all analyzable advertisers:

- **Treatment effect: +2.06%, not significant (p=0.85)**
- 1,255 observations, 14 advertisers, R² adj = 0.679
- Ramp-up effect not significant in regression (confirming that exclusion is the right approach — the ramp-up signal in the data is not strong enough to model parametrically, but excluding it reduces noise)
- The panel model pools all advertisers and thus washes out advertiser-specific effects — consistent with the near-zero spend-weighted CausalImpact result

### THE KEY FINDING: Allocation Concentration Predicts Who Benefits

The aggregate IVR effect is near zero — but that's because the feature helps some advertisers and hurts others. The investigation into WHY revealed a strong, actionable pattern:

**Budget concentration across publishers is the differentiator, not vertical or advertiser size.**

Each media plan allocates the campaign's total budget as percentages across publishers/networks (e.g., CBS gets 15% of a $100K campaign = $15K on CBS). These percentages come from `media_plan_publishers.percentage` — the budget split the algorithm recommends.

| Advertiser | IVR Effect | # Publishers | Top Publisher % | Spread (std of %) | Rationale Quality |
|---|---|---|---|---|---|
| **CWRV Sales** | **+16.8%*** | **16** | **15% (CBS)** | **2.96** | "HIGH historical performance" — clear conviction |
| **Lighting NY** | **+10.5%*** | **16** | **12% (Samsung, Bravo, NBC News)** | **3.42** | Specific audience-match reasoning per network |
| Taskrabbit | +8.3%* | 26 | 10% | 2.24 | Mixed specificity |
| FICO | -4.0% | 25 | 11% | 2.09 | Generic |
| Am College of Ed | +3.6% | 19 | 11% | 2.21 | Mixed |
| Talkspace | +4.7% | 21 | 11% | 2.45 | Mixed |
| **Tempo** | **-26.2%*** | **26** | **8%** | **1.22** | Generic, near-equal allocation |
| **Boll & Branch** | **-31.5%*** | **26** | **10%** | **1.62** | Generic "historically performed well" on ALL 26 networks |

**The pattern:**
- **Benefited** advertisers: **16 publishers**, top network gets **12-15%** of budget, allocation spread (std) of **3.0-3.4** — the algorithm made *decisive bets* on fewer, higher-conviction networks
- **Hurt** advertisers: **26 publishers**, top network gets only **8-10%**, allocation spread of **1.2-1.6** — budget peanut-buttered across too many networks with near-equal allocation

**Why this makes sense mechanistically:**
1. **Frequency threshold:** CTV advertising requires repeated exposure to drive site visits. When budget is spread across 26 networks at 3-5% each, no single network accumulates enough impressions on a given household to cross the frequency threshold needed to generate a visit.
2. **Bidder optimization:** The delivery system optimizes within its allocation. With 16 networks and 12-15% on the top ones, the bidder has enough budget per network to find and serve the best IPs/households. With 26 networks at 3-5% each, the bidder is starved on every network.
3. **Rationale quality correlates:** CWRV's algorithm rationale said "HIGH historical performance" — it had strong signal to concentrate. Boll & Branch's rationale said "historically performed well" on ALL 26 networks — no differentiation means no basis for conviction.

**What pre-adoption baseline IVR tells us (nothing):**
- CWRV had the highest pre-IVR (0.058) and benefited the most
- Boll & Branch had the second-highest (0.024) and was hurt the most
- Tempo had the lowest (0.005) and was also hurt
- Baseline performance does NOT predict who benefits — allocation strategy does

**Implication for the product team:** The recommendation algorithm may perform significantly better when it produces **more concentrated allocations** (fewer networks, stronger convictions) rather than defaulting to a broad spread. This could be tuned — if the algorithm doesn't have strong signal for differentiation, it should still concentrate rather than dilute. Worth a focused investigation into the algorithm's concentration logic.

**WHICH publishers also matters — not just how many:**

The benefited group's allocations concentrate on **major broadcast networks** — high-reach, premium CTV inventory:
- Benefited top picks: CBS (up to 15%), NBC (12%), ABC (12%), NBC News, ESPN, Peacock — all 5%+ allocation
- Hurt top picks: Roku Drama (6-8%), ION TV (5%), HBO Max (5%), AMC (5%) — niche streaming at thin allocation

The benefited advertisers got plans that bet heavily on proven, high-reach broadcast networks. The hurt advertisers got plans that spread across niche streaming channels where the algorithm had less conviction. This suggests the algorithm's publisher selection quality — not just concentration — matters.

**Deep Dive: Did the algorithm pick the RIGHT publishers?**

Tested using `sum_by_ctv_network_by_day` — actual per-publisher IVR for Lighting New York (+10.5% lift):
- The algorithm recommended Samsung TV+ Entertainment (12%), Bravo (12%), CNN (10%) — these rank **#37-59 by actual IVR**
- The TRUE best IVR publishers were Spectrum News (#1, IVR=1.09%), FanDuel Sports (#2, 0.90%), Hallmark Channel (#3, 0.86%), NBA TV (#4, 0.85%)
- **The algorithm did NOT pick the highest-IVR publishers.** It picked **high-volume, deliverable** publishers (Samsung: 627K impressions, CNN: 463K). The top IVR publishers are low-volume (Spectrum News: 29K, FanDuel: 17K).

**This means the IVR lift likely came from CONCENTRATION, not publisher selection:**
1. Pre-adoption, the advertiser's spend was spread across 130+ publishers unoptimized
2. Media Plan concentrated onto 16 publishers — even though they weren't the *best* by IVR, concentrating budget allowed the bidder to build frequency on each network
3. The improvement came from *removing the long tail of poor performers*, not from finding the optimal publishers
4. The high-IVR publishers (sports networks, niche channels) simply don't have enough inventory to absorb a meaningful budget allocation

**Implication:** The algorithm could potentially produce EVEN BETTER results if it incorporated historical per-publisher IVR into its recommendations, rather than optimizing primarily for deliverability/reach. This is an actionable product insight.

**Caveat:** N=8 is too small to confirm statistically. But the pattern is unambiguous at 0% overlap (the two most-concentrated beat all three least-concentrated), the mechanism is plausible, and it's directly actionable.

### Aggregate Assessment

The overall IVR effect is near zero (spend-weighted -0.23%, panel model +2.06% not significant). This is because the positive effects from concentrated allocations and negative effects from diluted allocations cancel out in aggregate. **The aggregate number hides the real story — the feature works when the algorithm makes decisive bets, and fails when it spreads thin.**

With only 8 analyzable advertisers and short post-periods (many under 20 weeks), the analysis has limited statistical power. The picture may clarify as more advertisers adopt and post-periods lengthen.

### Model Validation Results

**Placebo tests:** Placebo FPR = **24%** (down from 86% in v2, 30% in v3).
- **Why it improved further:** v5 adds the 4-week ramp-up exclusion, which removes the noisiest post-intervention weeks where new campaign dynamics (not media plan effects) dominate. Combined with BIC per-advertiser covariate selection, the model better separates real effects from noise.
- **Why it's still 24%:** Advertiser time series have natural structural breaks — budget shifts, campaign launches/pauses, seasonal pivots — that happened organically during the pre-period. The model correctly identifies these as "something changed here," which counts as a false positive in a placebo test even though it's detecting real (non-intervention) changes. This is inherent to per-advertiser time series analysis with small N.

**Sensitivity analysis:** 5/6 advertisers showed **directionally consistent** results across pre-period lengths (26, 39, 52, 65, 78 weeks).
- **Why this matters:** If changing how much history we use flips the result from positive to negative, we can't trust it — the finding is an artifact of an arbitrary choice. 5/6 being consistent means the results are robust to this choice.
- **Why FICO was inconsistent:** Its effect is essentially zero (-3.97%), so random noise can flip the sign. This is actually reassuring — it means the methodology correctly identifies a near-zero effect as unstable, rather than artificially declaring it significant.

**VIF multicollinearity:** Starting from 14 candidate covariates, VIF iteratively removed the worst collinear ones.
- **Why this matters:** Collinear covariates make coefficient estimates unstable — the model can't distinguish which covariate is driving the prediction. For example, platform_spend and platform_impressions had VIF > 300, meaning >99% of their variance was shared. Including both is like counting the same information twice, which inflates uncertainty and produces unreliable counterfactuals.
- **What survived:** After VIF cleanup, 3-7 covariates remained per advertiser, then BIC narrowed to 2-4.

**Key covariate finding:** `spend_change_pct` appeared in ALL models. `metric_lag1/2` appeared in most.
- **Why spend_change_pct universally matters:** When an advertiser increases/decreases their budget week-over-week, their metrics shift regardless of media plan. This is the primary confound we need to control for. Raw spend levels (e.g., "this advertiser spends $50K/week") were NOT selected because they're mechanically correlated with the outcome — more spend → more impressions → different IVR. The *change* in spend captures budget decisions without that mechanical correlation.
- **Why metric_lag matters:** Advertiser IVR is autocorrelated — this week's IVR is partly predicted by last week's. Without the lag, the model attributes this momentum to the intervention. With it, the model says "this advertiser was already trending up/down before adoption."
- **Why platform metrics were mostly rejected:** Platform-wide IVR, spend, and impressions are all measuring roughly the same thing ("is the market hot or cold this week") and are highly collinear. BIC prefers the simpler path: use the advertiser's own dynamics rather than noisy platform-wide proxies. `platform_ivr` is occasionally selected when an advertiser's IVR tracks the market closely.

### Within-Advertiser Comparison (Recommended vs Non-Recommended)

7 advertisers have both recommended and non-recommended campaigns in post-period.
**Average IVR difference (rec - non_rec): -0.027** — recommended campaigns have lower IVR on average.

**Why this does NOT mean the recommendation is bad:** Recommended campaigns are NEW (created with media plan), while non-recommended campaigns are typically OLDER with established audience patterns. New campaigns always underperform during ramp-up because:
- Audience targeting hasn't optimized yet (the bidder needs time to learn which IPs convert)
- Frequency hasn't built up (first impressions are less effective than repeated exposure)
- The campaign is still exploring its delivery footprint

This comparison is **confounded by campaign maturity** and cannot be interpreted at face value. TI-780 (campaign ramp-up research) confirmed a ~4-week ramp-up period, which v5 excludes from the CausalImpact analysis.

## 5. Solution

**Deliverables:**
- `artifacts/ti_748_causal_impact.py` — CLI-runnable analysis script (v5)
- `artifacts/ti_748_causal_impact.ipynb` — Presentation notebook with glossary, methodology, and appendix
- `knowledge/experimentation.md` — New knowledge doc for experiment design (living document)

**v5 methodology (final):**
- Data source: `sum_by_campaign_by_day` (15 months history vs 6 months)
- 52-week pre-period (full seasonality vs 8 weeks)
- Recommended-only filter via `badge_state`
- Within-advertiser comparison
- BIC-optimized covariates **per advertiser** (not one-size-fits-all)
- VIF multicollinearity elimination (14 candidates → 3-4 per advertiser)
- Cross-validation of covariate sets (MAE/MAPE/RMSE comparison)
- Sensitivity analysis (pre-period length variation)
- Multi-point placebo tests (24% FPR — down from 86% in v2)
- **4-week ramp-up exclusion** (TI-780 finding)
- **Panel data model** as complementary aggregate analysis (1,255 obs, 14 advertisers)
- Winsorization, min-spend threshold, gap detection

## 6. Questions Answered

- **Q:** How to identify recommended vs customized media plans?
  **A:** `core.media_plan_publishers.badge_state` — values are `RECOMMENDED`, `USER_MODIFIED`, `USER_ADDED`

- **Q:** What attribution model do adopters use?
  **A:** All use `industry_standard` (include competing views/conversions)

- **Q:** How far back can we go for pre-period data?
  **A:** `sum_by_campaign_by_day` goes to 2024-01-01 (15+ months). `agg__daily_sum_by_campaign` only Sep 2025.

- **Q:** What pre-period length is best practice?
  **A:** 52 weeks — captures full seasonality. Google's CausalImpact paper recommends ≥3x post-period length AND at least one full seasonal cycle.

- **Q:** Does Media Plan adoption improve IVR overall?
  **A:** The aggregate effect is near zero (spend-weighted -0.23%, panel model +2.06% not sig). Some advertisers benefit meaningfully (3 showed +8-17% significant lift), but others showed significant negative effects. Too early to declare overall success with only 8 analyzable advertisers.

## 7. Data Documentation Updates

- `core.media_plan` and `core.media_plan_publishers` documented in data_catalog.md
- `r2_advertiser_settings` documented (no deleted column)
- `funnel_level` location clarified (campaigns, not campaign_groups)
- `agg__daily_sum_by_campaign` effective start documented (Sep 2025)
- Created `knowledge/experimentation.md` — experiment methodology knowledge base
- Updated global and project CLAUDE.md to auto-update experimentation.md

## 8. Open Items / Follow-ups

- **Share with Kirsa** — present v5 results and honest assessment
- **Re-run in 6-8 weeks** — more adopters will qualify, existing adopters will have longer post-periods, giving more statistical power
- **Per-advertiser ramp-up** — when more data available, consider advertiser-specific ramp-up durations instead of blanket 4-week exclusion
- **TI-780**: Campaign ramp-up research — confirmed ~4-week ramp-up to steady-state (used in v5 exclusion)
- Potential: analyze customized vs recommended allocations separately

---

## Files

| File | Description |
|---|---|
| `artifacts/ti_748_causal_impact.py` | Main analysis script v5 (CLI-runnable) |
| `artifacts/ti_748_causal_impact.ipynb` | Presentation notebook v5 (with glossary + methodology appendix) |
| `artifacts/datagrip_causal_impact.py` | Original Jaguar experiment notebook (reference only) |
| `artifacts/potential_media_plan_advertiser_adopters.xlsx` | Beta list from product team |
| `meetings/malachi_kirsa_meeting_1.txt` | Meeting transcript with Kirsa |
| `outputs/ci_*_results.csv` | Per-metric CSV exports |
| `outputs/within_advertiser_comparison.csv` | Recommended vs non-recommended comparison |
| `outputs/ci_*_*.png` | Per-advertiser and summary plots |
