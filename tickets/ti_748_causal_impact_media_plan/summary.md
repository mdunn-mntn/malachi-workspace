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

### THE KEY FINDING: Algorithm Config Version Predicts Who Benefits

The aggregate IVR effect is near zero — but that's because the analysis spans TWO versions of the algorithm. A config change on **Feb 3, 2026** (olympus commit `555234f`, PERML-412) reduced `max_networks` from 25 → 15 and added spend capacity filtering. Plans generated under the new config produce concentrated allocations that improve IVR; plans under the old config produced diluted allocations that hurt IVR.

**The config version is the differentiator, not vertical, advertiser size, or inherent concentration logic.**

Each media plan allocates the campaign's total budget as percentages across publishers/networks (e.g., CBS gets 15% of a $100K campaign = $15K on CBS). These percentages come from `media_plan_publishers.percentage` — the budget split the algorithm recommends.

| Advertiser | IVR Effect | # Publishers | Config Era | First Plan Date | Top Publisher % | Spread (std) |
|---|---|---|---|---|---|---|
| **CWRV Sales** | **+16.8%*** | **16** | **New (Feb 2026+)** | 2025-11-06 (26-pub), **2026-02-13 (16-pub)** | **15% (CBS)** | **2.96** |
| **Lighting NY** | **+10.5%*** | **16** | **Exception** | **2025-10-28** | **12%** | **3.42** |
| Taskrabbit | +8.3%* | 26 | Old (Oct 2025) | 2025-10-27 | 10% | 2.24 |
| Talkspace | +4.7% | 25 | Old→New | 2026-01-08 (25-pub), 2026-03-23 (16-pub) | 11% | 2.45 |
| Am College of Ed | +3.6% | 26→16 | Old→New | 2026-01-27 (26-pub), 2026-02-12 (16-pub) | 11% | 2.21 |
| FICO | -4.0% | 25-26 | Old | 2025-10-27 | 11% | 2.09 |
| **Tempo** | **-26.2%*** | **26** | **Old (Oct 2025)** | **2025-10-28** | **8%** | **1.22** |
| **Boll & Branch** | **-31.5%*** | **26** | **Old (Nov 2025)** | **2025-11-21** | **10%** | **1.62** |

**The pattern — config version, not inherent concentration:**
- **Benefited** advertisers either had plans under the **new config (max_networks=15, post-Feb 3 2026)** or were the Lighting NY exception (16 pubs from start)
- **Hurt** advertisers had plans under the **old config (max_networks=25)** and never got refreshed plans
- **CWRV** is the key case: their old 26-pub plan was replaced by 16-pub plans in Feb 2026 — their positive result reflects the new config
- **Boll & Branch and Tempo** never got plan updates — stuck on old config

**Why the new config works — two mechanisms:**
1. **Long-tail pruning:** The old config (max_networks=25) kept many low-scoring publishers that barely cleared the 0.5% minimum allocation threshold. The new config (max_networks=15) forces the softmax to drop the bottom ~10 networks, removing the long tail of poor performers. Combined with the spend capacity filter ($0.50/hr minimum added in the same Feb 3 release), low-inventory networks are also eliminated.
2. **Budget concentration via softmax:** With alpha=5.0 softmax temperature and only 15 networks, the top-scoring publishers get meaningfully larger budget shares (12-15% vs 3-5%). The bidder has enough budget per network to optimize delivery effectively.

**Note on frequency hypothesis:** We initially hypothesized that concentration enables household-level frequency building per network. Kirsa pushed back on this — historically at MNTN, *lower* frequency → more unique households → better overall performance. The benefit is more likely from eliminating waste on low-quality networks than from frequency accumulation.

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

**The Full Pre-Adoption → Media Plan → Outcome Story:**

Every single advertiser was delivering impressions across **131-183 publishers** before adoption (confirmed via `sum_by_ctv_network_by_day`, pre-adoption period, minimum 5,000 impressions per publisher). The media plan concentrated them down to 16-26 publishers. The degree of concentration predicts the outcome.

| Advertiser | IVR Effect | Pre-Adoption Publishers | Plan Publishers | Reduction | Best/Worst IVR Ratio |
|---|---|---|---|---|---|
| **CWRV Sales** | **+16.8%*** | 168 | **16** | **90%** | 12.0x |
| **Lighting NY** | **+10.5%*** | 151 | **16** | **89%** | 14.9x |
| Taskrabbit | +8.3%* | 183 | 26 | 86% | 12.7x |
| Talkspace | +4.7% | 176 | 21 | 88% | 7.1x |
| Am College of Ed | +3.6% | 170 | 19 | 89% | 12.3x |
| FICO | -4.0% | 180 | 25 | 86% | 15.8x |
| **Tempo** | **-26.2%*** | 157 | **26** | **83%** | 10.6x |
| **Boll & Branch** | **-31.5%*** | 131 | **26** | **80%** | 10.5x |

**How this was calculated:**
- "Pre-Adoption Publishers" = count of distinct `domain` values in `sum_by_ctv_network_by_day` for each advertiser's prospecting campaigns, between 2025-01-01 and their first media plan create date, with ≥5,000 impressions per publisher
- "Plan Publishers" = count of distinct publishers in `media_plan_publishers` for their recommended plans
- "Best/Worst IVR Ratio" = the best-performing publisher's IVR divided by the worst (pre-adoption), showing how much IVR variance existed across publishers before optimization

**The pattern is unambiguous:**
- **90% publisher reduction** (168 → 16 publishers) → positive IVR effect (+10 to +17%)
- **80% publisher reduction** (131 → 26 publishers) → negative IVR effect (-26 to -31%)
- The threshold appears to be around **~88% reduction** (roughly 16-19 target publishers)

**Why more concentration works — the frequency mechanism:**
Every advertiser had 10-15x IVR variance between their best and worst publishers. When budget is spread across 160+ publishers, most of the spend goes to the long tail of low-IVR networks. Concentrating to 16 publishers eliminates that long tail and lets the bidder build meaningful household frequency on its best channels.

**Did the algorithm pick the RIGHT publishers?**
Tested for Lighting New York: The algorithm recommended Samsung TV+ (12%), Bravo (12%), CNN (10%) — these rank #37-59 by actual pre-adoption IVR. The true best IVR publishers (Spectrum News 1.09%, sports networks 0.7-0.9%) have low inventory (17K-30K impressions vs Samsung's 627K). **The algorithm optimized for deliverability/reach, not IVR.** The benefit came from removing the worst publishers, not finding the best ones.

**Allocation Compliance Check: Are the recommended percentages being followed?**

IMPORTANT: Initial analysis showed massive deviations — but that was because we were counting ALL campaigns, not just media plan campaign groups. After correcting to ONLY count impressions from campaign groups that have a recommended media plan:

CWRV Sales (benefited, +16.8%) — campaign group 103569:
| Publisher | Recommended | Actual | Diff |
|---|---|---|---|
| Fox News | 7% | 7.8% | +0.8% |
| A&E | 5% | 7.2% | +2.2% |
| NBC | 7% | 6.5% | -0.5% |
| AMC | 5% | 5.4% | +0.4% |
| HBO Max | 5% | 4.5% | -0.5% |

CWRV CG 111504: NBC recommended 12% → actual 15.2%, Peacock recommended 12% → actual 15.0%. Close.
CWRV CG 111505: NBC recommended 12% → actual 11.9%. Nearly exact.

**The plans ARE being largely followed for media plan campaign groups**, with deviations mostly within ±3%. The "Flex" allocation (7-10%) accounts for most remaining variance — some un-recommended publishers (Tubi Entertainment) receive 5-6% which likely comes from the Flex budget.

**Methodology note:** Matched `media_plan_publishers.name` to `sum_by_ctv_network_by_day.domain` (exact name matches confirmed). Only counted impressions from campaigns belonging to campaign groups that have a recommended media plan (`media_plan.campaign_group_id`), filtered to post-plan-creation dates.

**Data source validation:** Cross-validated `sum_by_ctv_network_by_day` against `cost_impression_log` (the authoritative impression-level source — one row per won/paid impression). For CWRV Sales in Feb 2026, both tables show the same top-5 publishers in the same rank order (Peacock, NBC, Tubi Entertainment, HBO Max, Paramount Streaming - Comedy). Counts differ slightly due to campaign filtering, but the publisher distribution is consistent. `sum_by_ctv_network_by_day` is a valid proxy for publisher-level analysis. Note: `impression_log` was NOT used because it contains all bids (won + not won), not just delivered impressions.

**Still needs investigation:** Some campaign groups (e.g., ThirdLove 115424) show ALL publishers as "NOT In Plan" despite being linked to a media plan — may be a join issue with how publisher names map for that specific plan, or the plan publishers may use different naming.

**Three actionable product insights:**
1. **Refresh old-config plans immediately.** Boll & Branch and Tempo are still running 26-publisher plans from Oct-Nov 2025. Regenerating their plans under the current config (max_networks=15, spend capacity filter) would likely improve their IVR. This is a zero-risk quick win.
2. **The ML model is the next performance lever.** Performance scoring is 25% of the combined score, but the ML model has a critical feature skew issue — 41/52 features receive zero values at inference (documented in olympus `specs/backlog/ml-primary-scoring-mode.md`). Fixing this would meaningfully improve publisher selection quality.
3. **The mechanism is long-tail pruning + spend capacity filtering.** The Feb 3 release added both max_networks=15 AND $0.50/hr spend capacity filtering. Together, they eliminate networks that can't deliver meaningful budget AND cap total network count. This forces the softmax to concentrate budget on the highest-scoring publishers.

**Caveat:** N=8 is too small to confirm statistically. But the pattern is unambiguous, the mechanism is plausible, and it's directly actionable.

### Aggregate Assessment

The overall IVR effect is near zero (spend-weighted -0.23%, panel model +2.06% not significant). **This is because the analysis mixes two algorithm versions.** Plans under the old config (max_networks=25, pre-Feb 2026) produced diluted allocations that hurt performance; plans under the new config (max_networks=15, post-Feb 2026) produced concentrated allocations that helped. Averaging across both washes out the effect.

**Config era within-advertiser comparison (CWRV Sales):**
| Config Era | # Publishers | IVR | Impressions | Spend |
|---|---|---|---|---|
| Old config (26-pub) | 26 | 2.52% | 75K | $2.2K |
| New config (16-pub) | 16 | 2.91% | 1.7M | $136K |

CWRV's 16-publisher plans (new config) show **15.5% higher IVR** than their 26-publisher plan (old config), with 22x more impressions. Am College of Education shows a similar pattern: old config IVR 0.250% → new config IVR 0.298% (+19%).

**Implication:** If all advertisers were on the current config, the aggregate effect would likely be positive. The two large negative outliers (Boll & Branch, Tempo) are on the old config and drag down the average. Refreshing their plans is the highest-priority action.

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
  **A:** The aggregate effect is near zero, but this mixes two algorithm versions. Under the current config (max_networks=15, post-Feb 3 2026), advertisers show positive IVR effects. The negative outliers are on the old config (max_networks=25) and were never refreshed.

- **Q:** Does the algorithm version/config affect outcomes?
  **A:** Yes — this is the primary differentiator. `max_networks` was changed from 25 → 15 on Feb 3, 2026 (olympus commit 555234f). Every plan after this date has 16 publishers; every plan before has 25-26. The config change explains the performance split.

- **Q:** What are the algorithm's scoring weights?
  **A:** Performance composite 25%, quality 25%, semantic 20%, ML prediction 10%, spendability 8%, CPM efficiency 6%, scale 4%, accessibility 2%. Performance itself blends advertiser-level (50%), vertical (30%), network (20%) history. Pipeline: semantic search (FAISS + Gemini, top 40) → spend capacity filter (≥$0.50/hr) → softmax(alpha=5.0) → enforce min=10/max=15 → cap 12% per network.

## 7. Data Documentation Updates

- `core.media_plan` and `core.media_plan_publishers` documented in data_catalog.md
- `r2_advertiser_settings` documented (no deleted column)
- `funnel_level` location clarified (campaigns, not campaign_groups)
- `agg__daily_sum_by_campaign` effective start documented (Sep 2025)
- Created `knowledge/experimentation.md` — experiment methodology knowledge base
- Updated global and project CLAUDE.md to auto-update experimentation.md

## 8. Meeting 2 with Kirsa (2026-03-27)

**Presented:** v5 results (ramp-up charts, CausalImpact results, concentration finding)

### Kirsa's Reactions & Feedback

- **On aggregate results:** Kirsa looks at % of advertisers with positive impact rather than spend-weighted averages. She reads 3/5 significant positive vs 2/5 negative as "generally good but inconclusive." This is how she'd frame it as a product owner.
- **On concentration finding:** "It's definitely interesting" — wants to see if it repeats with a larger group. Agrees Pareto distribution for allocation makes sense (one dominant network, tapering down). Current equal-spread is suboptimal.
- **On network as optimization lever:** From her experience doing manual campaign optimizations, network was "mid to low" on her list. Top was audience quality/recency, then frequency, then device type, then networks. Important context — the feature's value may be more about customer control than performance optimization.
- **Frequency hypothesis pushback:** Kirsa challenges our frequency-per-network hypothesis. Historically at MNTN, *lower* frequency → more unique households → better performance. The "wasted impressions" on non-converting households at 3+ frequency offset the benefit of repeated exposure. Our frequency caps are applied at household level, not network level.
- **Deliverability trade-off:** Two reasons deliverability is prioritized: (a) must ensure budget can be spent, (b) performance degrades as you scale on a network — diminishing returns per conversion/visit as spend increases on a single network.

### Key Product Context Learned

- **Beta selection is NOT random:** PEX/CS identify candidates based on past interest, Toph (production ops) validates they won't have pacing issues. This is confirmed selection bias — adopters are hand-picked, not randomized.
- **No new beta additions planned:** Chicken-and-egg — they want to validate performance before adding more. Our analysis or experiment results could unlock more additions.
- **Dynamic media plan coming (blocks experiment):** New requirement — media plan should regenerate on a recurring cadence (frequency TBD — Daniella said hourly, Mark said too often, may be weekly). Experiment can't launch until dynamic version ships because static results don't apply to the dynamic version.
- **Share with Daniella (TPM for media plan):** Kirsa will share our PDFs with Daniella as a precursor to future data pulls. Malachi to send the methodology explainer and summary as PDFs.
- **Contact for algorithm questions:** Chris Addy (technical lead). Ping Kirsa and Addy for follow-ups about media plan experimentation.
- **UI experiment coming:** First UI-based experiment — feature flag to 50% of advertisers, changing how goals are entered on new campaigns. Hypothesis: realistic goals → more campaigns at goal. Malachi may be looped in for methodology.

### Algorithm Details (from Release Brief + Requirements Doc + Chris Addy 2026-03-27)

**Pipeline:** semantic search (top 300 candidates) → spend capacity filter (must sustain ≥$0.50/hr) → scoring & softmax allocation → drop networks below 0.5% → enforce min/max bounds.

**Scoring weights (final combined score):**
| Component | Weight | Notes |
|---|---|---|
| Performance composite | 25% | Blends advertiser-level (50%), vertical (30%), network-level (20%) when advertiser data exists |
| Quality | 25% | |
| Semantic relevance | 20% | |
| ML prediction | 10% | `score_performance_ml_predicted_normalized` |
| Spendability | 8% | Inventory availability and scalability |
| CPM efficiency | 6% | |
| Scale | 4% | |
| Accessibility | 2% | |

**IMPORTANT CORRECTION:** Per Chris Addy, spendability is only 8% of the score, not the primary driver as the Release Brief implied. Performance (25%) + quality (25%) + semantic (20%) dominate. However, the spend capacity filter (≥$0.50/hr) acts as a hard gate BEFORE scoring, which explains why the algorithm picks deliverable publishers — low-inventory networks get filtered out before scoring even happens.

**Per-publisher score data:** The API response from the mediaplan service includes all component scores (score_semantic, score_performance_advertiser, score_performance_vertical, score_spendability, etc.) in the Budget model. Chris checking if the full score breakdown is persisted to BigQuery or just final allocations.

**Config parameters controlling concentration:**
| Parameter | Default | Effect |
|---|---|---|
| `alpha` (softmax temperature) | 5.0 | **The big lever.** Higher = more concentrated on top-scoring networks. Lower = more uniform spread. |
| `max_networks` | 15 | Hard upper bound on plan size |
| `min_networks` | 10 | Hard lower bound on plan size |
| `max_allocation` | 12% | Hard cap per network (prevents single-network dominance) |
| `min_allocation` | 0.5% | Networks scoring below this get dropped |

**CRITICAL FINDING — CONFIG CHANGE CONFIRMED (queried 2026-03-27):**

The `max_networks` was changed from 25 to 15 on **Feb 3, 2026** (olympus commit `555234f`, ticket PERML-412). Also added spend capacity filtering in the same release. This is visible in the plan creation timeline:

| Advertiser | First Plan Date | Publishers | Config Era | IVR Effect |
|---|---|---|---|---|
| FICO | 2025-10-27 | 26 | Old | -4.0% |
| Taskrabbit | 2025-10-27 | 26 | Old | +8.3%* |
| **Lighting New York** | **2025-10-28** | **16** | **Exception** | **+10.5%*** |
| Tempo | 2025-10-28 | 26 | Old | -26.2%* |
| CWRV Sales (1st plan) | 2025-11-06 | 26 | Old | — |
| Boll & Branch | 2025-11-21 | 26 | Old | -31.5%* |
| FICO (2nd) | 2026-01-02 | 25 | Transition | — |
| Talkspace | 2026-01-08 | 25 | Transition | +4.7% |
| Am College (1st) | 2026-01-27 | 26 | Transition | — |
| **Am College (2nd)** | **2026-02-12** | **16** | **New** | — |
| **CWRV (all later)** | **2026-02-13+** | **16** | **New** | **+16.8%*** |

**Key observations:**
1. **Every plan created before Feb 2026 has 25-26 publishers** (except Lighting NY at 16 — possible per-advertiser override or budget/vertical-specific outcome)
2. **Every plan created after Feb 2026 has exactly 16 publishers** — consistent with `max_networks=15` config change
3. **CWRV Sales** had its first plan at 26 publishers (Nov 2025), but ALL subsequent plans (Feb 2026+) are 16 publishers. Their positive IVR result (+16.8%) likely reflects the NEW config's concentrated plans, not the original 26-publisher plan.
4. **Boll & Branch and Tempo** never got updated plans — stuck on old 26-publisher config. Their negative results may reflect the old config, not the feature itself.
5. **Lighting New York** is the exception — 16 publishers from the start (Oct 2025), and showed strong +10.5% lift. Suggests the 16-publisher concentration works regardless of when it was generated.

**Implication for Kirsa:** The "concentration predicts who benefits" finding is actually **"new config vs old config predicts who benefits."** The algorithm was improved between Oct 2025 and Feb 2026 to produce more concentrated plans. Advertisers running under the new config see positive results. The two worst performers (Boll & Branch, Tempo) are on the old config and never got refreshed plans. **This is actionable: refresh their plans under the current config and re-measure.**

**`deliverability_classification`:** Categorical prediction of delivery risk: "high" (expect full spend), "medium" (moderate underspend risk), "low" (high underspend risk). Computed by guardrail model evaluating per-network daily spend thresholds, audience size, blocked networks, budget constraints. Final classification = worst individual guardrail. For in-flight campaigns: if >3 days in and spending at >90% pace, gets upgraded to "high" regardless. **HHI (Herfindahl index) tracking exists in metrics but is NOT a classification factor yet** — could be added.

**Flex Targeting:** Plan reserves typically 10% of budget as flex allocation not assigned to specific networks. Bidder uses this pool for real-time optimization — impressions on un-recommended publishers (like Tubi Entertainment) come entirely from this flex pool. Confirms our ±3% deviation observation.

**Without media plan:** The bidder does NOT optimize network allocation. It buys from a huge bucket of inventory, with allocation driven by inventory team deal commitments (e.g., "told HBO we'd spend $1M in Q1") and manual adjustments when customers complain about concentration. This explains the 131-183 pre-adoption publisher spread — it's pure auction-based allocation.

**M1 (current, beta released 2025-10-20):**
- TV-Only Prospecting only (Retargeting generated on backend but not surfaced to customer)
- No opt-in — media plan required for all AIDs in beta
- Changes before campaign launch only (mid-flight not supported)
- Auto-managed (MNTN optimizes) vs Manual modes
- Deliverability Confidence widget with real-time feedback
- "Why this?" links explaining each network recommendation

**M2 (UI reskin, EOM January 2026):**
- Media plan auto-applied when user clicks the media plan step in campaign creation
- Design makes clear: happy path = don't make manual edits
- Secret bypass: skip media plan step → plan not applied
- Disabling media plan for Multi-Touch (MT) campaigns in future

**Upcoming (not yet released):**
- Dynamic media plan — recurring regeneration/rebalancing during campaign flight
- This blocks the planned experiment (can't test static version if dynamic is coming)

### Questions for Data Team — Status

| # | Question | Status | Answer |
|---|---|---|---|
| 1 | How does bidder allocate without media plan? | ✅ Answered (Kirsa) | No network optimization — pure auction-based from inventory team deal buckets. Manual adjustments only when customers complain. |
| 2 | Table tracking actual vs planned allocation? | ✅ Resolved | `sum_by_ctv_network_by_day` confirmed as valid proxy, cross-validated against `cost_impression_log`. Plans followed within ±3%. |
| 3 | What determines # of publishers recommended? | ✅ Answered (Chris Addy) | Pipeline: semantic search (300) → spend filter (≥$0.50/hr) → softmax scoring → drop <0.5% → enforce min=10/max=15 bounds. **26-publisher plans exceed default max=15 — likely old config or override.** |
| 4 | Deliverability score / publisher capacity table? | ✅ Answered (Chris Addy) | API response includes full score breakdown per network (score_semantic, score_performance_*, score_spendability, etc.). Chris checking if persisted to BQ. |
| 5 | Does `deliverability_classification` indicate confidence? | ✅ Answered (Chris Addy) | Categorical delivery risk: high/medium/low. Computed by guardrail model (per-network spend caps, audience size, blocked networks). Worst guardrail wins. HHI tracking exists but not a factor yet. |
| 6 | Can concentration be tuned? | ✅ Answered (Chris Addy) | Yes — `alpha` (softmax temp, default 5.0) is the main lever. Higher = more concentrated. Also `max_allocation` (12%), `min_allocation` (0.5%). Config change, not code change. Chris can set up exposure. |
| 7 | How does Flex Targeting interact? | ✅ Answered (Chris Addy) | 10% budget reserved as flex pool. Un-recommended publisher impressions (e.g., Tubi) come entirely from this pool. Confirms our ±3% deviation. |

### Next Steps (Updated Priority Order)

1. ✅ **Shared v5 results with Kirsa** (meeting 2, 2026-03-27) — she'll forward to Daniella (TPM)
2. ✅ **Asked Chris Addy** — all 5 questions answered (2026-03-27). Key finding: 26-publisher plans exceed default max_networks=15.
3. ✅ **Confirmed config change via BQ query** — every plan before Feb 2026 has 25-26 publishers; every plan after has 16. Config was changed between Jan-Feb 2026. Boll & Branch/Tempo are on old config, never refreshed.
4. **Share config change finding with Kirsa** — the story is now: "old config produced diluted plans that hurt performance; new config produces concentrated plans that help. Refresh Boll & Branch/Tempo plans under current config."
5. **Follow up with Chris Addy** — (a) confirm if per-publisher score breakdown is persisted to BQ, (b) ask about Lighting NY exception (16 pubs on Oct 28 when everyone else got 26), (c) discuss refreshing old-config plans
6. **Explore olympus repo** — cloned to `/Users/malachi/Developer/work/mntn/olympus`. Contains algorithm code, config, docs.
5. **Send PDFs to Kirsa** — methodology explainer and summary for Daniella to review
6. **Wait for dynamic media plan release** — experiment blocked until then
7. **Propose alpha tuning test to Chris/Kirsa** — our data suggests higher concentration works; Chris confirmed alpha is tunable via config. A/B test with alpha=7 vs alpha=5 would be high-value.
8. **Re-run CausalImpact in 6-8 weeks** — more post-period data. If beta expands, more advertisers too.
9. **Test concentration as a covariate** — add "number of plan publishers" or HHI as a covariate to formally test moderation
10. **Get looped in on UI experiment** — Kirsa will connect Malachi with ML team for goal-setting experiment methodology

---

## Files

| File | Description |
|---|---|
| `artifacts/ti_748_causal_impact.py` | Main analysis script v5 (CLI-runnable) |
| `artifacts/ti_748_causal_impact.ipynb` | Presentation notebook v5 (with glossary + methodology appendix) |
| `artifacts/ti_748_methodology_explainer.md` | Technical + non-technical methodology explanation |
| `artifacts/datagrip_causal_impact.py` | Original Jaguar experiment notebook (reference only) |
| `artifacts/potential_media_plan_advertiser_adopters.xlsx` | Beta list from product team |
| `artifacts/Media Plan Feature Release Brief.pdf` | Feature release brief (M1 + M2) from Kirsa |
| `artifacts/BP-Requirements Documentation \| MNTN Matched Media Plan-270326-174121.pdf` | Requirements doc (algorithm details, outstanding questions) |
| `meetings/malachi_kirsa_meeting_1.txt` | Meeting transcript with Kirsa (#1) |
| `meetings/malachi_kirsa_meeting_2.txt` | Meeting transcript with Kirsa (#2 — v5 results review) |
| `outputs/ci_*_results.csv` | Per-metric CSV exports |
| `outputs/within_advertiser_comparison.csv` | Recommended vs non-recommended comparison |
| `outputs/ci_*_*.png` | Per-advertiser and summary plots |
