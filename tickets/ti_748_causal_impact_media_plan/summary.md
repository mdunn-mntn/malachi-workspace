# TI-748: Causal Impact — Media Plan Feature

**Jira:** https://mntn.atlassian.net/browse/TI-748
**Status:** Complete
**Date Started:** 2026-03-26
**Date Completed:** 2026-03-27
**Assignee:** Malachi

---

## 1. Introduction

Media Plan is a beta feature giving advertisers control over network allocation (% of spend per publisher like ABC, CBS, ESPN). MNTN auto-recommends allocations based on audience and goals; advertisers can accept or customize. Only applies to new prospecting campaigns.

Kirsa requested a causal impact analysis to determine whether advertisers who adopted the Media Plan feature (using MNTN's recommended settings) saw improved prospecting performance.

**Stakeholder-facing findings doc:** `artifacts/ti_748_media_plan_findings.md` — clean summary for Kirsa, Daniella, and media team. This `summary.md` is the internal working record with full detail.

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

**Frequency hypothesis rejected:** Benefit is from long-tail pruning, not frequency accumulation (lower frequency → more unique households → better performance historically at MNTN).

**Baseline IVR does NOT predict who benefits** — CWRV (highest pre-IVR) benefited most, Boll & Branch (second-highest) was hurt most. Allocation strategy (config era) is the differentiator.

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

**Data sources:** Pre-Adoption Publishers = distinct domains in `sum_by_ctv_network_by_day` (≥5K impressions, pre-adoption period). Plan Publishers = distinct publishers in `media_plan_publishers`.

**The pattern:** 90% reduction (→16 publishers) = positive IVR. 80% reduction (→26 publishers) = negative IVR. Threshold ~88% / ~16-19 target publishers. All advertisers had 10-15x IVR variance between best and worst publishers pre-adoption.

**Did the algorithm pick the RIGHT publishers?**
Tested for Lighting New York: The algorithm recommended Samsung TV+ (12%), Bravo (12%), CNN (10%) — these rank #37-59 by actual pre-adoption IVR. The true best IVR publishers (Spectrum News 1.09%, sports networks 0.7-0.9%) have low inventory (17K-30K impressions vs Samsung's 627K). **The algorithm optimized for deliverability/reach, not IVR.** The benefit came from removing the worst publishers, not finding the best ones.

**Allocation Compliance:** Plans are followed within ±3%. Validated on CWRV Sales across multiple campaign groups. ~5-6% going to un-recommended publishers (e.g., Tubi) comes from Flex budget (10% reserve). Cross-validated `sum_by_ctv_network_by_day` against `cost_impression_log` — same publishers, same ranking. IMPORTANT: must filter to media plan campaign groups only (not all advertiser campaigns).

**Open issue:** Some campaign groups (e.g., ThirdLove 115424) show ALL publishers as "NOT In Plan" — possible name mapping issue.

**Actionable insights:** (1) Refresh Boll & Branch/Tempo plans under current config — zero-risk quick win. (2) ML model has feature skew (41/52 features zeroed at inference, per olympus `specs/backlog/ml-primary-scoring-mode.md`) — fixing improves publisher selection. (3) Mechanism is long-tail pruning + spend capacity filtering from Feb 3 release.

**Caveat:** N=8 — pattern is clear and mechanism is plausible, but not statistically confirmable at this sample size.

### Aggregate Assessment

Overall IVR near zero (spend-weighted -0.23%, panel +2.06% n.s.) — **because the analysis mixes two algorithm versions.** CWRV within-advertiser: old-config IVR 2.52% → new-config IVR 2.91% (+15.5%, 22x more impressions). Am College similar: 0.250% → 0.298% (+19%). If all advertisers were on current config, aggregate would likely be positive.

### Model Validation Results

| Validation | Result | Notes |
|---|---|---|
| Placebo FPR | 24% (down from 86% v2, 30% v3) | Residual FPR from natural structural breaks in advertiser time series |
| Sensitivity | 5/6 directionally consistent | Across pre-period lengths 26-78 weeks. FICO inconsistent (near-zero effect, noise flips sign) |
| VIF cleanup | 14 candidates → 3-7 per advertiser | platform_spend/platform_impressions had VIF >300 (removed) |
| BIC selection | 3-7 → 2-4 per advertiser | `spend_change_pct` in ALL models, `metric_lag1/2` in most |

**Key covariate insight:** `spend_change_pct` (week-over-week budget changes) is the primary confound — captures budget decisions without mechanical correlation to IVR. `metric_lag` controls for IVR autocorrelation. Platform-wide metrics mostly rejected by BIC as collinear and noisy.

### Within-Advertiser Comparison (Recommended vs Non-Recommended)

7 advertisers have both recommended and non-recommended campaigns in post-period. Average IVR difference (rec - non_rec): -0.027. **Confounded by campaign maturity** — recommended campaigns are new (still ramping), non-recommended are established. Not interpretable at face value. TI-780 confirmed ~4-week ramp-up period, which v5 excludes from CausalImpact.

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

## 8. Meeting Notes & Product Context

### Meeting 2 with Kirsa (2026-03-27)

**Key feedback:**
- Network is "mid to low" on the optimization lever hierarchy. Top: audience quality/recency → frequency → device type → networks. Feature's value may be more about customer control than pure performance.
- Frequency hypothesis rejected: historically at MNTN, *lower* frequency → more unique households → better performance. Frequency caps applied at household level, not network level. Benefit is from long-tail pruning, not frequency accumulation.
- Deliverability prioritized because (a) budget must be spent, (b) performance has diminishing returns as spend scales on a single network.

**Product context:**
- Beta selection is NOT random — PEX/CS hand-pick candidates, Toph validates pacing. Confirmed selection bias.
- No new beta additions planned until performance is validated.
- Dynamic media plan coming (recurring regeneration during flight). Blocks randomized experiment — can't test static version if dynamic is imminent.
- Daniella is TPM for media plan. Chris Addy is algorithm technical lead.
- UI experiment upcoming — feature flag to 50% of advertisers, changing goal entry. May involve TAR for methodology.

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

**Note:** Spendability is only 8% of the score (not primary driver as Release Brief implied). The spend capacity filter (≥$0.50/hr) acts as a hard gate BEFORE scoring — explains why algorithm picks deliverable publishers. Per-publisher score data exists in API response (Budget model); Chris checking if persisted to BQ.

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

**`deliverability_classification`:** Categorical delivery risk (high/medium/low). Worst individual guardrail wins. HHI tracking exists in metrics but is NOT a classification factor yet.

**Flex Targeting:** 10% budget reserved as flex pool. Un-recommended publisher impressions come from this pool. Confirms ±3% deviation.

**Without media plan:** No network optimization — pure auction-based allocation from inventory team deal commitments. Explains 131-183 pre-adoption publisher spread.

**M1** (beta released 2025-10-20): TV-Only Prospecting only, pre-launch changes only, auto-managed vs manual modes.
**M2** (UI reskin, EOM Jan 2026): Auto-applied on campaign creation, happy path = accept recommendations.
**Upcoming:** Dynamic media plan (recurring regeneration during flight) — blocks planned experiment.

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
| `artifacts/ti_748_media_plan_findings.md` | **Stakeholder-facing findings doc** — What/Why/What's Next for Kirsa, Daniella, media team |
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
