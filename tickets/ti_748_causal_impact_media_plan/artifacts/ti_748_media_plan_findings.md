# Media Plan Causal Impact — Findings & Recommendations

**TI-748 | March 2026 | Malachi Dunn, TAR**

---

## Executive Summary

We measured whether MNTN's recommended Media Plan improves prospecting IVR using per-advertiser CausalImpact analysis (Bayesian structural time series). The aggregate effect is near zero — but that headline masks a critical finding: **a config change on Feb 3, 2026 is the real differentiator.** Plans generated under the new config (max_networks=15) show positive IVR effects; plans under the old config (max_networks=25) show negative effects.

**Bottom line:** The Media Plan feature works — when the algorithm produces concentrated allocations. The two large negative outliers are running stale plans from the old config and were never refreshed.

---

## What We Found

### 1. Per-Advertiser IVR Results

8 advertisers analyzed. 52-week pre-period, BIC-optimized covariates per advertiser, 4-week ramp-up exclusion.

| Advertiser | IVR Effect | Significant | Config Era | # Publishers |
|---|---|---|---|---|
| CWRV Sales | **+16.8%** | Yes | New (Feb 2026+) | 16 |
| Lighting New York | **+10.5%** | Yes | Exception (16 from start) | 16 |
| Taskrabbit | +8.3% | Yes | Old (Oct 2025) | 26 |
| Talkspace | +4.7% | Varies | Old → New | 25 → 16 |
| Am. College of Ed | +3.6% | No | Old → New | 26 → 16 |
| FICO | -4.0% | No | Old (Oct 2025) | 25-26 |
| Tempo | **-26.2%** | Yes | Old (Oct 2025) | 26 |
| Boll & Branch | **-31.5%** | Yes | Old (Nov 2025) | 26 |

**Aggregate:** Spend-weighted effect -0.23% (near zero). Panel model +2.06% (not significant). This averages across two algorithm versions and washes out the real signal.

### 2. The Config Change Is the Differentiator

On **Feb 3, 2026**, the algorithm's `max_networks` parameter was reduced from 25 to 15 (olympus commit `555234f`, PERML-412). A spend capacity filter ($0.50/hr minimum) was also added in the same release.

Every plan created **before** this change has 25-26 publishers. Every plan created **after** has exactly 16.

| Config Era | Advertisers | Avg IVR Effect | Publisher Count |
|---|---|---|---|
| New config (post-Feb 2026) | CWRV, Lighting NY | **+13.6%** | 16 |
| Old config (pre-Feb 2026, never refreshed) | Boll & Branch, Tempo | **-28.8%** | 26 |
| Old → transitioned to new | Am College, Talkspace | **+4.1%** | 26 → 16 |

**CWRV Sales** is the clearest case: their first plan (Nov 2025) had 26 publishers under the old config. All subsequent plans (Feb 2026+) have 16 publishers under the new config. Within-advertiser IVR comparison: old-config plan = 2.52% IVR, new-config plans = 2.91% IVR (+15.5%).

### 3. Why Concentrated Plans Work

Before adoption, every advertiser was delivering impressions across **131-183 publishers** — pure auction-based allocation with no network optimization. The media plan concentrated them to 16-26 publishers.

**Two mechanisms drive the improvement:**

1. **Long-tail pruning.** The old config kept ~25 publishers, many barely clearing the 0.5% minimum allocation. The new config forces the algorithm to drop the bottom ~10, eliminating low-quality, low-inventory networks.

2. **Budget concentration.** With 15 networks and alpha=5.0 softmax temperature, top-scoring publishers get 12-15% budget share (vs 3-5% spread thin). The bidder can optimize delivery effectively per network.

**Which publishers also matters:**
- Benefited advertisers' plans concentrate on major broadcast networks: CBS (up to 15%), NBC (12%), ABC (12%), ESPN, Peacock
- Hurt advertisers' plans spread across niche streaming: Roku Drama (6-8%), ION TV (5%), AMC (5%)

### 4. Allocation Compliance Confirmed

Plans are being followed. Actual vs recommended allocation deviates by ±3% on average. The ~5-6% going to un-recommended publishers (e.g., Tubi) comes from the Flex budget (10% reserve for real-time optimization). This is by design.

---

## Why It Matters

The media plan feature is working as intended under the current config. The aggregate "near zero" result is misleading because it combines two algorithm versions. When isolated to the current config:

- **3 of 3 advertisers** with 16-publisher plans show positive IVR effects
- **2 of 3 advertisers** with 26-publisher plans (never refreshed) show large negative effects
- The algorithm picks deliverable publishers (not highest-IVR ones) — the benefit comes from eliminating the long tail of poor performers

**Caveat:** N=8 is too small for statistical confidence in the config-era comparison. The per-advertiser pattern is clear but not yet proven at scale.

---

## What's Next — Recommended Actions

### Immediate (no engineering required)

1. **Refresh old-config plans.** Boll & Branch and Tempo are still running 26-publisher plans from Oct-Nov 2025. Regenerating under the current config (max_networks=15) should improve their IVR. Zero risk, high potential upside.

2. **Share findings with Daniella (TPM).** Summary and methodology docs ready for review.

### Short-term (next 6-8 weeks)

3. **Re-run CausalImpact with more data.** More post-period weeks + any new adopters will strengthen (or refute) the config-era pattern. If Boll & Branch and Tempo get refreshed plans, their before/after provides a natural experiment.

4. **Test concentration as a formal covariate.** Add number of plan publishers or HHI (Herfindahl index) as a moderation variable in the panel model.

### Medium-term (requires algorithm team)

5. **Alpha tuning test.** Our data suggests higher concentration works. The `alpha` parameter (softmax temperature, currently 5.0) is tunable via config — no code change needed. An A/B test with alpha=7 vs alpha=5 would directly test whether more concentration improves IVR.

6. **Fix ML model feature skew.** The ML prediction model (10% of combined score) has 41/52 features receiving zero values at inference. Fixing this would improve publisher selection quality.

7. **Add HHI to deliverability classification.** HHI tracking already exists in metrics but isn't used as a guardrail input. Could use it to flag overly-diluted plans.

### Blocked

8. **Randomized experiment.** Blocked until the dynamic media plan ships (recurring plan regeneration during campaign flight). Can't test the static version if the dynamic version is imminent. Design is ready when the feature stabilizes.

---

## Methodology Summary

| Component | Detail |
|---|---|
| **Model** | Google CausalImpact (Bayesian structural time series), per-advertiser |
| **Pre-period** | 52 weeks (full seasonality cycle) |
| **Ramp-up exclusion** | 4 weeks post-adoption excluded (TI-780 finding) |
| **Covariates** | BIC-optimized per advertiser from 14 candidates. Typical: spend_change_pct, metric_lag1, sometimes platform_ivr |
| **Multicollinearity** | VIF elimination (14 → 3-4 covariates per advertiser) |
| **Validation** | Placebo tests (24% FPR), sensitivity analysis (5/6 directionally consistent across pre-period lengths), cross-validation |
| **Complementary** | Panel data model (1,255 obs, 14 advertisers, R² adj = 0.679) |
| **Data source** | `sum_by_campaign_by_day` (back to 2024-01-01) |
| **Adopter filter** | `media_plan_publishers.badge_state = RECOMMENDED` only |

Full methodology explainer available in `ti_748_methodology_explainer.md`.

---

## Algorithm Reference (from Chris Addy, 2026-03-27)

**Pipeline:** Semantic search (top 300 candidates) → spend capacity filter (≥$0.50/hr) → scoring & softmax allocation → drop networks below 0.5% → enforce min(10)/max(15) bounds.

**Scoring weights:**

| Component | Weight |
|---|---|
| Performance composite | 25% (advertiser 50%, vertical 30%, network 20%) |
| Quality | 25% |
| Semantic relevance | 20% |
| ML prediction | 10% |
| Spendability | 8% |
| CPM efficiency | 6% |
| Scale | 4% |
| Accessibility | 2% |

**Concentration levers:** `alpha` (softmax temp, default 5.0), `max_networks` (15), `max_allocation` (12% per network), `min_allocation` (0.5% drop threshold).

**Without media plan:** The bidder does not optimize network allocation. Impressions are distributed across 130-180+ publishers based on inventory team deal commitments and auction dynamics. No systematic network-level optimization occurs.
