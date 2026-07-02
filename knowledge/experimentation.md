# Experimentation & Causal Inference — Knowledge Base
Last updated: 2026-05-28 | Started from TI-748 (Media Plan Causal Impact)

This is a living document. Add to it every time we learn something new about experimental design, covariate selection, test methodology, or edge cases at MNTN.

---

## ⭐ Standard Analysis Protocol — apply to every tiered rollout / experiment evaluation

**When to apply:** any analysis of a feature flip, tiered rollout, A/B test, audience-platform experiment, scoring-algorithm change, or holdout study. If the work answers "did this change move a KPI?", follow this protocol.

**Why:** TI-748 / TI-849 / TI-921 / TI-961 each re-derived this pipeline from scratch. The team will run dozens more experiments (incrementality overhaul, 5 external vendor tests, BUK rollout follow-ups, audience experiments). The protocol exists so we get the same rigor every time, and so a teammate can read one method-section sentence and know what we did.

### The 5-step pipeline

| # | Step | Purpose | Canonical implementation |
|---|------|---------|--------------------------|
| 1 | **Power analysis** | Up front, before the experiment ships. Compute required cohort size for the MDE we care about. CUPED-adjust ρ if covariates exist. Communicate "we need N advertisers for X weeks to detect Y% lift at 80% power." | [`tickets/ti_884_power_analysis_calculator/`](../tickets/ti_884_power_analysis_calculator/) — `mde_binomial`, `mde_continuous` |
| 2 | **Cohort + flip-date detection** | Define treated/control sets dynamically from a source-of-truth inclusion table (Postgres `tpa.*_inclusion` if it exists; else maintain a CSV). Auto-detect per-tier inclusion dates so analyses pick up new flips without code changes. | [`tickets/ti_921_fangorn_lift_dashboard/queries/`](../tickets/ti_921_fangorn_lift_dashboard/queries/) — wave-aware queries pattern |
| 3 | **Pre/post + DiD with cluster-bootstrap inference** | Per (tier, KPI): aggregate per-advertiser pre/post sums, resample advertisers with replacement N=1000, recompute pooled DiD lift each time. Report point estimate, 95% CI (2.5/97.5 percentiles), two-sided p-value (`2 × min(P(boot ≥ 0), P(boot ≤ 0))`). **Cluster unit = advertiser** (the right level of clustering; daily rates within an advertiser are autocorrelated). | [`tickets/ti_961_fangorn_causal_impact/artifacts/RolloutTierEvaluations.py`](../tickets/ti_961_fangorn_causal_impact/artifacts/RolloutTierEvaluations.py) — `did_inference()` + `_did_bootstrap()` |
| 4 | **CausalImpact with VIF→BIC covariate selection** | Per (tier, metric), fit statsmodels UCM on the pre-period: `level="local level"` + `freq_seasonal=[{"period": 7, "harmonics": 2}]` + selected exog. **Exogenous-only candidates** at the tier × day grain (4 candidates per metric): `control_rate`, `control_rate_lag1`, `control_scale`, `holiday`. **NO lags of treated `y` (target leakage — would bias counterfactual toward post-period actuals and shrink the estimated effect).** **NO `is_weekend` dummy — weekly periodicity handled by `freq_seasonal`.** VIF iteratively drops covariates with VIF ≥ 10. BIC best-subset search up to size 5. Forecast post-period, compare actual to predicted counterfactual. **Inference via simulation, not hand-rolled SE:** draw N=2000 paths from the fitted forecast distribution (carries correct cross-day covariance); 95% CrI = percentiles of the simulated effect distribution; p = two-sided tail probability the counterfactual beats the actual. Also report absolute effect (in pp) as headline because it doesn't blow up when counterfactual → 0. **Pre-period fit diagnostics (R², MAPE) on each tile — trust check.** | TI-961 same file — `run_ci_for_tier()`, `drop_high_vif()`, `best_subset_by_bic()` |
| 5 | **Standardized output + scheduled execution** | Write one row per `(experiment, tier, kpi, method, run_date)` with point/CI/p/n to a durable location (GCS today; long-term a `silver.experiments_eval.*` table). Schedule re-runs so results auto-refresh as post-period accrues. Powers a Mode dashboard reading from the same table. | Scheduled-run pattern shared with [TI-956](../tickets/ti_956_interest_segment_scoring_schedule/). Dashboard pattern: Alex's RolloutTierEvaluations notebook + Nick's Mode build (in flight) |

### Non-negotiables

- **Report SE / CI / p-value for EVERY point estimate, on BOTH methods.** Showing only "DiD lift = +27%" while CI says "p = 0.255" mis-frames the comparison — one tile is honest about uncertainty, the other isn't. Use the TI-961 cluster bootstrap to give DiD the same treatment as CI.
- **Control set is whatever has not yet been treated.** For tiered rollouts: future-tier advertisers from the inclusion table. For RCTs: the holdout group. Never use "never-flipped" advertisers as a substitute when a proper control exists — they introduce structural-difference confounds.
- **Visit rate is the headline KPI.** Conversions / CPA / ROAS are noisy at short post-periods (Alex catchup 2026-05-27); don't lead with them until n_post ≥ 28 days.
- **Daily granularity, not weekly.** Weekly gives too few post observations during early reads. Aggregate to daily; report cumulative effects across the post window.
- **Methods convergence is the strongest informal-causal argument.** When DiD and CausalImpact land on the same point estimate, that IS the evidence. When they disagree, the gap itself is diagnostic — investigate before reporting.

### When CausalImpact ≠ DiD — diagnostic checklist

If the methods give materially different point estimates:
1. **Control drifted in pre→post?** DiD subtracts the control's change directly; CI uses structural learning. If control coincidentally moved a lot, DiD over-corrects, CI under-corrects. Truth is in between.
2. **Pre-period covariates don't track treated tightly?** CI's local-level state absorbs the unexplained variance; DiD doesn't. Wider PI on CI is the symptom.
3. **Small N treated?** Both methods produce noisy point estimates. Cluster bootstrap surfaces this honestly via wide CI.
4. **Selected covariates per tier are unstable across re-runs?** BIC may be flipping between local minima. Run with `ci_pre_days = 30, 60, 90` and check stability.

### Lookback period heuristic

- **Rule:** pre-period ≥ 2-3× expected post-period length
- **Floors:** Kalman filter convergence needs 30+ pre days; BIC subset stability needs 60+ pre days; covariate variance estimation needs 7+ weekend cycles
- **Default:** 60 days for daily granularity, 12 weeks for weekly
- **Validate:** run `ci_pre_days = 30 / 60 / 90` as a robustness check; if `rel_effect` is stable across them, the lookback is appropriate

### p-value computation (both methods)

**DiD (cluster bootstrap):** `p = 2 × min( P(boot ≥ 0), P(boot ≤ 0) )` from the empirical distribution of N=1000 resamples.

**CausalImpact (simulation, posterior-predictive analogue):** draw N=2000 sample paths from the fitted UCM's forecast distribution via `res.simulate(nsimulations=n_post, anchor="end", repetitions=N, exog=X_post)`. For each path, compute the average counterfactual over the post-period — that yields a distribution `avg_cf_dist`. The effect is `actual − counterfactual` (actual is observed/fixed). 95% CrI = `np.percentile(actual - avg_cf_dist, [2.5, 97.5])`. `p = 2 × min(P(avg_cf ≥ actual), P(avg_cf ≤ actual))`.

**Why not normal-approximation SE?** *(updated 2026-06-03 after TI-961 methodology review)* The hand-rolled formula `SE = (avg_upper − avg_lower) / (2 × 1.96)` from per-day forecast bounds was wrong in three compounding ways: (a) it's the average per-day SD, not the SD of the post-period MEAN — missing the 1/n scaling and ignoring strong positive cross-day covariance of a local-level forecast; (b) `rel_CI = actual/bound − 1` has no distributional basis and explodes when the bound nears zero (literal source of TI-961's +681% upper bound on Tier 1 IVR); (c) Gaussian z-test layered on a skewed ratio compounds (a) and (b). Simulation carries the real covariance structure, can't explode, and makes no normality assumption.

### Future-state framework (not yet built)

This protocol will eventually live in a Python package `mntn_experiment_eval/` with config-driven runners. Until then, copy the canonical implementations referenced above into each new ticket's `artifacts/` folder. **As we build new experiments, capture any patterns that don't fit this protocol in a new subsection here — that's how we discover what the framework needs.**

---

## Experiment results archive (TI-1003 / TI-1033)

Every completed TI experiment is cataloged in the **TI experiment archive** — a manifest-driven internal static site that anyone can browse: a master "what TI has moved" view grouped by KPI (IVR, CVR, incrementality, …) plus one page per experiment (intention → big bold movement → every KPI moved → method → chart).

- **Source:** standalone repo `ti-experiment-archive` (build with `python build.py` → `dist/`). Hosting/deploy tracked in **TI-1033**.
- **Add a new experiment = drop one YAML** in `manifests/` with: `id`, `tone` (win=red / opportunity=blue / neutral=navy), `metric` (KPI group; `kpi_groups` for multi-KPI), an inline `chart` (bars/diverging — no image files), and `kpis` (every KPI moved, top 1–2 `highlight: true`). No template edits. **Do this whenever an experiment wraps** — it's the durable, shareable record.
- **Headline framing:** IVR is the proven KPI; CVR is the second target but noisier (firms up with post-period). Color encodes the result, not decoration — reserve red for genuine wins.

**Data caveat — TI-542 (Max Reach):** its results are **not recoverable** in the workspace. The notebook outputs were stripped and the only artifact, `ti_542_mullet_performance_report.pdf`, is a **placeholder/joke document** (literal mullet haircuts) — it contains zero Max Reach data. Do not cite Max Reach numbers or let any tool extract "results" from that PDF; the archive shows it honestly as "Mixed — no aggregate distilled." (An extraction agent fabricated plausible per-cluster numbers from it during TI-1003; caught by grepping the source.)

---

## Visualization rule for CausalImpact / pre-post results

**Charts: aggregate-only.** Per-advertiser detail belongs in tables, not in the visualizations. Notebooks should be flat and short.

**Why:** The Fangorn KPI notebook (TI-849 / TI-921) was flagged 2026-05-08 as "really really long and complex … wasn't clear what's going on" when handed off. Per-advertiser charts overwhelm the audience and obscure the headline. The headline is the aggregate effect; per-advertiser breakdowns are appendix-grade.

**Apply to:** all incrementality / lift / CausalImpact / pre-post deliverables — Fangorn, BUK, BER-2250 follow-ups, ad-hoc reads. One IVR chart, one CVR chart, one CPA chart — pooled. Per-advertiser rows live in a table next to it.

---

## Methodology Selection Guide

| Situation | Method | When to Use | When NOT to Use |
|---|---|---|---|
| Single intervention date, good covariates available | **CausalImpact (BSTS)** | Feature rollout affecting time series metrics | Very short pre-period (<20 weeks) |
| True RCT with control/treatment arms | **Direct comparison (t-test, bootstrap CI)** | Randomized A/B test with matched audiences (e.g., IP bucket split) | Groups not properly randomized |
| Treatment vs control group exists (not randomized) | **Difference-in-Differences (DiD)** | A/B test with comparable groups | Groups not parallel in pre-period |
| Staggered rollout, no control group | **Per-unit CausalImpact** | Feature adopted at different times by different units | Very small N (<5 units) |
| Need to test for lift on a specific metric | **t-test / Mann-Whitney** | Comparing two distributions | Time-dependent data (use time series methods) |
| RCT with skewed metrics or small N, need interpretable probabilistic results | **BEST (Bayesian t-test)** | CPA/ROAS comparison, small advertiser experiments, stakeholder-friendly reporting | Large-N rate metrics where z-test suffices |
| Multiple features changed simultaneously | **Regression with interaction terms** | Need to isolate each feature's effect | Multicollinearity between features |
| Staggered adoption, small N per unit, need one aggregate estimate | **Panel data model (two-way FE)** | Units adopt at different times, want a single population-level treatment effect | Want per-unit effects, or units have enough data for individual time series models |
| Want to understand feature importance | **SHAP / permutation importance** | Post-hoc explanation of what drove results | Not for causal claims |

### Deeper read — DiD vs CausalImpact selection guide

For the long-form "which one and why" with worked coffee-shop example, strength/weakness tables, and the canonical "use both" methods-convergence framing, see
[`documentation/docs/did_vs_causalimpact_method_selection.md`](../documentation/docs/did_vs_causalimpact_method_selection.md).
Shareable artifact — safe to send to anyone on the team or cross-functional
stakeholders asking "why are you using DiD here and CausalImpact there?"

### Experimental design — apply BEFORE the next major release

The Standard Analysis Protocol above describes HOW to analyze a rollout
once measurement is done. For HOW TO DESIGN the rollout itself — random
stratified assignment, permanent holdout, three cadence options matched
to operational constraints (5-week fast, 12-16-week standard, 7-month
conservative), pre-flight checklist, canonical references (Kohavi/Tang/Xu,
CUPED, geo experiments, modern DiD) — see
[`documentation/docs/feature_rollout_experimental_design.md`](../documentation/docs/feature_rollout_experimental_design.md).
**Apply this BEFORE the next major release rolls out.** The Fangorn TI-961
lesson: recovering a clean causal claim after a non-random rollout is
expensive (one clean tier out of four).

### Reviewing a third-party vendor's lift-test design (LiftLab, Haus, Measured…)

A third leg distinct from the two above: when a **vendor** (or a vendor's web
tool) proposes a lift-test design for a customer and we review it *before it
ships*. We don't own the method (geo holdout + synthetic control, or
switchback/time test are defensible — don't relitigate); **our job is to
confirm the design can detect the effect it claims, and that the inputs MNTN
controls won't pre-doom the result.** The failure to prevent: a clean-looking
test that returns "no detectable lift" because it was underpowered or aimed at
a high-intent audience — the customer reads that as "MNTN doesn't work," which
is worse than running no test.

**Canonical:** [`tickets/ber_2250_incrementality_overhaul/ti_1039_liftlab_design_review/artifacts/ti_1039_design_review_framework.md`](../tickets/ber_2250_incrementality_overhaul/ti_1039_liftlab_design_review/artifacts/ti_1039_design_review_framework.md) — 10-lever critique guide + fillable per-design G/Y/R scorecard. Reusable for any vendor design review (the 5-external-vendor OKR, the ~25-customer LiftLab beta pipeline).

**The 10 levers** (any RED = design needs a change before it ships): 1. estimand/method named · 2. **power/MDE** · 3. holdout % & assignment · 4. geo concentration · 5. duration/windows · 6. KPI breadth · 7. **audience strategy** · 8. confound hygiene · 9. reporting · 10. CX/churn risk.

**Three non-negotiables (the load-bearing levers):**
1. **Power first.** Get the pre-registered MDE + holdout per design. **Refuse MDE > ~15%** (iROAS playbook); below 5M impressions it's directional-only, no point estimate without a ±~50pp interval (Lewis-Rao).
2. **Audience strategy is the biggest swing, and it's ours to set.** High-intent/retargeting/previously-exposed audiences reliably underperform on incremental lift; broad prospecting wins (TI-835 "Two Stories" + Edgar's 50-test review). Flag a high-intent design as expected-weak *before* it runs.
3. **Protect the customer relationship.** ≥6-week test + 2-week post window, exclude the first ~4 weeks of new-campaign ramp (TI-780), no early readouts, frame a null as a retest input (Edgar Lessons 5-6). Also: vendors paid by the advertiser (LiftLab) skew conservative by construction — expect low numbers.

Established TI-1039 (2026-06-17). Cross-references Edgar's six lessons (below) and the iROAS playbook power section.

---

## Covariate Selection

### Principles

1. **Use covariates that predict the outcome but are NOT affected by the intervention.** If media plan affects network allocation, don't use network-level metrics as covariates (they're downstream of the treatment).

2. **More data points = more covariates allowed.** Rule of thumb: ≥10 pre-period observations per covariate. With 52 weekly observations, max ~5 covariates.

3. **Don't use the outcome's components as covariates.** If measuring IVR (vv/impressions), don't use VV as a covariate — it's the numerator.

4. **Test covariate significance.** Use forward/backward stepwise selection, AIC/BIC comparison, or cross-validation to determine which covariates actually improve prediction vs add noise.

### Formal Covariate Selection Methods

| Method | What It Does | When to Use |
|---|---|---|
| **Forward stepwise** | Start with no covariates, add one at a time, keep if significant | Quick screening |
| **Backward elimination** | Start with all covariates, remove one at a time | When you have candidate list |
| **AIC/BIC comparison** | Penalizes model complexity; lower = better | Comparing model variants |
| **Lasso regression** | L1 penalty automatically shrinks weak predictors to zero | Many candidate covariates |
| **Cross-validation** | Hold out pre-period data, test prediction accuracy | Gold standard for validation |
| **VIF (Variance Inflation Factor)** | Detects multicollinearity between covariates | Before running any model — drop if VIF > 10 |

### Covariate Candidates for MNTN Experiments

**Platform-level (market trends):**
- Platform-wide IVR/CVR/ROAS (from non-treatment advertisers)
- Platform total spend / impressions
- Platform active advertiser count
- Platform video completion rate (VCR)

**Seasonality:**
- Holiday binary flags (Thanksgiving/Black Friday, Christmas, New Year, Super Bowl)
- Day-of-week effects (if daily data)
- Month/quarter indicators

**Advertiser-specific:**
- Lagged metric (previous period's value — captures autocorrelation)
- Number of active campaigns (total)
- Number of active prospecting vs retargeting campaigns
- Total campaign groups active
- Budget level / budget changes
- Creative refresh frequency
- Audience segment changes

**Competitive / market:**
- Same-vertical competitor spend (if available)
- CTV market prices (CPM trends)

### Covariates Used in TI-748 (Media Plan)

| Covariate | Rationale | Concern |
|---|---|---|
| `platform_ivr` | Controls for market-wide engagement trends | None — good control |
| `platform_spend` | Seasonality / industry spending patterns | None |
| `platform_impressions` | Supply-side changes | None |
| `holiday` | Known seasonal spikes | Only covers major holidays |
| `platform_active_advertisers` | Competition proxy | Coarse measure |
| `platform_vcr` | CTV engagement not affected by network allocation | Good |
| `metric_lag1` | Autocorrelation | Loses first observation |

**Not yet tested (future improvement):**
- Advertiser's active campaign count (prospecting vs retargeting)
- Budget level changes around intervention
- Creative refresh timing
- Vertical-specific trends (instead of platform-wide)

---

## Statistical Test Selection for RCTs (TI-504 Lesson)

*Source: TI-504 analysis + Nick Martin (experiment owner), validated 2026-03-31*

### Which test for which metric?

| Metric Type | Test | Why | Example |
|---|---|---|---|
| **Rate metrics (IVR, CVR)** | Two-proportion z-test | Each impression is a Bernoulli trial (visit or not). Large N → normal approximation is valid. | IVR: z-test on (visits/impressions) control vs treatment |
| **Ratio metrics (CPA, ROAS)** | Nonparametric bootstrap (household-level, 5K+ resamples) | Ratio metrics have heavy skew — normal approximations break down. Bootstrap handles the distribution empirically. | CPA: resample households, compute CPA per resample, compare distributions |
| **Daily consistency check** | Welch's t-test on daily rates | Answers "did the effect hold day-to-day?" vs "was the aggregate different?" More conservative, smaller N. | Daily IVR: t-test with N≈21 days |

### Test comparison from TI-504 (same data, different tests):

| Test | Unit | N per group | Significant (of 20) | Notes |
|---|---|---|---|---|
| Proportion z-test | Impressions | 60K-170K | 15/20 | Matches Nick's results. High power detects small effects. |
| Chi-squared (2×2) | Impressions | 60K-170K | 15/20 | Mathematically identical to z-test (z² = χ²) |
| Welch's t-test | Daily IVR | ~21 | 4/20 | Conservative — requires effect to be consistent across days |
| Mann-Whitney U | Daily IVR ranks | ~21 | 4/20 | Non-parametric version of t-test |
| Bootstrap CI | Daily IVR | 10K resamples | — | Confidence interval, not p-value |
| CausalImpact (Bayesian) | Weekly IVR | 20-60 wks | Invalid | Requires population continuity (see gotcha below) |

### Key lessons:
- **Proportion z-test is the standard for IVR in RCTs** — use it as the primary test. Nick (experiment owner) confirmed this is the experimentation team's approach.
- **Bootstrap at the household level for CPA/ROAS** — not at the day level. Household-level preserves correlation structure (one household = multiple impressions). Use 5,000+ resamples.
- **t-test on daily rates is a useful secondary check** — if z-test is significant but t-test isn't, the effect may be driven by a few high-volume days rather than being consistent.
- **Chi-squared and z-test are interchangeable** for 2×2 contingency tables (z² = χ²). No need to run both.
- **Statistical significance ≠ practical significance** — with 170K impressions, even a -0.5% IVR difference is "significant" by z-test. Always report effect size alongside p-values.

### Bayesian Alternative: BEST (Bayesian Estimation Supersedes the t-test)

*Source: Kruschke (2013), J. Experimental Psychology: General 142(2), pp.573-603. Python package: `pip install best` (requires PyMC3).*

**What it is:** A Bayesian replacement for the t-test that models data with Student's t-distribution (not Gaussian), giving it built-in robustness to outliers. Instead of a binary significant/not-significant answer, it produces full posterior distributions for group means, standard deviations, effect size, and their differences.

**Key outputs:**
- **Posterior probability** of any hypothesis — e.g., "87% probability that treatment mean exceeds control by ≥0.5" — far more interpretable than p-values
- **Highest Posterior Density Interval (HDI)** — Bayesian credible interval (e.g., 95% HDI = the narrowest interval containing 95% of the posterior). Unlike frequentist CIs, this IS the probability that the parameter lies in the interval.
- **Effect size** — built-in: (μ₁ - μ₂) / √[(sd₁² + sd₂²) / 2]
- **Normality parameter (ν)** — controls outlier tolerance. ν < 10 = heavy tails (outlier-robust), ν > 30 ≈ normal distribution. Logarithmic scale because shape changes rapidly near ν=3 but stabilizes above 30.

**When to use at MNTN:**
- **Ratio metrics (CPA, ROAS)** where distributions are heavily skewed — BEST's t-distribution handles outliers that break normal-theory tests
- **Small N experiments** (5 advertisers) where p-values are underpowered but posterior probability and HDI still give useful information
- **Stakeholder communication** — "82% probability of improvement" is more intuitive than "p=0.07, not significant"
- As a **complement to (not replacement for)** the proportion z-test for rate metrics — z-test remains primary for IVR given the experimentation team's standard

**When NOT to use:**
- Rate metrics with large N (IVR with 170K impressions) — proportion z-test is standard and sufficient
- When the audience needs traditional frequentist reporting (some stakeholders expect p-values)

**Usage:**
```python
import best
result = best.analyze_two(control_data, treatment_data)
fig = best.plot_all(result)  # generates posterior plots for all parameters
result.posterior_prob('Difference of means', low=0)  # P(treatment > control)
result.hdi('Difference of means', 0.95)  # 95% credible interval
```

**Reporting note:** BEST displays the posterior **mean** for symmetric distributions (group means) and the **mode** for skewed distributions (standard deviations). Always report the HDI alongside either measure.

---

## Considerations & Gotchas

### Observational diagnosis: spend-saturation vs systemic-degradation (AUDI-1070)

**Trigger:** an advertiser (or PM) claims "performance/Matched is degrading over time" and points to a YoY visits/ROAS decline. Before accepting a *systemic* cause, run this 3-test decomposition — it cleanly separates **diminishing returns from spend scaling** (expected) from **a systemic targeting/model fault** (the alarming claim).

1. **Funnel waterfall (localize the lever).** `ROAS = (1000/CPM)·VisitRate·ConvRate·AOV`; YoY `Δln(ROAS)` decomposes additively/residual-free into the 4 `Δln` terms. Split aggregate ROAS into **within-stratum vs between-stratum (mix)** via Oaxaca-Blinder midpoint weights: `ΔROAS = Σ w̄ₖ ΔROASₖ (within) + Σ R̄ₖ Δwₖ (mix)`. Run at **campaign grain** to catch Simpson's. If the move is in CPM/AOV or the mix term → not targeting.
2. **Reach/frequency (expansion vs frequency-saturation).** `HLL_COUNT.MERGE(uniques)` for reach; frequency = imps/reach; visits/user = visits/reach. **Expansion** = reach grew at flat frequency with visits/user falling (incremental users lower-intent). **Frequency saturation** = same users hit more. AUDI-1070: HexClad +19% / Caraway +127% reach at flat freq, visits/user −38% / −68% = expansion.
3. **Cohort falsification (the decisive test).** Pull the full advertiser cohort active in both periods; compute per-advertiser YoY `VR_ratio` and `spend_growth`; **median `VR_ratio` by spend-growth decile**. If saturation: monotonic gradient (cut spend → VR rises ×1.5; grow 4× → VR falls ×0.9; `imp_growth`↔`VR_ratio` Spearman ≈ −0.47 across n=294). **Systemic degradation is FALSIFIED if flat-spend advertisers' VR did NOT fall** (AUDI-1070: flat-spend VR *rose* ×1.26). Then place the suspect AIDs as percentiles + a residual test (are they worse than peers at the *same* spend-growth level?). Canonical: `tickets/audi_1070_yoy_decline_caraway_avon_hexclad/artifacts/cohort_analysis.py`.

**The flat-spend control is the keystone:** a real "degradation over time" hits everyone regardless of spend; saturation only hits scalers. One flat-spend advertiser in the set (AUDI-1070's Avon: spend −14%, ROAS +16%) is worth more than any single-advertiser deep-dive. Also overlay platform dates (PP launch, Fangorn) on each advertiser's monthly VR — saturation tracks each advertiser's *own* spend ramp (idiosyncratic timing); a platform fault would be synchronized. **NOTE (AUDI-1070):** apparent "MaxReach-off" dates are usually per-advertiser HHST gate-FLOOR artifacts (gate set to a Mid floor like 3334 excludes MaxReach 1–3332, and MaxReach reappears the moment the gate reverts), NOT a synchronized platform event — verify against `household_score_threshold_archives` before treating any MaxReach-off date as global.

**The remedy is pacing, not "fix the model" (AUDI-1070 follow-up).** Once saturation is the diagnosis, the fix is to **ration high-intent (HI/PP) delivery across the flight** so spend doesn't exhaust the HI pool early and then crash into unscored/low-VR inventory. Concept: forecast days-to-HI-exhaustion (HI unique-reach × frequency cap ÷ daily impressions); if spend outruns HI supply, set the HI:MI/unscored ratio so HI is consumed at a constant rate over the whole flight → consistent performance instead of front-load-then-decline. Only matters for high-spend advertisers (where daily spend > sustainable HI burn). **Caveat 1:** this mostly *redistributes/smooths* performance (same finite HI pool served either way) — it's a **retention/consistency** play, not a net-ROAS lift; the one real efficiency gain is avoiding early over-frequency on HI IPs. **Caveat 2:** HI = demand-harvesting (low incrementality) — it stabilizes *reported* metrics, not incremental value. Depends on a bidder pacing-aware targeting control (overlaps the Permel pacing-controller roadmap).

**Fangorn helps only if it enlarges the HI pool — and only with headroom.** "Switch to Fangorn for a bump" needs the HI-**pool-size** gain measured per advertiser; the EX50 +36% PP IVR lift is a per-impression quality figure, not a pool-expansion number. Empirical caution (AUDI-1070): Caraway's existing small DS46/Fangorn prospecting campaigns ran at ~0.11–0.135% VR vs ~0.147% for its non-Fangorn prospecting — **no bump, because Caraway is over-scaled across the board.** Fangorn on an already-saturated advertiser still saturates; pacing/right-sizing is the primary lever, Fangorn-headroom is complementary.

### Campaign Maturity Bias (TI-780 — Empirically Determined)

New prospecting campaigns reach steady-state IVR in approximately **4 weeks** (N=6,917 campaigns, $10K+ spend):
- **Week 0:** 38% of steady state (just launched)
- **Week 2:** 84% (rapid improvement as bidder learns)
- **Week 4:** 89% — first week with <5% WoW change (ramp-up over)
- **Week 8+:** Fully stabilized

This pattern is consistent across spend tiers (high/mid/low) and is driven by bidder learning, frequency buildup, and delivery footprint exploration.

**Steady-state IVR varies by launch quarter** (0.008–0.013). Campaigns launched in different quarters converge to different baselines. Future analyses should consider cohort-specific baselines (by launch quarter) rather than assuming a single global steady-state value.

**Rule: Exclude the first 4 weeks of any new campaign from causal analysis.**

This applies to:
- CausalImpact post-period start (shift 4 weeks after first delivery)
- Within-advertiser comparisons (only include campaigns with 4+ weeks of delivery)
- Any future experiment comparing new vs existing campaigns

### Validating Production Holdout Enforcement Empirically (TI-837 Lesson)

When an experiment's identification depends on the production system enforcing a holdout (e.g., the 10% MD5(advertiser_id:ip) bucket for ghost-bidding lift), **don't trust documentation or asking — verify against served-IP data before publishing.**

**The check:** for every served IP in the treated arm, recompute the holdout-bucket assignment using the production hash. If holdout enforcement is real, **0% of served IPs land in the holdout bucket**. Any non-zero overlap means the bidder is leaking treatment into holdout, and your treated/holdout comparison is contaminated.

TI-837 result (2026-04-30): 0 of 5,432,546 served IPs across 8 (objective_id × funnel_level) cells landed in the holdout bucket. Holdout enforcement validated for both prospecting and retargeting.

**Adjacent check — audience-system coverage.** If the production system has multiple audience-evaluation paths, confirm the holdout enforces on all of them. TI-837: `audience.audience_segments` has both `expression_type=1` (OPM source representation) and `expression_type=2` (TPA, with embedded holdout JSON). Empirical: 0 of 64,202 type=1 retargeting rows have `is_targeted=TRUE` org-wide → only the type=2 path is ever live → there's no "OPM lane" that could bypass holdout. Always identify all evaluation paths and confirm `is_targeted` flagging before assuming uniform enforcement.

**Pattern, generalized:** before any production-experiment result depends on a system invariant (randomization integrity, holdout enforcement, eligibility gates), write the SQL that would falsify the invariant. If it returns the expected zero/ones, the result is defensible. If it doesn't, you've caught a methodology bug before the results meeting.

### Selection Bias
Advertisers who adopt a new feature may be systematically different:
- More engaged with the platform
- More sophisticated marketers
- Growing faster (or struggling — looking for improvements)
- Managed by specific account teams

**TI-748 confirmed:** Media Plan beta advertisers are hand-picked by PEX/CS (identified candidates with prior interest) and validated by production ops (Toph) for pacing risk. NOT randomized. This is the strongest form of selection bias short of self-selection.

**TI-961 confirmed (Fangorn Wave 3):** Tier 5 / Wave 3 holdout was selected by structural delivery-concern criteria (HHST low, audience shrink/grow risk, no impressions yet). NOT random. Per Confluence rollout plan, Wave 3 = "Hold for Manual Review" advertisers with score < 0.70 AND at least one blocking flag. Pool CVR ~6.5% vs treated tiers 2-4% reflects structural composition (vertical mix skews casual dining + home services + hospitality with multi-touch-attribution-heavy CVR). See [`reference_wave3_selection_bias`](../../.claude/projects/-Users-malachi-Developer-work-mntn-workspace/memory/reference_wave3_selection_bias.md).

Mitigation: Use pre-period trends as covariates, document the bias explicitly. For future rollouts, use waitlist control design with randomized wave ordering. Use **CausalImpact as primary inference** (builds counterfactual from treated tier's own pre-period structure, more robust to baseline composition differences than DiD).

### CUPED variance reduction requires randomization — don't bolt onto quasi-experiments
CUPED (Deng, Xu, Kohavi, Walker 2013) reduces variance on the post-period estimate using pre-period as a covariate. Variance reduction = Var(Y_adj) = Var(Y_post) × (1 − ρ²) where ρ = corr(pre, post). **This guarantee holds ONLY when E[pre_T − pre_C] = 0 in expectation** — i.e., when randomized assignment makes baselines equal in expectation.

**For non-randomized designs with deliberate baseline imbalance** (e.g., TI-961 Tier 2 random sample at IVR ~1.5% vs Tier 5 Wave-3 holdout at IVR ~1.0%):
- Standard CUPED estimator `τ̂_add = ȳ_adj,T − ȳ_adj,C` is biased — pre-period imbalance leaks into post-period comparison via θ
- Combining CUPED with DiD via `τ̂_add = (ȳ_adj,T − ȳ_adj,C) − (ȳ_pre,T − ȳ_pre,C)` double-counts the pre-period correction. Formula algebraically expands to `(post_T − post_C) − (1+θ)(pre_T − pre_C)`, multiplying baseline imbalance by `(1+θ)` instead of either 1 or θ alone
- **Verified synthetically (TI-961, 2026-06-10):** when treated and control have pre=post (no real time trend) but baselines differ, the buggy CUPED-DiD formula returns 33.8% spurious lift purely from the baseline imbalance flowing through the multiplier. Bootstrap CIs end up ~70% WIDER than raw additive DiD at ρ ≈ 0.75 instead of the theoretical √(1−ρ²) tightening.

**The three theoretically clean options for non-random designs:**

1. **Raw additive DiD under parallel-trends** — handles baseline imbalance but no CUPED variance reduction (this is the default; what TI-961 ships)
2. **CUPED on within-unit deltas** `Δ_i = post_i − pre_i` with a DIFFERENT covariate (pre-pre-period rate, advertiser size, vertical) — gets variance reduction with a covariate that isn't already used by the difference
3. **CUPED only on a properly-randomized sub-comparison** — apply within the random Tier 2, not across Tier 2 vs Tier 5

**Lewis-Rao 2015 stack components are upstream design choices, not retrofits:**
- CUPED × stratified randomization × ghost-ad = 0.595 σ ratio = 2.83× effective N (per `documentation/docs/feature_rollout_experimental_design.md`)
- None of the three components retrofit cleanly onto a non-randomized rollout
- For the NEXT major TI release, design CUPED + stratification + permanent randomized holdout in from the start

Discovered 2026-06-10 via TI-961 multi-agent verification workflow (8 agents, 651k tokens). The first CUPED implementation produced Tier 2 IVR swings from raw +11.3% → CUPED −60.2% — adversarial verifiers caught both the formula bug AND the deeper design mismatch before shipping. See [`feedback_cuped_needs_randomization`](../../.claude/projects/-Users-malachi-Developer-work-mntn-workspace/memory/feedback_cuped_needs_randomization.md).

### Leave-one-out control composition diagnostic
For any cluster-bootstrap comparison where the control group is non-random (Wave 3-style holdout, future-flip cohort, hand-picked control), run a leave-one-out diagnostic on the control pool to identify advertisers with disproportionate leverage on the pooled rate. This surfaces the structural-composition advertisers driving the comparison.

**Pattern:**
```python
# For each control advertiser, recompute pooled rate excluding that advertiser
total_num, total_den = control_pool["num"].sum(), control_pool["den"].sum()
pool_rate = total_num / total_den

control_pool["loo_rate"] = (total_num - control_pool["num"]) / (total_den - control_pool["den"])
control_pool["delta_pp"] = (control_pool["loo_rate"] - pool_rate) * 100  # in percentage points

# Sort by |delta_pp| descending — top advertisers are the leverage outliers
```

**What to look for:**
- `|delta_pp| > 0.5pp` on a 1000-advertiser pool with rates ~5% = significant single-advertiser leverage
- Persistent outliers in the same verticals (casual dining + home services + hospitality for CVR; CTV-heavy verticals for IVR) suggest vertical-mix bias rather than individual advertiser outliers
- Both directions matter: advertisers pulling pool UP (high adv_rate × high visit_share) make treated comparisons look worse; advertisers pulling DOWN make treated comparisons look better

**Canonical implementation:** [TI-961 control-composition diagnostic cell](../tickets/ti_961_fangorn_causal_impact/artifacts/RolloutTierEvaluations.py) — the `composition_diag_tier` widget renders top-20 by IVR delta and top-20 by CVR delta with HTML formatting.

**When to use:** any DiD/CI analysis where the control is non-random (Wave 3-style selection-biased holdouts, future-flip cohorts, never-flipped pools). Always run before reporting tier-level effects so you know which specific advertisers' structural characteristics are driving (or distorting) the comparison.

Discovered 2026-06-10 via TI-961 — diagnostic identified Angi (32766, adv_cvr 207%), Cheddar's (34834, adv_cvr 27%), Mountain Mike's Pizza (31297, 63%), Station Casinos (59584, 108%), SpotHero (35872, 48%), Goldfish Swim School (45921, 30%) as Tier 5 CVR-leverage advertisers. Removing Angi alone would drop Tier 5 pool CVR by ~0.57pp → would make treated DiD CVR comparisons look meaningfully better. Confirmed Wave 3 selection bias structurally, didn't change the methodology recommendation (use CausalImpact as primary inference).

### Staggered Adoption
When units adopt at different times:
- Run per-unit CausalImpact (not pooled)
- Aggregate results with spend-weighted averages
- Check if early adopters differ from late adopters

### Holiday / Seasonality Effects
CTV advertising has massive seasonality:
- Q4 (Oct-Dec): highest spend, Black Friday/Christmas
- January: post-holiday drop
- Super Bowl week: CTV spike
- Summer: typically lower
Always include holiday flags. Consider using 52-week pre-period to capture full cycle.

### Prospecting vs Retargeting
These behave VERY differently. Always analyze separately:
- Prospecting: new audience, lower conversion rates, more volume
- Retargeting: known audience, higher conversion rates, smaller pool
- Mixing them in one analysis will confound results

### Algorithm Version as Confound (TI-748 Lesson)
When analyzing a feature's impact over time, **check for algorithm/config changes during the observation window.** In TI-748, a config change on Feb 3, 2026 (`max_networks` 18→25→15, `min_allocation` 1%→0.5%, spend capacity filter added — PERML-412) split our 8 advertisers into two groups — pre-change plans hurt IVR, post-change plans helped.

**Refined understanding (Chris Addy, 2026-03-31):** The initial framing was "config version predicts who benefits." The corrected framing is **"concentration is the mechanism, and the config change made concentration the default."** Lighting New York proves this: they got 16 publishers under the *old* config (Oct 2025) — not from an override, but because natural pruning from their budget/vertical dropped networks below the old 1% `min_allocation` floor. Same concentration, same positive result, different config era. The mechanism is publisher concentration itself, regardless of how it was achieved.

**Rule:** Before attributing results to a feature, check:
1. Were there algorithm updates during the treatment period? (git log the relevant service repo)
2. Did all treated units receive the same version? (query plan metadata for config snapshots)
3. If versions differ, split the analysis by version era.
4. **If the mechanism is identified, test whether it operates independently of version** — as with Lighting NY proving concentration works under any config.

This is especially important for staggered adoption designs where early adopters may be on a different algorithm version than later adopters.

### Runtime Config as Experiment Prerequisite (TI-748 Lesson)
Per-advertiser config overrides are NOT natively supported for the media plan algorithm today. The only advertiser-level control is blacklisted networks. To run a clean A/B test on algorithm parameters (e.g., alpha=7 vs alpha=5), the `MediaPlanConfig` must be made runtime-updateable (Chris Addy is planning this). Without it, the only option is a time-based rollout (change the config globally), which confounds treatment with time.

**Lesson for experiment design:** Before proposing a parameter A/B test, verify:
1. Can the parameter be varied per-unit (per-advertiser, per-campaign)? If not, time-based rollout is the only option.
2. Is there an API-level override available, or does it require a deploy?
3. Who owns the config and can trigger changes? (For media plan: Chris Addy / olympus team)

### Population Continuity — When Synthetic Control Fails (TI-504 Lesson)

**Synthetic control / CausalImpact requires that the pre-period and post-period measure the SAME population.** If the experiment creates new campaigns targeting a different audience subset, you cannot use the parent campaign's history as a pre-period baseline.

**TI-504 example (Fangorn AIS experiment):**
- Nick cloned 5 advertisers' prospecting campaigns into control/treatment arms
- Each arm targeted a specific IP bucket range (e.g., 600-713 for control PP, 714-768 for treatment PP)
- The parent campaigns targeted the FULL audience (buckets 0-599)
- Parent campaign IVR: 0.03-0.12. Experiment campaign IVR: 0.005-0.024. That's a 3-10x gap.
- This gap is NOT a treatment effect — it's a different population (different IP buckets = different people)
- CausalImpact showed -37% to -79% effects with 69% avg placebo FPR. The model was measuring the population difference, not Fangorn's impact.

**When to use CausalImpact vs direct RCT comparison:**

| Scenario | Right Method | Why |
|---|---|---|
| Feature enabled on existing campaigns (same audience, same targeting) | CausalImpact with pre/post | Same entity, same population — pre-period baseline is valid |
| New campaigns created with split audiences (A/B test) | Direct RCT comparison (t-test, bootstrap CI) | Different populations — no valid pre-period exists for the experiment campaigns |
| Staggered rollout to existing campaigns | Per-unit CausalImpact | Same entity across time, different intervention dates |
| Cloned campaigns with holdout groups | Direct comparison of control vs treatment arms | The control group IS the counterfactual — no need to synthesize one |

**The rule:** If you have a real control group, use it directly. CausalImpact is a tool for when you DON'T have a control group and need to build a synthetic one. Using synthetic control when a real control exists is both unnecessary and prone to error.

**How to check:** Before running CausalImpact, verify:
1. Is the pre-period entity the same as the post-period entity? (Same campaign_id, same audience definition, same targeting)
2. If new campaigns were created for the experiment, do they target the same population as the baseline campaigns?
3. If audiences were split (IP hashing, holdout groups), the pre-period campaigns targeted the full audience — you CANNOT use their IVR as a baseline for the subset.

**Self-referencing covariate trap:** In TI-504, setting `control_ivr = response_variable` during the pre-period (because the control arm didn't exist yet) produced artificially perfect pre-period fit. The model looked great but was cheating — the "covariate" was the answer. When removed, the model collapsed to -70% effects with massive uncertainty. Always verify covariates are genuinely external to the response.

**Why TI-748 worked but TI-504 didn't — what's actually different:**
In TI-748, new media plan campaigns REPLACED the old campaigns and targeted the **same full audience**. The pre/post IVR levels were comparable (same order of magnitude). In TI-504, experiment campaigns ran at **8-40% of the advertiser's normal prospecting IVR** (tested with both parent-only AND all-prospecting baselines — same result). The gap is far too large for random IP bucketing to explain. Contributing factors: audience splitting (buckets 600-999 = ~40% of original audience), creative removal ("Only One Creative" for most experiment campaigns), lower budgets (experiment budget vs advertiser's full spend), and fresh campaigns with no optimization history. When pre/post IVR differs by 3-10x, no covariate can bridge the gap — the model just measures the structural difference, not the treatment.

**Note:** `is_test = true` is just a reporting flag — it does NOT affect delivery priority or bidder behavior. The IVR gap is from the experiment setup factors above, not the flag itself. However, `is_test = true` does exclude campaigns from all summary/aggregate tables, so you must query log-level tables (cost_impression_log, clickpass_log) directly.

**HI-tier segmented analysis (follow-up):** To rule out audience composition as the cause of the IVR gap, segmented by intent tier using `advertiser_household_score` (HHST >= 6666 = HI tier). Old prospecting campaigns were 89-99% HI tier traffic. Even within HI tier only, experiment campaigns ran at 0.08-0.53x of historical HI-tier IVR (historical: 0.025-0.117, experiment: 0.008-0.034). This confirms the gap is from experiment setup factors (audience split, creative, budget, maturity), not audience composition differences.

- **Experiment campaigns have structurally different IVR than production campaigns** — due to audience splitting, creative limitations, budget constraints, and campaign maturity. Cannot use production campaign baselines for experiment campaign CausalImpact.

**Lesson:** Before running CausalImpact, check the **IVR ratio** between pre-period and post-period. If it's less than ~0.5x or more than ~2x, the structural gap is too large for synthetic control — something fundamental changed beyond the treatment.

### Metric Source Selection — guid_log vs clickpass_log (TI-835 Lesson)

**guid_log and clickpass_log answer fundamentally different incrementality questions.** Choosing the wrong one will lead to correct math but wrong conclusions.

| Source | What It Captures | Incrementality Question It Answers | TI-835 Result |
|---|---|---|---|
| `guid_log` | All pixel visits (direct, organic, paid, VV — everything) | "Does MNTN targeting increase total site traffic?" | ~0% lift — holdout visits at same rate as targeted |
| `clickpass_log` | VV-attributed visits only (user clicked through from MNTN ad) | "Does MNTN targeting increase MNTN-attributed visits?" | 2-8x lift — targeted group has massively more VV redirects |

**Why guid_log shows no lift:** Users visit advertiser sites from Google, direct, email, etc. regardless of whether they saw an MNTN ad. The 10% holdout visits the site at the same rate as the 90% targeted group because most visits come from non-MNTN channels. The MNTN-driven visits are a small fraction of total guid_log traffic.

**Why clickpass_log shows massive lift:** VV redirects only happen when a user engages with an MNTN ad. Holdout IPs never receive ads, so they generate almost zero clickpass records. The signal is enormous (2-8x) because the mechanism being measured (ad exposure → click-through) is directly blocked for the holdout.

**Rule:** Before running any incrementality analysis, explicitly state:
1. What metric/table defines a "visit" or "conversion"
2. Whether you're measuring total traffic incrementality (guid_log) or attribution incrementality (clickpass_log)
3. What the expected result would be under the null hypothesis for that specific table

**Statistical approach that worked (TI-835):**
- **Primary test:** Binomial test — H0: proportion of visits from targeted IPs = 0.9 (matching the 90/10 split). Rejection means the targeted group visits at a disproportionate rate.
- **Effect size:** Bootstrap confidence intervals (10K resamples) on the targeted-to-holdout visit rate ratio.
- **Multiple testing:** Benjamini-Hochberg FDR correction across all advertisers (controls false discovery rate).
- **Sample:** 9 advertisers, 30-day window was sufficient for both guid_log and clickpass_log analyses.

### Data Quality Issues Found (TI-748)
- Weeks with <1,000 impressions can produce absurd rate metrics (IVR=366x) due to VV attribution lag after campaign pauses. Always filter.
- `uniques` column in agg tables is unreliable at campaign level.
- Some advertisers have data gaps (paused campaigns). Check for >20% missing weeks.

### Placebo Test Interpretation
- With short pre-periods (< 20 weeks), placebo tests split the data too thin → high false positive rate
- A failed placebo test doesn't invalidate results — it means the pre-period has structural breaks (natural variability)
- With 52-week pre-periods, aim for <20% placebo false positive rate
- **Run multiple placebos** (5+ per unit) at different split points — single placebo is unreliable

### Covariate Selection Lessons (from TI-748 validation)

**Key finding: advertiser-specific dynamics beat platform-wide metrics.**

| Covariate | Appeared in N/6 models | Lesson |
|---|---|---|
| `spend_change_pct` | 6/6 | Week-over-week budget changes are the strongest predictor. Always include. |
| `metric_lag1` or `metric_lag2` | 5/6 | Autocorrelation is real and important. Lagged metric almost always improves the model. **⚠️ NEVER use lags of treated `y` in a CausalImpact / synthetic-control counterfactual setup** — post-period lags contain the treatment effect, leak it into the counterfactual, and bias the estimated effect toward zero. Safe in TI-748's forecasting context (no counterfactual framing); use the UCM's local-level state for temporal correlation in CausalImpact instead. *(caveat added 2026-06-03 after TI-961 methodology review)* |
| `holiday` | 4/6 | Important for Nov-Jan adopters. Less important for others. |
| `adv_active_cgs` | 3/6 | Number of active campaign groups matters for some advertisers. |
| `platform_ivr` | 0/6 | Survived VIF in no models — too collinear with other platform metrics. |
| `platform_spend` | 0/6 | Also removed by VIF. |

**Implication:** Don't hand-pick covariates. Run BIC selection per unit. Platform-wide metrics are mostly collinear and get eliminated. Advertiser-specific dynamics (budget changes, momentum) are far more predictive.

**VIF thresholds:** Start with all candidates, iteratively remove VIF > 10. Typical 14 candidates → 3-7 survivors.

**BIC vs AIC:** BIC penalizes complexity more than AIC. For small N (weekly time series with 50-100 obs), BIC's stronger penalty produces more parsimonious models that generalize better. Prefer BIC.

**Effect of covariate optimization on TI-748:**
- Placebo FPR: 86% → 30% (from 7 hand-picked to BIC-selected per advertiser)
- IVR spend-weighted: -10.56% → +6.50% (FICO went from -14.79% to -0.18% — hand-picked covariates were distorting its prediction)

### Covariates That Were Tested But Not Selected

| Candidate | Why It Was Rejected | Lesson |
|---|---|---|
| `platform_ivr` | Eliminated by VIF (collinear with other platform metrics) | Platform rate metrics are all measuring "market is up/down" — redundant |
| `platform_spend` | Eliminated by VIF | Same as above |
| `platform_vcr` | Highest VIF (3,191) — first to be removed | VCR, IVR, CVR at platform level are all proxies for "market engagement" |
| `platform_impressions` | Eliminated by VIF | Collinear with platform_spend |
| `platform_active_advertisers` | Eliminated by VIF in most advertisers | Correlated with platform_spend |
| `ctv_share` (CTV/display mix) | 3/6 advertisers are 100% CTV (no variance). For the 3 with variance, BIC didn't select it. | Only useful for mixed CTV/display advertisers, and even then not predictive enough |
| `retargeting_cg_count` | Near-zero weekly variance for all advertisers | Static values don't help time series prediction |

### Why Raw Spend Isn't a Direct Covariate

Raw spend (`platform_spend`, `adv_spend`) was eliminated by VIF or not selected by BIC. BUT `spend_change_pct` (week-over-week % change) was selected for ALL 6 advertisers. This makes sense:

- **Raw spend** is collinear with impressions, conversions, and everything else (more spend → more of everything). Including it as a covariate would "explain away" the volume effects we're trying to measure.
- **Spend CHANGE** captures *budget shifts* — "did the advertiser increase/decrease their budget this week?" This is a genuine confound (budget changes affect metrics regardless of media plan) without being mechanically correlated with the outcome.

This is a general principle: **use rate-of-change covariates over level covariates** when the level is mechanically related to the outcome.

---

## Key Findings from MNTN Experiments (Institutional Knowledge)

*Source: Kirsa (Experimentation Lead), meeting 2026-03-30*

### Frequency — Most Counterintuitive and Consistent Result

Lower frequency = better performance. This has been validated across multiple experiments over several years:

- Capping frequency at very low levels (1 per 14 days, 1 per 30 days) achieved comparable performance to multi-touch campaigns
- The more unique households reached (lower frequency), the better performance is overall
- **Why "ideal frequency" from converter data doesn't work:** Data analysts identified the average frequency of converters and applied it to campaigns — performance got WORSE. Reason: that frequency is optimal for getting an individual household to convert, but all the non-converters also get that same frequency. The wasted impressions/spend on people who never would have converted has more negative impact than the incremental conversions from borderline prospects.
- **Principle:** Reach (unique households) matters more than repetition. Optimize for breadth, not depth.
- Frequency experiments haven't been run in a while — should be re-run every couple of years because platform changes may shift optimal frequency.

### Targeting — Most Impactful Lever (Not Even Close)

Every targeting-based experiment has been the most impactful lever for performance improvement:

- **Mountain Match V1 vs Interest Audiences:** ~500% performance improvement — "insane" by experiment standards where 10-15% is considered a win
- **Fangorn (bottoms-up keywords):** 50-100% peak performance improvements
- **Frequency/other optimizations:** Much more likely to have a very slight change
- Targeting improvements dwarf all other optimization levers

### Experiment Shelf Life — ~6 Months

Experiments have roughly a 6-month expiration date because the platform, feature set, and environment change so often. What was true 6 months ago may no longer hold because the "starting line" has moved.

**Example:** Multi-touch was disabled for new customers after experiments showed TV-only with low frequency performed comparably. But the control baseline was a 2021 multi-touch campaign. By 2024-2025, the platform had changed enough that TV-only no longer outperformed multi-touch against current baselines → multi-touch is being encouraged again.

### Performance vs Scale Tension

The most important tension in experimentation: **it's very easy to improve performance, but not so easy to do it without negatively impacting ability to spend.** Tighter targeting, lower frequency, better audience quality all improve performance metrics — but they shrink the addressable audience, reducing ability to deliver impressions and spend budget.

This is especially acute for retargeting, where audiences are already small. TV-only retargeting showed comparable performance but couldn't deliver at scale. The fix would require a massive increase in scale (more inventory, more targetable IPs, etc.) to offset the audience reduction.

### Metrics Philosophy (from Mark/Leadership)

- **IVR (impression-to-visit rate) is the primary metric** — Mark's rationale: MNTN can only drive visits; conversions depend on the advertiser's site experience. So the metric MNTN should optimize is visit generation.
- **CPA and ROAS are guardrail/supporting metrics** — always reviewed alongside IVR. If IVR improves but CPA gets worse or ROAS declines, that's flagged and caveated.
- **Visit quality matters** — not just visit volume. Visit conversion rate (conversions/visits) and effective conversion rate (conversions/impressions) are also tracked.
- **All metrics are reviewed for every experiment** — the more information the better. Looking for potential negative impacts beyond the primary metric.

### Experiment Budget & Structure

**Two types of experiments:**

| Type | Description | Who Pays | Budget Split |
|---|---|---|---|
| **Customer-facing** | Advertiser agrees to participate, visible in their campaign/reporting | MNTN typically covers 50% to incentivize participation | Shared |
| **Non-customer-facing (hidden)** | Behind-the-scenes experiments (e.g., Fangorn). Hidden campaigns, not visible to advertiser in UI/reporting | MNTN covers 100% | From Kirsa's monthly experiment budget |

**Current constraints:**
- 5 advertisers per experiment (limited by budget and manual setup)
- Budget is a set monthly amount

### Campaign Splits — Key Future Improvement

The ability to split out a portion of a live campaign and experiment with it (e.g., 10% of a campaign's audience gets a different frequency):

- **Eliminates budget constraint** — uses the advertiser's existing budget, no additional MNTN spend
- **Scales to 50+ advertisers per experiment** instead of 5
- **Enables much larger sample sizes** → more definitive causal impact results
- This is a near-term initiative Kirsa and team are planning

### Methodology Improvement (Needed Once Campaign Splits Land)

Once the 5-advertiser constraint is removed, the team needs:
- Power analyses for sample sizing
- Formal methodology for advertiser selection to avoid bias
- Documentation of selection criteria and randomization
- More rigorous statistical approaches that are feasible with larger N

---

## MNTN-Specific Experimental Design Notes

### How to Filter Prospecting Campaigns
- Use `campaigns.funnel_level = 1` (NOT objective_id, which is unreliable)
- Always filter `deleted = FALSE AND is_test = FALSE`
- `funnel_level` is on the `campaigns` table, NOT `campaign_groups`

### Attribution Models
- Check `bronze.integrationprod.r2_advertiser_settings.reporting_style`
- `industry_standard` = include competing views/conversions
- `last_touch` = exclude competing views
- All media plan adopters are industry_standard (as of TI-748)

### Best Data Source for Historical KPIs
- `summarydata.sum_by_campaign_by_day` — goes back to 2024-01-01, has all needed columns
- `aggregates.agg__daily_sum_by_campaign` — only from 2025-09-01 (too short for 52-week pre-period)
- `summarydata.sum_by_campaign_group_by_day` — same range as campaign level, useful for CG-level analysis

### Identifying Feature Usage
- `core.media_plan` — `media_plan_status_id = 3` for active plans
- `core.media_plan_publishers` — `badge_state` tracks RECOMMENDED vs USER_MODIFIED vs USER_ADDED
- Join on `media_plan_id` to determine per-plan recommendation status

### Panel Data Model vs Per-Unit CausalImpact

Two approaches for staggered adoption experiments:

| | Panel Data Model (Two-Way FE) | Per-Unit CausalImpact |
|---|---|---|
| **Use when** | Staggered adoption, small N per unit, need one aggregate treatment estimate | Want per-unit treatment effects, have enough data per unit (20+ pre-period obs) |
| **Output** | Single population-level ATT with one p-value | Per-unit effect sizes, can see heterogeneity |
| **Strengths** | Pools data across units for power; handles short per-unit series; one clean estimate | Transparent per-unit results; can identify which units drove effects; individual placebos |
| **Weaknesses** | Loses per-unit granularity; assumes homogeneous treatment effect; harder to diagnose issues | Requires sufficient pre-period per unit; aggregation choices (spend-weighted vs median) matter |
| **Covariates** | Time FE absorbs common shocks; unit FE absorbs level differences; add unit-varying covariates | Per-unit BIC selection from candidate set |

**Lesson from TI-748:** Panel model (v5) gave +2.06% not significant. Per-unit CausalImpact (v3) showed 3/6 significant with +6.5% spend-weighted. The panel model's homogeneity assumption may wash out real heterogeneous effects. When treatment effects vary across units, per-unit analysis can be more informative despite lower power per unit.

---

## Jira Practices

When creating Jira tickets, always include:
- **Story points** (`customfield_10012`) — required for sprint planning
- **PMO Rep** — assign the appropriate PMO representative
- **Release type** — specify the release type for the ticket

These fields are frequently missed but are required by PMO for sprint tracking and release management.

---

## Experimental Design for Feature Rollouts — Balancing Risk and Statistical Power

### The Core Tension
We need high N for reliable results, but can't roll out risky changes to all advertisers at once. Methods to balance this:

### 1. Staggered Rollout (What We Did for Media Plan)
- Roll out to a small beta group first, expand over time
- **Pros:** Low risk — can stop if early results are bad
- **Cons:** Small N reduces statistical power. Selection bias — early adopters are volunteers who may differ systematically. Staggered dates complicate analysis.
- **When to use:** Opt-in features, low-risk changes, exploratory analysis
- **TI-748 lesson:** N=8 was too small for conclusive results. The analysis was methodologically sound but statistically underpowered.

### 2. Randomized Controlled Trial (Gold Standard)
- Randomly assign advertisers to treatment (feature on) vs control (feature off)
- **Pros:** Eliminates selection bias. Clean comparison. Highest statistical credibility.
- **Cons:** Requires engineering support to randomize. Some advertisers may notice and complain. Can't "unsee" the results if negative.
- **When to use:** High-stakes features, need definitive answer, have engineering support
- **How to size it:** Standard power analysis. For detecting a 5% IVR lift with 80% power and 5% significance: need ~200 advertisers per group (rough estimate — depends on IVR variance).

### 3. Matched Pairs Design
- For each treatment advertiser, find a similar control advertiser (same vertical, spend tier, campaign count)
- **Pros:** Controls for selection bias without full randomization. Smaller N needed than population-level RCT.
- **Cons:** Matching quality depends on observable characteristics — unobservable differences remain. Hard to find good matches with small advertiser pool.
- **When to use:** Can't randomize, but can identify comparable non-adopters

### 4. Waitlist Control
- All advertisers get the feature eventually, but rollout is randomized in waves
- **Pros:** Everyone gets the feature (no ethical concerns). Early waves act as treatment, later waves act as control. Staggered adoption by design.
- **Cons:** Requires coordinated rollout schedule. Late-wave advertisers may learn about the feature from early adopters (contamination).
- **When to use:** Features that will eventually go to everyone. Best balance of risk and power.
- **How it works:** Wave 1 gets it in week 1, Wave 2 in week 5, Wave 3 in week 9. At week 4, compare Wave 1 (treated) to Waves 2+3 (not yet treated).

### 5. Synthetic Control (What CausalImpact Does)
- Each treated unit builds its own counterfactual from covariates
- **Pros:** No explicit control group needed. Works with staggered adoption. Per-unit effects.
- **Cons:** Relies on covariate quality. Short pre-periods reduce reliability. Placebo FPR can be high.
- **When to use:** Observational data, can't randomize, need per-unit effects
- **CRITICAL REQUIREMENT:** The pre-period and post-period must measure the **same entity** (same campaign, same audience, same targeting). If the experiment creates new campaigns with different audience definitions, the pre/post comparison is invalid — you're comparing different populations, not measuring a treatment effect. See "Population Continuity" gotcha below.

### Recommendation for MNTN
For future feature evaluations, **waitlist control** is the ideal approach:
1. Product decides the feature will eventually go to all eligible advertisers
2. Randomly order the rollout (not by request, not by account team preference)
3. First wave is treatment, remaining waves are control
4. After sufficient post-period (4 weeks + ramp-up), analyze treatment vs not-yet-treated
5. Expand to next wave, repeat

This gives us: randomization (no selection bias), adequate N (entire eligible population), ethical soundness (everyone gets it), and clean analysis (DiD with randomized treatment timing).

### Publisher-Level Analysis Lesson (TI-748)
When analyzing features that affect publisher/network allocation:
- `sum_by_ctv_network_by_day` has per-publisher, per-campaign, per-day performance data
- Publisher IVR varies dramatically (Spectrum News 1.09% vs Samsung TV+ 0.48% for Lighting New York)
- High-IVR publishers are often low-volume — the algorithm optimizes for deliverability/reach, not IVR
- **The benefit comes from CONCENTRATION (removing the long tail of poor performers) rather than SELECTION (picking the best publishers)**
- Lighting NY confirms: natural pruning from budget/vertical constraints produced the same 16-publisher concentration and the same positive result as the explicit config change did for later adopters
- Per-publisher scores (score_semantic, score_performance, score_spendability, etc.) are NOT in BQ — stored as JSON in GCS (`media-plan-artifacts` bucket, path `media-plan/{version}/{advertiser_id}/{plan_id}/response.json`). Would need BQ sink or GCS parsing for score-level analysis.
- This distinction matters for product team: the algorithm doesn't need to pick the *best* publishers — it needs to prune the *worst* ones. HHI as a guardrail input would operationalize this.

---

## Experiment Log

| Ticket | Experiment | Method | Outcome | Key Learning |
|---|---|---|---|---|
| TI-748 | Media Plan Causal Impact (v5) | Per-advertiser CausalImpact (BIC covariates, ramp-up exclusion) + panel model | Aggregate IVR near zero (-0.23% spend-weighted). BUT: config change on Feb 3, 2026 (max_networks 18→25→15, PERML-412) explains the split — new-config plans show +10-17% IVR lift, old-config plans show -26 to -31% decline. | **Primary lesson:** Concentration is the mechanism, config change made it the default. Lighting NY (16 pubs under old config from natural budget/vertical pruning) proves concentration works regardless of config era. Also: BIC covariate selection beats hand-picking; 4-week ramp-up exclusion needed for new campaigns; selection bias confirmed (hand-picked beta); per-publisher scores only in GCS (not BQ); runtime config needed for clean A/B tests. |
| TI-780 | Ramp-up window research | Empirical analysis of campaign maturity curves | 4-week ramp-up window identified (N=6,917 campaigns, $10K+ spend). Week 4 = first week with <5% WoW change. | Consistent across spend tiers. Steady-state IVR varies by launch quarter (0.008–0.013) — future analyses should use cohort-specific baselines rather than a single global baseline. |
| TI-504 | Fangorn AIS experiment (RCT) | Direct RCT (proportion z-test, Welch t-test, chi², Mann-Whitney, bootstrap CI) + CausalImpact validation + HI-tier segmentation | Edward Martin PP +123%, MI_PP +70%, Collector Store PP +60%, MI +43% — significant across ALL tests. G-Shock/Reedsy/Zumba no effect by t-test; z-test detects small effects at scale. CausalImpact invalid due to population discontinuity (audience split, creative removal, budget, maturity — NOT `is_test` flag which is just a reporting flag). | **Primary lesson:** (1) Synthetic control fails when experiment creates new campaigns with split audiences — use direct RCT comparison instead. (2) Proportion z-test is the standard for IVR in RCTs (confirmed by Nick Martin, experiment owner). (3) t-test on daily rates is a useful secondary consistency check — if z-test is significant but t-test isn't, effect may not be day-over-day reliable. (4) Bootstrap at household level for ratio metrics (CPA/ROAS). (5) Self-referencing covariates produce artificially perfect fits. (6) `is_test = true` excludes from summary tables but does NOT affect delivery. |
| TI-835 | Control group incrementality (holdout 10% analysis) | Binomial test (H0: targeted proportion = 0.9), bootstrap CIs, BH FDR correction across advertisers | guid_log: ~0% lift across all 9 advertisers (holdout visits at same rate as targeted). clickpass_log: 2-8x incremental lift across all 10 advertisers (targeted group has massively more VV-attributed visits). | **Primary lesson:** guid_log vs clickpass_log answer fundamentally different questions. guid_log = total site traffic (pixel fires from all sources) — shows no targeting effect because users visit from Google/direct/etc regardless. clickpass_log = MNTN-attributed visits (VV redirects) — shows massive lift because holdout IPs never get ads, so they never click through. "Incremental" means different things depending on which table you use. Must define success metric before running any incrementality experiment. Statistical approach: binomial test + bootstrap CIs + BH FDR correction worked well for 9-advertiser, 30-day window. |
| BUK Exp 1 (Sep 2025) | BUK vs MM V2 — fixed 200 keywords | A/B with audience isolation blocking (old method, had IP contamination) | Control outperformed across ALL 5 advertisers (IVR 1.53% vs 0.67%, CPV $5.07 vs $9.08). 200 keywords pushed vertical coverage to 80-88%. | **Primary lesson:** More keywords ≠ better. Fixed keyword count is wrong approach — need a threshold. Within treatment, IPs overlapping with control keywords performed 5-10x better than non-overlapping — validates keyword quality but not audience sizing strategy. |
| BUK Exp 2 (Nov-Dec 2025) | BUK vs MM V2 — percentile threshold + score adjustments | True A/B via MD5 hash on IP (no bleed). 5 advertisers, 2 weeks (11/21–12/4). Percentile threshold ~top 42%, popularity penalty, advertiser lift. | +26.84% avg relative IVR lift, but inconsistent across advertisers. Hello Molly +137% (audience -83%), Hatch -25% (audience +93%). | **Primary lesson:** Performance inversely correlated with audience size change — size confounding makes results ambiguous. All 5 advertisers show negative rank-performance correlation (higher BUK rank → higher visit rate), confirming rankings are directionally aligned. Next experiment must control for audience size to isolate keyword quality effect. |

---

### BUK Experiment Design Lessons (2026-03-31, Alex Knorr knowledge transfer)

- **Campaign ramp-up period ~1 month**: Beeswax bidding dynamics take ~1 month to stabilize after campaign changes. Cannot reliably compare experiments during this window. For RCTs with simultaneous test/control launch this is less of an issue (both arms ramp together), but for pre/post or sequential designs it's critical.
- **Audience size is the #1 confounding variable in keyword experiments**: Changing keywords almost always changes audience size. Larger high-intent audience → lower IVR (more diluted pool), smaller → higher IVR (more concentrated). Must either (a) size-match BUK and MM V2 audiences within 5%, (b) adjust keyword count to force alignment, or (c) use continuous scoring which sidesteps the size problem.
- **Old A/B split method (audience isolation) has contamination**: IP gets impression from one campaign → added to block list for other campaign. But processing delay means IPs can appear in both groups. New method: MD5 hash on IP → deterministic split into hash buckets → no bleed by design. All future experiments should use this.
- **Optimal cutoff exists between include-all and exclude-bad**: (Malachi's Etsy experience) Removing the worst items/keywords always decreased total ROI past a certain point — there's an optimal threshold, not a binary decision. Directly applicable to BUK keyword count thresholding.
- **Cost vs. performance differential is negligible between intent tiers**: Cost difference between high/mid/low intent is only a few percent, but visit rate difference is 10-50x. Should always bid on highest-value IPs first — continuous scoring enables this without sacrificing audience size.

### Keyword Analysis Methodology Lesson (TI-804, 2026-04-02)
- **Per-advertiser keyword analysis >> global keyword analysis**: Global keyword ranking shows only 3x visit rate range. Per-advertiser ranking shows 184x. Always analyze keyword performance per-advertiser, not globally.
- **"Best keyword rank" approach works better than per-keyword visit rates**: Computing visit rate per individual keyword is too sparse (most keyword-advertiser pairs have 0 visitors in a 10-day window, medians are 0). Instead, find each IP's best-ranked matched keyword, bucket by that rank, and compute visit rates per bucket. This aggregates signal effectively.
- **Temporal separation prevents circularity**: ipdsc DS19 keywords are from PAST browsing behavior. Measure visits in a FUTURE window. Same IP universe, different time periods. No campaign-scoping needed for "does the signal predict?" questions — campaign-scoping needed for "did our ads cause?" questions (TI-806).
- **50-advertiser sample is sufficient for directional findings** but only 15 had >10 visitors in a 10-day window. Scale to 500 for presentation-quality results.

### Exploitation vs Exploration in Optimization (Kale, 2026-03-31)

Purely exploitative optimization — targeting only the highest-intent users — is "somewhat dangerous" (Kale's words). Two risks:
1. **Incrementality problem:** Highest-intent users are also targeted by Google/Meta. When MNTN only bids on these users, incrementality reports look bad because the conversions would have happened anyway via other channels.
2. **Local maximum trap:** Without exploration, the system never discovers potentially better audience segments. Multi-armed bandit framing: 100% exploit means 0% discovery.

**Kale's direction:** Media plan budget allocation, Fangorn scoring, and any future optimization should eventually bake in exploration — allocate a slice of budget to test alternative paths, not just exploit the current best guess. Not prescriptive on shape yet, but conceptually: the system needs to explore.

**Implication for experiments:** When measuring a targeting change, consider both:
- The standard KPI (IVR, ROAS, etc.) — the "exploitation" metric
- An "opposing" metric (Malachi referenced Andy Grove's management principle) — e.g., reach vs performance, incrementality vs conversion rate

### Retargeting Frequency Diminishing Returns (Kirsten, reported 2026-03-31)

Kirsten's observation from past experiments: retargeting the same people repeatedly → performance always went down. More unique people targeted → better. Aligns with the frequency experiments documented above (lower frequency = better). Kirsten caveat: "things change all the time" — worth re-validating.

### Feature Importance Methodology Lessons (TI-790, 2026-03-31, updated 2026-04-01)

- **Pre-visit vs feedback feature leakage**: guid_log and conversion_log features are outcome-adjacent — they exist BECAUSE the IP visited. Including them produces AUC ~0.999 (tautological — `gl_n_events > 0` perfectly identifies visitors by definition). Pre-visit features alone give AUC 0.831. Always separate features by when they become available (bid time vs post-visit).
- **Temporal leakage — use day N-1 features, day N labels**: Same-day features and labels allow post-visit impressions to leak into "pre-visit" features (an IP that visited at 8am has 9pm impressions counted as features). Fixing this (day N-1 features, day N labels) dropped AUC from 0.842 to 0.831 — leakage was real but small (0.011). Rankings were stable across the correction, confirming findings hold. Always enforce strict temporal separation.
- **Sample selection bias in targeting-system evaluation**: When training data is pre-filtered by an existing targeting system (e.g., Fangorn selects which IPs get impressions), you CANNOT claim new features "work independently." Every IP in the sample was already chosen by the system. A NEW-only model (existing features removed) showing 7x lift means the new features add discriminative power *within the Fangorn-selected pool* — not that they'd perform equally on a random untargeted population. To test true independence, you'd need a random sample of IPs that were NOT pre-filtered by targeting, which doesn't exist in production data. Frame claims accordingly: "adds signal on top of current targeting" not "predicts visits independently."
- **Composite importance method works**: XGBoost with gain, weight, cover averaged into composite rank produces stable rankings. SHAP (mean absolute Shapley value) is preferred for final reporting — it captures per-prediction contributions, not just tree-structure importance.
- **Existing Fangorn signals dominate pre-visit features**: `ci_pct_new`, `n_wins_this_adv`, `al_avg_segments` hold top 3 ranks. New bidstream features (device diversity, content genre, clearing price) add incremental signal. When existing features are removed, new features still achieve AUC 0.777 (7x lift at top 1%) within the Fangorn-selected population.
- **Content genre rises with proper sampling**: With 1-hour augmentor/BAE samples, content features ranked mid-tier. With 4-hour augmentor + full-day BAE, `bae_pct_ent` jumped from #26 to #8. Sampling window materially affects count-based AND genre-percentage features. Use full-day data when possible; disclose sampling window when not.
- **Content genre is mid-tier for prediction but high-value for segmentation**: content_genre ranked ~8th for general visit prediction (up from ~25th with proper sampling), and is the best candidate for *vertical classification* (mapping IPs to advertiser categories). Different use case than IVR prediction — both valuable.
- **Scale matters for feature extraction**: augmentor_log = 241 GB/day (~43 GB for 4-hour sample), bidder_auction_events = ~400 GB/day. Always dry-run first. guid_log and win_logs are cheap (~13-75 GB/day).
- **fillna(0) vs NaN for ratio features**: XGBoost handles NaN natively. For ratio/percentage features, 0 is a real value (e.g., avg_price=0 means free inventory), while NaN means "no data." Preserving NaN for ratios and using fillna(0) only for counts is the correct approach.

---

## Intent Score Shuffling — Incrementality Experiment (BER-2250, Q2 2026)

### The Core Question
Is MNTN's intent tier targeting generating **incrementality**, or are we buying audiences who would have converted anyway?

High-intent audiences are a shared targeting pool across CTV, Meta, and Google. Overlap reduces the probability that any single platform is driving marginal conversion. We have never tested whether the intent scoring methodology itself drives incremental lift.

### Phase 1: Observational Analysis (Existing 10% Holdout)
**Discovery (Matt Brorby, 2026-04-07):** Every campaign already has a 10% holdout group. This IS the control group — no shuffling needed for the initial analysis.

**Holdout hash function (Zach Schoenberger, 2026-04-07):**
```sql
-- Greenplum/Postgres (replicates Rust audience service):
SELECT (('x' || substr(md5('{AID}:{IP}'), 1, 16))::bit(64)::bigint % 1000) AS bucket;
-- bucket 0-99 = holdout (10%), 100-999 = targeted (90%)
```
- Hash input is `{AID}:{IP}` — holdout is **per-advertiser per-IP**, not global
- Same IP can be holdout for one advertiser but targeted for another

**IMPORTANT: Holdout hash ≠ experiment bucket hash.** These are two DIFFERENT random assignments:
- **Holdout hash:** `MD5('{AID}:{IP}')` mod 1000 — embedded in the audience expression JSON. Determines the 10% incrementality holdout.
- **Experiment bucket hash:** Hashes on IP address directly using a different prefix (e.g., ex46). Determines which experiment arm an IP falls into (e.g., control vs treatment for Fangorn).
- The two are **independent** — an IP in the 10% holdout can be in any experiment bucket.

- **Control:** 10% holdout (never served impressions, same intent tier distribution as targeted group)
- **Treatment:** 90% targeted group (eligible for impressions)
- **Methodology:** ITT — compare ALL IPs in 90% targeted group vs 10% holdout, regardless of whether impressions were actually served. Only a fraction of the 90% actually gets impressions (budget-constrained), so ITT avoids selection bias from impression delivery.
- **Breakdown:** By intent tier (high/mid/peak performance), by advertiser vertical, by spend level
- **Nick** (experimentation team) has the holdout query. **Kristen** may already be doing related work.

### How to Identify Experiment Campaigns
Nick identifies experiment campaigns by parsing `campaign_group.name` for the pattern **"EX-{number}"** (e.g., "EX-46"). This is hacky but is the current method — there's no dedicated experiment flag in the schema. The regex extracts everything after the first space in the name and checks for an EX-{number} pattern. If null, it's not an experiment.

### Phase 1 Results: ITT Shows Zero (April 2026)
**No statistically significant incrementality detected** under ITT across high-intent, mid-intent, and peak performance groups. This is NOT a statement that ads don't work — the methodology cannot determine incrementality with available data.

**Why ITT shows zero — coverage dilution:**
- Analyzed 10 advertisers across 8 verticals (10-day pre-period, 20-day post-period)
- High-intent impressions: 69% of total impression share despite representing only 16% of scored IP universe
- **Coverage problem:** e.g., Boosted Safe — only 14% of high-intent treatment group (400K impressions / 2M IPs) actually received ads
- When 86% of "treatment" receives no treatment, comparison groups become nearly identical → biases toward zero
- This is a structural limitation of ITT when impression coverage is low

**Incrementality measurement spectrum (current state):**
- **High end (inflated):** Current dashboard compares exposed group vs random sample — "wildly inflates" results because exposed group is pre-selected high-intent users likely to convert anyway
- **Low end (conservative):** ITT framework — biased toward zero due to coverage dilution
- **Middle ground:** LATE estimates show larger lifts (4%+ coverage threshold) but inconsistent — only 2 mid-intent examples (Hexclad, Zazzle) moved in opposite directions

**Technical constraints discovered:**
- Prospecting intent scores only retained for 35 days in active storage
- Assigned max score per IP during 10-day pre-period as fixed treatment assignment
- Peak performance impressions appear despite not being enabled — likely from real-time conquesting before scores are available

**Alex Knorr's pre-analysis repo and external table:**
- Repo: SteelHouse/databricks_targeting, branch `TI-835`, path `notebooks/Incrementality_Pre_Analysis/`
- External table: `dw-main-bronze.external.TI_835_prospecting_scores` (GCS: `gs://mntn-data-archive-dev/alex.knorr/TI_835_prospecting_scores/*.parquet`)
- Report: `reports/TI_835_Pre_Analysis_v4.html`
- Coverage rates even lower than meeting estimates: high-intent median 3.4% (not 14%), peak 0.2%, mid 0.04%
- 10 advertisers, 8 verticals, 25-day post-period (Mar 21 – Apr 14)

### Phase 2: Ghost Bidding Experiment (REPLACES Shuffling — April 2026)

**PIVOT:** Intent score shuffling has been replaced by **ghost bidding methodology** + **dedicated mid-intent experiment**. Key decisions:
- Treatment group must be **fully mid-intent focused** (not shuffled high/mid mix) to generate stronger signal
- Use **ghost bidding** to calculate Average Treatment on the Treated (ATT) rather than ITT
- **Target timeline:** April 30th deadline for experiment setup and ghost bidding implementation

**Ghost bidding mechanism (revised 2026-04-21 per Matt Brorby):**
1. Holdout IPs are deterministically identified via hash and **do appear in augmentor_log** — empirically verified 2026-04-20 (see RESOLVED note below).
2. For each holdout IP appearance in `augmentor_log`, retain the event-level targeting signals (intent score, HHST, segments) and filter to events where MNTN would have bid — i.e., the IP cleared the advertiser's active intent threshold and HHST gate at that moment. Those are the candidate holdout IPs.
3. Propensity-match the candidate-holdout group to the actually-served treatment group (from `cost_impression_log`) on intent score (and other covariates as needed) so distributions are similar.
4. Compare visit rates: exposed treatment IPs vs propensity-matched holdout IPs.

The pre-2026-04-21 plan (compute campaign win rate, apply as sampling probability) has been archived. Matt noted the per-event targeting signal is already in augmentor_log, so aggregate win-rate approximation is unnecessary. Alex Knorr has the structure started.

**RESOLVED (2026-04-20):** Augmentor_log contains holdout IPs at the uniform-expected rate. For advertiser 31357 (WGU), 1 hour of `bronze.raw.augmentor_log` on 2026-04-19 shows 10.0% of unique IPs in hash buckets 0-99 — exactly the uniform expectation for a 10% holdout. augmentor_log has no advertiser_id column and is pre-bid, so it is IP-complete regardless of any given advertiser's holdout status. Alex Knorr's read was correct; the ghost bidding pipeline can proceed using existing data without an ETL change from Zach/Jordan or a bidder-side change from Kevaughn. Query + results: `tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/queries/ti_837_augmentor_holdout_bucket_verification.sql`, output in the same ticket's `outputs/` folder. Open follow-up: row-level holdout fraction was 8.4% (vs 10.0% at the unique-IP level), suggesting holdout IPs may have slightly fewer augmentor events per IP than non-holdout IPs — worth a follow-up with Ryan/Zach to understand whether frequency-cap or dedupe logic treats holdouts differently. Does not block the methodology.

**Matt Brorby's T-learner prototype (2026-04-20):** Matt built a T-learner with Platt Scaling on branch `mbrorby/workspace/impression-uplift` in `SteelHouse/databricks_targeting`. Two XGBoost Spark models (treatment + control), calibrated so subtraction is in probability space. Uplift = Platt(Model_T(x)) − Platt(Model_C(x)). Ranks by incremental probability, not absolute. Qini curve evaluation. Holdout hash ported from Greenplum to Spark: `MD5(advertiser_id:ip)` first 16 hex → mod 1000, buckets 0-99 = holdout. Uses `decimal(20,0)` to avoid overflow. Control IPs drawn from `cost_impression_log` (IPs served by other advertisers in same window), NOT feature store — fixes support mismatch (feature store IPs include households no ad system would target, ~0.03% visit rate vs ~9% for targeted).

**Ownership update (2026-05-08):** TI-886 (T-learner productionization) has been reassigned away from the TI side — owned by another person (confirmed via Alex Knorr). MNTN's TI work on the BER-2250 follow-up is the **bidder-process ghost-bidding implementation**, not the model. The 30-day window run, the 30-net-new-advertiser cohort, and most other follow-up analyses are **blocked on the bidder-process implementation going live** — the post-hoc Databricks-on-augmentor-logs path is no longer the plan. Don't start them in parallel.

**Power constraint (Malachi's framing, 2026-04-20):** Our minimum detectable effect lands around ~15% while realistic CTV lift is 2-8%. This is the whole ballgame — why geo doesn't work at MNTN budget scale, why observational ML fails, and why ghost bidding (reusing existing 10% holdout instead of carving a new one) is structurally the only path. TI-884 quantifies this precisely per advertiser.

**Advantages over ITT:**
- Eliminates coverage dilution by comparing only IPs that would have been served
- Answers: "Of people who received an impression, what is the incremental lift?"
- Can be implemented observationally without bidding logic changes — only needs logging enhancements
- Initial analysis will use win rate approximation method
- Trade Desk previously built this methodology; Alex Bloore was involved in alpha testing at Goodway

**Key insight — mid-intent performance context:**
When customers shift budget to mid-intent, "performance drops off a cliff" and costs increase. The experiment aims to validate whether this group is **incremental despite being more expensive**. The answer won't be "performance is better in mid-intent" — it will be "people are more influenced incrementally when served ads in mid-intent vs high-intent."

### Phase 3: Lift-Optimized Model (Future)
Matt Brorby outlined training a model focused on *lift* rather than visit prediction:
- Training data: same features as Fangorn + whether the IP received an impression (and when)
- Target: incremental visit/conversion (visits WITH impression vs visits WITHOUT)
- Output: rank-ordering by incremental lift rather than by intent
- This would replace intent scoring with lift scoring

### Fellowship System — Weighted Model Combination (Alex Knorr, April 2026)
Conceptual framework for balancing performance and incrementality:
- Build a **toolbox of independent targeting models** (conversion probability, incrementality, new-to-brand, keyword intent, etc.)
- **Combination engine** aggregates models with adjustable weights, agnostic to underlying architecture
- **Feedback loop** adjusts weights based on campaign performance — either Bayesian updating or Performance team APIs requesting more incrementality vs performance signal
- Enables threading the needle between competing objectives without rebuilding models
- Connects to continuous scoring roadmap: once implemented, score thresholds can be dynamically adjusted per customer goal (narrower for performance, wider for incrementality)

### Key Tension: Performance vs Incrementality
Optimizing for incrementality and performance (visit rate) are partially opposed:
- High-intent users → high visit rate, low lift (would have visited anyway)
- Low-intent users → low visit rate, high lift (wouldn't have visited without the ad)
- A group with 0% natural visit rate + 10% post-impression visit rate = infinite lift but only 10% VR
- Need to find the balance: maximize absolute performance WHILE maximizing incremental contribution

### Marketing Philosophy (confirmed April 2026)
Mountain functions as a **mid-funnel priming channel** that feeds bottom-funnel conversions in search/social, NOT as a direct conversion driver:
- CPA comparison is apples-to-oranges — Mountain should not match Meta/search CPAs on same reach
- Value proposition shift needed: position Mountain as driving **incremental reach that improves blended efficiency** across the marketing mix
- Measurement window must be longer than current approach to capture mid-funnel effects
- Site visits are a "late" signal — by that point, lower-funnel channels claim attribution
- Need reporting that shows acceleration of users moving toward high-intent status because of Mountain exposure

### Why ITT (Intent to Treat) — and Its Limitations
ITT is the established methodology for intent-assignment questions. It compares outcomes based on the group an IP was *assigned* to, not what actually happened. This prevents selection bias — e.g., the 90% targeted group includes IPs that never actually received impressions (budget constraints, not watching TV at the time, etc.). Comparing only impression-recipients vs holdout would introduce bias because impression-receipt correlates with behavioral differences.

**Limitation discovered (April 2026):** When impression coverage is very low (14-16% of treatment group), ITT structurally biases toward zero because the vast majority of "treated" IPs are behaviorally identical to holdout. Ghost bidding (ATT) addresses this by comparing only IPs that would have been served.

### Key Metrics
- Incremental lift by original intent tier vs assigned tier
- Baseline incrementality metric (benchmark for future scoring iterations)
- Per-vertical breakdown

### Business Stakes
- If high-intent targeting → low incrementality → we're charging for outcomes that would have happened → **retention risk**
- If we can prove incrementality → competitive moat vs Meta/Google → **revenue growth + retention**
- First mover on incrementality proof = **differentiation play**

### Incremental ROAS Measurement — Industry Context (Matt Brorby, 2026-04-08)

**Time-delta bucketing method (Matt's prior role — mobile, deterministic):**
- In mobile (deterministic, app installs): bucket users by time from ad impression to conversion event
- Short windows (5 seconds) ≈ 100% incremental
- Signal becomes "barely noticeable" beyond ~6.5 hours for app installs
- For most apps, useful signal window was 30 minutes to 6.5 hours max
- This is "more art than science" — huge variation by app/advertiser

**Incremental ROAS benchmarks (industry):**
- Good advertisers: ~$0.90 incremental per dollar spent
- Poor advertisers: ~$0.50 or worse
- Trade Desk: ~$1.15 incremental ROAS (considered good)
- Companies claiming $8 ROAS are measuring attributed, not incremental — massively inflated
- Over $1.00 incremental ROAS is "awesome" and rare

**CTV-specific challenges for incrementality:**
- NOT deterministic — IP-based, not device-based
- Long conversion windows (weeks, not seconds/hours like mobile)
- Hard to separate signal from noise at longer time intervals
- Should filter out cellular IPs (T-Mobile, etc.) — use identity graph as filter
- Conversion events vary wildly by advertiser/product type
- Time-delta bucketing may work differently for CTV vs mobile — needs investigation

**LiftLab measurement context:**
- LiftLab is paid by the advertiser → bias toward conservative measurement
- Their incremental reports will be as conservative as possible
- MNTN is "at the mercy of these third parties"

**Ensemble approach for models:**
- "No one model to rule them all" — separate optimization for different objectives
- IVR model for performance-focused advertisers
- Incremental ROAS model for incrementality-focused advertisers
- Trade-off is inherent: optimizing for incrementality hurts IVR and vice versa
- Only applies to advertisers who opt into incrementality measurement

### Product Brief
Full brief: [Confluence](https://mntn.atlassian.net/wiki/external/NTM1ZmViMzc1YzczNDQ0YjgzZDVlMjdkNTk2ZGY4NmY)

### Tickets
- BER-2250: Incrementality Overhaul (parent initiative)
- TI-831: Audience Deciles for Advertiser Experimentation
- TI-835: Control group design and measurement methodology
- TI-837: Implementation plan for intent score shuffling
- TI-839: Measure incrementality results
- TI-842: Present results to broader audience

<!-- slack-extracted: 2026-04-08-full -->
- ### Incrementality Experiment Design — Intent Tier Hypothesis

A hypothesis under active exploration: lower-intent audience groups (Mid Intent, Max Reach) may generate more incremental lift than High Intent groups, because High Intent users are also heavily targeted by Google, Meta, and other platforms — meaning MNTN's attribution to those users may not represent true incrementality.

Analysis of the Incrementality dashboard by intent group (High Intent, Mid Intent, Peak Performance, Max Reach) is being pursued via BAE-4007. Metrics of interest: incremental lift on IVR and VVR by intent tier.

---

## Incrementality Measurement — Causal Method Reference

*Source: iROAS Measurement Playbook (2026-04-19). Full document: `tickets/ber_2250_incrementality_overhaul/artifacts/iroas_measurement_playbook.md`*

### Ranked Method Reference — MNTN Stack Feasibility

| Rank | Method | Stack Status | MNTN Application | Key Reference |
|---|---|---|---|---|
| 1 | **Geo randomized holdouts + ASCM** | **Green** (feasible today) | DMA-randomized holdout, analyzed with GeoLift/augsynth. No bidder changes needed. Privacy-durable, walled-garden-agnostic. | Abadie 2021 JEL; Ben-Michael et al. 2021 JASA; Vaver-Koehler 2011 Google |
| 2 | **Household RCT with ghost bidding** | **Yellow** (building now — BER-2250 Phase 2) | Gold standard for CTV. Our approach: win-rate approximation from augmentor/bid logs as stopgap; full bidder-side ghost logging as target state. | Johnson-Lewis-Nubbemeyer 2017 JMR |
| 3 | **Bayesian MMM (Meridian)** | **Yellow** (needs calibration from 1 & 2) | Production surface for per-advertiser iROAS. Weak alone; strong when calibrated with experimental priors. Connects to Kale's 5 external vendor experiments. | Jin et al. 2017; Zhang et al. 2024 Google |
| 4 | **CausalImpact / BSTS** | **Green** (already in-house from TI-748) | Single-market switch-ons, always-on campaigns. Keep using for feature rollouts. Know its SUTVA limits. | Brodersen et al. 2015 AoAS |
| 5 | **Switchback experiments** | **Green** (data), **Yellow** (power) | When geo holdouts are politically impossible or carryover spillover is severe. Time-block randomization. | Bojinov et al. 2023 Mgmt Science |
| 6 | **Staggered DiD (Callaway-Sant'Anna)** | **Green** | Platform-level rollouts (new supply partner, bid-shading change, identity graph update). Avoid canonical TWFE. | Callaway-Sant'Anna 2021 JoE; Goodman-Bacon 2021 |
| 7 | **IV via auction variation** | **Yellow** | Stopgap before ghost logging: exploit bid-shading/frequency-cap/pacing as instruments. Log the randomized component. | Waisman et al. 2024; Gui-Nair-Niu 2022 |
| 8 | **Regression discontinuity** | **Yellow** | Local effects at frequency caps, reserve prices, quality-score thresholds. Narrow but nearly free. | Calonico-Cattaneo-Titiunik 2014 Econometrica |
| 9 | **Uplift / CATE modeling** | **Yellow** | **For targeting optimization, NOT headline iROAS.** Deploy tau_hat(x) as bid multiplier once experimental data exists. | Kunzel et al. 2019 PNAS; Wager-Athey 2018 JASA |
| 10 | **DML / TMLE / PSM-IPW** | **Red** (for iROAS claims) | Internal diagnostics only. Never report as customer-facing iROAS. | Gordon et al. 2023 Marketing Science |

### Statistical Power Constraints — Lewis & Rao Applied to CTV

**The fundamental constraint on all CTV incrementality measurement:**

Lewis & Rao (2015 QJE) formula: **N_per_arm = 2 * ((z_{alpha/2} + z_beta) * sigma / delta_y)^2**

Applied to a typical MNTN campaign:
- 10M impressions, $25-$50 CPM, frequency ~5, ~2M households
- Household sigma ~$70 weekly sales, cost per HH ~$0.125
- **MDE at 50/50 split, 80% power: ~$0.28/HH/week — right at break-even iROAS**

Variance reduction stack (compounding):
- CUPED on pre-period visit history: **20-50% SE reduction**
- Ghost-ad conditioning (Johnson-Lewis-Reiley 2017): **25-31% SE reduction** (equivalent to 1.7x sample)
- Stratified randomization on pre-period sales: **10-20% SE reduction**
- Combined: **~40% SE reduction**, equivalent to 2.7x the sample size

**Rules of thumb:**
- Do NOT report an iROAS point estimate without +/-50 pp confidence interval for campaigns below 5M impressions
- Below 5M impressions: "directionally consistent with positive ROI" — not a point estimate
- Pre-compute MDE via GeoLift's power simulator on 52 weeks of historical revenue; refuse pilots with MDE > 15%
- Lewis-Rao's original finding: median SE on ROI was 26.1% for retail, 115% for brokerage — even with 25 RCTs totaling millions of users

**MNTN implication:** Set expectations with Kale/leadership that CTV incrementality measurement is inherently noisy. The goal is calibrated uncertainty, not false precision. This is the scientific reality every competitor faces.

### Observational Methods Are Not Defensible for Headline iROAS

**Key evidence (cite when vendors claim otherwise):**

| Study | Finding | Implication |
|---|---|---|
| **Gordon et al. 2019** (15 Facebook RCTs, 500M observations) | Observational methods off by **3x** in half the studies | Rich platform features don't fix selection bias |
| **Gordon et al. 2023** "Close Enough?" (663 Meta RCTs, 5,000+ features, deep-learning DML) | Median absolute error: **62-115 pp** on lift vs true RCT values of 6-28% | Even best-in-class ML can't close the gap |
| **Blake-Nosko-Tadelis 2015** (eBay paid search) | OLS ROI: **>4,100%**. True causal ROI: **-63%** | Not a nuance — it's a sign-flip |

**Vendor assessment:**
- **Defensible:** Measured (geo experiments), Haus (geo + synthetic control), LiftLab (geo switchback) — when using their experimental methods
- **Directional only:** Incrmntal ("causal AI" on aggregated change-events — novel but unverified), Rockerbox/Northbeam (MTA-first with MMM bolt-ons, no published methodology)
- **MNTN's current dashboard:** Inflates results by comparing exposed group vs random sample (confirmed Matt Brorby April 2026). Exposed group is pre-selected high-intent users likely to convert anyway.

### Ghost Bidding — Academic Foundation

Connecting BER-2250 Phase 2 implementation to the literature:

**Core paper:** Johnson, Lewis & Nubbemeyer (2017 JMR, SSRN 2620078) — won 2022 Weitz-Winer-O'Dell Award
- For each bid request: hash(household_id, campaign_id, salt) mod N assigns arms
- Control arm suppressed BUT auction outcome logged as "ghost impression"
- ITT: mean KPI difference across all assigned households (exposed or not)
- **LATE/CACE:** tau_LATE = ITT / first-stage exposure rate (Imbens-Angrist Wald estimator)
- Filtering to exposed + would-have-been-exposed recovers treatment-on-treated

**Precision gain:** Johnson, Lewis & Reiley (2017 Marketing Science, "When Less Is More")
- Ghost-ad conditioning delivers **25% SE reduction = 31% more precision**
- Equivalent to growing a 3.1M-user experiment to 5.3M users
- Google runs >100M predicted ghost ads/day; TTD and Viant market commercially

**MNTN's implementation (win-rate approximation):**
- Holdout IPs appear in augmentor_log (bid stream) but are suppressed from serving
- Calculate campaign-level win rate from actual served impressions in cost_impression_log
- Apply win rate as sampling probability to pseudo-expose holdout IPs
- Compare visit rates: exposed treatment IPs vs pseudo-exposed holdout IPs
- This is a valid approximation without bidder-side product changes
- Full ghost-bid logging (bidder change) is the target state

**Key gotchas for MNTN:**
- Randomization must be at bid-request, not post-hoc (we use deterministic hash — good)
- Walled-garden contamination: holdout HH may see same campaign on Amazon/Meta — dilutes control
- Divergent delivery (Eckles-Gordon-Johnson): bidder optimizes differently when control excluded — freeze model during experiments
- Co-viewing inflates numerator — keep conversions and exposure at same unit (household)

### Co-viewing Bias in CTV

**Industry measurements:**
- TVision: average viewers per viewing household — **1.46 on linear, 1.44 on CTV**, peaking at **1.52 in primetime**
- iSpot: co-viewing contributes incremental **~41% of viewership** on streaming
- Range: **1.23 to 1.90 per impression** depending on daypart, demo, genre

**The industry's frequent use of a single "1.2x" factor masks material variation.** These multipliers are dynamic, not static.

**Rule:** Keep the numerator (conversions) and denominator (exposure) at the same unit of analysis:
- If conversions are household-resolved via identity graph → use household-level impressions and spend
- If conversions are individual-pixel (typical for web purchases) → accept that co-viewing inflates apparent iROAS, apply daypart/genre adjustment or flag the bias

**Corollary for MDE / power-calc baselines (TI-1019, verified 2026-06-24):** the two-proportion binomial MDE engine (`ti_884_mde_calculator.py`) treats each advertised IP as one Bernoulli trial, so the baseline `p` MUST be a per-IP *probability* = distinct visiting-and-served IPs / distinct served IPs. **Never feed it an event-count rate.** `graph.visits` is an *event count* (data_catalog.md), not distinct IPs — `graph.visits / usersReached` inflates the baseline by (visit events per visiting IP) × (visiting-but-unserved leakage). For WGU that was 3.36x (10.70% → 35.95%). Because MDE_rel ∝ sqrt((1−p)/p) — monotonically decreasing in p — an inflated baseline makes the tool report a **smaller, over-optimistic MDE and overstated power**. At WGU the over-optimism factor was 2.16x (0.68% true vs 0.31% naive). Rule: numerator and denominator must be the same unit (distinct IPs over distinct IPs); use `graph.SiteVisitors`-style distinct counts, never `graph.visits`, when prefilling a power calculator. A baseline >1.0-implied (here 0.36 only because events ≈ 3× IPs) is the tell that an event count leaked into a per-trial probability.

### Incrementality-test eligibility screen (INCR-75, 2026-06-25)

Reusable funnel for "which advertisers should we run a lift study on?" — fork TI-1019's per-advertiser metrics SQL (`incr_75_advertiser_metrics.sql`: IVR `p_visit`, CVR `p_cvr`, CPM, imps/IP, trailing-30d spend, 12-mo typical-active-month spend, +B2B bucket +56d distinct-IP reach over the full delivering universe) → run TI-884 `ti_884_mde_calculator.py` per advertiser at **var_reduction=1.0** → score/tier → one Excel (funnel waterfall + exhaustive flagged list + tiered final).

- **MDE is RELATIVE** (the question the ticket always asks): `mde_rel = mde_abs/p`. A 5% MDE on a 0.5% IVR = detect 0.525% (a 5% proportional lift), NOT 5.5pp. State this explicitly in any eligibility deliverable.
- **IVR gates eligibility; CVR is informational.** CVR baseline ~30× lower → ~7–10× harder (5% CVR MDE needs ~$2–5M/mo). Compute both IVR targets (5% credible / 10% realistic); CVR reported at a looser 15% target, never pass/fail.
- **Hard filters (minimal):** clean/active · not-B2B (`fpa_advertiser_verticals` type=0 bucket = "B2B Software & Services") · measurable IVR (≥100 visiting IPs). Spend / IVR-position / powerability are **scored, not cut** — keeps the list complete and labels Top/Mid/Low.
- **8-week horizon ≈ 1.84 months** of spend; budget-for-MDE = `spend_required()` total test budget, an **optimistic floor** for large gaps (imps/IP grows with window). Cross-check with a **direct 56-day distinct-IP MDE** (no extrapolation).
- **Extra-ask bands (label, never cut):** ≤25% easy / 25–50% stretch / >50% unreasonable.
- **Prior-lift bonus:** TI-933 (Select clickpass visit-rate pp, significant only) + TI-837 (guid total-traffic pp, all-funnel; permissive "has shown lift" signal). Report in **pp not relative** — relative blows up for low-organic-traffic brands.
- Result (run 2026-06-25): 2,009 delivering → 1,841 non-B2B → **1,287 eligible**; Top 56 / Mid 266 / Low 965; smaller/mid consumer brands ($27k–$172k/mo, IVR 2.5–7.5%) dominate Top. Canonical: `tickets/incr_75_eligible_advertisers/`.

### Triangulation Architecture — The End-State Pattern

The meta-pattern endorsed by Meta, Google, PyMC-Marketing, and the BCG 2025 "trifecta" study (46% of leading marketers):

```
Geo lift tests (4-6 per advertiser per year)
    ↓ point estimates + SEs
Feed as LogNormal(log(est), SE) priors on channel ROI
    ↓
Meridian / PyMC-Marketing MMM (refresh weekly)
    ↓
Reported iROAS = posterior mean + 80% credible interval
```

**MNTN mapping:**
1. **Now:** Ghost bidding ATT from existing 10% holdout (BER-2250 Phase 2)
2. **Next:** DMA-randomized geo holdout pilot (6-10 advertisers, >=$500k/month)
3. **Then:** Calibrated Meridian MMM with geo + RCT results as priors
4. **End state:** Bayesian blending of geo, household RCT, switchback, clean-room, and MMM — with explicit uncertainty per advertiser

**Critical alignment:** Experiment estimand != MMM estimand. Experiments measure short-window partial-reduction effects; MMM ROI is against zero-spend counterfactual. Meridian's `roi_calibration_period` parameter aligns these.

### Walled Garden Measurement Constraints

**Household-level ghost bidding is impossible inside walled gardens** — the auction happens publisher-side. MNTN cannot control bid suppression for Disney, Netflix, Amazon, YouTube inventory.

| Platform | Clean Room | Access Type | Lift Support |
|---|---|---|---|
| Amazon | AMC (AWS Clean Rooms) | Custom SQL, ~100-user threshold | Yes (holdout-based) |
| YouTube | Google ADH | Custom SQL, 50-user threshold | Yes |
| Disney | InfoSum/Snowflake/LiveRamp | Templated queries (no open SQL) | Via VideoAmp/Samba TV/EDO |
| Netflix | Snowflake/InfoSum/LiveRamp | Post-campaign measurement | Transitioning (Xandr → in-house) |
| Roku | Snowflake | Templated queries | 20+ measurement partners |

**What works across walled gardens:**
- **Geo holdouts** — identity-agnostic, walled gardens participate whether or not we control the auction
- **MMM with walled-garden spend as channel input** — aggregate-level, no clean-room required
- **Clean-room lift studies** (AMC, ADH) — where platforms support holdout-based measurement

**Integration priority for MNTN:** BigQuery Clean Rooms (native) > Snowflake (publisher reach) > LiveRamp Safe Haven (identity resolution) > AMC/ADH (on demand)

### Incremental ROAS Benchmarks — Expanded

| Source | iROAS | Context |
|---|---|---|
| **Matt Brorby (MNTN, April 2026)** | Good: ~$0.90, Poor: ~$0.50 | Internal industry knowledge |
| **Trade Desk** | ~$1.15 | Considered good; >$1.00 is "awesome" and rare |
| **Measured 2025 CTV Insights** (274 experiments, 60 brands) | **Median CTV: $2.88** | vs Meta $2.30, Google $2.39 |
| **Companies claiming $8+ ROAS** | Attributed, NOT incremental | Massively inflated — observational, not causal |
| **Shapiro-Hitsch-Tuchman 2021** (288 brands) | Median ad elasticity 0.01; marginal ROI negative for >80% of CPG | Reality check for TV/CTV priors |

**Note:** Measured's geo experiments are methodologically sound (randomized DMA holdouts). Their attribution dashboards are separate products with weaker methodology. Distinguish between the two when citing.

### Open Decisions for Leadership (from Playbook Part 4)

These 11 decisions, once made, unblock the full iROAS measurement rollout. To be driven with Product, Engineering, Legal, and Finance:

1. **Holdout size policy** — Is 10% the floor, or allow advertisers to opt up to 20-50%? Controls MDE directly.
2. **MDE floor** — Refuse lift tests where MDE > 15%, or run and caveat? Recommend: refuse.
3. **Co-viewing treatment** — Flat 1.2x multiplier, dynamic Nielsen/iSpot adjustment, or bias disclosure? Both scientific and sales decision.
4. **Feature pricing** — Free for enterprise, paid add-on, or standard UI KPI? Comparable vendors charge $50k-$500k/year.
5. **Build vs buy geo experimentation** — Open-source GeoLift (own methodology) vs Haus/LiftLab/Measured (managed, faster, less transparent, ongoing cost)?
6. **Bidder-side ghost-bid logging** — Green-light the bidder change. Which eng team? Is suppression-with-logging allowed under supply-partner contracts?
7. **Clean-room priority** — Which first? Recommend: BQ + Snowflake + LiveRamp, then AMC/ADH on demand.
8. **MMM build vs buy** — Build on Meridian (open-source, best methodology) vs license Recast/Analytic Partners? Recommend: Meridian.
9. **Randomization unit** — Household default. Policy for advertisers with device-level pixels pushing for user-level?
10. **Measurement window** — 7/14/28-day post-impression for conversions? Affects numerator and lift magnitude.
11. **Reporting uncertainty** — Point estimate only, point + interval, or interval only?

### Incrementality Reading List (Consumption Order)

*Priority-ordered. Annotations explain relevance to MNTN.*

1. **Lewis & Rao (2015) QJE** — Power analysis ground truth. Read first so every method is sized correctly.
2. **Gordon et al. (2019) Marketing Science** — Proof that observational methods fail even at Facebook scale.
3. **Gordon et al. (2023) "Close Enough?" Marketing Science** — DML with 5,000+ features still fails on 663 RCTs. Read before anyone proposes ML-only iROAS.
4. **Johnson, Lewis & Nubbemeyer (2017) JMR** — Ghost ads canonical design. Directly relevant to BER-2250 Phase 2.
5. **Johnson, Lewis & Reiley (2017) Marketing Science** — Exposure conditioning adds 31% precision. Core variance-reduction argument.
6. **Blake, Nosko & Tadelis (2015) Econometrica** — eBay sign-flip. Attribution ROI >4,100% vs true causal -63%.
7. **Shapiro, Hitsch & Tuchman (2021) Econometrica** — 288 brands: median elasticity 0.01, ROI negative for >80%. TV/CTV reality check.
8. **Brodersen et al. (2015) AoAS** — BSTS/CausalImpact method already in use at MNTN. Read for identification assumptions.
9. **Abadie (2021) JEL** — Synthetic controls review. Foundation for GeoLift and Meridian geo priors.
10. **Ben-Michael et al. (2021) JASA** — Augmented SCM. The ridge-augmented estimator GeoLift defaults to.
11. **Jin et al. (2017) Google** — Bayesian MMM with carryover and saturation. The model under Meridian.
12. **Zhang et al. (2024) Google** — MMM calibration with Bayesian priors. The experiment-as-prior pattern.
13. **Callaway & Sant'Anna (2021) JoE** — Staggered DiD. Reference for platform-level rollout studies.

Also useful: Imbens-Angrist (1994) for LATE; Calonico-Cattaneo-Titiunik (2014) for RDD; Bojinov et al. (2023) for switchback; King-Nielsen (2019) on why PSM matching is wrong.

## Past Incrementality Tests — Edgar's Lessons Learned (2026-04-20)

Source: Edgar von Trotha review of 50+ past MNTN incrementality tests. Full docs in `tickets/ber_2250_incrementality_overhaul/artifacts/`:
- `lessons_from_past_incrementality_tests.md` — 6 themes
- `incremental_lift_tests_customer_tracker.xlsx` — 55 tests, 8 platforms (Haus 23, Internal 9, LiftLab 9, Measured 8, others 6)

### Six lessons (directional)

1. **Well-designed test can still produce poor efficiency** — high power + clean geo + sufficient duration still yielded sub-1% lift in some cases. Good design = trustworthy results, not automatically good results.
2. **Audience strategy drives more impact than test structure** — high-intent / previously-exposed audiences underperform incrementally. Broader prospecting yields stronger incremental outcomes. (Directly validates TI-835's "Two Stories" finding.)
3. **Exposure density > total spend** — national wide-geo tests with low per-market spend fail; concentrated geo tests with same budget succeed. Frequency matters more than reach.
4. **CTV impact often outside the primary KPI** — strongest signal frequently appears in retail/marketplace revenue, repeat-customer LTV, or downstream conversion behavior rather than primary DTC KPI. Single metric rarely captures full effect.
5. **Short or reactive tests increase customer churn risk** — premature pauses, early readouts, and short durations drive customer dissatisfaction more than poor media performance does.
6. **Weak results still valuable for retest** — strongest outcomes often emerged after one or more weak tests + clear diagnosis + focused input changes. Testing compounds value.

### Test-design implications (for TI-884 and TI-885)

- **6-week minimum test + 2-week post-treatment window** — per tracker, most completed successful tests hit this threshold
- **Holdout %** varies widely in tracker — 50% Haus geo holdouts common; 33% seen in 3-cell tests
- **Power Score column** exists in tracker but sparsely populated — we can fill this in as TI-884 output
- **Cross-reference Power Score against Lift Achieved** to empirically validate MDE predictions

## Continuous Scoring Experiment Design (Kirsa meeting 2026-04-23)

Source: Meeting with Kirsa Haenebalcke, Nick, Matt Brorby, Mike Dolt, Alex Knorr. Transcript: `tickets/ti_803_buk_value_analysis/meetings/ti_803_01_kirsa_buk_experiment_design_2026_04_23.txt`.

### Intent "groups" disappear under continuous scoring

With full continuous scoring, discrete intent buckets (high / peak / mid / max reach) **stop existing as a targeting concept**. Targeting is a slider over a continuous score; the threshold is set by **campaign pacing toward demand** (slide the threshold until delivery matches the budget line), not pre-campaign bucket cuts.

**Implication for experiment design:**
- Mid intent today is already continuous, so it provides no new information when testing continuous scoring.
- Audience-size parity across treatment arms becomes less important: the algorithm always targets best-performers first, so adding more IPs on the end should not dilute performance the way it does in discrete-bucket targeting (theoretical, unproven — flag as a validation target).
- The unit of variation in a continuous-scoring experiment is the *starting threshold* on the control arm, because that represents what real advertisers currently run.

### Treatment arm count: balancing isolation vs throughput

Past continuous-scoring experiments ran 8 arms (4 thresholds × 2 treatments) to disentangle "is this working differently at different thresholds?" from "is this advertiser-specific weirdness?". **Each threshold in the experiment was not a treatment — it was a noise isolator.**

Design heuristics from Kirsa:
1. **Drop mid intent** if continuous scoring is in play — it's already continuous, adds no signal.
2. **Higher threshold success implies lower threshold success** (unproven heuristic). If the treatment wins at high intent (hardest, most constrained audience), we can more confidently assume it wins at max reach. Justifies dropping the easiest thresholds if arm count is a constraint.
3. **Prioritize thresholds by customer distribution** when trimming arms: at MNTN today roughly 40% high intent, 30% max reach, 10% peak performance, rest mid.

### Budget + audience-size control does not force a threshold cleanly

Attempts to hard-control the threshold via budget × audience-size manipulations have not worked reliably — the campaign switches thresholds even after careful sizing. **Lesson: hard-code the threshold value in the campaign config rather than trying to steer it with budget.** Simpler, more reliable.

### Combined-feature experiments: which arm isolates what?

When testing multiple related features simultaneously (e.g., Fangorn + continuous scoring + BUK), decide up front:
- Do we only care about the combined end state? → single treatment arm, maximize power.
- Do we need to isolate each feature's contribution? → arm per isolation, accept lower power.
- Do we need to cover fallback populations (e.g., cold-start advertisers who won't get BUK)? → add an arm for the fallback config (e.g., MM V2 keywords under continuous scoring without BUK).

For the upcoming BUK + Fangorn + Continuous experiment, Kirsa's emerging design is 3 arms: control = Fangorn + mini-continuous; treatment 1 = full-continuous + BUK; treatment 2 = full-continuous + MM V2. Treatment 2 exists to cover the cold-start fallback, not to isolate continuous-scoring-alone impact.

## Bidder-Level Ghost Bids — Live Stream Methodology (2026-06-01)

Deployed in production 2026-05-27 (Ryan Kleck DM). The bidder now evaluates each holdout household with the same eligibility logic as treatment and drops the bid, tagging the row `threshold_failure_reasons = 'ghost-bid'` in BQ silver. This **replaces** the TI-837 v5 post-hoc methodology for any analysis whose window starts on/after 2026-05-27. Pre-2026-05-27 windows must continue using v5 (augmentor + random-hash subsample).

**See [`knowledge/data_knowledge.md`](data_knowledge.md) §"Ghost Bids — Bidder Feature" for column locations and the canonical filter; see [`memory/reference_ghost_bid_columns`](../../.claude/projects/-Users-malachi-Developer-work-mntn-workspace/memory/reference_ghost_bid_columns.md) for the same in memory.**

### Query collapse — 6 tables → 2-3

The v5 lift query (`tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/queries/ti_837_lift_analysis_30adv_7day_v5_segments.sql`) reads six source tables:

| v5 source table | What it provided | Live ghost-bid equivalent |
|---|---|---|
| `bronze.external.household_scoring__prospecting_intent__v1` | per-advertiser intent score → tier (high/peak/mid/max_reach) | `bidder_bid_events.household_score` (directly on the row, per-bid) — same scoring grain, no separate scan |
| `bronze.raw.augmentor_log` | "IP was biddable in window" (10-day TTL → analysis ceiling) | implicit in `bidder_bid_events` — being in the table means the bidder evaluated it |
| `bronze.integrationprod.campaigns` (dim) | `objective_id`, `funnel_level`, `advertiser_id` per campaign | `objective_id` is **on the bid event row directly**; `funnel_level` is NOT — still need campaigns dim for Stage 1/2/3 cuts |
| `silver.logdata.cost_impression_log` | treated cohort = IPs with served impressions | optional — see ATT vs ITT trade-off below |
| `silver.logdata.clickpass_log` | attribution-credited visits (treatment-only) | unchanged — still needed for the attribution wedge |
| `silver.logdata.guid_log` | total page-view visits (the honest causal-lift outcome) | unchanged — outcome side |
| hardcoded `win_rates` CTE (30 STRUCTs) | random-hash subsample to match bidder's win-rate | **eliminated** — ghost bids are the bidder's actual eligibility decision, not a proxy |

**Net:** v5 needed 6 source tables + a 30-row hand-maintained win-rate CTE. The new approach needs **2 tables minimum** (`bidder_bid_events` for both cohort labels + `guid_log` for outcome), **+ 1 dim** if segmenting by funnel_level, **+ 1 more table** if using ATT-on-served instead of ITT-on-bid (see below), **+ 1 more table** if also reporting the clickpass wedge.

### ITT vs ATT — which treatment definition

The new stream lets us pick either cleanly; v5 was effectively ATT-only because the augmentor proxy made ITT undefined.

| Estimand | Treatment cohort definition | Tables touched | What it answers |
|---|---|---|---|
| **ITT** | `bidder_bid_events` rows with `threshold_failure_reasons IS NULL` (passed all bidder gates) | bid_events alone | "What's the lift of the *policy* of choosing to bid?" — most defensible for measuring the bidder's decision quality. |
| **ATT (won)** | bid_events ⋈ `win_logs` on `auction_id` | + win_logs | "What's the lift conditional on winning the exchange auction?" — adds an exchange-loss confounder. |
| **ATT (served)** | DISTINCT `ip` from `cost_impression_log` over the same window | + cost_impression_log | What v5 used. "What's the lift conditional on rendering an impression?" — closest to "this household was actually exposed." |

**Recommendation: lead with ITT.** ITT directly answers the BER-2250 question ("is MNTN's bidding policy incremental?") and needs only one source table for the cohort. ATT-on-served is the v5-comparable estimand and worth reporting as a robustness check, but the gap between ITT and ATT-served is informative in its own right — it's the leakage from "bid placed" to "ad delivered."

### Window ceiling: 10 → 90 days

The augmentor's 10-day TTL was the binding constraint on v5; Phase 2a needed Databricks GCS reads to reach 30 days. `bidder_bid_events` is 90-day TTL — long pre-period (60-90d) AND long post-period (30-60d) both fit in plain BQ. The Standard Analysis Protocol's "lookback ≥ 2-3× post-period" heuristic is now genuinely satisfiable for incrementality.

### What survives unchanged from v5

- The two-outcome wedge (clickpass-ATT / guid-ATT) — `clickpass_log` and `guid_log` are unchanged
- The IVW vs median vs sample-weighted pooling decision (lead with median or sample-weighted; IVW for sanity)
- Tier stratification by `household_score` buckets (high=10000, peak=7000-9999, mid=3333-6999, max_reach<3333)
- Per-advertiser-segment cuts (objective_id + funnel_level)
- The visit-window vs analysis-window asymmetry (still add +3 days post-period for visit lookahead)

### What's deliberately *not* solved by going live

- **Fcap-boundary bias** (see Confluence "Ghost Win Simulation Discussion"). Holdout fcap state diverges from treatment after treatment's first impression because no impression is logged for ghost bids. Effect is bounded; only material if measurement moves to per-bid-attempt or per-impression matching. Our household × window grain is robust. Document the asymmetry in any BER-2250 deliverable; revisit if anyone proposes impression-level matching or heavy-fcap CTV-only studies. **SUPERSEDED for the ghost_frac-inflation symptom (INCR, 2026-06-23):** the inflated holdout fraction + spurious negatives are **bid-multiplicity / win-history-exit selection, not fcap-asymmetry** (fcap is config-OFF for prospecting advertisers). See the "Ghost-bid lift — bias register + the persuadables gradient" section (Matt Brorby).
- **Pre-2026-05-27 analysis windows.** No backfill. For lift measurements covering March/April/May, the v5 post-hoc augmentor approach remains the only path.

### Empirical verification (2026-05-30, full day)

Single-day shape of `bidder_bid_events` confirms the methodology can be built off this stream:

| Arm | rows | n_adv | n_camp | n_IPs | has_price=TRUE | price>0 | objective_id |
|---|---:|---:|---:|---:|---:|---:|---:|
| `'ghost-bid'` | 753K | 22 | 106 | 181K | **0%** | 100% | 100% |
| eligible-no-failure | 3.95M | 22 | 127 | 887K | 100% | 100% | 100% |
| other-failure | 69.17B | 22 | 145 | 30M | 0% | <0.1% | 100% |

**What this verified:**
- Ghost-bid rows carry full attribution (advertiser_id, campaign_id, objective_id, household_score, ip, price) — cohort labels are complete on the row.
- `has_price = TRUE` is the right ITT-treatment filter — cleanly separates ghost bids (has_price=FALSE) from eligible real bids. **Do NOT filter on `price > 0`** to exclude ghost bids; price is populated for both.
- Holdout fraction ≈ **16%** on 2026-05-30 (753K ghost / 4.7M total eligible), consistent with the Confluence "~10% of successful bid count" rough sizing.

**Coverage caveat (confirmed 2026-06-02):** only **22 distinct advertisers** appear in the BQ silver `bidder_bid_events` for the entire day, against ~300-400 live MNTN advertisers. This is the MNTN-bidder stream only (Rust `rtb-campaign-service`). Beeswax-bidder bid events (`rtb-bidder-service` Kotlin, `threshold_failure_reasons = 'ghostBid'` camelCase per the Confluence page) land in a different BQ table or aren't ingested to silver yet — that BQ surface is the open question for Ryan. Incrementality coverage off `bidder_bid_events` alone is capped at the MNTN-bidder advertiser set, likely a subset of TI-837 Phase 2's 30 advertisers.

### Open questions still to confirm

1. **Scope of `holdout_cids` (Aerospike).** Per the Confluence page, holdout assignment comes from `membership-consumer` → Aerospike `holdout_cids`. Whether this is a single global random fraction, per-campaign-group, or per-advertiser determines whether arm assignment is independent of advertiser × tier × time. If per-campaign-group, the same IP can be holdout for one advertiser and treatment for another — analyses must cohort by (advertiser, IP, window), not just IP.
2. **"Won at exchange" indicator on `bidder_bid_events`.** No explicit `won` boolean. `has_price = TRUE AND price > 0` defines "passed bidder, sent to exchange" — that's the ITT definition. Exchange-loss is not visible without joining `win_logs` on `auction_id`. Confirm with Ryan whether ITT (= sent to exchange) is the right estimand or whether ATT-on-won is required.
3. **Beeswax-bidder BQ surface.** See coverage caveat above — find out where Beeswax `'ghostBid'` (camelCase) ghost-bid rows land in BQ.

### Why this is "the BER-2250 unblock," not just an iteration

Pre-2026-05-27, every BER-2250 follow-up analysis (30-day window run, net-new cohort, segment-level lift refresh, vendor lift comparisons) needed: (a) Databricks compute to reach beyond the 10-day augmentor TTL, (b) per-advertiser empirical win-rate calibration of the random hash subsample, (c) ~18 TB scans per advertiser per week (TI-837 Phase 2a cost reality). With the live stream, each of those analyses collapses to a 2-3 table BQ query over a 60-90 day window. Iteration cycle on incrementality measurement drops from days to hours.

---

## Ghost-Bidding ATT — TI-837 Application Notes (2026-04-27)

First end-to-end ATT run on Zazzle (advertiser 37775), 1 day. Record key methodological reusable lessons, separate from the ticket-specific findings (those live in `tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/summary.md`).

### Why ATT, not ITT — articulated cleanly

**ITT failure mode for MNTN:** Targeted-vs-holdout comparison dilutes any real effect by the (1 − coverage) factor. With ~14-16% coverage, a true 4pp effect on served IPs becomes ~0.6pp ITT effect. With finite sample noise, that becomes "no statistically significant lift" — exactly what TI-835 ITT-on-guid reported.

**ATT fixes this** by restricting to:
- Treatment side: IPs that received a Zazzle impression (`cost_impression_log` filter).
- Control side: IPs that *would have* received an impression if not for the holdout — proxied by "appeared in `augmentor_log` during window" (biddable holdouts).

**Concrete recovered signal (Zazzle, 1 day):** ITT-on-guid for high-intent (TI-835) showed ~0%. ATT-on-guid for high-intent shows +1.30pp lift, p<0.0001. The signal was always there; ITT couldn't see it.

### Attribution lift vs real lift — the right framing for the deck

**Two outcome variables, different things:**
- `clickpass_log` = MNTN-attributed visits (impression + later visit + pixel match). Structurally requires impression for a row to exist → holdouts mostly cannot appear in clickpass at all → clickpass lift over-rewards being treated.
- `guid_log` = total pixel visits regardless of channel → the only honest test of whether MNTN drives new traffic.

**The wedge between clickpass-lift and guid-lift = attribution capture.** It quantifies how much MNTN credit is borrowed from search/direct/email/etc. For Zazzle high-intent: clickpass +1.49pp, guid +1.30pp → ~78% of MNTN-attributed visits would have happened anyway through other channels. This is exactly what LiftLab/Kochava measure when they tell advertisers MNTN's incrementality is overstated.

**Implication for any incrementality presentation:** lead with guid lift (real causation), reference clickpass lift as the wedge that explains the gap with external vendors. Never present clickpass lift alone — it overstates by ~5-20× depending on tier.

### Tier stratification is not optional

Intent tiers (high / peak / mid / max_reach) have different baseline visit rates AND different treatment-effect magnitudes. Treatment IPs are over-represented in high-intent vs the underlying targetable universe (the algorithm preferentially serves higher-intent IPs first when budget is constrained). Computing unstratified ATT mixes tier composition and effect together — the result is uninterpretable.

**Always stratify by intent tier**, then compute weighted-overall ATT using treatment counts as weights (this is the ATT-style stratification, not ITT-style which would weight by tier sizes in the holdout).

### Biddable-holdout filter: loose vs tight

Loose filter (any augmentor appearance) overcounts the control with IPs whose augmentor appearance was for a different advertiser's bid request. The treated side has equivalent overcounting (cost_impression_log includes any Zazzle impression regardless of which campaign), so the bias is mostly symmetric and cancels. The asymmetry is on **broad-audience tiers (peak)** where the control's "online but not Zazzle-relevant" inclusion outpaces the treatment's. This may explain the negative guid-ATT we saw at peak — selection bias, not real negative incrementality.

**Tightening options:**
1. Augmentor row had `mntn_segments` matching Zazzle's neighboring tier segment (medium tightness)
2. Augmentor row was within Zazzle's geo/daypart/inventory targeting envelope (tight)
3. Augmentor row was a real Zazzle bid request (tightest — requires bidder-side data we don't have)

For BER-2250's April 30 deliverable: stay loose, document the bias direction (toward zero on broad tiers), and flag negative findings as suspect-pending-tightening rather than chasing them.

### Cost reality: federated dry-runs are unreliable

BQ dry-run on a query touching `household_scoring__prospecting_intent__v1` (federated Parquet external table) reported 610 GB. Actual processed bytes: 18.1 TB (~30× under-estimate). The estimator can't see into the federated source's actual scan footprint.

**Practical rules:**
- For federated-table queries, treat dry-run as a lower bound only.
- Sample the actual scan cost on a small window before scaling up. (1 day → 18 TB → predicts 7 days ≈ 125 TB / ~$625 / one advertiser.)
- Materialize the prospecting + holdout-hash intermediate to a sandbox table (write access permitting) before iterating on visit-side queries.

### Visit window matters

Visit-window = analysis-window in the smoke test means cross-day attribution is missed (impression Day N → visit Day N+1 is lost when N is the last analysis day). For a 7-day analysis window, add a 3-day post-period for visit lookahead. The treated side is more affected (impressions skew earlier in any window), so the bias is asymmetric — undercounts treatment visits — and the resulting lift estimate is conservative.

### Stage 2 update — 7-day, 7-advertiser primary findings (2026-04-27)

After Stage 1 smoke (Zazzle 1-day) confirmed the methodology, Stage 2 ran 7 advertisers × 7 days (window 2026-04-20 → 04-26 UTC, +3-day visit post-period). All 26 (advertiser, tier, outcome) cells passed the 0.5pp guid-ATT CI half-width N-gate.

**Headline numbers:**
- High-intent IVW pool: clickpass +4.17pp, guid +3.36pp (CI ±0.02pp). Wedge ratio 1.24× — clickpass over-credits real lift by 24%.
- Peak-intent IVW pool: clickpass +0.55pp, guid +0.88pp. Wedge ratio 0.62× — clickpass *under*-credits real lift by 38%. **The wedge inverts at peak.** This is consistent across the 3 advertisers that retained a peak tier (Ferguson, Ancient Nutrition, Clayton) under the MAX-score collapse.
- Mid-intent IVW pool: both outcomes ~0.005-0.01pp — at the noise floor.
- Per-advertiser high-intent guid-ATT spans **200×** (Northern Tool −0.05pp to Ferguson +10.55pp). Six of seven significant.

**Why the wedge inverts at peak.** At high intent (vertical+keyword match), the funnel is sharp — IPs that visit after a treated impression reliably fire clickpass attribution. At peak (vertical-only), the funnel is more diffuse — visits do happen more often when treated, but a smaller fraction of those visits trigger clean clickpass events for MNTN. So clickpass captures a smaller share of treated peak-tier visits than guid does, and the ATT (treated − holdout) ends up smaller in clickpass than in guid.

**Implication.** The wedge isn't directional in a single direction. Don't describe it as "clickpass overstates lift." Describe it as "clickpass-attributed lift and guid-traffic lift diverge in different directions at different funnel positions." Aggregate hides both errors (they roughly cancel pooled across tiers); per-tier reporting surfaces them.

### MAX-household-score collapse (when aggregating per-IP across multi-day windows)

For per-IP analyses spanning ≥2 days of `household_scoring__prospecting_intent__v1`, you must decide how to assign a single tier per (advertiser, IP) when scores fluctuate across days. Three valid choices:

- **MAX over the window** — assign the IP its highest observed tier. Matches how the bidder treats the IP at any given moment; one row per (advertiser, IP) in the output. **What we used in TI-837 Stage 2.**
- **Latest** — assign the most recent day's score. Closer to "current state" semantics.
- **Per-day subjects** — keep (advertiser, IP, day) as the subject unit. Preserves daily tier composition but introduces within-IP correlation that needs clustered SEs.

**Trade-off observed in TI-837 Stage 2.** Under MAX, four of seven advertisers (HexClad, First Watch, Zazzle, Northern Tool) had virtually 100% of their targetable IPs hit `score = 10000` on at least one day in the week — so their peak/mid tiers came back EMPTY. The peak-tier IVW pool only reflects three advertisers (Ferguson, Ancient, Clayton). Generalization to "all-MNTN peak-tier lift" is therefore limited by sample composition, not statistical precision.

**Rule for future multi-day per-IP aggregations.** Decide explicitly which collapse rule applies. If peak/mid stratification matters for your analysis, prefer per-day subjects over MAX — or accept that some advertisers will not contribute to the peak/mid pools.

### IVW pathology — pooling across cells with very different rates

Inverse-variance-weighted meta-analysis across stratified cells gives each cell a weight of `1/var = n / [p(1−p)]`. For very low base rates (mid-tier visit rate ~0.005-0.01%), `p(1−p) ≈ p`, so variance is roughly `p/n`. Combine that with mid-tier sample sizes in the millions, and 1/var becomes orders of magnitude larger than for high-tier cells. The "pool across all cells" then collapses to ~the mid-tier ATT — which is near zero.

**TI-837 Stage 2 observation.** MNTN-overall IVW across all 26 cells = +0.16pp guid. Leave-one-out drop of Ancient Nutrition (whose mid-tier cell had massive 1/var weight) jumps the pool to +1.33pp. The per-tier pools are stable across leave-one-out; the all-cells pool is not.

**Lesson.** When stratifying across tiers with very different base rates, an IVW pool across all cells is mathematically valid but answers the wrong question. Lead with per-tier pools; report the all-cells pool only as a sanity check, with the leave-one-out swing alongside.

**TI-837 Phase 2 update (2026-04-27, 30-advertiser cohort).** The IVW pathology also bites WITHIN a single tier when many cells are at noise-floor magnitudes. Phase 2 peak-tier IVW: clickpass +0.22pp / guid +0.22pp / wedge 1.00× — appears to show no wedge. But the per-advertiser distribution is bimodal: 8 advertisers with low-magnitude noise-floor wedge ≈1.0× (Casper, Re-Bath, NET-A-PORTER, etc.) get high IVW weight from low variance, while 11 advertisers with substantial under-credit (wedge 0.1-0.5×) get lower weight. Alternative pooling reveals the true pattern:

| Pooling method | clickpass | guid | wedge |
|---|---|---|---|
| IVW | +0.22pp | +0.22pp | 1.00× |
| Arithmetic mean (advertiser-equal) | +0.84pp | +2.55pp | 0.33× |
| Median | +0.36pp | +1.19pp | 0.30× |
| Sample-size weighted | +1.02pp | +2.96pp | 0.34× |

Three of four methods agree: clickpass under-credits guid by ~3× at peak intent. **For peak-tier reporting, prefer sample-size-weighted or median over IVW.** IVW remains the right tool for high-tier (where all cells are well-powered with similar variance and the four methods all converge to ~1.0× wedge).

### Attribution and incrementality answer different questions — frame accordingly

Last-touch attribution, view-through attribution, and multi-touch attribution are designed to credit specific channels for outcomes. Causal-incrementality estimators are designed to measure what would have changed if the channel didn't exist. **They aren't supposed to produce the same number** — the wedge between them is informative about how much "credit" each channel claims vs. causes.

**Reporting principle for MNTN:** publish both. Use clickpass for billing, attribution dashboards, and per-impression analytics where attribution is the right unit. Use guid-ATT for incrementality claims, vendor benchmarking, and pricing/iROAS conversations where causation is the right unit. The wedge ratio is itself a metric — it tells leadership how much they should discount clickpass-attributed lift to get to a true-incrementality estimate, by tier.

This is the same framing LiftLab and Kochava use externally. Adopting it internally aligns MNTN's narrative with what advertisers already hear.

### Northern Tool case — when incrementality ≈ 0 despite strong attribution

Northern Tool (advertiser 40563) showed +5.56pp clickpass-ATT but −0.05pp guid-ATT at high intent (CI [−0.17pp, +0.06pp], not statistically distinguishable from zero). The biddable-holdout IPs visited at 5.90% in the 7-day window without ever being served an MNTN ad. The targeted IPs visited at 5.85% — within sampling noise of the holdout.

**Interpretation.** For Northern Tool's high-intent IPs in this window, MNTN didn't cause incremental visits. The IPs were going to visit anyway (likely from search, direct, or other channels). Yet the clickpass-attribution chain credits MNTN with +5.56pp because every served impression followed by a visit fires a clickpass row.

**This is the type of case the wedge methodology is designed to surface.** Not all MNTN-attributed lift is real; the magnitude of the gap varies by advertiser, vertical, and intent tier. Northern Tool is an extreme case but not an outlier in kind — it's a quantitative version of the qualitative concern external vendors have been raising.

**Diagnostic next step (Phase 2):** check whether Northern Tool's natural visit rate is driven by their own brand strength / search dominance / repeat-customer base. If so, MNTN targeting them at high intent has near-zero room to add — the visits happen anyway. iROAS for that advertiser is likely much lower than clickpass-ROAS would suggest.

<!-- slack-extracted: 2026-04-28 -->
- ## NTB (New-to-Brand) Experiment Results — Identity Graph (treat_crm)

**Experiment:** Identity Graph CRM targeting experiment testing whether CRM-based audience resolution increases new-to-brand customer acquisition.

**Key finding (Beddy's excluded):** Results are favorable and consistent across the remaining advertisers:
- **Cost per New Customer (primary target):** 47% cheaper in treatment; 85% posterior probability favorable; 82% of new-advertiser holdout favorable → **Roll**
- **CPA (primary guardrail):** 30% cheaper; 74% avg favorable, 71% new-advertiser favorable; 27% degradation risk → **Caution**
- **NTB Conversion Rate (secondary target):** 51% higher rate; 90% avg favorable, 85% new-advertiser favorable → **Roll**
- **Overall Conversion Rate (secondary guardrail):** 29% higher rate; 94% avg favorable, 85% new-advertiser favorable; 12% degradation risk → **Roll**

**Data quality note:** Beddy's had their treatment flipped back to control for at least one week mid-experiment. Results quoted above exclude Beddy's. Safe inclusion date range for Beddy's is pending confirmation from Nick.

**Open question:** The overall conversion rate also improved (not just NTB rate), which is unexpected. The mechanism is not fully explained — whether this reflects a true lift or a composition effect warrants follow-up investigation if bandwidth allows. (via Alexander Jerneck, #identity_core_dev, 2026-04-28)
- ## Incrementality Measurement — ATT (Augmentor-Based Ad-hoc Analysis)

Initial incrementality lift analysis using ATT (likely Augmented Synthetic Control or similar augmentor-data method) was run across 7 advertisers:
- Lift was demonstrable for all 7 advertisers.
- 4 of 7 advertisers had only HI (high-intent) targeting, which limits visibility into lift on lower-intent audience segments.
- Current computation constraints limit the analysis to ~3 days of augmentor data at a time; advertiser selection is being done strategically to improve signal across targeting groups.
- Ghost bidding implementation in the bidder is **not** a prerequisite for ad-hoc incrementality measurement — ad-hoc analysis can proceed independently to prepare for the formal experiment. (via malachi, #dev-incremental-lift, 2026-04-28)

<!-- ti_884: 2026-04-30 -->
- ## Haus Benchmark: Geo Incrementality Experiment Sample-Size Threshold

**Source:** Alex Knorr (Slack, 2026-04-30) sharing Haus's stated recommendations.

Haus (third-party incrementality measurement vendor) recommends two thresholds for valid geo incrementality experiments:

1. **500–1000 conversions per week minimum** for the experiment to be statistically valid.
2. **$10M/year minimum total cross-channel spend** ("brands that spend at least $10,000,000 per year across all channels is where they see incrementality benefits").

$10M/year ≈ **$833k/month** total cross-channel spend. For comparison against TI-884's MNTN-Stage-1-only thresholds:

| Threshold | TI-884 (MNTN Stage 1 only) | Haus (cross-channel) |
|---|---|---|
| Visits-rate measurable | **~$200k/month** Stage 1 spend | n/a (Haus doesn't break out by metric) |
| Conversion-rate measurable | **~$2M/month** Stage 1 spend | **~$833k/month** total cross-channel |

**Reconciliation:** Haus's $10M/year benchmark sits between our visits ($200k) and conversions ($2M) MNTN Stage 1 thresholds. This is consistent — Haus measures full cross-channel incrementality (lower σ/μ from richer signal) while TI-884 isolates MNTN Stage 1 only. The Haus 500–1000-conversions-per-week heuristic is a useful concrete benchmark when stakeholders push back on the spend-threshold framing — translates directly to N for sample-size calculations.

**Implication for TI-885 / advertiser recruitment:** advertisers with <500 conversions/week on MNTN Stage 1 alone should not be promised conversion-rate readouts. Visits-rate readouts are still viable above ~$200k/month.

**See also:** [TI-884 spend curve](../tickets/ber_2250_incrementality_overhaul/ti_884_power_sample_size_analysis/outputs/ti_884_spend_threshold_curve.csv); [TI-884 methodology](../tickets/ber_2250_incrementality_overhaul/ti_884_power_sample_size_analysis/artifacts/ti_884_methodology.md) §4.

<!-- ti_917: 2026-05-05 -->
## TI-917 — iROAS / revenue MDE extension and the rate→spend inversion

### Per-IP revenue MDE: the calculator already supported it

`ti_884_mde_calculator.py` exposes `mde_continuous(n_t, n_c, mu, sigma, var_reduction=...)` which is identical to `mde_binomial` except `sigma` is passed directly instead of derived from `p`. For revenue, the unit of analysis is per-IP revenue (sum of `order_amt` per IP across the window — most IPs are zero, a long-tail are positive), and σ is the cross-IP standard deviation of that distribution.

**Workflow** (TI-917 implementation):
1. Pull per-IP revenue per advertiser via `SUM(order_amt) GROUP BY (advertiser_id, ip)` over the window. Only filter to advertiser-served IPs (the treated arm) for σ — assume σ_treated ≈ σ_control under H0.
2. Aggregate to per-advertiser μ + σ.
3. Plug into `mde_continuous`. Use the canonical post-stack `var_reduction=0.595`.
4. Convert relative MDE to per-IP dollars and to iROAS: min iROAS = (mde_abs × n_treated) / monthly_spend.

Reference: `tickets/ber_2250_incrementality_overhaul/ti_917_combined_loom/artifacts/ti_917_run_revenue_mde.py`. Output CSV (`ti_917_revenue_mde_per_advertiser.csv`) ships per-advertiser tier labels.

### Headline: iROAS is dramatically harder than visits or CVR

For the TI-884 top-50 cohort, post-stack tier counts:

| Outcome | Well-powered (<5% MDE) | Borderline (5-10%) | Underpowered (>10%) | No data |
|---------|-----------------------:|-------------------:|--------------------:|--------:|
| Visits  | 46 | 1 | 1 | 2 |
| CVR     | 8  | 12 | 28 | 2 |
| **Revenue / iROAS** | **2** | **7** | **23** | **18** |

**Two binding constraints make iROAS the hardest outcome:**

1. **Revenue must be reported.** 18 of 50 (36%) have `order_amt = $0` (see `data_knowledge.md` for the gap). Education, services, lead-gen, financial — iROAS is unmeasurable for these advertisers at any spend.

2. **σ/μ must be tolerable.** Per-IP revenue is heavy-tailed for almost every advertiser — most cluster at σ/μ between 30 and 200, *orders of magnitude* harder than visit rates (where σ/μ ≈ √((1-p)/p), bounded by the rate). The 2 well-powered advertisers (AID 34835, AID 34834) hit a sweet spot of high CVR + tight σ/μ.

### The Lewis-Rao spend inversion — derive minimum spend from baseline rate

`spend_required(p, target_mde_rel, cpm, impressions_per_ip, holdout_frac, var_reduction)` inverts the same Lewis-Rao math, solving for n then converting to dollars. Three lines:

```
n_total       = (z · σ · var_red / target_mde_abs)^2 / (h · (1 − h))
impressions   = n_total · (1 − h) · imps_per_ip   # only treated arm gets served
spend         = impressions · cpm / 1000
```

**Dominant lever: baseline rate p.** σ scales as √(p(1−p)), and the inversion squares it. **Halving p roughly quadruples required spend.** CPM and imps/IP scale linearly — they matter, but rate dominates by orders of magnitude.

### Recommended monthly Stage 1 spend by baseline rate

Target 5% relative MDE, $25 CPM, 10 imps/IP, 10% holdout. Both columns (raw and post-stack) computed via `spend_required`:

| Baseline rate (p) | Spend — raw | Spend — post-stack | Where this hits |
|---|---:|---:|---|
| 0.01% CVR | $78M | $28M | nobody at MNTN |
| 0.1% CVR | $7.8M | $2.8M | typical CVR floor (the "$2M wall") |
| 0.5% CVR | $1.6M | $553k | high-CVR commerce |
| 1% (low IVR) | $777k | $275k | low-traffic verticals |
| **2% IVR** | **$385k** | **$136k** | **typical IVR / cohort median (the "$200k visits" rule)** |
| 5% IVR | $149k | $53k | high-rate advertisers |
| 10% IVR | $71k | $25k | very-high-rate (e.g. WGU) |

**Adjustment rule (linear):** for an advertiser with non-default CPM or imps/IP, multiply the table value by `(advertiser_cpm / 25) × (advertiser_imps_per_ip / 10)`. Example: 1% IVR, $35 CPM, 15 imps/IP, post-stack → $275k × 1.4 × 1.5 = ~$578k.

### When to use which method

- **Forward direction** (`mde_binomial` / `mde_continuous`): given an advertiser's current sample size and rate, what's the smallest lift we can detect today? This is the screening-rule check.
- **Inverse direction** (`spend_required` / `n_required_binomial`): given a target MDE we want to promise, what's the minimum spend? This is the "should we recruit this advertiser into TI-885 / a new pilot?" check.

Both directions live in the same calculator file. The variance-reduction stack (post-stack `var_reduction=0.595`) is canonical and should be used as the default post-stack value across all calls.

**See also:** [TI-917 combined deck](../tickets/ber_2250_incrementality_overhaul/ti_917_combined_loom/artifacts/ti_917_combined_deck_standalone.html); [revenue MDE per advertiser](../tickets/ber_2250_incrementality_overhaul/ti_917_combined_loom/outputs/ti_917_revenue_mde_per_advertiser.csv).

<!-- ti_1019: 2026-06-24 -->
### Canonical MDE baseline-rate definition — and the `graph.visits` numerator trap

The MDE engine is a **two-proportion binomial** power calc (`mde_binomial`): σ = √(p(1−p)), MDE_rel = MDE_abs/p. The unit of analysis is the **advertised IP** — each served IP is a Bernoulli trial (visited yes/no). So `p` **must be a per-served-IP probability in [0,1]**, not a rate-per-IP of an event count.

**Canonical baseline (what the team tool + per-advertiser prefill use):**
```
IVR = COUNT(DISTINCT served IP with ≥1 verified visit) / COUNT(DISTINCT served IP),  trailing 30d
CVR = COUNT(DISTINCT served IP with ≥1 conversion)     / COUNT(DISTINCT served IP),  trailing 30d
```
Numerator is distinct visiting/converting IPs **intersected with served IPs** (LEFT JOIN on `(advertiser_id, ip)` in `ti_xxx_advertiser_prefill_metrics.sql`); denominator is distinct served IPs from `cost_impression_log`. Both are distinct-IP counts at the **same grain (raw IP)**.

**The trap (load-bearing):** do **not** use `graph.visits` as the numerator. `graph.visits` is an *event count* of verified visits (catalog: it counts repeat visits; `graph.SiteVisitors` is the unique-IP metric). For WGU (31357), trailing 30d, fresh pull 2026-06-24:

| Numerator choice | Value | /15.73M served IPs | vs canonical |
|---|---:|---:|---|
| `graph.visits` (events) | 5.66M | **35.95%** | 3.36× too high |
| distinct visiting IPs (all, not ∩ served) | 1.94M | 12.3% | ~15% too high |
| **distinct visiting ∩ served IPs (canonical)** | 1.68M | **10.70%** | ✓ baseline |

Each visiting IP fires ~2.9 visit events. For the full derivation of why this is anticonservative (inflated p → smaller σ → reported MDE ~2.16× too optimistic), see the "Corollary for MDE / power-calc baselines" note above (under the "same unit of analysis" rule).

**Premier-UI / gary-ql matching (Chris Franz PR #4445, `Advertiser.mdeInputs`).** The customer wizard historically used `conversions / first_party_audience` — wrong denominator *and* sparse numerator (WGU 2.2% vs our 10.3%). To match the team tool: (1) keep `usersReached` (distinct served IPs) as the denominator — cross-check via imps/IP (WGU 24.7 ≈ our 22.5 confirms same grain); (2) numerator = distinct served IPs that visited, **not** `graph.visits`; (3) default to IVR, gate CVR behind a power warning (usually underpowered — the "$2M wall"); (4) keep numerator/denominator at the same grain (don't mix IP and household — cf. TI-1044, 2.83% same-IP overlap); (5) reconcile `var_reduction` (resolver uses 1.0/raw; standalone shows raw + 0.595 post-stack — show raw-only in the UI and label it).

**Premier-UI source update (2026-06-24): the R2/graph columns are at a DIFFERENT IP grain than our calculator.** What R2 can actually pull is `graph.sitevisitors`/`graph.usersreached` = `summarydata.all_facts.site_visitors`/`uniques`. **`uniques` (= `graph.usersreached`) is built from the SAME served `cost_impression_log` our calculator uses, but with a channel-conditional key — NOT `device_ip`, NOT a different table** (verified from the SQLMesh model + BQ reconstruction; see data_catalog `impression_facts`/`all_facts` entries). `uniques` = `HLL_COUNT.INIT(CASE WHEN channel_id=8 OR objective_id IN (5,6) THEN ip ELSE guid END)`: CTV/video counted by `ip`, display counted by `guid` (cookie). For WGU trailing-30d: `site_visitors/uniques` = 1.90M/32.1M = **5.92%** vs our `count(distinct ip)` per-served-IP IVR = **10.70%**. The 2× is **display counted by cookie/guid (~18.4M, ~2.4× IP fan-out), not a different/broader universe** (CTV leg = 14.06M ip ≈ served CTV ip). So `graph.usersreached` mixes IP + cookie namespaces and over-counts display — wrong denominator for a per-IP baseline. So `sitevisitors/uniques` does **not** match the team tool; shipping it would roughly halve the baseline and ~double the reported MDE. Note `(2)` above (the `graph.visits` event-count trap) is moot if they use `site_visitors` (already distinct), but the **IP-field grain** problem replaces it. Also likely internally grain-mismatched: numerators are ~equal (1.90M ≈ 1.94M) while denominators differ 2×, so `site_visitors` appears resolved-IP-grained while `uniques` is raw-device_ip — confirm how `site_visitors` is keyed. **RESOLVED 2026-06-24 — the experiment unit is the resolved `ip`, so our calculator is correct.** Applied the production holdout hash (`MD5(advertiser_id:ip)` → bucket, 0–99 = holdout; canonical BQ port in `ti_837_augmentor_holdout_bucket_verification.sql`) to WGU's *served* IPs in `cost_impression_log`: **0 of 2,356,886 served IPs landed in holdout buckets** (≈10% expected if the holdout used a different field; chance of 0 if uniform ≈ `0.9^2.36M` ≈ 0). Since `MD5(aid:resolved_ip)` ⟂ `MD5(aid:device_ip)`, this proves holdout + serving suppression run on the resolved `ip`; `clickpass_log` (VV attribution) keys on the same resolved `ip`. So the MDE baseline denominator must be the resolved served-`ip` count (15.7M → 10.70%); `graph.uniques` (raw `device_ip`, 32M → 5.92%) is the wrong grain and understates ~2×. **Implication for the UI fix:** R2's graph table gives the right numerator (`site_visitors`, resolved-IP) but NOT the right denominator (`uniques` is device_ip), so the baseline must be sourced from the `cost_impression_log` grain (our calculator) or data-eng must add a resolved-IP served-unique to the reporting table. **Zach Schoenberger (authority) confirmed the mechanism:** holdout and VV are two separate sides — holdout = targeting (done on the IP in the targeting system = the served event-log `ip`); VV = attribution (no md5; matches event-log `ip` to guid-log `ip`). Both operate on the resolved event-log `ip`; neither uses `device_ip`.

**Deeper caveat (applies to both tools equally; methodology, not a matching issue):** this served-arm clickpass IVR is the *observed* visit rate among the exposed — fine as a screening magnitude, but it is **not** the holdout/unexposed baseline an incrementality test measures lift against. VV-attributed (clickpass) visits are structurally ~0 for never-served holdout IPs, so the true control-arm rate is far below the served-arm rate. For an honest incrementality estimand the binomial `p` should be the holdout's **total-traffic** visit rate (guid_log), where holdout ≈ served (TI-835 showed ~0% total-traffic lift). Worth formalizing before the customer-facing forecast is final.

## TI-933 — Per-impression attribution window (lesson learned)

**Issue surfaced 2026-05-07** while building the Select lift deck.

In the TI-917 / TI-933 ATT lift methodology, the visit window is set as a **fixed calendar range** (e.g., 2026-04-29 → 2026-05-08 for the Select 7d analysis: 7-day impression window + 3-day post-period). The query then attributes any visit within that calendar range to the impression.

**The unintended consequence:** each impression gets a *different* number of attribution days depending on when in the impression window it served:

| Impression date | Days available for visit attribution |
|---|---:|
| Day 1 of impression window | up to 9 days (full +3 post-period plus the rest of the impression window) |
| Mid-window | ~6 days |
| Last day of impression window | exactly 3 days |

This **does not bias the lift estimate** — both treated and holdout arms are evaluated against the same calendar window, so the asymmetry cancels in the difference. But it's methodologically inelegant and makes the per-impression "attribution window" claim ambiguous when explained to non-statistical audiences.

**Fix for next time:** instead of a fixed calendar visit window, give every impression a **constant per-impression lookahead** — e.g., 3 or 7 days starting from the impression's own timestamp. Possible implementations:

1. **Per-row attribution:** join cost_imp_log to visit logs with a temporal predicate `visit.time BETWEEN impression.time AND impression.time + INTERVAL 3 DAY`. Slightly more complex query, fully consistent attribution per impression.
2. **Cohort-style binning:** bin impressions by day, run separate attribution per cohort, average. Simpler but gives less per-impression precision.

Option 1 is preferred — same SQL pattern works in both BQ and Spark. The win-rate denominator subsampling needs to be revisited in this design (probably stays per-(advertiser, IP) but now we're double-attributing IPs that got served on multiple days; need to dedup).

**For the holdout arm:** the equivalent is "if this IP had been served on day X, would it have visited within X+3 days?" This is harder to define without a counterfactual impression timestamp. Two reasonable choices: (a) treat the entire visit window as eligible (current behavior), or (b) randomly assign each holdout IP a "would-have-been-served" date drawn from the impression-day distribution and apply the same +3 lookahead. Option (b) makes treated and holdout fully symmetric on attribution-window length.

**Why we shipped without the fix:** the asymmetry doesn't bias the result, deck timeline pressure, and Victor's Spark run was already well-optimized. Logging this so the next iteration (likely once ghost-bidder lands) gets it right.

<!-- slack-extracted: 2026-05-09 -->
- **Select Campaign Incrementality Analysis (TI-933)**

A quasi-experimental lift analysis was run on Select-only campaigns with the following findings:

- **Estimated average lift:** ~1.5–3 percentage points (pooled average), subject to advertiser. Zazzle was a notable outlier at +11.6 pp.
- **3-day window result:** >2.0% lift on pooled average. Likely an underestimate due to the short conversion window.
- **14-day follow-up:** Confirmed positive lift, slightly lower than initial estimate. Variance attributed to unequal conversion windows across impressions in the first pass.
- **Why Select may be more incremental:** Select campaigns are not subject to MNTN's standard targeting bias, so enhanced incrementality is expected.
- **Methodological caveat:** A 3-day window was used due to compute limitations. The 14-day follow-up capped conversion/visit windows at 7 days consistently, but the most recent impressions had not yet had the full 7-day window at time of analysis.

**Ideal experimental design for a definitive lift measurement:**
1. 3–4 week ramp-up period for new campaigns to reach steady state
2. 4 weeks of control/treatment measurement
3. 30-day post-experiment period so each impression gets an equal 30-day conversion window
- Total: ~60–90 days end-to-end

**Note:** Results are not yet ready for client-facing use. The incrementality ascent team will gather and verify more data before publishing findings. Ghost-bidding infrastructure, once implemented, will enable long-term lift analysis on virtually any campaign type by saving every bid. (via malachi, #incremental-lift-stakeholders, 2026-05-08)
- **Incrementality Experiment Power Analysis — Spend Thresholds**

For IVR-based incrementality measurement, reaching statistical power requires approximately **$200K/month in spend** when measuring over a single month. This threshold is driven by the typically low lift percentages observed — when lift is small, very large sample sizes (impressions/spend) are needed to achieve significance. Results will sometimes reach significance and sometimes not at this threshold; it is not a guarantee. (via malachi, #incremental-lift-stakeholders, 2026-05-08)

<!-- slack-extracted: 2026-05-30 -->
- **Fangorn for Conversions Experiment — Prerequisites and Design Considerations**

Before the Fangorn-for-Conversions experiment (EX-84) can begin, two TI-side tasks must be completed: (1) TI-1005: build a Vertex-based pipeline for the conversion model; (2) TI-1006: implement ROAS-based scoring capability for specific advertisers identified in the experiment (same functionality as the prior Fangorn experiment). Both tasks are estimated to be completable in one sprint by a DS/ML and DE resource.

Experiment design considerations specific to conversion-outcome experiments:
- Minimum experiment duration: 3 weeks per campaign; 4 weeks is the preferred floor.
- Longer durations affect how many advertisers can be included in the experiment cohort.
- A Power Analysis dashboard is in development to identify advertisers that can reach significance at different spend/time/lift thresholds, reducing the need to compromise on experiment duration. (via Alex Knorr, #dev_fangorn-model_ex, 2026-05-28)
- **Experiment Advertiser Selection Error — Identity Core Exclusion Rollout**

A methodological error was made during advertiser selection for the Identity Core exclusion experiment: 50 advertisers were selected into the experiment and then split 50/50 into treatment and control, resulting in only 25 treatment-group advertisers. The correct approach was to select 50 advertisers into the treatment arm. The resulting sample is sufficient to detect only larger effects. Correction requires re-submitting the advertiser list to PEX (partner/external team) for re-approval, adding at least one day of delay. Lesson: when designing tiered rollout experiments, clearly distinguish between "experiment inclusion list size" and "treatment arm size" — the inclusion list should be sized to the treatment arm target, not the full experiment. (via Alexander Jerneck, #identity_core_dev, 2026-05-29)

<!-- TI-1044 2026-06-23 -->
## Ghost-ad lift: ATT (served-vs-ghost) carries win-selection bias — use clean ITT (TI-1044)
When measuring ghost-ad/holdout lift, **served-vs-ghost is NOT clean**: "served" = auction WINNERS (the
bidder's highest-value households), "ghost" = a pre-auction random holdout (ghost bids are logged at
would-have-BID, not would-have-WON — Ryan Kleck). So served > ghost partly because winners are higher-value,
not because of the ad → **ATT lift biased UP**. (Plus Matt's frequency bias: ghost not freq-capped → control
over-represents high-frequency IPs → biased down. Opposing, non-cancelling.)
**Fix:** compute the **clean ITT** = targeted-and-bid (`threshold_failure_reasons IS NULL/''`) vs ghost-holdout,
both pre-auction random partitions → removes win-selection (lift is diluted by win-rate but unbiased).
TI-1044 ElevenLabs: ATT conversion lift +35% (p<.001) → **ITT −2% (NS)** — the +35% was entirely win-selection;
the true incremental conversion lift is ≈0, matching the vendor's geo test. Always report ITT (or win-rate-
corrected ATT à la TI-837/TI-933), never raw served-vs-ghost, as the incrementality number.
**Also:** clickpass (attributed) lift hugely overstates (TI-1044 +143–276%) vs guid_log total traffic — always
pull guid_log for the true visit-incrementality signal (north star / TI-835). Ghost holdout source for Beeswax
advertisers = `bronze.raw.bid_price_log` (`threshold_failure_reasons='ghostBid'`), live 2026-05-27, 10-day TTL.

## Ghost-bid lift — bias register + the persuadables gradient (Matt Brorby, databricks_targeting `INCR`, 2026-06)
Source: `SteelHouse/databricks_targeting` → `exploration/INCR-first-ascent/ghost_bid_lift_bias_register.md` (the most complete catalogue of ghost-bid lift biases A1–A8/B/C/D/E/F — read it before any ghost-bid lift analysis). Beeswax leg, interim; signed magnitude pending the MNTN fcap-symmetric leg (INCR-63). Key durable findings:

- **The dominant bias is `ghost_frac` bid-MULTIPLICITY selection, NOT frequency-cap asymmetry (re-diagnosed 2026-06-23, supersedes the "Matt's frequency bias" framing above).** A holdout IP never wins → never marked reached → never exits the prospecting pool → re-enters auctions repeatedly → over-accumulates qualifying bids → concentrates in high-bid-count buckets, inflating the distinct-IP holdout fraction (`ghost_frac` 0.10 at 1 bid → 0.47 at 11+ bids) → manufactures a **spurious NEGATIVE lift**. (fcap-reconstruction was falsified: fcap is config-OFF for prospecting advertisers — `bid_price_log.threshold_failure_reasons` has no fcap tokens for obj=1.) **Fix: gate to clean `ghost_frac` (.09–.11)** — every prior "Beeswax negative lift" then vanishes (e.g. High band z−4.68 → z+0.12).
- **Measurement fix — single first-bid anchor.** Carrying `visited` as `MAX()` over per-day-anchored 7d windows gave holdout IPs (which bid on more distinct days) a wider observation window (union of windows) → spurious negative (~6.7pp of it). Use the IP's **earliest-bid** single 7d window (`ARRAY_AGG(visited ORDER BY dt LIMIT 1)`), dose-invariant across arms. Distinct-IP dedup alone is necessary but not sufficient.
- **Bias-floor at scale — read magnitude, not z.** At 100M+ IPs a ~0 magnitude (+0.001pp) yields z 5–13; adding days raised z (5.5→7.5) but NOT magnitude → the wall is bias, not power. Report absolute pp + per-campaign FDR; never headline the pooled z.
- **The persuadables gradient (population-wide, all advertisers, clean-gf, 2026-06-25) — refines the earlier "no intent gradient" read** (there was no gradient *in the per-campaign artifact*; pooling 100M+ clean IPs surfaces a small real monotonic one). Incremental visit lift (rel) by intent band: **High +0.2% (~0) · PP +1.6% · Mid +3.3% · MaxReach +3.4% · no_score +0.1% (~0)**. So **mid-intent (PP/Mid/MaxReach) carry the lift; top-intent (High) and untargeted reach (no_score) are incrementally dead.** Note no_score has a *higher baseline visit rate* (0.96%) than PP (0.57%) — would-visit-anyway, not incremental (the "no_score looks good on raw visits but is incrementally dead" trap; WGU 31357 is 100% no_score). Cross-advertiser-saturation cut agrees: most-contested IPs (21+ other advertisers bidding) are incrementally dead (+0.1%) despite the highest baseline — same households as the High band. **Targeting implication: incremental opportunity = mid-intent × less-saturated IPs; avoid top-intent / heavily-cross-targeted / untargeted-reach.** This is population-data backing for the perf-vs-incrementality opposition (see "Optimizing for incrementality and performance are partially opposed" above) and the TI-999/TI-956 mid-intent recs. Directional sizing only (z is N-inflated; signed magnitude needs the MNTN leg).
- **Score bands collapse to ~1–2 effective bands per advertiser** (intent-gated advertisers populate only top bands above their HHST; reach advertisers are 100% `no_score`) and `ghost_frac` is **flat across bands** (arm ⊥ band) → within-campaign band standardization is inert for bias-correction. For real heterogeneity use advertiser-relative quantile bands or the simplest robust split {scored, no_score}.
- **MNTN bidder leg = the clean reference.** The MNTN bidder writes ghost bids into the fcap cache (`apply_ghost_bids`) so holdout IPs accrue counterfactual frequency and exit symmetrically → multiplicity equalizes. Beeswax applies the ghost gate last and never writes to the fcap cache → residual multiplicity bias. DiD fingerprint: Beeswax skews negative (18.9%/11.5% sig), MNTN symmetric (14.3%/14.3%).
- **Verdict:** pooled lift ≈0; clean-gf per-campaign FDR has **zero** significant negatives + a one-sided-positive tail → the real Beeswax effect is **non-negative (zero-to-positive)**, never the A1 negative — but signed magnitude is still pending the exchangeable-arm MNTN leg. Report the bid-grain ITT (exchangeable at t=0) → scale to ATT by win rate `w_c`; DiD is corroboration only (no pre-period at t=0 for a real launch).
- **Reconciled headline (2026-06-05):** lift ≈ 0 on BOTH bidders (null + underpowered, NOT negative); conversions also ≈ 0 (pooled +0.06%). Binding limitation is **power + visit-window truncation, not residual bias** — the rigorous confirmation of the north-star "guid ≈ 0% lift." **Publish gate (INCR-69):** until a powered, fully-observed MNTN-leg run lands, the only defensible statement is "no significant lift detected; effect indistinguishable from zero; underpowered" — do not publish a non-zero point estimate.
- **The holdout is a >1-YEAR always-on advertiser-level randomized control** (the 10% `MD5(advertiser:ip)<100` hash has run >1yr; only ghost-bid *logging* started 2026-05-27) → treatment−holdout = long-run cumulative lift; validity from randomization, not a pre-period. **Negative control = visits to OTHER advertisers**: clean ~0 certifies exchangeability (covariate-balance battery: arms balanced on everything except focal-advertiser bid dose).
- **Rare-outcome reporting:** for conversions, gate per-campaign `rel%` behind ≥20 holdout conversions (NaN-drop + small-N manufacture a spurious conv positive) and lead with z; always read **median AND mean** (divergence = outlier vs broad bias); the clean-gf z-distribution ≈ N(0,1) is the strongest "true-zero" test.

### Ghost-bid lift is queryable in BQ — MNTN-leg tables + debias recipe (INCR-75, 2026-07-02)
Matt's ghost-bid lift is exposed as **BigQuery SQLMesh views** (no Databricks needed for the ITT read):
`dw-main-silver.enriched__dev_matthewbrorby.lift__ghost_bid_visits` (outcomes: `arm` ∈ {`ghost`=holdout, `submitted`=treatment}, `visited`/`converted`/`won` bools, `first_bid_time`, `bid_count`, `household_score`) and `…lift__ghost_bid_audiences` (exposure only). An identical INCR-66 copy lives in `enriched__dev_mbrorby_incr66.*`. Both resolve to `sqlmesh__enriched.enriched__lift__*`.
- **Rolling ~10-day window, TTL-capped** (source `bid_price_log` = 10-day TTL). Logging live 2026-05-27. So calendar-day coverage per advertiser maxes at ~10 — **a "≥30 days in-table" filter is impossible until a daily snapshot accumulates or the TTL is extended.** ~1,180 advertisers present; each ~1.08B rows/full-scan ≈ 30–45 GB.
- **This IS the MNTN (clean-reference) leg, empirically confirmed.** `ghost_frac` by bid-multiplicity barely moves: **0.095 @1 bid → 0.103 @10 → 0.116 @11+** (vs Beeswax 0.10→0.47). So the clean `[.09,.11]` gate keeps bid-buckets 1–10, drops only 11+.
- **Debias recipe (both corrections needed):** (1) gate to clean `ghost_frac` (bid-multiplicity ≤10); (2) **single earliest-bid anchor** for `visited` — `ARRAY_AGG(CAST(visited AS INT64) ORDER BY first_bid_time LIMIT 1)[OFFSET(0)]`, NOT `MAX()` over the window (MAX unions per-day 7d windows → holdout gets a wider window → spurious negative). Dedup to advertiser×ip×arm first (arm deterministic per adv×ip). The anchor fix does most of the work.
- **Validated sign flip:** pooled abs visit lift RAW (MAX, all bids) **−0.034pp (z−27)** → DEBIASED **+0.049pp (z+33)**. Per-advertiser two-proportion z-test; publish-gate significant only at p<.05 AND ≥20 holdout clean visits. **Read magnitude/relative-lift + direction, NOT z (N-inflated at millions of IPs/advertiser).** Absolute pp are bid-grain ITT (diluted by win rate → scale by `w_c` for served-user ATT). 7d window truncates for late-first-bid IPs inside the 10-day span.
- Face validity: Zazzle debiased +18% ≈ its prior +11.6pp Select lift. Canonical replication: `tickets/incr_75_eligible_advertisers/artifacts/incr_75_debiased_lift.py`.

**SUPERSEDED / corrected by Matt Brorby's entry-cohort method (INCR, 2026-07-02) — use this instead.** The "earliest-bid anchor + bid_count≤10 gate" above is directionally right but flawed: on a rolling window the earliest-bid anchor lands most IPs on **day 1 of the window (the left edge)**, which is a **left-censored STOCK** (everyone active before the window first-appears there) that over-represents accumulated holdout IPs → inflated ghost_frac and a large **spurious NEGATIVE**. Matt's cleaner recipe:
1. **Entry-cohort anchor at (advertiser, campaign, ip) grain:** `ROW_NUMBER() OVER (PARTITION BY advertiser_id, campaign_id, ip ORDER BY dt)`, keep `rn=1`; use that entry row's `visited`. Group by `entry_dt`.
2. **Drop the left edge** (first window day) — it is the left-censored stock, not a flow. Empirically ghost_frac there ≈ 0.12–0.13 (vs 0.10 design) and lift is spuriously negative.
3. **Keep only fully-observed entry cohorts** (`entry_dt ≤ max_dt − visit_window`; the baked-in `visited` uses a ~7d window, so with max_dt 07-01 that means `entry_dt < 06-24`). Later cohorts are **right-truncated** — their 7d visit window runs past the data end, so VR decays mechanically by entry day.
- **Consequence for a 10-day window:** exactly ONE clean interior cohort survives (06-23). Pooled clean-cohort visit lift = **+2.8% rel (z=7.8)** — positive, real, modest; the honest current read. The left edge (06-22) alone is **−18% (z=−61)** — that single day was driving the raw negative. Per-advertiser this is THIN: only ~376 advertisers are powered (≥20 holdout visits) on the one clean day vs 619 in the (contaminated) 10-day pool; of a prior "positive_sig" set of 89, only 34 stay clean-day-significant-positive, 1 flips, the rest go null (power, not contradiction).
- **To get more clean data now** (before the window ages): use a **short fixed visit sub-window** via `first_visit_time` (e.g. visited-within-K-days = `first_visit_time ≤ entry_dt + K`, K small enough that all included entry cohorts are fully observed) — lets many interior entry days count under a consistent, un-truncated window. Not yet implemented; confirm exact K/approach with Matt. Canonical: `tickets/incr_75_eligible_advertisers/artifacts/incr_75_clean_cohort_compare.py`; Matt's per-day diagnostic SQL is in the ticket `queries/`.

### HI pool / pacing analysis — pace against the LIVE 30d pool, not the lifetime stock (AUDI-1070)
When sizing sustainable HI spend or diagnosing HI exhaustion:
- **Pacing denominator = the LIVE trailing-30-day distinct-HI-served series (RTC-excluded), NOT cumulative lifetime distinct-HI.** The lifetime figure is ~2x the live pool and overstates capacity. A daily trailing-30d series sharpens crossing-date alignment vs monthly snapshots.
- Compute **new-HI inflow/day** (first-seen HI IPs). Sustainable HI spend ≈ inflow/day ÷ new-share ÷ reach-per-$.
- **Ceiling-month triangulation** (report where signals converge): (1) cumulative distinct HI crossing the nominal pool size; (2) brand-new SHARE of reach falling while TTL-refresh share rises ('running on refresh'); (3) reach-per-$ rolling over; (4) matched-spend months showing higher frequency / lower reach-per-$.
- **Separate supply-constraint from gate-cause (matched-gate comparison):** compare reach-per-$ and frequency at a MATCHED strict gate across the two periods. Flat reach/$ + flat frequency despite higher spend ⇒ no supply degradation ⇒ the GATE (not the pool) drove the decline.
- Caveats to state every time: CIL = served IPs only ⇒ pool figures are lower bounds; distinct-IP overcounts households (CGNAT/DHCP) ⇒ true tightening earlier/sharper; 'would have exhausted' is a counterfactual confounded by any in-window gate change.

### Gate event-study + constant-spend swing test — attributing composition swings (AUDI-1070)
When a delivery-composition / HI-share shift needs a cause, the HHST intent gate is the dominant suspect — thrashed daily (dozens of changes/quarter on active prospecting), so treat it as a first-class confound alongside algorithm-version changes.
- **Gate event-study:** join daily HI-share (from CIL) to gate-change events in `silver.archives.household_score_threshold_archives`, lagged ~1 day (propagation). Look for step-changes in HI-share the day AFTER each gate flip; correlation ≈ 1.0 confirms the gate — not supply or model degradation — drives the swings. Steep/sudden drops are gate flips; gradual drift would be reach/pool decline.
- **Verify gate binding:** in windows where the gate held at 10000 for many consecutive days, count the exact `household_score` distribution per day — expect ~99.99% at exactly 10000 on the gated path. SPLIT by serving path (gated prospecting vs RTC); RTC bypasses the gate and looks like a leak if not separated.
- **Constant-spend HI-swing test (supply vs budget):** hold spend roughly constant across two sub-periods and compare HI-share. Large swings at constant spend ⇒ the binding constraint is HI SUPPLY, not budget.

### Lookback-window heterogeneity is a hidden confound when pooling advertisers (AUDI-1070)
Before pooling advertisers for a YoY / cohort / DiD visit-rate or conversion analysis, confirm they share the same lookback window. Per-advertiser windows differ (conversion default 30d, but 90d page-view is common) — see `silver.audience.advertiser_configurations` (`page_view_lookback`, `conversion_lookback`). A mixed 30d-vs-90d set makes VR/conversion LEVELS non-comparable across advertisers and can distort the effect. Also reproduce any decline under BOTH first-touch (`industry_standard`) and last_touch, and resolve the lens as-of BOTH years (advertiser_setting_archives) — a lens/window mismatch alone can manufacture a large apparent decline.
