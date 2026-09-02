# Experimentation & Causal Inference — Knowledge Base
Last updated: 2026-09-02 | Started from TI-748 (Media Plan Causal Impact)

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
- **Table choice (avoid a silent truncated pre-period):** for any pre-period reaching before 2025-09, pull from `summarydata.sum_by_campaign_by_day` / `sum_by_advertiser_by_day` (history to 2024-01-01, working uniques). Do **not** use `aggregates.agg__daily_sum_by_campaign` — it is frozen to 2025-09-01…2026-04-30 with empty reach/uniques, so a long lookback silently starts partway through. (`_ROUTING` keyword **long pre-period**.)

### p-value computation (both methods)

**DiD (cluster bootstrap):** `p = 2 × min( P(boot ≥ 0), P(boot ≤ 0) )` from the empirical distribution of N=1000 resamples.

**CausalImpact (simulation, posterior-predictive analogue):** draw N=2000 sample paths from the fitted UCM's forecast distribution via `res.simulate(nsimulations=n_post, anchor="end", repetitions=N, exog=X_post)`. For each path, compute the average counterfactual over the post-period — that yields a distribution `avg_cf_dist`. The effect is `actual − counterfactual` (actual is observed/fixed). 95% CrI = `np.percentile(actual - avg_cf_dist, [2.5, 97.5])`. `p = 2 × min(P(avg_cf ≥ actual), P(avg_cf ≤ actual))`.

**Why not normal-approximation SE?** *(updated 2026-06-03 after TI-961 methodology review)* The hand-rolled formula `SE = (avg_upper − avg_lower) / (2 × 1.96)` from per-day forecast bounds was wrong in three compounding ways: (a) it's the average per-day SD, not the SD of the post-period MEAN — missing the 1/n scaling and ignoring strong positive cross-day covariance of a local-level forecast; (b) `rel_CI = actual/bound − 1` has no distributional basis and explodes when the bound nears zero (literal source of TI-961's +681% upper bound on Tier 1 IVR); (c) Gaussian z-test layered on a skewed ratio compounds (a) and (b). Simulation carries the real covariance structure, can't explode, and makes no normality assumption.

### Future-state framework (not yet built)

This protocol will eventually live in a Python package `mntn_experiment_eval/` with config-driven runners. Until then, copy the canonical implementations referenced above into each new ticket's `artifacts/` folder. **As we build new experiments, capture any patterns that don't fit this protocol in a new subsection here — that's how we discover what the framework needs.**

### The standardized Causal Impact dashboard (config-driven, operationalizes step 5)

The productionized version of this protocol. A Mode dashboard that measures **"what did a BETA rollout actually cause?"** for MNTN features shipped as a wave-flip (no in-experiment holdout) rather than a controlled A/B test. **Onboarding a new experiment = seed one config row, no code change.** Full statistics walkthrough: [`documentation/docs/HOW-IT-WORKS.embedded.md`](../documentation/docs/HOW-IT-WORKS.embedded.md).

- **Unit of analysis = advertiser.** A per-feature **scope recipe** resolves in-scope `campaign_group_id`s once at seed time; metrics are summed across all relevant campaign groups to an advertiser×day series (also smooths short-flight advertisers into one cohesive line).
- **Architecture:** config tables in `dw-main-silver.experiments.*` → 7 parameterized SQL panels (UI-faithful attribution) → ~23-cell Python notebook (statsmodels + numpy/pandas, **no `causalimpact` library**) → one self-contained HTML file reading notebook outputs as Mode "datasets." Three color-coded views: **BSTS (teal)**, **DiD (violet)**, **3-Point (amber, methods-agreement: naive vs BSTS vs DiD)**.
- **Config table `dw-main-silver.experiments.causal_experiment`** — 1 row/experiment, selected by `Experiment Key`; holds all knobs so a team tunes per-rollout with no dashboard code change. Key knobs (defaults): `ci_pre_days`=60 (BSTS pre-history), `did_pre_days`=14 (DiD before-window), `bsts_harmonics`=2 (weekly seasonality), `maturity_days`=14 (settle window ≈ conversion attribution), `min_window_impressions`=1000 (volume floor, treated **and** control), `pre_active_window_days`=25 (**decoupled from `ci_pre_days` 2026-07-09** so an advertiser active only early then dark near flip fails the gate), `measurement_end_date`=NULL (NULL=live; a date freezes the whole experiment for reproducibility). Sim counts / `ALPHA` / VIF threshold stay in the notebook; BSTS seed derived per-experiment from the key.
- **Metrics:** IVR, CPV, CVR, CPA, ROAS — all five fit independently under both methods.
- **Eligibility gate is a single per-advertiser gate applied identically to BSTS and DiD** (cell `eligibility` → `ELIGIBLE_AIDS`), so both methods measure the same roster and 3-Point stays apples-to-apples. Gates: min pre/post impression volume, `min_pre_days`=14 active days within `pre_active_window_days`, `min_post_active_days`=1, `min_mature_post_active_days`=4 (dropped if "mature but dark").
- **Cohort pooling (staggered waves):** BSTS fits each cohort on its own flip date, aligns on a **flip clock** (day-since-flip), pools within-day by volume then averages days equal-weight (matches single-cohort recipe). DiD combines cohorts by **pooling counts, not rates** — `counterfactual_post = treated_pre_rate × market_drift × treated_post_volume` per cohort, then `Σ actual ÷ Σ counterfactual − 1`. **Clean-holdout control is the default** (fixed-membership yardstick; parallel-trends line uses the same static holdout). Bootstrap resamples advertisers (treated pooled across cohorts, control from whole holdout) 1,000×; small cohorts can drop from draws → widens interval, errs conservative.

This aligns with the protocol non-negotiables (SE/CI/p on both methods, clean-holdout control, methods-convergence as the strongest informal-causal argument). Extends the DiD/CI math canon in [`documentation/docs/causal_impact_did_math_reference.md`](../documentation/docs/causal_impact_did_math_reference.md).

---

## ⭐ Before you report a number — the null, the gate, the checklist

This applies to **every reported number, not just causal experiments** — a COUNT, a rate, a "184x", a "$274K/yr". The Standard Analysis Protocol above is for *"did this move a KPI?"*; this is the discipline for *"is this number real, and am I about to mislead someone with it?"* All three are cheap, unenforced, and high-leverage — the payoff is not shipping a confident wrong headline.

### 1. Write the null first (before you run the query)

Before writing the SQL, write two lines in the ticket `summary.md`:
- **Null:** what the number would be under the boring explanation (no effect / it's an artifact / it's mechanical). E.g. *"if keyword rank doesn't matter, top-vs-bottom VR ratio ≈ 1x."*
- **Signal:** what the hypothesis predicts. E.g. *"if selection matters, ratio ≫ 1x."*

**Why:** without a pre-committed null, every result confirms the hypothesis — a big number "proves the effect," a small one "proves it's subtle." Writing the null first turns the query into a *test* instead of a search for confirmation, and front-loads the power question: if the null and signal predictions are indistinguishable at your sample size, stop and do a power analysis (§ pipeline step 1) before spending the query. TI-804's "184x" persuades precisely because the null *"rank shouldn't matter → 1x"* was stated first; a 184x with no null is just a big number.

### 2. The Shocking-Number Gate

**Trigger: any number surprising enough that you'd lead a slide with it** — a huge lift, a suspiciously round result, a near-zero where you expected signal, an order-of-magnitude ratio. Before it leaves your hands, clear four gates:

1. **Triangulate — get it a second, independent way.** A different table, grain, or method that should land in the same ballpark. Methods convergence is the strongest informal-causal argument (Non-negotiables above); for a descriptive number, a second source is the equivalent. Can't reproduce it a second way → it's a lead, not a finding.
2. **Uncertainty on BOTH, always.** SE / CI / p on every point estimate, on both methods — never a bare DiD point next to a CI with full uncertainty. For a rate, carry the denominator: a dramatic-looking rate resting on 1 visit is a power statement, not a performance estimate (the DS24 sole-IP VR — 1 visit / 11,919 imps).
3. **Sign the bias — one sentence, in the summary.** Which way is this number wrong, and does that help or hurt the claim? (DS24 IPv4-only undercounts IPv6 ~20% → understates the vendor → *conservative for KEEP*.) A number whose bias you can't sign isn't understood yet.
4. **One adversarial pass — fresh context, "assume it's wrong."** For an expensive or headline result, have an independent reviewer (or a fresh-context agent) try to refute it before you present it. Cheap vs. shipping a wrong headline; on multi-TB scans it also catches cost/truncation bugs (AUDI-1089 patterns below).

First reflex for a shocking number is one of the existing adjudication checks: **impossibly-low rate → unconditional-activity check; outlier cohort → calibration buckets before disbelief** (see *"Adjudication patterns from AUDI-1089"* at the end of this doc).

### 3. Consolidated sanity checklist (scan before you report)

One place for the checks otherwise scattered across CLAUDE.md, `data_knowledge.md`, and this doc. Not all apply every time — scan, run what's relevant:

- **Null written first?** (§1) **Bias direction signed?** (§2.3)
- **Low-volume denominators removed** — weeks < 1,000 impressions produce extreme rates; filter them.
- **IPv4-only method?** IPv6 is ~20% of some vendor feeds (DS24), near-0 for others — an IPv4-keyed join undercounts *unevenly*. Sign the direction.
- **Epoch units per table** — spend_log=ns, bidder_bid_events=ms, bidder_auction_events=µs. A units slip is a 10³–10⁹ error.
- **Holdout = ITT** — `MD5('{AID}:{IP}') mod 1000`, 0–99 = holdout, per-adv per-IP; report intent-to-treat, not treated-only.
- **Stage from `funnel_level`, not `objective_id`** (objective_id is unreliable for stage).
- **Boundary-identity check** — assert every savings/attribution/cost model's degenerate cases against their known-exact answers (drop ALL → recovery = total; keep ALL → savings 0). See *"Adjudication patterns from AUDI-1089"* below.
- **Cohort algebra** — "sole" cohorts don't sum; a degenerate 100% → an alternative or an em-dash, never the trivially-true 100% (see *"Adjudication patterns from AUDI-1089"*).
- **Outlier advertiser?** WGU (AID 31357) is the **#1 advertiser by spend at 3.58x #2** — check whether one advertiser drives the result. (The long-standing "≈30% of MNTN spend" figure, Zach 2026-03-17, is **contradicted by measurement**: 8.75% of a full-universe 30d pull, 2026-08-27. Quote rank, not share, until settled — see `data_knowledge.md` § Notable Advertisers.)

The point isn't to run all nine every time — it's that a shocking number should never ship without a deliberate scan of the list.

---

## ⭐ Reconciling an experiment metric to a client-reported metric (the cost-per-incremental pattern)

**Reusable pattern (generalized from AUDI-1172 CPIV/CPIA, 2026-07-29).** When you need a cost-per-incremental
metric (CPIV, CPIA, CPI-anything) and the LIFT experiment counts the KPI on a DIFFERENT basis than the
client-facing Reporting number, **do NOT divide spend by the experiment's raw incremental count.** The
experiment's KPI (e.g. a fast pixel-fire proxy in a fixed short window) is tuned for clean lift measurement,
not for matching Reporting — it typically under- or over-counts vs the dashboard, by a DIFFERENT factor per
segment, so `spend ÷ experiment_incremental` is biased (on AUDI-1172 it inflated the losing arm ~3-4x and
manufactured a false "5x cheaper" headline).

**The fix — transfer the RELATIVE lift, not the raw count.** A relative lift is a ratio, so it is
basis-independent ("+22%" is +22% however you count the underlying metric). So:

1. Get the **relative lift** `L` from the experiment (for a cost-per-TOTAL metric use the **volume-weighted /
   raw-count pool**, `(Σtreated/Σn_t)/(Σholdout/Σn_h)−1`, NOT the IVW campaign-average — they answer different
   questions and can differ 5x).
2. Get the **client-reported metric** `R` (the exact dashboard definition — verify it against a source-of-truth
   reproduction; on MNTN, Verified Visits = `clicks+views+competing_views`, conversions = the analogous sum,
   reproduced to the dollar in AUDI-1070).
3. **Incremental (on the reported basis) = `R × L/(1+L)`.** The `/(1+L)` strips the organic baseline, since
   `R = baseline + incremental` and `L = incremental/baseline` (Matt Brorby confirmed this exact identity; it
   matches MNTN's customer-facing incrementality dashboard).
4. **Cost per incremental = spend ÷ incremental**, spend = actual metered billing scoped to the SAME cohort
   (match objective/product/window; a scope mismatch is a silent bias — e.g. include only `objective_id=1` if
   the lift cohort is prospecting-only).

**The one inherent caveat:** the lift is measured over the experiment's window (e.g. 7d) but applied to the
full-window reported metric — you assume the treated/holdout ratio is basis-invariant. This is unavoidable
(the holdout is never served, so it has NO reported metric to measure directly) and is the accepted approach.
**Verify each input against source before shipping** — this session, an intermediate attempt bridged via the
wrong column (`first_day..seventh_day_visits` = last-touch day-buckets, NOT the Verified Visit) and would have
shipped wrong; reproducing the exact dashboard definition caught it. See ticket `audi_1172_*` §8 +
[[reference_select_vs_nonselect_incrementality]].

## ⭐ Comparing self-selected groups is OBSERVATIONAL — and weighting can flip the story (AUDI-1172, 2026-07-30)

**Two traps when you compare outcome across groups that chose their own treatment** (e.g. "advertisers who
run Select vs those who don't", "accounts on feature X vs not"):

1. **It is associational, not causal — label it.** Advertisers self-select into Select, so a higher lift for
   Select-runners shows correlation, not that Select *caused* it (they may be bigger, more sophisticated,
   different verticals). The only causal quantity here is the *within*-advertiser holdout lift; the
   *across-group* comparison is a descriptive cut. Always state "observational, not causal" on the deliverable.

2. **The pooling weight can invert the finding when the group has a heavy tail.** On AUDI-1172, PTV-only
   advertisers pooled to **+0.2% IVW** (inverse-variance weighted → for proportions the weight ≈ sample size,
   so a few huge spenders, who are barely incremental, dominate) but **+12.4% for the median advertiser**. Same
   group, 60× apart. With a heavy tail (one advertiser 294× the median), **report BOTH the IVW-pooled and a
   median-advertiser number** — they answer different questions ("where does the incremental volume sit" vs
   "what is the median advertiser"), and showing both is the honest, robust read for leadership. **Label it
   IVW / inverse-variance — NOT "volume-weighted" (loose) and NOT "average-campaign" (implies equal weight
   per campaign, which is wrong: IVW is dominated by big/low-variance units, so "average" conflicts with
   "big spenders dominate" in the same doc — AUDI-1172, relabeled in review 2026-07-30)** — the two coincide
   for proportions but the method is inverse-variance, and a reviewer will (correctly) call out the loose label. Prefer the **median** over the
   mean for the per-advertiser number — per-unit relative lifts explode when a denominator (holdout rate) is
   tiny, which skews a mean but not a median. Exclude internal/test accounts and the single most extreme outlier.

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

**Rule: Exclude the first 4 weeks of any new campaign from causal analysis.** Empirically load-bearing: excluding the first 4 weeks of ramp cut the placebo false-positive rate from 30% to 24% in TI-748 v5.

This applies to:
- CausalImpact post-period start (shift 4 weeks after first delivery)
- Within-advertiser comparisons (only include campaigns with 4+ weeks of delivery)
- Any future experiment comparing new vs existing campaigns

### MDE shortcut correction — 2/√N is ~50% power, not 80% (TI-923)
The quick `MDE ≈ 2/√N` (the 95% CI half-width on a Poisson count) is a **~50%-power detection threshold, not the 80%-power MDE**. The standard 80%-power / α=0.05 two-tailed formula is `(z_{α/2} + z_β)·√(2/N) ≈ 4/√N` — roughly **twice** the shortcut. Using `2/√N` understates the real MDE by ~2x (e.g. 600 conversions/cell gives a real MDE ~16%, not the ~8% the shortcut implies). Always quote the 80%-power figure when telling someone "we can detect X% lift."

### Validating Production Holdout Enforcement Empirically (TI-837 Lesson)

When an experiment's identification depends on the production system enforcing a holdout (e.g., the 10% MD5(advertiser_id:ip) bucket for ghost-bidding lift), **don't trust documentation or asking — verify against served-IP data before publishing.**

**The check:** for every served IP in the treated arm, recompute the holdout-bucket assignment using the production hash. If holdout enforcement is real, **0% of served IPs land in the holdout bucket**. Any non-zero overlap means the bidder is leaking treatment into holdout, and your treated/holdout comparison is contaminated.

TI-837 result (2026-04-30): 0 of 5,432,546 served IPs across 8 (objective_id × funnel_level) cells landed in the holdout bucket. Holdout enforcement validated for both prospecting and retargeting.

**Adjacent check — audience-system coverage.** If the production system has multiple audience-evaluation paths, confirm the holdout enforces on all of them. TI-837: `audience.audience_segments` has both `expression_type=1` (OPM source representation) and `expression_type=2` (TPA, with embedded holdout JSON). Empirical: 0 of 64,202 type=1 retargeting rows have `is_targeted=TRUE` org-wide → only the type=2 path is ever live → there's no "OPM lane" that could bypass holdout. Always identify all evaluation paths and confirm `is_targeted` flagging before assuming uniform enforcement.

**Pattern, generalized:** before any production-experiment result depends on a system invariant (randomization integrity, holdout enforcement, eligibility gates), write the SQL that would falsify the invariant. If it returns the expected zero/ones, the result is defensible. If it doesn't, you've caught a methodology bug before the results meeting.

**Reconstruct the targetable audience externally — holdout IPs hide their own segment (TI-837).** Holdout IPs appear in `augmentor_log` at the uniform 10.0% rate, but their `mntn_segments` array does NOT include the segment they are a holdout of. You cannot read holdout membership off the served-side segment array; reconstruct the targetable audience externally, then intersect the holdout hash.

**Ghost-bidding lift scans are near-free to widen — batch them (TI-837).** The `augmentor_log` scan dominates bytes, so a 7-advertiser Stage-1 scan cost the same 18.2 TB as a 1-advertiser smoke test. Batch all cohort advertisers into one query rather than looping per advertiser.

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

### Concentrate a lift panel on eligible volume (TI-921)
For a Fangorn (or any audience-feature) lift read, restrict to campaign groups that actually carry the treated audience before measuring. The `mntn_matched_cgids` filter (campaign groups carrying an MNTN-Matched DS13/19/46 audience) drops ~25-45% of impressions but concentrates the panel on Fangorn-eligible volume, cleaning the lift signal. Diluting the denominator with never-eligible impressions shrinks and noises the estimate.

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

**The concentration threshold (TI-748, N=8, directional — not statistically confirmable):** ~90% publisher reduction (→ ~16 publishers) yields positive IVR; ~80% reduction (→ ~26 publishers) yields negative IVR. Threshold ≈ 88% reduction / 16-19 target publishers. All eight advertisers had 10-15x IVR variance between their best and worst publisher pre-adoption, which is why pruning the worst publishers moves IVR.

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
- **Old A/B split method (audience isolation) has contamination**: IP gets impression from one campaign → added to block list for other campaign. But processing delay means IPs can appear in both groups. New method: MD5 hash on IP → deterministic split into hash buckets → no bleed by design. All future experiments should use this. **⚠ Audience Isolation is STILL a live per-advertiser toggle in the product** (Audience > Exclusions > Audience Exclusions, alongside Converter and Site Visitor Exclusions), advertised in-UI as "enabling clean experimentation and accurate performance comparison". It is not clean. If an advertiser has it on, treat any campaign-vs-campaign comparison on that account as contaminated. See `data_knowledge.md` § "Audience Exclusions is an OPT-IN, prospecting-only UI surface".
- **Optimal cutoff exists between include-all and exclude-bad**: (Malachi's Etsy experience) Removing the worst items/keywords always decreased total ROI past a certain point — there's an optimal threshold, not a binary decision. Directly applicable to BUK keyword count thresholding.
- **Cost vs. performance differential is negligible between intent tiers**: Cost difference between high/mid/low intent is only a few percent, but visit rate difference is 10-50x. Should always bid on highest-value IPs first — continuous scoring enables this without sacrificing audience size.

### Keyword Analysis Methodology Lesson (TI-804, 2026-04-02)
- **Per-advertiser keyword analysis >> global keyword analysis**: Global keyword ranking shows only 3x visit rate range. Per-advertiser ranking shows 184x — a ~60x signal-strength gain attributable to BUK's per-advertiser ALS collaborative filtering (keyword value is advertiser-specific, not a universal keyword-quality score). Always analyze keyword performance per-advertiser, not globally.
- **"Best keyword rank" approach works better than per-keyword visit rates**: Computing visit rate per individual keyword is too sparse (most keyword-advertiser pairs have 0 visitors in a 10-day window, medians are 0). Instead, find each IP's best-ranked matched keyword, bucket by that rank, and compute visit rates per bucket. This aggregates signal effectively.
- **Temporal separation prevents circularity**: ipdsc DS19 keywords are from PAST browsing behavior. Measure visits in a FUTURE window. Same IP universe, different time periods. No campaign-scoping needed for "does the signal predict?" questions — campaign-scoping needed for "did our ads cause?" questions (TI-806).
- **50-advertiser sample is sufficient for directional findings** but only 15 had >10 visitors in a 10-day window. Scale to 500 for presentation-quality results.

### BUK Score Validation & Live Read (TI-797)
- **The DCG-score → visit-rate curve is monotonic at scale**: Independent BQ replication across 500 advertisers is perfectly monotonic over all 16 score bins, with 771x visit-rate lift in the top bin (score 0.95) vs bottom (0.20). At 50 advertisers the dips at 0.45/0.60 were sample noise; the full 5,699-advertiser run exceeds BQ resource limits (needs Databricks). Scale resolves apparent non-monotonicity before you distrust the score.
- **Signal holds per-advertiser, not just pooled**: all 7 BUK beta advertisers show large visit-rate lift for IPs scored ≥0.9 vs below (Experience Scottsdale 129x, Global Rescue 73x, Samy's 50x, West Bend 65x, Amsterdam Printing 101x, Apollo.io 1,152x, Apolla ∞). A per-advertiser check guards against an aggregate that's driven by a few advertisers.
- **Live pre/post confirms the size-performance tradeoff**: on the same campaign group, Samy's Camera (CG 104020, switch 2026-03-04) +55.2% IVR and West Bend (CG 107024, switch 2026-02-27) +137.4% IVR, both with a ~57-71% impression drop (Alex's Greenplum definition: +64% / +278%). Concentrating on the highest-scored IPs raises IVR but shrinks audience — report both, never IVR alone.

### Day-of-Week Effects in Visit-Rate Validation (TI-809)
- **Visit rate varies by day of week**: 0.84% (Fri) to 1.13% (Mon) — higher early week, lower Fri/Sat. Any pre/post or feature-ranking read spanning uneven weekday coverage must control for DoW (or align on full weeks) or the mix leaks into the estimate.
- **Sunday is a feature-ranking outlier**: Sunday 3/22 broke the otherwise-stable Spearman rank correlation (ρ = 0.10-0.41 vs other days); excluding it, mean ρ ≈ 0.90. Traffic composition on Sunday differs from Mon-Sat — check a suspicious low-correlation day against day-of-week before treating it as instability.

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
- **Feature set is stable — few features carry the signal (TI-789)**: In the larger pre-visit run, pre-visit features alone reach AUC ~0.896 (vs ~0.999 with feedback features — the leakage ceiling), and importance barely changes between 11 and 58 features (AUC stable at 0.896 ± 0.005). Adding features past the top ~11 buys almost nothing; separate pre-visit from feedback features first, then stop early.
- **Composite importance method works**: XGBoost with gain, weight, cover averaged into composite rank produces stable rankings. SHAP (mean absolute Shapley value) is preferred for final reporting — it captures per-prediction contributions, not just tree-structure importance.
- **Existing Fangorn signals dominate pre-visit features**: `ci_pct_new`, `n_wins_this_adv`, `al_avg_segments` hold top 3 ranks. New bidstream features (device diversity, content genre, clearing price) add incremental signal. When existing features are removed, new features still achieve AUC 0.777 (7x lift at top 1%) within the Fangorn-selected population.
- **Content genre rises with proper sampling**: With 1-hour augmentor/BAE samples, content features ranked mid-tier. With 4-hour augmentor + full-day BAE, `bae_pct_ent` jumped from #26 to #8. Sampling window materially affects count-based AND genre-percentage features. Use full-day data when possible; disclose sampling window when not.
- **Content genre is mid-tier for prediction but high-value for segmentation**: content_genre ranked ~8th for general visit prediction (up from ~25th with proper sampling), and is the best candidate for *vertical classification* (mapping IPs to advertiser categories). Different use case than IVR prediction — both valuable.
- **Scale matters for feature extraction**: augmentor_log = 241 GB/day (~43 GB for 4-hour sample), bidder_auction_events = ~400 GB/day. Always dry-run first. guid_log and win_logs are cheap (~13-75 GB/day).
- **fillna(0) vs NaN for ratio features**: XGBoost handles NaN natively. For ratio/percentage features, 0 is a real value (e.g., avg_price=0 means free inventory), while NaN means "no data." Preserving NaN for ratios and using fillna(0) only for counts is the correct approach.
- **Conversion-history features carry standalone IP-grain signal (TI-832)**: a conv-history-only XGBoost reaches test AUC 0.7485; folded into the combined model AUC is 0.8187 (+0.0097 over pre-bid-only 0.8090), 18.8x lift at the top 1% (60.6% conv rate vs 0.32% base). Real, but a small marginal add on top of pre-bid features — worth including, not a headline mover.
- **Not every requested feature earns its place — test SHAP before shipping (TI-832)**: device-class conversion counts (desktop/mobile/tablet) showed no measurable SHAP signal at IP grain (redundant with bidstream device features at bid time — same household/gear) and were dropped despite an explicit ask. Also, the (IP, advertiser) feature-pair grain is intentionally avoided in Fangorn/Fangorn V2 — features generalize to IP level so inference stays fast and scores all advertisers without per-request munging.

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

**Validating a model across a GRAIN CHANGE — champion/challenger is invalid (AUDI-1105, Fangorn-on-MNTN-ID, 2026-07-28).** When a new model scores at a *different unit* than the incumbent (here: household/MNTN ID vs IP), you **cannot** run a like-for-like champion/challenger — the two produce scores for different keys. The valid offline design: **roll the finer-grain (IP) model up to the coarser grain (household)** to build the baseline, score households with the new model, and compare **both on a shared coarser-grain label** (household visit from `guid_hh_log`) — per-vertical AUC/PR-AUC, audience-size deltas, and **rank correlation** (does the new model reorder units vs the rolled-up baseline?). **The label choice is itself a confound** — run a **label-sensitivity analysis** (roll-up-VV-through-graph vs HHID-native attribution models 31/33) and report whether the verdict flips before trusting it. The roll-up step inherits the multi-IP→household **collapse function** (random-pick vs max/sum/recency), which can dilute the signal — hold it fixed across both arms.

**Testing "does performance decay with prior exposure?" — the two colliders and the flat prior (2026-08-27).** The recurring hypothesis is adverse selection: repeatedly-served non-responders accumulate, so the residual pool converts worse than a fresh one. Two documented artifacts manufacture that answer before any real effect can show: (1) **prior-exposure count / `bid_count` is post-treatment** — never a feature or a stratum (see the uplift hazards below); (2) **ghost-bid coverage rises steeply with bid count** (`ghost_frac` 0.10 at 1 bid → 0.47 at 11+), producing spurious NEGATIVE lift at high frequency. So the one instrument that looks like it tests the hypothesis is broken in the direction of the hypothesis. **Current evidence is FLAT:** cross-sectionally across advertisers, conversion hazard runs 0.031–0.108% and visit rate 0.66–1.77% across frequency deciles spanning 1.1 to 35.6 imps/IP, with no decline. **The discriminating test has never been run:** `logdata.cost_impression_log` at `(advertiser_id, bid_ip, cumulative_exposure_index)` grain for one gated advertiser over a fixed window, joined to `ui_visits`/`ui_conversions`, testing whether the hazard declines with prior-exposure count **within** that advertiser's own HI pool (`household_score = 10000` at bid time). Purge shared IPs first (~37% of impressions). Use total visits from `guid_log`, not attributed visits, since last-touch credit splits ~1/n across impressions and falls with frequency on its own. Full framing: [[reference_hi_depletion_adverse_selection]].

**Uplift label-construction hazards (RFD B "Fangorn-Like Incrementality Uplift Model", 2026-07).** Three traps that manufacture spurious lift, now standard fixes in `lift__ghost_bid_visits`: (1) **left-censoring + multi-day anchor collapse** → apply `washout_days=2` + a **single first-bid anchor**, one 7-day window per IP; (2) **`bid_count`/frequency is a post-treatment collider** — never use it as an uplift feature or stratum (it reflects the treatment); (3) **vertical memorization** (e.g. a 92% "Dental persuadable" cluster) must be adjudicated as real vs memorized before any launch. Also: adding an impression-history feature to a propensity model and scoring with impressions="served" does **not** estimate the counterfactual — served households are selected for high intent, so the feature is **endogenous** (reflects selection, not lift); use a proper meta-learner (X-learner beat T-learner: Qini 1.86 vs 1.73) with the always-on ~10% ghost-bid holdout as both training label and eval instrument.

**Ownership update (2026-05-08):** TI-886 (T-learner productionization) has been reassigned away from the TI side — owned by another person (confirmed via Alex Knorr). MNTN's TI work on the BER-2250 follow-up is the **bidder-process ghost-bidding implementation**, not the model. The 30-day window run, the 30-net-new-advertiser cohort, and most other follow-up analyses are **blocked on the bidder-process implementation going live** — the post-hoc Databricks-on-augmentor-logs path is no longer the plan. Don't start them in parallel.

**Power constraint (Malachi's framing, 2026-04-20):** Our minimum detectable effect lands around ~15% while realistic CTV lift is 2-8%. This is the whole ballgame — why geo doesn't work at MNTN budget scale, why observational ML fails, and why ghost bidding (reusing existing 10% holdout instead of carving a new one) is structurally the only path. TI-884 quantifies this precisely per advertiser.

**Geo-holdout power: client communication when budget is close to threshold (2026-08-27).** For a geo-holdout test with sound design (matched pairs, clean isolation, appropriate duration), power adequacy depends heavily on **baseline metric variance within matched markets**, not raw spend. When a client's budget runs 10–15% below the recommended power model — test design is still valid, but risk of null result increases. **Client-facing principle: flag this gap upfront** ("your planned $278k is close to, but not within our 90% confidence threshold for 5% detection") rather than hope for stat sig. Most clients will accept the risk or bump incrementally (+$10–20k); none want to discover underpowering *after* a null result. State it plainly, give a specific path forward (e.g. "bump Sep/Dec by $10k each"), and let them decide.

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

**ATT recovers lift ITT cannot see (TI-837 v5, 30-advertiser cohort, 2026-04-20→04-26).** ITT-on-guid showed ~0% lift because only 14-16% of the "treated" group is actually served; ghost-bidding ATT (served-vs-would-have-been-served) recovers it. ATT lift is concentrated by funnel position: retargeting-only high-intent guid IVW **+21.07pp**, all-campaigns-combined **+3.12pp**, prospecting-all-stages **+0.78pp**, Stage 1 only **−0.06pp**. Report ATT by segment — the pooled number hides that essentially all guid lift sits in retargeting.

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

### Screening a LAPSED advertiser — and why "estimate spend from VR/CR" is the wrong instrument (2026-08-11)

Al Beretta asked for "a quick heuristic on spend based on Visit and Conversion rate" so a churned advertiser could be screened. Four durable lessons.

**1. Rates cannot predict spend. Required spend is the answerable question.** Visit rate and conversion rate are **ratios** — scale-free by construction — so they carry almost no information about how much an advertiser spends. Measured on the 1,566 INCR-75 advertisers with `spend_30d > $1,000` and `IVR > 0`: OLS on `log(spend_30d)` gives R² = **0.045** on `log(IVR)`, **0.098** on `log(CVR)`, **0.100** on both (Pearson r +0.21 / +0.31). Within a single IVR decile, spend spans **15–66x** p10→p90 while the median moves only ~3x across the entire IVR range. **What the rates DO determine is the inverse: the spend a test would require** — `spend_required(p, target_mde_rel, cpm, imps_per_ip)` in `ti_884_mde_calculator.py`, which already produced INCR-75's `budget_for_mde_ivr_*` columns. When someone asks to predict spend from performance rates, redirect to required-spend; don't fit the regression. Evidence: `tickets/audi_1204_lapsed_advertiser_test_eligibility/artifacts/audi_1204_vr_cr_spend_check.py`.

**2. The 1/IVR shortcut is delivery-shape-conditional — never quote the bare version.** Since σ²=p(1−p), required N ∝ (1−p)/p ≈ 1/p, so **required budget scales as 1/visit-rate** (halve the visit rate, double the budget). At α=.05, power=.80, 10% holdout, 8 weeks, **$30 CPM and 15 imps-per-IP**, that lands on ≈ **$14,100 ÷ IVR** for a 5% relative MDE. **That constant is only valid at those two defaults.** The general form is `$14,100 / IVR × (CPM/30) × (impsPerIP/15)`. Real advertisers diverge hard: BoggBag (46426) runs $12.33 CPM and 3.65 imps/IP, a combined factor of 0.10, so the bare shortcut overstates their budget **10x** ($129k quoted vs $11.5k actual). Always scale by the advertiser's own CPM and imps/IP.

**3. A lapsed advertiser structurally caps at Mid tier.** INCR-75's final tier is POWER × CONFIRMED-LIFT; `confirmed +` requires ≥20 holdout visits at p<.05 from a **live** ghost-bid holdout. A non-delivering advertiser generates no bids, so no measured lift exists and none can be produced. Best achievable is **Mid**, on the a-priori power gate alone. State this before anyone sets a customer expectation on "top-tier candidate".

**4. Re-windowing the screen onto a lapsed advertiser is cheap, and the blocker people assume is imaginary.** All three metric inputs retain years: `cost_impression_log` (no TTL, floor 2023-10-01), `clickpass_log`, `ui_conversions`. The `incr_75_advertiser_metrics.sql` header comment claiming a "90-day TTL" was **wrong** and was the only thing making the ask look expensive. Two real constraints instead: (a) the 12-month spend CTE reads `agg__daily_sum_by_campaign`, **frozen at 2026-04-30** — swap to `summarydata.sum_by_advertiser_by_day` (2024-01-01+); (b) **BigQuery cannot prune partitions on a date derived from a subquery**, so resolving "last active day" and pulling metrics in one statement scanned 39.5 GB vs **5.5 GB** when split into two steps with the window substituted as a literal. Fork validated against INCR-75 for BoggBag at the same window: `cpm`/`imps_per_ip`/`p_visit`/`p_cvr` reproduce within **0.21%** (volume columns +1.6–2.9% from a partial trailing day plus conversion attribution backfill). Canonical: `tickets/audi_1204_lapsed_advertiser_test_eligibility/`.

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

<!-- audi_1213: 2026-08-20 -->
## Power-calculator mechanics: arm split, lookback, and sizing against the CI floor (AUDI-1213, 2026-08-20)

### The unserved holdout must not be charged for impressions

`ti_884_mde_calculator.py:104-109` is correct: `n_treated = n_total * (1 - holdout_frac)`, because only treated IPs receive impressions. The shipped HTML prefill calculator instead splits a **spend-derived** IP pool 90/10, which charges the budget for impressions the holdout never gets. Consequences at defaults (p=2.15%, 5% target, $24.84 CPM, 3.5 imps/IP, 10% holdout):

- Required spend runs `1/(1-h)` = **1.1111x high** ($138,028 quoted vs $124,225 true).
- Displayed MDE runs `1/sqrt(1-h)` = **1.054x high** (3.06% vs 2.90% at $200k/mo over 8 weeks).
- It also puts the tool 11.11% off the `$14,100 / IVR` shortcut above.

**Fix `computeMDE` and `spendRequired` in the same pass.** Correcting one alone breaks the budget-to-MDE inverse: a corrected budget then renders 5.270% against a 5% target.

Empirical backing that the holdout is genuinely unserved: 0 of 2,356,886 WGU served IPs (one day) fell in holdout buckets.

### How far back a power calculator actually needs to look

Per advertiser, exactly two windows, both anchored on that advertiser's **last active day**:

1. **56 days of delivery** for the rate and reach inputs (baseline rate, CPM, imps/IP, distinct IPs). 56 rather than 30 because the horizon is 8 weeks and a direct `distinct_ips_56d` removes the linear extrapolation, which overstates reach 1.25-1.32x (measured 56d/30d distinct-IP growth is 1.393 median, against a linear comparator of 1.839).
2. **12 months** for the typical-active-month budget basis.

**Deeper history only decides who appears in the picker, never what is computed for them.** So the floor is the recency cut plus 12 months behind it. Against the silver 2024-01-01 floor that makes a **365-day recency cut** the widest one served losslessly: all 4,409 advertisers (1,863 delivering + 2,546 lapsed) keep both windows whole, earliest day needed 2024-08-20. At a 730-day cut, 396 of 5,739 need history from before the floor and their budget basis silently computes on a truncated window. Detail: `tickets/audi_1213_mde_calculator_refresh/outputs/audi_1213_history_floor_options.md`.

### Size the test against the CI floor of the prior, not its point estimate

MDE scales as `1/sqrt(budget)`, so a known budget-at-target-MDE rescales directly: `MDE_new = MDE_ref * sqrt(budget_ref / budget_new)`.

Worked case, Orangetheory National (39718): 5% MDE at $190,064 total, therefore **3.08% at $500,000**. Measured prior lift on them is +9.59% relative (`incr_75_gold_clean_ivw.csv`, z 3.32) with a **95% CI of 3.93% to 15.24%**.

The design rule that falls out: **power against the lower bound of the prior's CI, not its point estimate.** At $500k the test detects even the pessimistic 3.93% end; at $190k (5% MDE) it does not. That reframes a larger budget as downside coverage rather than extra precision, and it is the honest answer to "would you recommend a different budget for max lift."

Corollary on quoting a prior: an entry-cohort read and a full-window IVW read of the same advertiser are **not the same number** and can differ materially (Orangetheory: `current_rel_lift` +6.3%, whose own `current_lift_confirms` field says "flat so far", against the full-window IVW +9.59%). Name which one you are quoting.

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

**Regression anchor for the MDE engine (TI-884).** The two-proportion binomial calculator is self-tested against a Lewis-Rao hand calc: at `p=0.05`, `N=10,000`, no variance reduction, it returns `MDE_rel = 17.27%`. Keep this as a fixed unit-test anchor — any refactor of `mde_binomial` that shifts this number has changed the math. Cross-validated against Lauren's three completed lift tests: every reported lift landed 4.7×–8.2× *below* its own MDE, i.e. statistically indistinguishable from zero — GLD (reported +0.67% vs 3.12% raw / 1.86% post-stack MDE), Ownerly (+0.72% vs 5.92% / 3.53%), Boll & Branch (+1.00%, paused/no traffic, 88.4% / 52.6% MDE). The lesson these three make concrete: a reported point lift is meaningless without the MDE beside it — under-powered tests routinely produce small "positive" numbers that the design could never have detected.

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
- **Final pooled result (TI-933):** visit-rate lift **+2.055 pp** (95% CI [+2.011, +2.100]), conversion-rate lift **+0.140 pp** (95% CI [+0.133, +0.147]) — both significant, 7-day holdout window, 23 active Select advertisers. Select is incremental, and sits between TI-917's all-campaigns baseline (+3.12 pp) and its prospecting-only baseline (+0.78 pp). **Pooling is required, not a convenience:** Select advertisers run entirely prospecting/awareness campaigns with zero retargeting, and no single Select advertiser has the visit volume to be individually powered — the per-advertiser MDE exceeds any realistic per-advertiser lift, so the incrementality signal only clears detection when advertisers are pooled.
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

**Cross-device IP-matching biases a guid-based total-visit holdout DOWNWARD (TI-1044).** The guid join matches the CTV-impression IP (the TV / home router) to the web-visit IP (phone / laptop). Cross-device, cellular, and away-from-home visits carry a *different* IP → they are missed, so the absolute served-arm visit rate is an undercount (ElevenLabs: 2.83% observed). This is **not a symmetric loss**: it preferentially drops ad-induced cross-device visits from the *served* arm (the holdout arm has no ad to induce a device switch), so the measured visit lift is biased **downward**, not just noisier. A device-agnostic geo test does not have this leak and is the cleaner instrument. **Fix:** rebuild the total-visit holdout with household / identity-graph matching (IP → household → all visits on any household device) to remove the cross-device undercount. General rule: any IP-keyed exposure→outcome match on a CTV impression silently loses the cross-device tail — sign the direction (it flatters the null) before reporting.

**Cross-device coverage is coverage-INVARIANT for a RELATIVE contrast, but scales the ABSOLUTE pp — so size arm-symmetric RCTs on relative margin (AUDI-1173, 2026-07-28).** When the coverage miss is **symmetric and multiplicative across arms** (observed = c·true, with the same c≈0.85–0.90 in both treatment arms — true when arms differ only by a frequency-cap knob, not by device mix), the **relative** contrast is coverage-invariant: `(c·VR_a)/(c·VR_b) = VR_a/VR_b`, the c cancels. But the **absolute** difference is scaled: `c·VR_a − c·VR_b = c·(VR_a − VR_b)` — so a fixed absolute-pp non-inferiority margin is **anti-conservative** (you accept a true gap of `Δ/c > Δ`). **Rule:** for an arm-symmetric-coverage RCT, define and power the NI/superiority margin in **relative** terms (e.g. 5% relative), not a fixed absolute pp. (This corrects the common "report absolute pp" instinct — absolute pp is only safe when coverage is arm-asymmetric, e.g. the served-vs-holdout cross-device leak above, where the miss does NOT cancel.)

**For a FREQUENCY experiment, use attribution-independent total visits (guid_log), not attributed VV (ui_visits) — attributed VV is mechanically biased by frequency (AUDI-1173).** Frequency drives last-touch attribution: the higher-frequency arm wins the attribution tiebreak more often, inflating its *attributed* visits independent of any real behavior change. So attributed VV confounds "more ads → more credited visits" with "more ads → more actual visits." Total visits (guid_log page-views deduped to visit-days per `(advertiser_id, ip, date)`) is attribution-independent and isolates the causal effect. **TI-835 caveat:** total guid_log traffic barely moves with MNTN ads (~0% platform lift), so a total-visit metric is well-suited to **non-inferiority** (stable total visits = a safe cap) but is **insensitive for superiority**; keep attributed VV as a diagnostic companion, never the headline. See data_knowledge "Total-site-visit unit" + [[reference_total_visit_signal]].

**Ghost-win value-selection is not a frequency artifact (TI-1044).** To ask whether the served-vs-ghost ATT gap is just a frequency difference, form the served-counterfactual by sampling ghost bids at the per-bid win rate `w = 0.27` (10.96M imps / 40.79M real bids) and frequency-weight the control: visits move +35% → +33%, conversions +32% → +26%. The frequency correction is only ~2–6 pp, so the ATT bias is **value-selection, not frequency** — the bidder wins impressions for the households it bid highest on, who visit/convert anyway. The takeaway: **uniform win-rate sampling cannot remove value-selection bias** — only a randomized ITT (or IV/TOT scaled from it) does, and both were ≈0 here, matching the vendor geo test.

## Ghost-bid lift — bias register + the persuadables gradient (Matt Brorby, databricks_targeting `INCR`, 2026-06)
Source: `SteelHouse/databricks_targeting` → `exploration/INCR-first-ascent/ghost_bid_lift_bias_register.md` (the most complete catalogue of ghost-bid lift biases A1–A8/B/C/D/E/F — read it before any ghost-bid lift analysis). Beeswax leg, interim; signed magnitude pending the MNTN fcap-symmetric leg (INCR-63). Key durable findings:

- **The dominant bias is `ghost_frac` bid-MULTIPLICITY selection, NOT frequency-cap asymmetry (re-diagnosed 2026-06-23, supersedes the "Matt's frequency bias" framing above).** A holdout IP never wins → never marked reached → never exits the prospecting pool → re-enters auctions repeatedly → over-accumulates qualifying bids → concentrates in high-bid-count buckets, inflating the distinct-IP holdout fraction (`ghost_frac` 0.10 at 1 bid → 0.47 at 11+ bids) → manufactures a **spurious NEGATIVE lift**. (fcap-reconstruction was falsified: fcap is config-OFF for prospecting advertisers — `bid_price_log.threshold_failure_reasons` has no fcap tokens for obj=1.) **Fix: gate to clean `ghost_frac` (.09–.11)** — every prior "Beeswax negative lift" then vanishes (e.g. High band z−4.68 → z+0.12).
- **Measurement fix — single first-bid anchor.** Carrying `visited` as `MAX()` over per-day-anchored 7d windows gave holdout IPs (which bid on more distinct days) a wider observation window (union of windows) → spurious negative (~6.7pp of it). Use the IP's **earliest-bid** single 7d window (`ARRAY_AGG(visited ORDER BY dt LIMIT 1)`), dose-invariant across arms. Distinct-IP dedup alone is necessary but not sufficient.
- **Bias-floor at scale — read magnitude, not z.** At 100M+ IPs a ~0 magnitude (+0.001pp) yields z 5–13; adding days raised z (5.5→7.5) but NOT magnitude → the wall is bias, not power. Report absolute pp + per-campaign FDR; never headline the pooled z.
- **The persuadables gradient (population-wide, all advertisers, clean-gf, 2026-06-25) — refines the earlier "no intent gradient" read** (there was no gradient *in the per-campaign artifact*; pooling 100M+ clean IPs surfaces a small real monotonic one). Incremental visit lift (rel) by intent band: **High +0.2% (~0) · PP +1.6% · Mid +3.3% · MaxReach +3.4% · no_score +0.1% (~0)**. So **mid-intent (PP/Mid/MaxReach) carry the lift; top-intent (High) and untargeted reach (no_score) are incrementally dead.** Note no_score has a *higher baseline visit rate* (0.96%) than PP (0.57%) — would-visit-anyway, not incremental (the "no_score looks good on raw visits but is incrementally dead" trap; WGU 31357 is 100% no_score). Cross-advertiser-saturation cut agrees: most-contested IPs (21+ other advertisers bidding) are incrementally dead (+0.1%) despite the highest baseline — same households as the High band. **Targeting implication: incremental opportunity = mid-intent × less-saturated IPs; avoid top-intent / heavily-cross-targeted / untargeted-reach.** This is population-data backing for the perf-vs-incrementality opposition (see "Optimizing for incrementality and performance are partially opposed" above) and the TI-999/TI-956 mid-intent recs. Directional sizing only (z is N-inflated; signed magnitude needs the MNTN leg).

- **The ghost-bid holdout is a FIXED ~10% platform-wide — not a per-test tunable (AUDI-1148, Gruns, 2026-07-22).** No plans to raise/lower it, so "use a bigger holdout" is not a lever; ghost-bid power scales only with campaign SIZE and with POOLING across campaigns/advertisers (how the population-wide gradient gets its power). A small, low-VR campaign cannot resolve a few-percent lift on its own. Canonical: Gruns CGID 126905 ("CTV Prospecting TOFU High DMA", excludes high intent) — 10% holdout of a 0.19% raw-VR prospecting campaign = only ~19 holdout visits over 3 weeks (Jun 24-Jul 14) → +15% rel, 95% CI [-32%, +63%], p=0.53. Holdout accrues ~1 visit/day so even the Aug 1 flight end (~29 visits) stays inconclusive; the binding constraint is holdout visit COUNT, not window length. Window ends 7d before the data edge (`visited` = visit within 7d of first bid → recent cohorts right-censored until their 7d window matures). Query pattern: `enriched.lift__ghost_bid_visits` filtered on `campaign_group_id` (entry-cohort, drop the left-censored first day).
- **Score bands collapse to ~1–2 effective bands per advertiser** (intent-gated advertisers populate only top bands above their HHST; reach advertisers are 100% `no_score`) and `ghost_frac` is **flat across bands** (arm ⊥ band) → within-campaign band standardization is inert for bias-correction. For real heterogeneity use advertiser-relative quantile bands or the simplest robust split {scored, no_score}.
- **MNTN bidder leg = the clean reference.** The MNTN bidder writes ghost bids into the fcap cache (`apply_ghost_bids`) so holdout IPs accrue counterfactual frequency and exit symmetrically → multiplicity equalizes. Beeswax applies the ghost gate last and never writes to the fcap cache → residual multiplicity bias. DiD fingerprint: Beeswax skews negative (18.9%/11.5% sig), MNTN symmetric (14.3%/14.3%).
- **Verdict:** pooled lift ≈0; clean-gf per-campaign FDR has **zero** significant negatives + a one-sided-positive tail → the real Beeswax effect is **non-negative (zero-to-positive)**, never the A1 negative — but signed magnitude is still pending the exchangeable-arm MNTN leg. Report the bid-grain ITT (exchangeable at t=0) → scale to ATT by win rate `w_c`; DiD is corroboration only (no pre-period at t=0 for a real launch).
- **Reconciled headline (2026-06-05):** lift ≈ 0 on BOTH bidders (null + underpowered, NOT negative); conversions also ≈ 0 (pooled +0.06%). Binding limitation is **power + visit-window truncation, not residual bias** — the rigorous confirmation of the north-star "guid ≈ 0% lift." **Publish gate (INCR-69):** until a powered, fully-observed MNTN-leg run lands, the only defensible statement is "no significant lift detected; effect indistinguishable from zero; underpowered" — do not publish a non-zero point estimate.
- **The holdout is a >1-YEAR always-on advertiser-level randomized control** (the 10% `MD5(advertiser:ip)<100` hash has run >1yr; only ghost-bid *logging* started 2026-05-27) → treatment−holdout = long-run cumulative lift; validity from randomization, not a pre-period. **Negative control = visits to OTHER advertisers**: clean ~0 certifies exchangeability (covariate-balance battery: arms balanced on everything except focal-advertiser bid dose).
- **Rare-outcome reporting:** for conversions, gate per-campaign `rel%` behind ≥20 holdout conversions (NaN-drop + small-N manufacture a spurious conv positive) and lead with z; always read **median AND mean** (divergence = outlier vs broad bias); the clean-gf z-distribution ≈ N(0,1) is the strongest "true-zero" test.
- **Diagnosing ONE campaign's negative lift — read the `lift__ghost_bid_results` flags, not the sign (AUDI-1172, 2026-07-28).** A per-campaign negative that clears nominal significance is usually NOISE around ~0, not real suppression: bid-grain `z` is N-inflated (bids within an IP are correlated → SE understated) and low `ip_compliance` (~40%) dilutes the ITT toward zero. Before believing a negative, pull `reporting.lift__ghost_bid_results` (`stratum_type='overall'`) and check `ghost_frac_inflated`, `arm_imbalance_suspect`, `has_valid_holdout`, `holdout_won_rate` (should be 0), `meets_min_n`/`meets_min_compliance`. **All clean → noise, not a flagged bias artifact.** *(Corrected 2026-09-02, TI-1313: "all flags clean" is NOT a sufficient validity gate — `ghost_frac_inflated` fires only ABOVE the band, so a DEPLETED holdout passes every flag. Add `ghost_frac BETWEEN 0.09 AND 0.11` yourself; see "The gold `ghost_frac_inflated` flag is ONE-SIDED" below.)* Worked example: adv 59460 non-Select −13.8% (z −2.8) had every bias flag clean → noise; its Select side is the one flagged `arm_imbalance_suspect=true` yet reads +262% → **the flags mark imbalance RISK, not direction.** A genuine causal negative at bid-grain prospecting is essentially never the real story.
- **The 7-day attribution window is SHORT vs the advertiser standard → ghost-bid lift is a conservative FLOOR (Malachi, 2026-07-28).** Advertisers' own lookback/attribution windows vary but cluster at **30–45 days** (some run 7–14d; most 30–45). Ghost-bid `visited`/`converted` is a **fixed 7-day-from-first-bid** window, so it captures only the front of the response curve → the true incremental effect over an advertiser's horizon is almost certainly larger than what we report. Don't compare a 7-day ghost-bid lift head-to-head with an advertiser's 30–45d attributed numbers. **Why not just widen the window now:** (a) standardization — a lift measured over a longer window is mechanically bigger, so variable/longer windows break cross-advertiser/cross-product comparability (the whole point of a Select-vs-non-Select cut); (b) DATA AGE — only ~35 days of ghost-bid data exist (floor 2026-06-22), so a *matured* 30d window qualifies only IPs first-bidding by ~6/27 (a sliver), 45d ≈ none; 7d keeps ~28 days of usable cohorts. The views now accumulate (no TTL), so a matured **30-day window (advertiser-standard band) becomes feasible ~September 2026** — the natural point to re-measure Select vs non-Select on a 30d horizon (expect a larger gap). Fixed window, not variable "max-to-edge": variable injects right-censoring (recent cohorts mid-window read artificially low) that needs survival modeling, not a rate difference.
- **CPIV/CPIA denominator = `incremental_visits` raw count (Matt Brorby confirmed, 2026-07-29).** `lift__ghost_bid_rollup/_results.incremental_visits` (= `abs_itt * n_treatment`) is the correct, sanctioned count to divide spend by for cost-per-incremental-visit; safe as-is (holdout 8-11% is his good-results band; ours ~10%). **Raw-count pooling vs IVW answer DIFFERENT questions:** SUM of `incremental_visits` = volume-weighted total (emphasizes high-spend campaigns) = the right denominator for a cost-per metric; IVW-pooling `abs_itt` = "average CAMPAIGN-level lift". Same cohort: raw implies ~+100% (treated 2.3% vs holdout 1.15%), IVW ~+22%, platform-clean ~+5% - not 3 estimates of one thing, but volume-weighted vs campaign-average vs platform-wide-clean. So a Headline that shows raw treated/holdout RATES next to an IVW abs-lift is internally inconsistent (they answer different questions) - pick one basis per table. **Leg alignment RESOLVED (Matt call, 2026-07-29): no spend restriction, no within-CG confound — every campaign_group sits entirely on ONE bidder (Select→Mountain/Rust p79, PTV→Beeswax p8, never split), so all_facts spend maps cleanly to a CG's single leg; the leg IS the product split.** **SPEND BASIS (Matt call, 2026-07-29) — the blocker is characterized and now bridgeable, not unknown.** Matt tracks NO spend (does not use CIL); his pipeline "visit" = pixel/page-view fire within 7d of an IP's earliest bid, NOT a Reporting Verified/attributed Visit. So dumping all_facts spend over his `incremental_visits` OVER-counts (his visit basis is a narrower subset than Reporting → CPIV biased high). **SUPERSEDED (see the "CPIV/CPIA — the 5x is a PIPELINE ARTIFACT" bullet below for the FINAL method):** the intermediate plan here was to bridge visits via `all_facts.first_day_visits…seventh_day_visits` treated as "Matt's 7d window" and compute a pipeline→VV scaling factor `k`. **That was WRONG** — those are last-touch-only day-buckets, NOT the Verified Visit. The correct approach uses `Reporting_VV = clicks+views+competing_views` and `incremental = VV × rel_lift/(1+rel_lift)` (no k factor). First-pass pipeline-basis (obj=1, 2026-06-22..07-27): Select CPIV ~$6 vs non-Select ~$30 — directional only, superseded by the client-VV-basis $5.23/$8.23. Transcript: `tickets/audi_1172_.../meetings/audi_1172_01_matt_brorby_spend_scaling_2026_07_29.txt`.
- **CPIV/CPIA — the "Select is 5x cheaper per incremental visit" headline is a PIPELINE-MEASUREMENT ARTIFACT; on the client basis it's ~1.6x (AUDI-1172, self-verified 2026-07-29; `queries/audi_1172_cpiv_vv_correct.sql` + `artifacts/audi_1172_cpiv_vv_compute.py`).** Two bases:
  - **Pipeline basis** (spend ÷ Matt's `incremental_visits`): Select CPIV $6.01 vs non-Select $30.47 (5.1x); CPIA $130 vs $1,273 (9.8x).
  - **CLIENT / Reporting Verified-Visit basis** (what the advertiser sees): **CPIV Select $5.23 vs non-Select $8.23 (1.6x); CPIA Select $84 vs non-Select $256 (3.0x).**
  The gap collapses because **Matt's ghost-bid pipeline massively UNDERCOUNTS Reporting visits, far more for non-Select (PTV) than for Select** — non-Select: 223K pipeline incremental vs **6.5M** Reporting VV (Reporting counts ~2.9x the pipeline's treated visits); Select roughly agrees (~1.1x). So the pipeline basis overstates non-Select cost ~3-4x. (Mechanism not firmly established; do NOT label it "display vs CTV" — both products deliver on CTV, Select is the media-marketplace layer, PTV the automated performance product. The undercount difference is empirical.) **The client basis is the one to ship.**
  - **CORRECT method (Matt's call): `incremental_VV = Reporting_VV × rel_lift/(1+rel_lift)`**, `Reporting_VV = SUM(clicks+views+competing_views)` (AUDI-1070 authoritative UI defn; conv = click+view+competing_view_conversions), `rel_lift` = volume-weighted raw-count pooled pipeline lift. This uses the exact UI column + the relative lift, no scaling factor.
  - **SUPERSEDES the earlier k-factor attempt (was WRONG):** I first bridged via `first_day..seventh_day_visits` giving k=0.28 CTV/0.83 display and CPIV_VV $21/$37 — those day-buckets are **last-touch-only, omit clicks + first-touch + CTV view paths**, so they are NOT the Verified Visit. Discard k=0.28/0.83 and CPIV_VV $21/$37.
  - **SPEND METHOD (stands): join metered `all_facts` spend on `campaign_group_id`, filter `objective_id=1`** to match the lift cohort (non-Select CGs also run obj 5/6 ≈$0.6M the lift never measured; all-obj inflates non-Select CPIV). Select is 100% obj=1.
  - **Estimator note:** CPIV uses the **raw-count (volume-weighted) rel_lift** (Select +102.7%, non-Select +14.5%) — correct for a cost-per-TOTAL-incremental metric — NOT the IVW +22%/+4% in the shipped lift sheet (that answers "average campaign lift"). Label bases separately so the two lift numbers don't read as contradictory.
  - **CONFIRMED by Matt Brorby (Slack, 2026-07-29):** "This makes sense and aligns with how i was calculating this for a customer-facing dashboard." The method is validated against his own client-facing dashboard calc. The only residual is inherent: the 7-day pipeline rel_lift is applied to full-window Reporting VV (assumes a basis-invariant treated/holdout ratio; unavoidable, holdout is suppressed so it has no Reporting VV).

### Ghost-bid coverage was silently partial before 2026-08-25 — env filter dropped live burnin traffic (AUDI-1223)

The silver/gold ghost-bid lift tables filtered `bid_price_log` to `env='prod'`, but `env` names which of TWO LIVE Beeswax bidder deployments served the bid — `burnin` is the release-soak deployment (only functional difference per Abbas: IHP in-house pacing) serving real auctions and real spend, with campaigns routed to it by `burnin_bidding_enabled` (campaign/group/advertiser sync-config). Consequence: **142 of 1,215 eligible, actively-prospecting advertisers had zero lift rows** (ThirdLove entirely; Shea Homes 100% burnin), and covered advertisers could still miss individual burnin-routed campaigns (Gruns 626276). Fixed 2026-08-25 in `SteelHouse/sqlmesh` PR 1346: `env IN ('prod','burnin')`, disjointness from the test-campaigns sibling enforced by `is_test`, not env. **Forward-only** (raw 10-day TTL) — so any pre-2026-08-25 "no data" or thin per-advertiser read may be a coverage artifact, not evidence about the advertiser. Post-fix left edge: newly covered IPs first-appear at the deploy date (same stock artifact as the 2026-06-22 left edge — drop the first days). Open: burnin/IHP ghost share reads ~2.3% vs 10% design for ThirdLove — validate before trusting burnin-campaign lift. Trail: `tickets/audi_1223_ghost_bid_coverage_gap/summary.md`.

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

**CONFIRMED by Matt Brorby (meeting + Slack, 2026-07-02) — corrections to my earlier notes:**
- **Visit window = "7 days from first bid time"** (Matt, verbatim). Results **excluding 2026-06-22** are "somewhat close to the actual ITT effect for **biddable IPs**" (his words — i.e. bid-grain ITT, diluted by win rate). Pragmatic recipe: entry-anchor at first bid per (advertiser, campaign, ip), drop 06-22, aggregate the rest. Pooled = **+5.0% rel (z≈26), ghost_frac lands exactly on 0.100** (design) — clean, positive. (Strict single-fully-observed-day 06-23 = +2.8%; so the honest current range is ~+3–5%.)
- **CORRECTION — this is the BEESWAX / JVM-bidder leg, NOT the MNTN "clean" leg.** Source is `bronze.raw.bid_price_log` (JVM/Beeswax bidder only). The **MNTN (Rust) bidder** ghost bids live in `raw bidder_bid_events` (partner_id ~79, ~2–3B rows/hr) and are **not yet folded in** — Ryan deployed the MNTN-bidder ghost-bid change ~2026-07-02 (writes ghost bids to the fcap cache → should be symmetric/cleaner); Matt will fold it in (likely a separate audiences table, unioned at the visits grain). My earlier "MNTN clean leg" label on `enriched__dev_matthewbrorby.lift__ghost_bid_visits` was wrong.
- **Coverage:** every currently-running prospecting campaign with a holdout in its audience expression gets ghost bids logged (~1,700 campaigns) — NOT just newly-started campaigns, and not capped at ~22 advertisers. (Retargeting/CRM may lack a holdout.)
- **The silver tables now ACCUMULATE (no TTL)** — `enriched.lift__ghost_bid_audiences` / `lift__ghost_bid_visits` are materialized daily and persist. So the current 10-day window is only because logging started ~06-22; **a true ≥30-day window arrives ~late-July** (the "30 days" ask becomes feasible without a new pipeline). audiences = real-vs-ghost bids at adv×campaign×ip×day; visits = looks forward 7d for a visit/conversion.
- **Gold layer** (`dw-main-gold.reporting.lift__ghost_bid_results` = per campaign×stratum with ghost_frac/compliance flags; `lift__ghost_bid_rollup` = advertiser & campaign_group weighted rollup w/ abs_itt, SE, CI, z, MH stratified estimator, conversions). **The rollup is NOW correctly time-boxed (verified AUDI-1148, 2026-07-22, Matt confirmed):** it applies the entry-cohort + drop-left-censored logic and reproduces the windowed silver calc **to the digit** (Gruns CGID 126905: `rel_itt` 0.15223, abs 0.00013574, z 0.628, `significant_95` false, n 207,324/21,309 — identical to the silver entry-cohort read). **So the simplest path is now a one-liner:** `SELECT * FROM …lift__ghost_bid_rollup WHERE entity_id=<campaign_group_id>` (aggregate) or `…lift__ghost_bid_results WHERE campaign_group_id=<id>` (per stratum: `stratum_type` overall/score_band/bid_count, score_band High/Mid/no_score). *(SUPERSEDED: the earlier "rollup reads −1.8% all-time" caveat was pre-time-boxing.)* Silver entry-cohort method still valid for custom windows. Canonical: `artifacts/incr_75_fold_final.py`; query `queries/incr_75_entry_cohort_excl_leftedge.sql`.
- **"Excludes high intent" can mean ~100% NO-SCORE, not mid-intent — check the score_band strata (AUDI-1148, 2026-07-22).** Gruns CGID 126905 ("excludes high intent") is actually ~100% **no_score** — `lift__ghost_bid_results` `stratum_type='score_band'` shows High=5 and Mid=30 of 207K prospects (both essentially excluded); the rest are no_score. Per the persuadables gradient no_score is the incrementally-DEAD reach band (+0.1%), NOT the mid-intent band that lifts (+3.3%). **So "blocking high intent" ≠ "targeting mid intent"** — it can shift spend to unscored reach (dead), and then ~0 incremental lift is the *expected* result, not just an underpowered one. Always confirm the audience's score_band composition (via `_results`) before assuming an exclude-high-intent audience should lift incrementality.

### Fresh-window refresh + the naive-pool Simpson trap + raw-visit/incremental rank inversion (BER-2250 / AUDI-789, 2026-07-24)
Refreshed the persuadables gradient on the now-longer gold window (`dw-main-gold.reporting.lift__ghost_bid_results`, `stratum_type='score_band'`, clean-gated: `has_valid_holdout AND meets_min_n AND meets_min_compliance AND NOT ghost_frac_inflated AND NOT arm_imbalance_suspect` — *superseded 2026-09-02, TI-1313: that gate is incomplete, 53% of the campaign groups passing it sit BELOW the ghost_frac validity band and read upward-biased; see the one-sided-flag block below*). Three durable lessons:

**1. Do NOT pool raw counts across campaigns — Simpson trap.** A naive `SUM(vis)/SUM(n)` collapse across ~985 heterogeneous no_score campaigns reported **no_score +29% rel** — the opposite of the established "no_score dead." The fix is the register's own method: aggregate the per-campaign `abs_itt` with **inverse-variance weights** (`SUM(abs_itt/se²)/SUM(1/se²)`), which collapses no_score back to **+0.002pp ≈ 0**. N-weighting (`SUM(abs_itt·N)/SUM(N)`) also lets a few huge campaigns dominate — same confound. Use IVW or median-per-campaign, never the pooled count ratio. (Reinforces [[feedback_no_naive_pre_post]].)

**2. The gradient reproduces on the fresh window** (IVW abs lift → relative ÷ base): **Mid +9.2% · MaxReach +6.6% · PP +1.8% · High +1.7% · no_score +0.2% (~dead)**. Same shape as the 2026-06-25 register — mid-intent bands carry the lift, no_score dead — on a wider window, so the finding is stable, not a window artifact. (Magnitudes run a bit higher than 2026-06-25 rel; ordering identical.)

**3. Raw-visit rank and incremental-lift rank are ~inverted — the scoring warning for AUDI-789.** By raw holdout visit rate: High 1.14% > no_score 0.79% > PP 0.63% > Mid 0.25% > MaxReach 0.16%. By incremental rel lift: Mid > MaxReach > PP > High > no_score. **The bands a visit/spend-optimized scorer chases (High, no_score) are the incrementally deadest.** So AUDI-789 WS1 (make Fangorn better at predicting Spend/Visits) will pull spend toward would-visit-anyway audiences and AWAY from the persuadables unless incremental lift is added as a scoring target/guardrail. WS2 ("validate incrementality") has a ready instrument: the gold ghost-bid tables gated by band. Posted as AUDI-789 finding 2026-07-24. Caveat: bid-grain ITT (win-rate-diluted, not served ATT); per-campaign significance counts are bias-floor-inflated at these N's (read magnitude, not sig-counts).

### CONTRADICTION — the band gradient reverses under a correct relative-effect estimator (INCR-75 rerun, 2026-08-19)

**Both readings stand. The disagreement is an ESTIMATOR disagreement on the same data, not a data disagreement, and the discriminating test has been run.** Do not delete the 2026-07-24 block above; read it with this one.

**The claim above** (Mid +9.2% · MaxReach +6.6% · PP +1.8% · High +1.7% · no_score +0.2%, "mid-intent carries the lift, no_score dead") is computed as **IVW absolute effect ÷ IVW base rate**: `SUM(abs_itt/se²)/SUM(1/se²)` divided by `SUM(rate_holdout/se²)/SUM(1/se²)`. The numerator is sound. **The denominator is not** — an inverse-variance-weighted base rate is dominated by the lowest-variance strata and collapses toward zero (measured 0.0000–0.02% against true band base rates of 0.08–1.1%). Dividing by a collapsed denominator inflates exactly the LOWEST-baseline bands, which are Mid (0.16%) and MaxReach (0.08%). That is the whole of the "mid-intent carries the lift" ordering.

**Pool relative lift on the LOG RISK RATIO instead.** `log(p_t/p_h)`, variance `(1-p_t)/(p_t·n_t) + (1-p_h)/(p_h·n_h)`, inverse-variance combined. It carries the relative effect and its own variance together, so it needs no external baseline. Helper: `tickets/incr_75_eligible_advertisers/artifacts/incr_75_lift_stats.py` (`pool_rr`).

**Pool it RANDOM-effects (DerSimonian-Laird), never fixed-effect (TI-1313, 2026-09-02).** Campaign-to-campaign heterogeneity ran above **85% I²** on nearly every cut of this data, so a fixed-effect inverse-variance pool reports a confidence interval **several times too narrow** and manufactures significance from between-campaign variance it refuses to model. Add the DerSimonian-Laird between-unit variance τ² to each unit's own variance before weighting (`w_i = 1/(v_i + τ²)`), and report I² next to every pooled estimate so the reader can see whether the pool means anything. Every TI-1313 figure in the blocks below is DerSimonian-Laird on the log risk ratio.

**Discriminating test (2026-08-19) — the SAME gold clean-gated `score_band` strata, both estimators:**

| Band | log-RR pool | IVW-abs ÷ IVW-base (the 2026-07-24 method) |
|---|---|---|
| no_score | +23.8% (z=95.6) | +64.9% |
| PP | +12.1% (z=20.7) | +60.2% |
| High | +11.5% (z=69.9) | +29.5% |
| MaxReach | +3.9% (z=2.9) | **+169.9%** |
| Mid | +2.6% (z=3.2) | **+86.7%** |

**RETRACTED (same day, 2026-08-19): the INCR-75 clean-window band split that originally sat here.** It banded silver `enriched.lift__ghost_bid_visits.eff_score` with the documented household-score cutpoints (High ≥8001 · PP 6666–8000 · Mid 3333–6665 · MaxReach 1–3332 · unscored NULL). **That rule does not reproduce the platform's own `score_band`: joined per campaign×band against gold `lift__ghost_bid_results`, only 256 of 6,968 cells match exactly (3.7%), 458 within 1%.** `eff_score` is NOT `household_score` — on one day's partner-8 rows they agree on 59% of rows, `eff_score` is NULL for 27% where `household_score` is never NULL (it uses −1 for unscored), and 43% of `eff_score` values sit at exactly 10000. **Do not band this table by hand — use the gold `stratum_type='score_band'` strata, or get the rule from Matt Brorby.** The table above (gold strata) is unaffected; it is the only banded read here that stands.

**What survives from the 2026-07-24 block:** lesson 1 is still right — the naive count pool is wrong (it reads unscored +8.3% / Mid +2.1% on the clean window). **What does not survive:** the magnitudes in lesson 2, the "Mid > MaxReach > PP > High > no_score" ordering, and therefore the AUDI-789 WS1 warning in lesson 3, which rests on that ordering. **Revisit AUDI-789 WS1 before acting on it.**

**What would settle it further:** a served-grain ATT with an instrument for win-rate (both readings are bid-grain ITT, win-rate diluted), and a re-read once the MNTN Rust bidder leg (partner 79) has a trustworthy holdout — see the INCR-75 note that it entered the table 2026-07-05 reading +128% to +290% at ghost_frac 0.066–0.083.

**THIRD READING — APPENDED, not a replacement (TI-1313, 2026-09-02). All three readings stand; do not overwrite either one above.**

On **208 campaign groups** (partner 8 / Beeswax, prospecting, inside the `ghost_frac` 0.09–0.11 validity band, delivering on ≥54 of the 71 window days), pooling the platform's own `stratum_type='score_band'` strata with **DerSimonian-Laird random effects on the log risk ratio**:

| `stratum_value` | plain name | pooled rel lift (DL log-RR) |
|---|---|---|
| `High` | High Intent | +8.4% |
| `PP` | Peak Performance | +7.4% |
| `no_score` | Unscored | +6.6% |
| `Mid` | Mid Intent | +4.8% |
| `MaxReach` | Max Reach | +3.7% |

**The between-level test is NOT significant — Cochran Q on 4 df, p = 0.74.** On this population the intent bands **do not separate lift at all**; the ordering above is inside noise and must not be quoted as a gradient (see the Cochran-Q block below for why an ordering alone is not evidence of separation).

This is a THIRD ordering, differing from BOTH the 2026-08-19 log-RR reading (`no_score` highest) and the 2026-07-24 IVW-abs ÷ IVW-base reading (`Mid` highest). **The likeliest reconciling hypothesis is POPULATION, not estimator:** this reading gates on the ghost_frac validity band and on days-live, which neither prior reading did, and a holdout depleted below the band biases lift UP (53% of the earlier clean-gated population sits there — see the one-sided-flag block below). **Discriminating test:** rerun the 2026-08-19 log-RR pool on exactly this ghost_frac-gated, days-live-gated cohort. If the ordering converges, population settles it; if it does not, the estimators still disagree.

### The entry-cohort ghost-bid estimator self-poisons past ~15 days (INCR-75 rerun, 2026-08-19)

**The measurable window did NOT grow when the ghost-bid tables started accumulating.** `enriched.lift__ghost_bid_visits` held 2026-06-22..2026-08-18 (58 days, 4.22B rows, 1,498 advertisers) on 2026-08-19, and a naive pool over everything after the known-bad left edge reads **+18.6% visit lift**. It is an artifact.

**Mechanism.** The entry-cohort anchors each IP at its FIRST bid, permanently (`ROW_NUMBER() … PARTITION BY adv,campaign,ip ORDER BY dt = 1`). A holdout IP never wins, so it never exits the prospecting pool and is anchored almost immediately; treatment IPs win, exit, and are replaced by new arrivals. Each successive entry day therefore samples a pool that is more treatment-only than the last. **Observed ghost_frac decays monotonically 0.1054 (06-23) → 0.0836 (08-11) against a FIXED 10% platform holdout, and measured rel lift climbs in lockstep +2.8% → +16–26% (peak +94% on 07-16).** Holdout entries decay 8.2x across the window vs treatment 6.6x — holdout exhaustion, not a lift trend.

**Rule: the estimate is valid only while observed ghost_frac sits in the clean 0.09–0.11 band.** For the 2026-06-22 table floor that ends **2026-07-07** — about 15 days. This is the same failure class as the 06-22 left-edge stock artifact (which ran the other way, ghost_frac 0.118 → spurious −18.1%), so **check ghost_frac by entry day on BOTH ends before quoting any windowed ghost-bid number**: `queries/incr_75_entry_cohort_byday_window.sql`. Also right-censor: `visited` = visit within 7d of first bid, so entries after MAX(dt)−7d are incomplete.

**Consequence:** a >15-day windowed ghost-bid lift **read as an ungated pool** needs a different anchor (re-entry after a cooldown, or a served-grain ATT with IV), not a longer date range. Raised with Matt Brorby. **Scope correction (TI-1313, 2026-09-02):** this holds at the ungated pooled grain, where the decaying `ghost_frac` is baked into the estimate. It does NOT license picking the shorter window once the population is gated per campaign group on power and the 0.09–0.11 band — gated, the windows converge. See the window-choice block below before choosing a window.

**The gold `ghost_frac_inflated` flag is ONE-SIDED and does not implement this rule — gate the band yourself (TI-1313, 2026-09-02).** In `dw-main-gold.reporting.lift__ghost_bid_results`, `ghost_frac_inflated` fires only ABOVE the band: measured over `stratum_type='overall'`, FALSE spans ghost_frac **0.0 to 0.1476** and TRUE spans **0.1501 to 0.2308** (just 6 rows in the entire overall stratum). It catches an over-represented holdout and is **blind to a DEPLETED one**, which is the failure mode this section describes. Of **930** campaign groups passing the full standard clean gate, **490 (53%) sat below 0.09**. Pooled lift is strongly monotone in holdout depth — the depletion signature, not an effect (DL log-RR on `rel_itt`):

| observed `ghost_frac` | pooled rel lift | k |
|---|---|---|
| < 0.08 | **+16.4%** | 165 |
| 0.08–0.09 | **+16.7%** | 281 |
| 0.09–0.10 | **+8.4%** | 369 |
| 0.10–0.11 | **+2.1%** | 40 |
| > 0.11 | **−13.4%** | 22 |

Spearman(`ghost_frac`, `rel_itt`) = **−0.325, p = 2.8e-24**, and it survives inside every holdout-visit tercile and on partner 8 alone, so it is not a size or leg artifact. (`ghost_frac` = `n_holdout/(n_holdout+n_treatment)` to a correlation of 1.000.)

**Rule: always add `ghost_frac BETWEEN 0.09 AND 0.11` explicitly — `NOT ghost_frac_inflated` is necessary, never sufficient.** **Design consequence:** holdout depletion is a first-order threat to ANY ghost-bid readout, not a silver-only artifact — the gold tables are built on the same entry-cohort anchor (they reproduce the silver windowed calc to the digit, AUDI-1148). Report observed ghost_frac beside every ghost-bid lift number you publish. ~~*so a longer window buys a more depleted and more upward-biased holdout, not more signal*~~ — **that clause is CORRECTED the same day it was written (TI-1313, 2026-09-02): across windows that gradient is a property of the UNGATED population and does not survive gating. See the next block.**

**Per-period gating exception (AUDI-1215, 2026-08-21):** an entry-cohort PRE/POST read past the global clean cutoff CAN be valid when observed ghost_frac is gated PER PERIOD and holds in the 0.09-0.11 band in each period. ElevenLabs CGID 122748 held pre 0.09505 / post 0.09193 (band floor; one week at 0.0877 sat below). Sign the residual bias every time: post-period holdout depletion biases post lift UP, so a flat or rising post read is an upper bound and a "did not improve" verdict is strengthened, not undermined.

### Choose a ghost-bid window on POWER, never on a believed bias gradient — the across-window gradient is a property of the UNGATED population (TI-1313, 2026-09-02)

**This CORRECTS the window-choice inference in the two blocks above, including the TI-1313 line written earlier the same day.** The depletion mechanism itself stands unchanged: holdout IPs never exit the entry cohort, observed `ghost_frac` decays by entry day, and WITHIN a window pooled lift is monotone in `ghost_frac` (Spearman −0.325). What does not stand is the step from that to "the earlier/shorter window is therefore the cleaner read".

Same partner-8 prospecting cohort, three ghost-bid windows, two populations:

| ghost-bid window | ungated: ratio-of-sums over EVERY campaign group | gated: `vis_holdout >= 100` AND `ghost_frac` 0.09–0.11 |
|---|---|---|
| clean band (earliest, inside the band by entry day) | +6.3% | +6.0% |
| full span (71 days — the TI-1313 primary window) | +12.0% | +8.3% |
| trailing 30 days | +18.4% | +8.2% |

Ungated, measured lift climbs as the window moves later, which reads exactly like the depletion story and invites "pick the early window". Gated on power and the validity band, the same three windows **converge**. The gradient is a property of the ungated population, not of the window.

**Honest limit — the convergence is NOT proof the bias is absent.** `ghost_frac` is the mediator of the depletion bias, so gating on it removes the contrast **by construction**; the gated comparison could not show the bias even if it were present. What the pair of columns does establish is the negative: an ungated window comparison cannot demonstrate that one window is cleaner, because the same comparison on the gated population does not reproduce it.

**Rules:**
- Gate the population first (power + `ghost_frac BETWEEN 0.09 AND 0.11`), **then choose the window on POWER — how many campaign groups survive the gate — never on a believed bias gradient across windows.**
- **Never quote an ungated window comparison as evidence that one window is cleaner.** It confounds the window with the composition of the population that delivered in it.
- Still report observed `ghost_frac` beside whatever window you ship, per the block above.

**The power cost of a trailing-30-day read is most of the population.** Of **874** campaign groups on the full span (2026-09-01 read of `dw-main-gold.reporting.lift__ghost_bid_results` after the `is_test`/`deleted` inner joins — the table is all-time with no `dt` and is rebuilt daily, 877 pre-join on 2026-09-01 against 897 on 2026-09-02, so attach the read date to any count from it):

| trailing-30-day gate, applied cumulatively | campaign groups |
|---|---|
| any entry-cohort row in the trailing 30 days | 669 |
| + `vis_holdout >= 100` | 308 |
| + inside the `ghost_frac` 0.09–0.11 band | 126 |
| + delivering on 23+ of the 30 entry days | 115 |

A **208**-campaign-group full-span primary set becomes **95**, and the conversion side does not survive the trailing window at all. That is the real trade: the short window costs more than half the population and all of the conversion power, to buy a difference in measured lift that the gated comparison cannot detect.

### A null change-test is "cannot see", never "no change": compute the MDE on the DELTA (AUDI-1215, 2026-08-21)

When a clean randomized change-test reads null on a pre/post delta but a second instrument shows a significant decline, compute the null test's MDE ON THE DELTA before calling it an instrument conflict. AUDI-1215: the ghost-bid ITT pre/post delta on conversions carried a log-RR SE of 0.263, so at 95% it could only detect an RR-ratio outside [0.60, 1.67]; the fixed-holdout instrument's measured 0.639 ratio (z -8.98, p 2.6e-19 on its own, far better-powered data) sat INSIDE that blind spot. The instruments never disagreed; the randomized test was blind exactly where the powered one could see. Lead the synthesis with the powered instrument's decline and caveat with the null, never the reverse.

**Companion gotcha, holdout-lineage carryover:** `v_lift__conversions` attributes treated conversions through a 43-day (3,715,200s) impression lookback, so a post-change window is contaminated by pre-change impressions (AUDI-1215: 27.8% of post conversions attached to pre-period impressions, 14.5% to the blackout). The carryover imports pre-change lift into the post window, flattering POST, so a measured post decline is a LOWER bound. Split windows on the impression date and sign the carryover every time. Lineage detail: memory `reference_holdout_lift_lineage` + `data_catalog.md` §"silver.enriched.lift__ghost_bid_*".

### Ranking which attribute drives lift — best-minus-worst spread is an ORDER STATISTIC, use a between-level Cochran Q (TI-1313, 2026-09-02)

**Trigger:** any "which campaign attribute explains the lift?" question — vertical, creative length, intent band, frequency, targeting class — where the attributes have different numbers of levels.

Ranking attributes by the spread between their best and worst level is **biased toward high-cardinality attributes**. Max-minus-min over L noisy level estimates grows with L, so 17 verticals out-spread 4 creative-length levels by chance alone (**~4–5x bias measured** on this data). Spread answers "how far apart are the extremes", which is not the question asked — "does this attribute separate lift at all".

**Rank on a between-level Cochran Q with its p-value on L−1 df instead**: `Q = Σ wᵢ(θᵢ − θ̄)²` with `wᵢ = 1/SEᵢ²`, taking each level's pooled estimate θᵢ and backing SEᵢ out of that level's confidence interval **on the log scale**. The df grows with L, so it is cardinality-aware where the spread is not.

Applied to TI-1313 (208 partner-8 prospecting campaign groups, ghost_frac-gated, ≥54 of 71 days delivering):

| Attribute | between-level Q p-value |
|---|---|
| Bid frequency (`stratum_type='bid_count'`: 1 / 2-3 / 4-10 / 11+) | **4.1e-12** |
| Vertical (`fpa.advertiser_verticals`, parent `type = 0`) | **0.0023** |
| Creative length (`integrationprod.creatives.length`, seconds) | **0.014** |
| Campaign average frequency | **0.024** |
| Intent band (`stratum_type='score_band'`) | 0.74 |
| Geographic targeting class | 0.99 |

Raw-spread ranking had put vertical 2nd and intent band 3rd. The test moves **intent band and geographic targeting to "does not separate lift at all"** — the opposite conclusion off the same numbers, and the basis of the third intent-band reading above. **Generalize: never rank heterogeneity sources by observed spread; run the between-level test.**

### The conversion side of ghost-bid lift is ~NULL on the clean prospecting population — report that, don't ship a selected subset (TI-1313, 2026-09-02)

On the same 208-campaign-group gated cohort, pooled **CONVERSION** lift clears zero for **1 of 27 attribute levels tested** (Education vertical, **+27.1%**, 95% CI [+18.8%, +35.9%], k=15). Only **177 of 208** campaign groups recorded ANY holdout conversions at all, so most cuts have no usable holdout denominator.

**Consequence for any attribution-inflation or incremental-CPA analysis on this data: it is UNESTIMABLE for almost every cut.** Say so, with the count of levels that cleared zero, rather than presenting the handful that reached significance — 1 of 27 tests at p<.05 is what a null looks like, not a finding.

**Denominator prerequisite — this is what produced the false headline.** Conversion lift on `lift__ghost_bid_results` must be pooled on the VISIT denominators `vis_treatment`/`vis_holdout`, never the household counts `n_treatment`/`n_holdout`: `conv_rate_treatment`/`conv_rate_holdout` are **per VISITOR**, verified exactly across all 3,384 partner-8 `stratum_type='overall'` rows (max |conv_treatment/vis_treatment − conv_rate_treatment| = **0.0**; against `n_treatment` the same statistic is **0.99999687**), and the platform's own `conv_se` matches the visit-denominator binomial formula to a ratio of 1.000000. Pairing the conversion rate with `n_*` inflates effective sample size by a **median ~80x**, understates the log-RR variance by that factor, and corrupts both the weights and the point estimate. It produced a TI-1313 "attribution inflation" headline that **collapsed from 8 attribute levels clearing zero to 1** once corrected. The VISIT-side `rate_treatment`/`rate_holdout` ARE per household and do correctly pair with `n_treatment`/`n_holdout` (gold `se` matches that formula to 1.000000).

### Folding measured lift into a test-candidate screen — the staged gate + the two-instrument gotcha (INCR-77, 2026-07-02)
Two durable lessons from folding live ghost-bid lift into the INCR-75 eligibility screen (workbook `incr_75_eligible_advertisers.xlsx`; INCR-77 is the dedicated lift-measurement ticket, INCR-75 is the a-priori screen).

**1. Staged measured-lift gate — when the live holdout window is still young, gate in STAGES, not strictly.** Once a live always-on holdout exists you can gate test-candidate eligibility on *actual measured* lift, not just power. BUT if the window is short (~10 days here) most advertisers are "inconclusive" for lack of TIME, not lack of lift — so a strict "must have a confirmed positive lift" gate over-cuts. The staged recipe:
- **EXCLUDE only the proven-negatives** (significant-NEGATIVE measured lift — we've actually shown they don't work). Here: 17 advertisers → a new hard filter `F4_measured_neg`.
- **Require a confirmed-positive lift only for the TOP tier** (the a-priori power/score criteria — MDE≤5% + value_score≥60 — still apply). Demote a-priori-Tops that aren't confirmed yet to Mid; keep an `apriori_tier` column so nothing is lost and they auto-re-promote as data matures.
- **Keep 'inconclusive' advertisers eligible** at Mid/Low; the window fills toward 30 days (~late-July, tables now accumulate) and re-gates itself.
- Sizing the choice for the user: STRICT (require confirmed+ to be eligible) → 112 eligible; STAGED (exclude 17 neg + gate Top) → 1,270 eligible, Top 56→21 (of the 56: 21 stay, 33 demote to Mid, 2 F4-excluded).
- **Tiering: tried folding lift into value_score + score-band tiers (monotonic 67/126/1077) — REJECTED; reverted to the a-priori score + a power×lift gate (2026-07-06).** FINAL design: **value_score stays the pure a-priori 0–100 quality score** (measured lift NOT baked in); **tier = POWER × CONFIRMED-LIFT 2×2 — Top = `can_hit_ivr_5pct_8w='Yes'` AND `confirmed +`; Mid = powered-5% OR confirmed (one, not both); Low = neither** (→ Top 28 / Mid 152 / Low 1,090). **Durable lessons: (1) a confirmed *current* ghost-lift does NOT make an advertiser testable — if it can't power a clean 5% 8-wk study it belongs in Mid, not Top (e.g. Axos +55% confirmed but MDE 5.19%); the future-MDE 'power' gate is the binding Top gate. (2) Keep measured lift as a tier GATE + a display column, NOT folded into the ranking score — folding it in inflates scores, lets unpowered advertisers rank as Top, and forces a false choice among {power-gated Top, tier-grouped, monotonic score}. (3) You can satisfy at most 2 of those 3; a power gate + tier grouping is the right pick for a 'which can we test' list, so value_score becomes a within-tier rank.** Only 17 of the ~1,175 non-confirmed were truly negative; the rest were short-window artifacts. **Verdict buckets:** `confirmed +` (≥20 holdout visits, p<.05, positive) / `flat so far` (≥100 holdout visits, not sig — enough data, ~0) / `too early` (<100 holdout visits — thin) / `no data yet`.

**2. NEVER compare a measured-lift magnitude to a power MDE — they are DIFFERENT INSTRUMENTS.** A measured ghost-bid lift and an MDE both read as "% IVR lift," which invites the error "measured +8.8% > 2.57% detectable floor, so it must be significant." It is not, because:
- the **MDE** is a *future 8-week, served-IP-grain* detectability floor (baseline = the ~5% served-IP IVR);
- the **measured lift** is a *current ~10-day, bid-grain* ITT result (baseline = the ~0.5–1% visit rate across ALL bid-eligible IPs, diluted by win rate).
The current short bid-grain window is a far weaker instrument than a full 8-wk served-grain test — its *own* effective MDE is ~9%, not 2.57%. **Judge a measured lift against ZERO on its own CI / p-value, NEVER against the power MDE.** Canonical example: **Meritage Homes CTV (37880)** — a-priori Top (score 78.8, powered for 5% & 10%), power-MDE 2.57%, but measured **+8.8% at p=0.062** (z=1.87, 507 holdout visits, 95% CI on the abs lift = [−0.004, +0.158] pp — includes 0, lower bound just below zero) → verdict "flat so far", demoted Top→Mid, one good week of accumulating holdout visits from confirming.

**Presentation convention (reusable):** tag every "lift %" column by family so the two instruments can't be conflated — `[CAN-DETECT]` (future-test sensitivity/MDE, lower=better, drives the score), `[MEASURED NOW]` (actual holdout lift, judged vs zero, higher=better), `[PRIOR]` (past-test evidence). The classic trap this fixes: a header like "measured 8-wk reach" where "measured" means measured *reach* (impression-log distinct IPs), not measured *lift*.

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


## Adjudication patterns from AUDI-1089 (2026-07-13) — reusable beyond vendor evals

- **Impossibly-low attributed rate? Run the unconditional-activity check.** When a cohort's
  attributed metric (visits per imp) looks too low to believe, count the cohort's UNCONDITIONAL
  events (any clickpass row on those IPs, any advertiser, no attribution join). If unconditional ≈ 0
  too, the cohort is genuinely dark; if unconditional is rich, your attribution join is dropping
  real events. (q7f: 33Across sole IPs dark both ways → 0.026% IVR is real.)
- **Calibration buckets before disbelief:** compute the platform-wide metric split by the cohort's
  structural drivers (funnel × scored: 2.89 / 1.11 / 0.72% VR) — tells you whether an outlier cohort
  is outside the plausible envelope or just in a cold bucket.
- **Adversarial pre-launch review pays for itself on expensive scans (2026-07-15):** before launching
  a multi-TB/multi-hour query, run 2-3 independent reviewers with DISTINCT lenses (arithmetic/masks,
  replication-fidelity vs the reference query, execution mechanics+cost). The mechanics lens must
  verify output caps against EMPIRICAL cardinality (a COUNT DISTINCT beats an assumption — bq
  truncates silently) and count external-table re-reads (CTEs re-execute per reference). One such
  review caught a silent-truncation blocker AND a 3x cost bug before a ~40TB launch.
- **Cohort algebra: "sole" cohorts do not sum, and a union's sole can EXCEED its components'
  soles summed (2026-07-15).** Per-source sole = vs ALL other sources; a two-source union's sole =
  vs the rest only — items the two sources co-hold count in the union's sole but in NEITHER
  per-source sole (free-logs union $603K vs guid $278K + aug $167K). Corollary: degenerate cells
  (a group's share of a world that contains only itself) must display a meaningful alternative or
  an em-dash, never the trivially-true 100%.
- **Boundary-identity checks catch cost-model bugs:** every savings/attribution model has degenerate
  cases with known-exact answers (drop ALL meters -> recovery must equal total bills; keep ALL ->
  savings 0). Assert them — a $7.2K reconciliation gap in AUDI-1089's LOO came from applying a
  reassignment deduction whose destination had also been dropped (user-caught, 2026-07-14).
- **Decimal-residue analysis detects billing/credit regime changes:** metered tables whose unit
  counts show clean 1/N fractions (.5, .33, .25...) imply split-credit; all-integer months imply
  winner-takes-all. The Jan→May 2026 DDP meter switch was found purely from residues — check
  residues BEFORE building cost models on metered data.
- **Last-touch attribution mechanically confounds any "value vs frequency/exposure" curve.** Last-TV-touch
  (and last-touch) credit exactly ONE impression per visit, so a household's attributed visits are ~bounded
  by its intrinsic visit count `k`, roughly independent of ad count `n`. Attributed-visits-per-impression
  therefore falls toward ~1/n BY CONSTRUCTION even with zero true diminishing returns (a constant-value null
  would be flat). NEVER read a declining attributed-visits/impression (or rising CPV) curve as causal
  saturation. For frequency/exposure questions use HOUSEHOLD-grain total visits + cost-per-household, and get
  the causal answer from a household-randomized RCT (cap arms vs suppression holdout), not the observational
  curve. (freq_cap_sizing 2026-07-28, [[reference_frequency_capping]].)
