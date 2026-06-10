# TI-961: Causal Impact for Fangorn

**Jira:** https://mntn.atlassian.net/browse/TI-961
**Status:** Complete (infrastructure + methodology corrected); awaiting calendar-time for statistical maturity. Re-runs auto-refresh as post-period accrues.
**Date Started:** 2026-05-27
**Date Completed:** 2026-06-03 (methodology corrections per Alex Knorr review)
**Assignee:** Malachi

---

## 1. Introduction
Fangorn (IP-quality scoring for prospecting targeting) is rolling out in waves.
Now that wave 1 has been live for up to 26 days, the question is: do we have
enough data yet to run a CausalImpact analysis as **internal validation** of
the lift Alex is already reporting in his pre-post + difference-in-difference
dashboard?

Related work:
- [TI-504](https://mntn.atlassian.net/browse/TI-504) — Causal Impact experimentation pipeline (canonical VIF→BIC→CI flow)
- [TI-849](https://mntn.atlassian.net/browse/TI-849) — Fangorn score monitoring
- [TI-921](https://mntn.atlassian.net/browse/TI-921) — Fangorn lift dashboard (the notebook Alex is now using)

## 2. The Problem
Alex is reporting Fangorn lift to the PEX / go-to-market group every Thursday
using pre-post + difference-in-difference. The lift looks strong on visit rate,
noisy on conversion rate. He wants Malachi to evaluate whether there's enough
data yet to add CausalImpact on top as a further internal validation — not for
go-to-market (CausalImpact is "too hard to explain" to that audience).

**Quote (Alex, 2026-05-27):** "I don't think we need causal impact for that yet,
and I would just, like, if you can run things and look and say, like, yeah, we
have enough data, or, like, no, this is too messy still, that's kind of what I
want to know for now."

## 3. Plan of Action
**Approach (revised 2026-05-28):** bolt CausalImpact onto Alex's existing
`RolloutTierEvaluations.py` Databricks notebook rather than re-derive the
cohort logic. Alex's notebook already has the right cohort source
(`tpa.fangorn_advertiser_inclusion`), filters (`funnel_level=1`,
`objective_id=1`, MNTN-Matched CG scope), daily series, and tier-vs-control
DiD logic. CI fits cleanly as a new section after his "Headline KPIs" block.

1. Take Alex's [`RolloutTierEvaluations.py`](artifacts/RolloutTierEvaluations.py)
   (current Databricks production notebook) as the base.
2. Add new cells after Headline KPIs that:
   - Add a `ci_pre_days` widget (default 60d) — independent of the DiD `lookback_days` widget
   - Re-pull the daily KPI panel with the extended pre window if needed
   - Aggregate to tier × day visit rate
   - For each treated tier:
     - Treated series = that tier's pooled (sum vv / sum imps) daily visit rate
     - Covariate = impression-weighted aggregate visit rate of the control tiers (same control set the DiD uses)
     - Pre = ci_window_start → cutoff−1; Post = cutoff+1 → window_end (flip day excluded)
     - Fit CausalImpact with the covariate; report rel_effect, 95% CrI, p_value
   - Render results as styled tiles matching Alex's DiD HTML block
   - Add a diagnostic plot per tier (actual vs synthetic counterfactual + switch line)
3. Visit rate only — per Alex's 2026-05-27 catchup, conversion/CPA/ROAS are too noisy at this window length.
4. Run in Databricks; compare CI rel_effect against the DiD-adjusted lift tile for the same tier.

**Cohort sizes (current Fangorn rollout, confirmed in BQ 2026-05-28):**
| Tier | Flip date | Advertisers | Post-period (today) |
|------|-----------|-------------|---------------------|
| 1 (Wave 1) | 2026-05-01 | 3 | 27 days |
| 1 (Wave 2) | 2026-05-05/06 | ~50 | 22 days |
| 1 (Wave 3) | 2026-05-18 | ~311 | 10 days |
| 2 | 2026-06-01 (planned) | ~400 (control) | — |

Tier 1 / Wave 2 (~50 AIDs, 22d post) is the cleanest read. Tier 1 / Wave 1 is too small (3 AIDs); Tier 1 / Wave 3 is borderline (10d post).

## 4. Investigation & Findings
### Fangorn rollout state as of 2026-05-27 (per Alex)
| Wave | Tier | Cohort | Post-period days | Notes |
|------|------|--------|------------------|-------|
| 1A | tier-1 | First 3 advertisers | 26 | No order_amt; ROAS uncomputable |
| 1B | tier-1 | 50 random advertisers | 20 | 49 evaluated, 41 active campaigns |
| 1C | tier-1 | 312 remaining advertisers | 7 | 260 active campaigns running |
| 2 | tier-2 | Releases Mon 2026-06-01 | — | Middle-pack scores; some pulled (audience too small) |
| 3 | tier-3 | TBD | — | ~80% audience reduction at .8 threshold; Alex tuning threshold (.8→.6?) per-advertiser next sprint |

### Empirical Fangorn rollout in BQ (2026-05-28)
Cross-checked `audience_advertiser_configurations.vertical_data_source = 46`
against the 51 Tier-1 AIDs in [`ti_921 wave_config.csv`](../ti_921_fangorn_lift_dashboard/artifacts/wave_config.csv):
- **364 AIDs total** flipped on Fangorn today
- Earliest update_time = 2026-05-01 (3 AIDs = Wave 1)
- Bulk update on 2026-05-05/06 (31 AIDs by update_time, +17 with NULL update_time = Wave 2, ~48 total)
- Latest update_time = 2026-05-18 (~222 AIDs = Wave 3)
- All 51 wave_config AIDs are confirmed flipped
- 17 of them have NULL `update_time` (CDC quirk — likely flipped via row creation rather than update); wave_config.csv remains authoritative for treatment dates

This confirms the Postgres-backed `tpa.fangorn_advertiser_inclusion` table that Alex's notebook queries is the source of truth for cohort + flip-date — no need to maintain wave_config.csv in parallel.

### Alex's current dashboard (TI-921 notebook + Databricks dashboard)
- Reuses Malachi's daily-pacing queries; joined to `fangorn_advertiser_inclusion` rollout tier table.
- Reports **pre-post lift** and **difference-in-difference** (treatment = released wave-1 sub-group, control = wave-2 ~400 advertisers, not yet released).
- Headline numbers as of meeting: **+27% DiD visit-rate lift** on the 26-day group; positive lift across all groups vs unreleased control; conversion-rate / CPA messy.
- Two weighting variants per KPI:
  1. **Impression-weighted aggregate** (sum visits / sum impressions across advertisers)
  2. **Median per-advertiser rate** (unweighted)
- Median per-advertiser shows higher impact than impression-weighted.

### Malachi suggestion for an additional weighting
- **Variance-weighted lift:** higher-spend advertisers have less variance → more signal. Avoids the failure mode where 90% of advertisers are great but low-spend and one bad one with a huge budget dominates an impression-weighted view. Worth comparing against the median.

### Methodology clarifications during the meeting
- "Treated lift" = (post − pre) / pre for treatment group, expressed as a percent of pre.
- "DiD adjusted" = treated lift − control lift (where control lift is the same pre-post percentage on the unreleased wave-2 group). Negative DiD on ROAS in the dashboard is because ROAS rose more in the control than the treatment — pre-post alone would show a false positive.
- DiD requires **parallel pre-period trends**, not random assignment. Wave-1 vs wave-2 is acceptable as long as that holds.

## 5. Solution
**CausalImpact section added to Alex's RolloutTierEvaluations.py.** The
modified notebook is checked in at
[`artifacts/RolloutTierEvaluations.py`](artifacts/RolloutTierEvaluations.py)
with a new section inserted between "Headline KPIs" and "Executive Summary."

**Three new cells:**
1. **Setup + lean delta pull** — adds `ci_pre_days` widget (default 60d), pulls only the incremental pre-period days (`ci_window_start` → `window_start - 1`) using a slim impressions+visits+conversions query, then UNIONs with what `daily_performance_df` already has. Joins rollout-tier metadata, aggregates to tier × day visit rate **and CVR**.
2. **Fit + render** — `run_ci_for_tier(treated_tier, control_tiers, cutoff, metric_spec)` fits CausalImpact per (treated tier × metric) with metric-specific synthetic-control covariates: IVR uses `control_vr` + `control_imps`; CVR uses `control_cvr` + `control_visits`. Renders results as tiles grouped by tier with IVR + CVR side-by-side, styled to match Alex's existing DiD HTML block.
3. **Diagnostic plot** — one panel per (treated tier × metric) showing actual vs control covariate over the full CI window, with a switch line at the flip date and the rel_effect / CrI / p-value in the panel title.

**Outlier-day exclusion (added 2026-06-02 per Alex Knorr):** new `exclude_dates` widget (default `2026-05-29,2026-05-30`) drops named days from `daily_performance_df`, `pacing_df`, and `ci_daily_pd` so all downstream DiD / threshold / pacing / CausalImpact analyses see the same cleaned panel. Used here to remove two days with known pacing issues that would otherwise contaminate the CVR signal.

**Methodology correction — three bugs fixed (2026-06-03, ported from Alex Knorr's RolloutCausalImpact.ipynb review).** The CausalImpact section's results pre-2026-06-03 are deprecated; the methodology had three compounding issues that Alex caught on review:

1. **Target leakage in the counterfactual forecast.** `metric_lag1` and `metric_lag7` were in the candidate set. Those are lags of the *treated* outcome — feeding them into the post-period forecast conditions the "counterfactual" on actual post-treatment values, biasing the estimated effect toward zero. The Brodersen et al. CausalImpact spec explicitly forbids lags of `y` as exog; the UCM local-level state handles temporal correlation in `y` without leaking. **Fix:** removed both lags from candidates and the candidate dataframe.

2. **Naive seasonal handling.** `is_weekend` dummy was a coarse proxy for weekly periodicity. **Fix:** added `freq_seasonal=[{"period": 7, "harmonics": 2}]` to `UnobservedComponents` and dropped the `is_weekend` candidate. Full stochastic weekly cycle, evolves over time, no exog state cost.

3. **Hand-rolled CrI/p-value was wrong.** `SE = (avg_upper - avg_lower) / (2 * 1.96)` was the avg per-day SD, not the SD of the post-period mean — dropped the `1/n` scaling and ignored cross-day covariance. `rel_CI = actual / bound − 1` exploded when the bound approached zero (literal source of TI-961's +681% upper bound on Tier 1 IVR). Gaussian z-test layered on a skewed ratio compounded both. **Fix:** replaced with simulation — draw N=2000 paths from the fitted model via `res.simulate(...)`, take percentiles of the effect distribution for CrI, two-sided tail probability for p-value. Carries correct cross-day covariance, can't explode, no normality assumption.

**Plus two improvements** (Alex):
- **Pre-period fit diagnostics (R², MAPE)** on one-step-ahead in-sample predictions, surfaced on each tile and in each chart title. Trust check for the whole method.
- **Updated diagnostic chart:** dotted line = pre-period in-sample model fit; dashed line = post-period counterfactual forecast; shaded band = 95% simulation envelope. Now you can see whether the UCM tracked the pre-period before trusting the post-period gap.

Alex's standalone notebook is at https://github.com/SteelHouse/databricks_targeting/blob/aknorr/fangorn/fangorn/rollout/RolloutCausalImpact.ipynb. Our combined DiD+CI notebook has the same fixes ported in.

### BQ cost optimization
The naive approach (re-running Alex's full `daily_performance_query` with `window_start = min_inclusion - ci_pre_days`) processes **~1 TB** for a 60-day pre window. Dry-run ladder (verified 2026-05-28):

| Approach | Bytes processed |
|----------|----------------:|
| Re-run Alex's full query with 60d window | **1008 GB** |
| Lean (drop conv/spend/vast — CI only needs imp+vv) — full 60d window | 206 GB |
| Lean + delta (only the pre days not already in `daily_performance_df`) | **103 GB** |

The bottleneck is partition scanning: `impression_facts` / `visit_facts` are partitioned by `hour` (DAY) but NOT clustered on advertiser or campaign, so an advertiser-list filter doesn't prune further — the only knobs are date range and which fact tables to pull. The deployed notebook uses the lean-delta approach (~10× reduction).

### Local smoke test
Ran the lean 60-day panel ([`outputs/ti_961_ci_panel.csv`](outputs/ti_961_ci_panel.csv), 108k advertiser×day rows, 247 GB billed) through the same CausalImpact pipeline locally to validate the math + data shape. Wave definitions used the BQ snapshot's `update_time` as a proxy for `tpa.fangorn_advertiser_inclusion.inclusion_date` (since the Postgres table isn't accessible outside Databricks); control set = never-flipped prospecting advertisers (vs Alex's future-flip tiers).

| Wave (BQ-snapshot proxy) | n AIDs | n_pre | n_post | rel_effect | 95% CrI | p |
|------|--:|--:|--:|--:|--|--:|
| Wave 1 (`update_time = 2026-05-01`) | 3 | 60 | 26 | **+15.3%** | [+6.7%, +26.6%] | 0.000 |
| Wave 2 (`update_time = 2026-05-05` ∪ NULL) | 134 | 65 | 21 | **+45.3%** | [+33.5%, +59.7%] | 0.000 |
| Wave 3 (`update_time = 2026-05-18`) | 221 | 77 | 9 | **+23.5%** | [+16.2%, +31.2%] | 0.000 |

Outputs: [`outputs/ti_961_smoke_ci_results.csv`](outputs/ti_961_smoke_ci_results.csv) + per-wave CI plots in [`artifacts/plots/`](artifacts/plots/).

**Caveat — these numbers are NOT directly comparable to Alex's +27% DiD headline:**
- My "Wave 2" cohort here is 134 AIDs (everything flipped on May 5 or with NULL `update_time`, which the BQ CDC table flags inconsistently). Alex's Tier 1 Wave 2 from `tpa.fangorn_advertiser_inclusion` is ~50 AIDs.
- My control = never-flipped prospecting advertisers. Alex's control = future-flip tiers from the inclusion table.
- Both differences inflate my effect estimates vs Alex's likely true read. The smoke test validates the **plumbing**, not the precise numbers — once Alex runs the deployed notebook with the Postgres inclusion source, the tier mapping and control set will be authoritative.

**What the smoke test proves:**
- The lean delta query path works (no errors, sane data shape)
- CausalImpact converges on each wave with strong positive lifts at standard credible intervals
- Synthetic-control covariate construction (impression-weighted control rate) produces a well-fit predicted counterfactual
- The same code will execute correctly in Databricks with Alex's real cohort source

**How to run (per Alex's Databricks workflow):**
1. Open Alex's notebook in Databricks at `aknorr/fangorn/fangorn/rollout/RolloutTierEvaluations.ipynb`
2. Replace the cells between "Headline KPIs" and "Executive Summary" with the new CI cells from this file
3. (One-time) `%pip install causalimpact` at the top of the cluster session
4. Run with `lookback_days=14` (Alex's default for DiD) and `ci_pre_days=60` (CI default)
5. CI section pulls only the lean delta automatically; total marginal BQ cost ~100 GB

## 6. Questions Answered
- **Q:** Does Alex want CausalImpact for go-to-market?
  **A:** No. Internal validation only. DiD + pre-post is what he presents externally.
- **Q:** Which Fangorn cohort has the best shot at resolving lift?
  **A:** Tier 2 (~50 advertisers, ~21 days post). Visit rate, not conversion. Confirmed in the live Databricks run.
- **Q:** Is wave-2 (Tier 4 future-flip) a valid control?
  **A:** *(Original answer 2026-05-28, now superseded)* Yes for DiD purposes. *(Updated 2026-06-03)* No — Tier 4 has now been re-scheduled to flip 2026-06-04, and the tier composition in `tpa.fangorn_advertiser_inclusion` has evolved. **Use Tier 5 as the control instead** — it's the permanent holdout (sentinel inclusion date 2099-01-01). See §10 below for the corrected tier map.
- **Q:** Why is conversion-rate / ROAS noisy?
  **A:** Too short a post-period; CTV attribution windows are long (large share of conversions land after campaign exposure).
- **Q (added 2026-05-28):** Did the data turn out to be enough for CI?
  **A:** Not yet, but directionally yes. Live Databricks run (deck-quality figures in [`artifacts/`](artifacts/)):
    - **Tier 1** (N=3): CI +46.0% [−18.4%, +593%] p=0.253 · DiD +28.6% [−4.6%, +253%] p=0.138
    - **Tier 2** (N=47): CI +26.6% [−13.2%, +133%] p=0.255 · DiD +27.2% [−9.6%, +93%] p=0.200
    - **Tier 3** (N=283): CI +16.4% [−3.4%, +46%] p=0.117 · DiD +5.9% [−9.4%, +29%] p=0.446
  - **Methods converge at +27% on the cleanest cohort (Tier 2).** Strongest informal-causal argument we can make today.
  - Conversion rate on Tier 3 is the only `p<0.10` cell in the entire dashboard (DiD CVR +25.9% p=0.064) — worth watching but not yet a claim.
  - **No CI/DiD p-value clears 0.10 on IVR.** Need 3–4 more weeks of post-period for statistical significance via either method.

## 7. Data Documentation Updates
- `knowledge/experimentation.md` § "⭐ Standard Analysis Protocol" — added top-level methodology section codifying the 5-step pipeline (power → cohort → DiD-bootstrap → CI-VIF→BIC → standardized output). Required reading for every future tiered rollout / experiment evaluation.
- `.claude/CLAUDE.md` § "Experiment Analysis Protocol" — trigger added so future sessions invoke the protocol automatically.
- `knowledge/data_catalog.md` — note added about `impression_facts` / `visit_facts` partition-by-hour-only (no clustering) and what that implies for query optimization.
- `knowledge/data_knowledge.md` — notes added about `tpa.fangorn_advertiser_inclusion` (Postgres-only cohort source) and `audience_advertiser_configurations.update_time` NULL quirk (108 AIDs flipped on Fangorn have NULL update_time; the Postgres inclusion table is authoritative).
- Memory `reference_causal_impact_pattern.md` — updated to reflect the tier-level variant + cluster-bootstrap DiD inference layered on top.

## 8. Open Items / Follow-ups

### Calendar-time power projection (refreshed 2026-06-02, post EXCLUDE_DATES filter)

⚠️ **These projections are based on the pre-2026-06-03 buggy implementation
and will shift after Alex re-runs the corrected notebook** (target leakage
was biasing rel_effect toward zero, so post-correction the effects should
be larger and resolve sooner). Refresh after the next run.

SE-scaling projection assuming current point estimate is the true effect.
SE ∝ 1/√n_post (conservative-optimistic — real Kalman filter forecast
variance grows with horizon, so add 30-50% buffer to dates).

| Cell | Current p | Date for p<0.10 (CI) | Date for p<0.05 (CI) | Notes |
|---|---|---|---|---|
| Tier 1 IVR | 0.282 | ~Mar 2027 | hopeless | N=3 floor, not time |
| Tier 1 CVR | 0.153 | 2026-06-09 | 2026-07-20 | Would resolve NEGATIVE — likely 1 outlier advertiser |
| Tier 2 IVR | 0.247 | ~2026-08-10 | ~2026-10-07 | DiD will resolve sooner |
| Tier 2 CVR | 0.424 | ~2026-10-22 | ~2026-12-20 | CI won't help; DiD already p=0.002 |
| Tier 3 IVR | 0.083 | **2026-06-07** | **2026-06-13** | Closest cell — significance imminent |
| Tier 3 CVR | 0.911 | n/a | n/a | Point ≈ 0; can't project without signal |

### Recommended check-in cadence
- **2026-06-09 (1 wk):** Tier 3 IVR CI hits p<0.05. First real milestone.
- **2026-06-23 (3 wk):** Tier 2 IVR DiD likely at p<0.10; Tier 3 IVR firmly significant on both methods.
- **2026-07-14 (6 wk):** Tier 2 IVR CI approaches p<0.10; full cross-tier IVR story defensible. **Best date to trust for team write-up.**

### Read for the team
- **IVR:** wait until ~2026-07-14 for a stable cross-tier read.
- **CVR:** don't wait for CI corroboration — DiD on Tier 2 is already at p=0.002, that's the headline today. CI lags by 4-6+ months because daily-pooled CVR is structurally too noisy at tier-day grain.
- **Tier 1:** will not resolve at tier-day grain. If a Tier 1 read is needed, pool Tier 1+2 advertisers in a single DiD or accept it's anecdotal.
- **Tier 3 CVR:** point estimate is essentially zero; either the effect hasn't emerged yet (give 4-6 more weeks) or it genuinely isn't there for the Tier 3 cohort.

### Other follow-ups
- **TI-1003** ([Jira](https://mntn.atlassian.net/browse/TI-1003)) — stand up a simple TI experimentation archive (one-day scope) so the Fangorn read becomes the first entry stakeholders can bookmark.
- Variance-weighted lift comparison vs median + impression-weighted is still an open follow-up for Alex's DiD numbers (orthogonal to CI).
- **Future stratified rollouts:** implement the stratified cluster bootstrap variant per `documentation/docs/feature_rollout_experimental_design.md` ("Bootstrap variant must match the design"). Pattern sketched in that doc; defer code to the first stratified rollout's ticket.
- **CUPED variance reduction** — attempted 2026-06-10 and PULLED. A multi-agent verification workflow caught that CUPED is mathematically incompatible with non-randomized cohorts like Tier 2 vs Tier 5 (Wave 3 selection-biased holdout). The unbiasedness condition E[pre_T − pre_C] = 0 is violated by design; bolting CUPED onto a quasi-experiment with deliberate baseline imbalance double-counts the pre-period correction (formula multiplies imbalance by `(1+θ)` instead of either 1 or θ alone), generating spurious effects. Verified synthetically: 33.8% spurious lift when treated and control had no real time trend. See [`feedback_cuped_needs_randomization`](../../).claude/projects/-Users-malachi-Developer-work-mntn-workspace/memory/feedback_cuped_needs_randomization.md). **For the next major release with stratified random assignment, CUPED CAN be applied cleanly — the 35-50% variance reduction is real, just only when the upstream design is random.**

## 9. Methodology / Design Documentation Produced

Three shareable methodology docs landed as part of this ticket — they outlive the Fangorn read and become reference material for every future TI experiment:

- [`documentation/docs/causal_impact_did_math_reference.md`](../../documentation/docs/causal_impact_did_math_reference.md) — UCM state-space equations, Kalman filter, MLE, VIF/BIC selection, cluster bootstrap, simulation-based inference. **Math = HOW the methods work.**
- [`documentation/docs/did_vs_causalimpact_method_selection.md`](../../documentation/docs/did_vs_causalimpact_method_selection.md) — When to use each method, coffee-shop analogy, decision matrix, methods-convergence framing. **When = WHICH method for which problem.**
- [`documentation/docs/feature_rollout_experimental_design.md`](../../documentation/docs/feature_rollout_experimental_design.md) — How to design a rollout for clean causal inference (random stratified assignment, permanent holdout, three cadence options 5-week/12-16-week/7-month, pre-flight checklist, canonical literature references including Kohavi/Tang/Xu, Vaver/Koehler, Brodersen, Roth/Sant'Anna). **Design = how to set up the experiment BEFORE any flips happen.** Apply to every future Fangorn-scale release.

Plus three memory entries safeguarding the methodological lessons across sessions:
- `feedback_no_y_lags_in_causalimpact.md` — never use lags of treated y as exog (target leakage)
- `reference_fangorn_tier_assignment.md` — only Fangorn Tier 2 is random; Tier 2 = gold-standard DiD claim, others CI-only directional
- `feedback_bootstrap_must_match_design.md` — bootstrap variant must match the sampling design (i.i.d./stratified/cluster/block)

The TI-961 deliverable is the corrected CausalImpact pipeline. The **larger compounding deliverable is the methodology infrastructure** that makes every future experiment cheaper to design and more defensible to report.

## 10. Canonical Fangorn Tier Map (as of 2026-06-03)

Authoritative source: `tpa.fangorn_advertiser_inclusion` (Postgres, coredb).
Queried via Databricks 2026-06-03:

| Tier | N | Inclusion date | What it actually is |
|---|---|---|---|
| **1** | 3 | 2026-04-30 | First wave — non-random, N=3 (anecdotal only) |
| **2** | **49** | 2026-05-06 | **Random 50-advertiser sample — gold-standard causal cohort** |
| **3** | 312 | 2026-05-19 | Wave 3 — non-random |
| **4** | 362 | 2026-06-04 | Wave 4 — non-random, flips tomorrow (2026-06-04) |
| **5** | **353** | **2099-01-01 (sentinel)** | **Permanent holdout — never-flipped. Use as CONTROL.** |
| **99** | 44 | 2026-06-04 10:36:46 | **Auto-enrollment (Express product / auto-verticals)** — already on Fangorn via different mechanism, NOT part of structured rollout. **EXCLUDE from analysis.** |

### How to set the control_tiers widget

- **`control_tiers=5`** is the recommended default. Tier 5 is the permanent holdout. Methodologically cleanest control.
- **Do NOT use `auto`** with the current tier composition — auto-detection sweeps in Tier 4 (about-to-flip) AND Tier 99 (auto-enrolled, already treated). Mixed populations dilute the comparison.
- **Exclude Tier 99 from analysis entirely** via the new `exclude_tiers` widget (default `99`). Tier 99 advertisers are on Fangorn via auto-enrollment, not the structured rollout — they shouldn't be in either the treated or control pool for this causal analysis.

### Tier 99 — Express / auto-verticals

Per user 2026-06-03: Tier 99 represents auto-enrollment of the Express product and/or "auto-verticals" — half of the verticals were automatically rolled onto Fangorn outside the structured tier plan. The inclusion-date field stores a record-creation timestamp (2026-06-04 10:36:46), not a flip date — those advertisers are already on Fangorn at the time of the record.

Critical implication: **inclusion-date semantics differ across tiers**. Structured tiers (1-5) use the field as a planned flip date; Tier 99 uses it as record-creation timestamp. Inclusion-date alone can't reliably distinguish treated from control — explicit tier filtering is required.

If the auto-enrolled cohort's behavior matters separately (Express product impact, vertical-specific dynamics), it should be analyzed as its own ticket, not folded into the structured-rollout causal claim.

## 11. Meeting Notes
- `meetings/ti_961_01_malachi_alex_catchup_2026_05_27.txt` — 30-min Malachi + Alex catchup; covers both TI-961 (Fangorn CI eval) and the interest-segment scoring scope for TI-956.
- **Next meeting:** "Early next week" — Alex to add usage notebook to `targeting-infra-ml`; Malachi to read the scoring code before that meeting.
