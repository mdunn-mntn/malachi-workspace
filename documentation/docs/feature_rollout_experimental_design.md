# Feature Rollout Experimental Design

A methodology for designing targeting feature rollouts at MNTN scale (~1400
advertisers) so that the resulting causal claims are defensible to
leadership, GTM, and external stakeholders. Companion to the math reference
and method-selection guide.

**Author:** Malachi Dunn · TI
**See also:**
- `causal_impact_did_math_reference.md` — the inference math
- `did_vs_causalimpact_method_selection.md` — which method to lean on
- `../../knowledge/experimentation.md` — Standard Analysis Protocol

---

## TL;DR

For every major targeting release, do three things:

1. **Randomize at the advertiser level**, stratified on spend quartile,
   vertical, and channel mix. Pre-commit assignment in
   `tpa.<feature>_inclusion` BEFORE any flips.
2. **Reserve a 10-15% permanent holdout** that never gets the treatment
   during measurement. This is the gold-standard control.
3. **Pick a cadence** from the three options below based on operational
   constraints. **Final measurement window ≥ 28 days regardless of
   cadence** — that's the non-negotiable for a defensible causal claim.

The current Fangorn rollout (TI-961) only gives a clean causal claim on
Tier 2 because Tier 2 was randomly sampled. Tiers 1, 3, 4 are
non-random — DiD parallel-trends fails, and even CausalImpact has partial
exposure through BIC-selected control covariates. This doc is the
framework to avoid that problem on every future release.

---

## Core principles

These don't change regardless of cadence:

### 1. Pre-committed random assignment beats retroactive analysis

Once advertisers are sorted by anything that correlates with the metric
(GTM readiness, vertical priority, sales relationship), parallel-trends
is gone — and no amount of statistical sophistication recovers it. The
fix is upstream: randomly assign before flip dates, pre-register the
assignment in the source-of-truth inclusion table, and never modify it
mid-experiment.

### 2. Stratified randomization, not pure random

A few large advertisers dominate platform-wide metrics. Pure random
assignment can put all of them in one arm by chance, and the imbalance
breaks the comparison. Stratify on:

- **Spend quartile** (4 strata) — controls for advertiser size
- **Vertical** (top 8-10 verticals as their own strata; long tail bucketed)
- **Channel mix** — CTV-heavy vs display-heavy vs balanced
- **Funnel stage** (if applicable) — prospecting-only vs full-funnel

Then randomize within each stratum. ~32-40 strata typically; that's enough
to balance the major confounders while leaving enough advertisers per
stratum (~30-45) for the assignment to actually be random.

### 3. Permanent holdout > rotating control

The Fangorn approach used "future-flip tier as control." This works
operationally but breaks the moment GTM decides which advertisers go
into which tier non-randomly. **A 10-15% permanent holdout** —
advertisers who are pre-committed never to be flipped during the
measurement window — gives you a clean control that doesn't degrade as
the rollout progresses.

After measurement concludes (typically 6-12 weeks post-final-wave), the
holdout can be flipped as a final wave. They get the treatment, just
last.

### 4. Final measurement window is non-negotiable

You can compress the inter-wave cadence, but **the final window after
the last wave needs ≥ 28 days** for IVR-led KPIs and ≥ 56 days for
conversion-led KPIs (CTV attribution is long). This is where the causal
claim is made; the rest is rollout pacing.

---

## Three cadence options

Pick the one that matches operational constraints. **Statistical rigor
decreases as cadence accelerates.** Each option assumes the same
randomized + stratified + holdout design as core.

### Option A — Conservative (full statistical rigor)

**Total measurement window: ~7 months**

| Phase | Treated | Holdout | Duration | Purpose |
|---|---|---|---|---|
| Pre-period | 0% | 100% (everyone untreated) | 60 days | Baseline collection, CUPED covariates |
| Wave 1 | 10% | 90% | 28 days | Safety + early sanity read |
| Wave 2 | 25% (cumulative) | 75% | 28 days | First powered interim read |
| Wave 3 | 50% (cumulative) | 50% | 28 days | Mid-rollout confirmation |
| Wave 4 | 85% (cumulative) | 15% (permanent) | 56 days | **Final causal claim window** |
| Wave 5 (optional) | 100% | 0% | — | Deploy to holdout after measurement |

**Use when:**
- Highest-stakes releases (Fangorn-scale, public-facing claims)
- Long-attribution KPIs are the primary outcome (CVR, ROAS, CPA)
- GTM can accommodate the timeline
- Leadership is going to scrutinize the result

**Statistical strength:** maximum. Each interim read is independently
powered; methods-convergence (DiD + CausalImpact) is robust; weekly
seasonality fully observed in every phase.

### Option B — Standard (recommended default)

**Total measurement window: 12-16 weeks**

| Phase | Treated | Holdout | Duration | Purpose |
|---|---|---|---|---|
| Pre-period | 0% | 100% | 42-60 days | Baseline + CUPED |
| Wave 1 | 15% | 85% | 7-14 days | Safety check (look for breakage, not effect) |
| Wave 2 | 50% (cumulative) | 50% | 14 days | Power start to accrue |
| Wave 3 | 85% (cumulative) | 15% (permanent) | 28-42 days | **Final causal claim window** |
| Wave 4 (optional) | 100% | 0% | — | Deploy to holdout |

**Use when:**
- Most TI releases — this is the default
- Mix of IVR-led and CVR-led KPIs
- Standard GTM cadence
- Need a defensible causal claim but can't burn 7 months

**Statistical strength:** strong on the final wave; interim waves are
safety-monitoring only (underpowered for effect estimation). The causal
claim comes from Wave 3 vs holdout, which has 28-42 days of post-period —
enough for IVR to clear and for CVR to be directionally informative.

### Option C — Fast (compressed safety monitoring)

**Total measurement window: 6-8 weeks**

| Phase | Treated | Holdout | Duration | Purpose |
|---|---|---|---|---|
| Pre-period | 0% | 100% | 30-45 days | Baseline (shorter; expect higher MDE) |
| Wave 1 | 25% | 75% | 5-7 days | Smoke test for breakage |
| Wave 2 | 85% (cumulative) | 15% (permanent) | 28-42 days | **Final causal claim window** |
| Wave 3 (optional) | 100% | 0% | — | Deploy to holdout |

**Use when:**
- Lower-stakes rollouts (minor algorithm updates, bug fixes that change behavior)
- IVR-led KPIs only — CVR/ROAS will be underpowered
- GTM has a tight deadline
- Acceptable to land with "directional + p < 0.10" rather than "p < 0.05"

**Statistical strength:** moderate. Effective MDE is ~1.5-2× the
conservative option. Final wave still gets enough post-period for IVR;
CVR/ROAS read should be reported as directional with caveats.

### Option D — Emergency (no statistical claim)

**Total measurement window: 1-2 weeks**

- Direct 100% rollout, no holdout
- Pre/post comparison only — no causal claim possible
- Report as operational metric only ("post-rollout IVR was X")

**Use when:**
- Urgent ship requirement (bug fix, regulatory)
- The decision to ship is already made
- Communicate clearly: this is operational monitoring, not causal inference

Avoid by default. Document explicitly when used so future analyses don't
misinterpret the data as randomized.

---

## Cadence decision matrix

| Question | If yes | Pick |
|---|---|---|
| Will leadership / external stakeholders cite the result? | Yes | A |
| Is the primary KPI CVR or ROAS? | Yes | A or B |
| Is the primary KPI IVR? | Yes | B or C |
| Is the rollout reversible if measurement reveals harm? | No | A (more safety monitoring) |
| Does GTM have firm constraints < 12 weeks? | Yes | C |
| Is this an emergency / bug fix? | Yes | D + document the constraint |

**Default: Option B.** Override up to A for high-stakes releases or down
to C when GTM pace dictates. Avoid D unless truly urgent.

---

## Pre-flight checklist (every rollout, every cadence)

Before any flip happens:

- [ ] **Power analysis** — compute MDE per KPI given expected sample
      size and cadence. Document expected detectable effect sizes.
- [ ] **Stratification variables defined** — spend quartile, vertical,
      channel mix. Verify each stratum has ≥ 30 advertisers.
- [ ] **Random assignment generated and pre-committed** — written to
      `tpa.<feature>_inclusion` with phase + holdout flag. Never modify
      mid-experiment.
- [ ] **Holdout group locked** — 10-15% pre-committed never-flipped.
      Document the holdout advertiser list in the experiment record.
- [ ] **Pre-period data collection running** — 30-60 days of baseline
      KPIs at advertiser × day grain before first flip.
- [ ] **Analysis plan registered** — primary KPI, secondary KPIs,
      method (DiD primary, CausalImpact secondary), stopping rules,
      multiple-testing correction (Holm or Bonferroni for K > 1 KPI).
- [ ] **Dashboard scheduled** — Databricks notebook or Mode dashboard
      auto-refreshes daily/weekly during measurement.
- [ ] **Pre-period parallel-trends visual** — overlay all advertiser
      groups' KPIs across the pre-period normalized to baseline. Confirm
      parallelism BEFORE flipping. If groups diverged pre-flip,
      stratification is broken; restratify and re-randomize.

Mid-experiment:

- [ ] **Weekly safety check** — alert if any flipped group's KPI is
      worse than holdout's by > MDE (early-stopping signal for harm)
- [ ] **No mid-experiment reassignment** — if an advertiser's
      circumstances change, document but don't re-randomize

At measurement conclusion:

- [ ] **Methods convergence check** — DiD and CausalImpact agree on
      direction + roughly on magnitude? Yes → confident. No → investigate.
- [ ] **Pre-period parallel-trends re-check** — confirm the assumption
      still holds. Document any violations.
- [ ] **Multiple-testing correction applied** if testing ≥ 2 KPIs
- [ ] **Effect-size estimates with 95% CI / CrI** — not just point
      estimates. Report uncertainty honestly.
- [ ] **Write up findings** in a TI-archive entry pointing to data,
      code, methodology, and known limitations

---

## Analysis stack reference

The methodology to apply once measurement is done — same as the
canonical Standard Analysis Protocol in
[`knowledge/experimentation.md`](../../knowledge/experimentation.md):

1. **Power analysis** (also done pre-flight)
2. **Cohort + flip-date detection** from the inclusion table
3. **DiD with cluster bootstrap** as primary inference for the
   holdout-vs-treated comparison (random assignment makes
   parallel-trends defensible)
4. **CausalImpact with VIF→BIC + simulation inference** as secondary
   inference (corroboration via independent statistical machinery)
5. **Standardized output + scheduled execution** to a durable location

Reference implementation:
[`tickets/ti_961_fangorn_causal_impact/artifacts/RolloutTierEvaluations.py`](../../tickets/ti_961_fangorn_causal_impact/artifacts/RolloutTierEvaluations.py)

---

## Variance reduction — CUPED

CUPED (Controlled-experiment Using Pre-Experiment Data) uses pre-period
KPIs as covariates to reduce post-period variance. Typically 20-50%
sharper effect estimates at the same sample size. **Free statistical
power — every rollout should use it.**

Concretely: regress post-period KPI on pre-period KPI per advertiser,
use the residual as the effect estimate. Implementation pattern adapted
from Deng et al. 2013 (Microsoft). Adds ~30 lines of code to the analysis
notebook.

---

## When stratification breaks down

If pre-period parallel-trends fails on the visual check (e.g., the
holdout was on a different trajectory pre-flip):

- **Re-stratify on the unobserved confounder** if you can identify it
- **Add the missing covariate to the CausalImpact candidate set**
- **Use a synthetic control method** — Abadie/Diamond/Hainmueller 2010
  is the canonical reference; matrix completion methods (Athey et al.
  2021) are more modern
- **Report findings as directional** with the parallel-trends violation
  explicitly noted, not as a clean causal claim

Better to be honest about the methodological limit than to overstate.

---

## Operational integration

**Pre-registration** lives in a TI experiment archive (TI-1003 will
establish this). Each release gets a one-page entry:

- Hypothesis + primary KPI + MDE
- Design choice (A/B/C/D) + rationale
- Stratification variables + holdout %
- Pre-committed assignment table reference
- Analysis plan + stopping rules
- Stakeholders + timeline

**Auto-refresh** — every experiment runs on a Databricks schedule.
Dashboard is the source of truth; nobody re-runs ad-hoc analyses
manually.

**Alerting** — Slack notification if a treated arm underperforms holdout
by > MDE for ≥ 5 consecutive days (early-stopping harm signal).

---

## Canonical references

The literature for this is mature. Standout reads (ordered by relevance
to MNTN's situation):

| Reference | Year | Why it matters |
|---|---|---|
| **Vaver & Koehler** — *Measuring Ad Effectiveness Using Geo Experiments* (Google) | 2011 | Closest published analog to our situation — measuring ad-tech intervention effects at unit-level grain. Open-access. **Read this first.** |
| **Kohavi, Tang, Xu** — *Trustworthy Online Controlled Experiments* (book) | 2020 | THE industry reference for at-scale A/B testing. Microsoft/Bing/Google practices, written for practitioners. ~$45 on Amazon. **Read this second.** |
| **Deng, Xu, Kohavi, Walker** — *Improving the Sensitivity of Online Controlled Experiments by Utilizing Pre-Experiment Data* (Microsoft) | 2013 | CUPED. 20-50% sharper effect estimates with no design change. Implement in every rollout. |
| **Brodersen, Gallusser, Koehler, Remy, Scott** — *Inferring Causal Impact Using Bayesian Structural Time-Series Models* (Google) | 2015 | The CausalImpact paper. The methodology we use as secondary inference. |
| **Roth, Sant'Anna, Bilinski, Poe** — *What's Trending in Difference-in-Differences?* | 2022 | Modern DiD review. Parallel-trends violations, staggered adoption, the right ways to do DiD when treatment timing varies. |
| **Athey & Imbens** — *The State of Applied Econometrics: Causality and Policy Evaluation* | 2017 | Survey of DiD, synthetic control, IV. The "what's the right method for which problem" reference. |
| **Abadie, Diamond, Hainmueller** — *Synthetic Control Methods for Comparative Case Studies* | 2010 | Synthetic control methodology — useful when randomization fails and you need to construct a counterfactual. |
| **Athey, Bayati, Doudchenko, Imbens, Khosravi** — *Matrix Completion Methods for Causal Panel Data Models* | 2021 | Modern synthetic-control evolution. Handles staggered adoption + non-randomized data cleanly. |
| **Imbens & Rubin** — *Causal Inference for Statistics, Social, and Biomedical Sciences* (book) | 2015 | The canonical textbook. Heavy but authoritative on potential outcomes framework. |
| **Bojinov, Simchi-Levi, Zhao** — *Design and Analysis of Switchback Experiments* | 2023 | Not directly applicable (no marketplace switches), but useful for understanding when within-unit randomization beats between-unit. |
| **Berry & Berry** — *Bayesian Adaptive Methods for Clinical Trials* (book) | 2010 | Bayesian sequential analysis. Useful if we want adaptive cadence — continuously update posterior, stop when posterior probability of meaningful effect exceeds threshold. Replaces fixed-window cadence. |

---

## Summary

**Random assignment + stratification + permanent holdout + ≥28-day
final measurement window.** That's the irreducible methodology.
Everything else — cadence, number of waves, interim reads — is
operational pacing that trades statistical strength for speed.

The current Fangorn rollout taught us what the cost of skipping
randomization looks like: clean causal claim on Tier 2 (random sample),
methodologically suspect everywhere else. Building the rollout the
right way upfront is dramatically cheaper than recovering the read
afterward.

**Default to Option B.** Upgrade to A for high-stakes releases. Downgrade
to C when GTM truly can't accommodate. Avoid D unless emergency. Always
do the pre-flight checklist.
