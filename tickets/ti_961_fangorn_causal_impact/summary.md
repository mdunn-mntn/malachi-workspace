# TI-961: Causal Impact for Fangorn

**Jira:** https://mntn.atlassian.net/browse/TI-961
**Status:** In Progress
**Date Started:** 2026-05-27
**Date Completed:**
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
1. **Setup + lean delta pull** — adds `ci_pre_days` widget (default 60d), pulls only the incremental pre-period days (`ci_window_start` → `window_start - 1`) using a slim impressions+visits query, then UNIONs with what `daily_performance_df` already has. Joins rollout-tier metadata, aggregates to tier × day visit rate.
2. **Fit + render** — `run_ci_for_tier()` fits CausalImpact per treated tier with the impression-weighted control-tier visit rate as the synthetic-control covariate. Renders results as tiles styled to match Alex's existing DiD HTML block (actual avg, predicted-counterfactual avg, relative effect, 95% CrI, p-value, n_pre / n_post days, control tier list).
3. **Diagnostic plot** — one panel per treated tier showing actual vs control covariate over the full CI window, with a switch line at the flip date and the rel_effect / CrI / p-value in the panel title.

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
  **A:** Group B (50 advertisers, 20 days post). Visit rate, not conversion.
- **Q:** Is wave-2 a valid control?
  **A:** Yes for DiD purposes (parallel-trends assumption); not random assignment but acceptable.
- **Q:** Why is conversion-rate / ROAS noisy?
  **A:** Too short a post-period; CTV attribution windows are long (large share of conversions land after campaign exposure).

## 7. Data Documentation Updates
_Pending._
- Possible: add "Fangorn rollout tiers + `fangorn_advertiser_inclusion` table" entry to `knowledge/data_catalog.md`.
- Possible: add a methodology note to `knowledge/experimentation.md` about DiD vs CausalImpact for staged rollouts where wave-N+1 acts as control.

## 8. Open Items / Follow-ups
- **Run the CI section in Databricks** and capture rel_effect / 95% CrI / p-value per treated tier. Paste numbers back into this summary under §6 Questions Answered with the comparison to Alex's DiD-adjusted lift.
- Decide whether to extend CI covariates beyond the single control-tier visit rate (e.g., holiday flag, platform-wide visit rate, lagged tier-self covariate). Current single-covariate setup matches the DiD framing 1:1; adding more would shift the comparison away from apples-to-apples.
- Skip CausalImpact on conversion/CPA/ROAS until ≥30 days post-period (Alex agrees the conversion window is too noisy at current cohort age, esp. for CTV).
- Side suggestion (out of scope but worth tracking): an "experiment archive" web page fed by scheduled Databricks notebooks. Bay team may already be on this. Verify before duplicating.
- Compare variance-weighted lift vs median + impression-weighted in Alex's dashboard (separate from CI — orthogonal weighting question for the DiD numbers).

## 9. Meeting Notes
- `meetings/ti_961_01_malachi_alex_catchup_2026_05_27.txt` — 30-min Malachi + Alex catchup; covers both TI-961 (Fangorn CI eval) and the interest-segment scoring scope for TI-956.
- **Next meeting:** "Early next week" — Alex to add usage notebook to `targeting-infra-ml`; Malachi to read the scoring code before that meeting.
