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
1. Pull wave 1 cohort + tier definitions from `fangorn_advertiser_inclusion`
   (same join Alex uses in his dashboard).
2. Define pre-period and post-period dates per wave-1 sub-group:
   - **Group A (3 advertisers):** 26 days post, but only 3 advertisers — likely too thin for CI.
   - **Group B (50 random tier-1 advertisers):** 20 days post — best candidate, but 41 of 50 have active campaigns.
   - **Group C (312 remaining tier-1):** 7 days post — almost certainly too thin.
3. Decide cohort + KPIs:
   - **Visit rate** is the only KPI with a real chance of resolving at this window length.
   - Conversion / CPA / iROAS: defer — Alex's view is even 20 days is light, and CTV attribution windows are very long.
4. Apply the canonical MNTN CausalImpact pipeline (TI-504/542/803/504/849
   pattern): platform-aggregate covariates + holiday + lag, VIF→BIC→CI.
   See [reference_causal_impact_pattern.md](../../knowledge/experimentation.md)
   in memory.
5. Use Wave 2 (Tier 2, releasing Monday 2026-06-01) as the **control / counterfactual**
   — they are not yet treated. Note: not random assignment, but DiD's parallel-trends
   assumption is acceptable if pre-period trends match.
6. Compare CausalImpact output to Alex's DiD numbers from the dashboard.
   If CI confirms the visit-rate lift, that's the validation signal.
7. **Open follow-up:** Malachi raised the idea of a permanent "experiment archive"
   web page that updates from a scheduled Databricks notebook. Bay team is
   supposedly working on a dashboard but progress is unclear (likely held up by
   BQ migration). Out of scope for this ticket — capture as a follow-up.

## 4. Investigation & Findings
### Fangorn rollout state as of 2026-05-27 (per Alex)
| Wave | Tier | Cohort | Post-period days | Notes |
|------|------|--------|------------------|-------|
| 1A | tier-1 | First 3 advertisers | 26 | No order_amt; ROAS uncomputable |
| 1B | tier-1 | 50 random advertisers | 20 | 49 evaluated, 41 active campaigns |
| 1C | tier-1 | 312 remaining advertisers | 7 | 260 active campaigns running |
| 2 | tier-2 | Releases Mon 2026-06-01 | — | Middle-pack scores; some pulled (audience too small) |
| 3 | tier-3 | TBD | — | ~80% audience reduction at .8 threshold; Alex tuning threshold (.8→.6?) per-advertiser next sprint |

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
_Pending CausalImpact run._

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
- Run CausalImpact on **Group B visit rate** as the primary read.
- Skip CausalImpact on conversion/CPA/ROAS until ≥30 days post-period.
- Side suggestion (out of scope but worth tracking): an "experiment archive"
  web page fed by scheduled Databricks notebooks. Bay team may already be on
  this. Verify before duplicating.
- Compare variance-weighted lift vs median + impression-weighted in Alex's
  dashboard.
- Decide CI covariates: platform-aggregate visit rate, holiday flag, day-of-week,
  pre-treatment lag of the cohort itself.

## 9. Meeting Notes
- `meetings/ti_961_01_malachi_alex_catchup_2026_05_27.txt` — 30-min Malachi + Alex catchup; covers both TI-961 (Fangorn CI eval) and the interest-segment scoring scope for TI-956.
- **Next meeting:** "Early next week" — Alex to add usage notebook to `targeting-infra-ml`; Malachi to read the scoring code before that meeting.
