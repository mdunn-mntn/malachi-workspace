---
doc_type: ticket
title: "TI-1313: incrementality attributes"
status: in_progress
date: 2026-09-01
summary: "incrementality attributes"
result: "not started"
question: ""
framing_state: locked
question: "Which campaign attributes correlate with strong incrementality performance?"
---

# TI-1313: incrementality attributes

**Jira:** https://mntn.atlassian.net/browse/TI-1313
**Status:** backlog
**Date Started:** 2026-09-01
**Assignee:** Malachi

---
## 0. Framing  ← agree this via /frame BEFORE work starts; set `framing_state: locked` when done
The agreed question, why it matters, and how we plan to answer it. Locked before `status: in_progress`.
- **Question (the unknown):** Which campaign attributes (vertical, audience composition, frequency, channel mix, device split, attribution windows, etc.) correlate with strong incrementality performance?
- **Goal (why / the decision):** Build internal AM/CSM optimization playbook; surface product settings/gaps worth defaulting. Requestor: Kirsa (current experiment results inconclusive at advertiser level; hypothesis is that signal emerges when sliced by campaign attributes).
- **Objective (done-when):** .xlsx with one row per powered campaign (950+ with 100+ holdout visits) showing: visit/conversion lift %, CI, p-value, significance, baseline rates, cost-per-incremental, attributed % · plus all campaign attributes (score dist, freq, audience type/size, spend, device mix, geo, stage mix, advertiser health, attribution windows, etc.) · plus stratified summaries by vertical × primary channel.
- **Approach (how):** (1) Join `lift__ghost_bid_rollup` (entity_id=campaign, level=campaign_group, 30d trailing) with campaign attributes from bid logs + advertiser metadata. (2) Filter: prospecting only, 75%+ days live, 100+ holdout visits. (3) Compute outcome metrics (lift %, CI, p-value, sig, baseline) for each campaign. (4) Append all attribute columns. (5) Output raw data + stratified summaries (vertical × channel). *Critical frame:* observational, not causal — attributes are advertiser-chosen and confounded; output is ranked hypotheses for testing, not causal claims.
- **What would change the answer:** Powered population <500 campaigns = underpowered for reliable patterns; recommendation would be to wait for more historical data or run randomized experiment on high-signal attributes.

## 1. Introduction
Brief context: what system/feature/data is involved, and why this ticket exists.

## 2. The Problem
What exactly is broken, unclear, or needed? Include:
- Symptoms observed
- Who reported it / who it affects
- Impact (data quality, revenue, user experience, etc.)

## 3. Plan of Action

### Phase 1: Data Exploration & Query Design (IN PROGRESS)
1. ✅ Locate lift metrics table: `dw-main-gold.sqlmesh__reporting.reporting__lift__ghost_bid_rollup__*`
2. ✅ Locate campaign attributes: `dw-main-silver.public.campaign_groups`
3. ✅ Locate advertiser metadata: `dw-main-bronze.integrationprod.advertisers` + `dw-main-silver.fpa.advertiser_verticals`
4. ✅ Map available columns for all required attributes
5. ⚠️ Draft main query (SQL file created; TODO: verify CIL impression aggregation columns)
6. ⚠️ Test query: sample run on lift data, validate row counts & shapes
7. Aggregate campaign attributes from cost_impression_log (device, scores, spend, impressions)
8. Build final .xlsx with raw data + stratified summaries (vertical × channel)

### Known Gaps (To Resolve)
- **Stage mix (S2/S3)**: Not in campaign_groups; may need campaign-level join or objective mapping
- **Attribution windows**: Not found in campaign_groups; may be in flight config
- **CRM exclusion, Display MT, media_plan**: Unknown table locations (may need PM/Jira inquiry)
- **Spend/impressions window**: Lift is all-time, CIL can be windowed; using all-time for consistency
- **Device columns**: Need to verify `sh_device` + other device fields in CIL
- **Household scores**: NULL before 2025-06-01 in CIL (recoverable from model_params back to 2025-05-06)

## 4. Investigation & Findings

### 4a. Schema resolution (2026-09-01)

Three join defects were found and fixed during the first build. All three are durable schema facts:

1. **`cost_impression_log.group_id` is NOT `campaign_group_id`.** CIL `group_id` values sit in the ~1.1-1.2M
   range; `campaign_group_id` values are ~24K-131K. Joining them returns zero rows, silently, as a LEFT JOIN
   producing all-NULL attribute columns. The correct bridge is
   `cost_impression_log.campaign_id -> public.campaigns.campaign_id -> campaigns.campaign_group_id`.
   Verified: CIL campaign_id 643620 -> CG 130485, 397337 -> 85144, 147574 -> 24081.
2. **`fpa.advertiser_verticals` has multiple rows per advertiser** (type 0 parent, type 1 sub). A naive
   `SELECT DISTINCT advertiser_id, vertical_name` join Cartesian-multiplies the output: 2,224 campaign groups
   became 4,409 rows, with WGU appearing under both "Education" and "Colleges & Universities". Fixed with
   `QUALIFY ROW_NUMBER() OVER (PARTITION BY advertiser_id ORDER BY vertical_id) = 1`. NOTE: this picks the
   lowest vertical_id arbitrarily and may mix parent and sub verticals in one column. Still open.
3. **`cost_impression_log.sh_device` is unusable for partner_id = 8 (Beeswax).** It is NULL on ~72% of
   impressions and never takes the values 'CTV'/'Display'/'Mobile'; the real domain is
   'COMPUTER'/'MOBILE'/'TABLET'. Every `pct_ctv`/`pct_display`/`pct_mobile` column read 0.0000. Channel must
   come from `public.campaigns.channel_id` (8 = CTV, 1 = Display) instead. Consistent with the documented
   Beeswax CIL enrichment gap in `data_catalog.md`.

Tooling note: `.claude/scripts/bq_run.sh` passes through to `bq query`, which defaults to **legacy SQL**.
Every CTE query fails with `Encountered "WITH" ... Was expecting: <EOF>` until `--nouse_legacy_sql` is passed.
The SQL must also be the final positional argument; piping it on stdin does not work.

### 4b. Pre-delivery audit (2026-09-01) — v1 workbook REJECTED

A 17-agent adversarial audit (4 independent lenses, every high-severity finding put to a refutation agent)
was run against the first workbook before it reached Kirsa. **11 findings confirmed, 1 refuted.** Three are
critical and invalidate the v1 deliverable. It was not sent.

**CRITICAL 1 — the population was gated on the wrong column.** The build filtered `n_holdout >= 100`, which is
holdout **IPs**. The locked framing in section 0 specifies 100+ holdout **visits** (`vis_holdout`), and that is
what the reference number in the ask meant. Shipped 2,215 campaign groups of which **1,441 (65%) had under 100
holdout visits**; median shipped row had 42, and 129 rows had exactly zero. Consequence: 130 rows shipped with a
blank lift and blank CI, 94 of them displaying `Significant = TRUE`, contradicting the workbook's own glossary.
Fixed-effect inverse-variance weighting largely protected the *pooled* column (CTV 13.5% vs 13.0% correctly
gated), but the *unweighted* columns moved hard: Food & Beverage median lift 20.8% -> 5.8%, Healthcare
22.3% -> 10.1%. The top-ranked vertical on the headline sheet, Professional Services at 29.05%, rested on 48
campaigns of which **only 4** were powered; correctly gated it falls below the 5-campaign floor and vanishes.

**CRITICAL 2 — delivery attributes mixed all funnel stages into a prospecting-only outcome.** The impression
aggregate grouped CIL by `campaign_group_id` with no objective or funnel filter, so the attributes describe
Prospecting + Multi-Touch S2/S3 + Ego, while the lift outcome is 100% prospecting by construction. Non-prospecting
is 21.2% of impressions and 3.8% of media spend in the audited week. Critically, `channel_id = 1` (Display) occurs
**only** at objective_id 5 and 6 in this cohort, so `pct_display_chan` was never a channel choice: the "By channel"
sheet's Display bucket (183 groups, +16.6% pooled, ranked ABOVE CTV) actually meant "groups whose multi-touch
display retargeting out-delivered their prospecting CTV". The headline channel finding was a confounded artifact.
`data_knowledge.md` already documents this: grouping delivery by campaign_group_id conflates stages; always split
by objective_id.

**CRITICAL 3 — intent-band percentages divided by a denominator that is mostly unscored.** The four `pct_*_intent`
columns divided by `COUNT(*)`, which includes the `household_score = -1` unscored sentinel (69.3% of impressions
platform-wide; 57.5% impression-weighted in this population). No `pct_unscored` column was emitted despite the
scope doc specifying one. 456 campaign groups had all four bands at exactly 0.0000; the build script's
`idxmax` tie-break then silently resolved all of them into **"High Intent"**, so the shipped High Intent stratum
(1,718 campaigns, $16.8M spend) was 26.4% campaigns with zero scored impressions. The band cutpoints themselves
(>=8001 High, 6666-8000 PP, 3333-6665 Mid, 1-3332 MaxReach) were verified correct.

**MAJOR findings also confirmed:** fixed-effect pooling reports CIs on I-squared = 93% heterogeneity, which is
false precision (random-effects DerSimonian-Laird required); `% significant` and `Pooled lift` were computed over
different campaign sets within the same table row; the period label "Lift: all-time" is false, the ghost-bid data
floor is 2026-06-22 with no backfill.

**Refuted (1):** a claim that CPIV divides a 2-month spend by an all-time visit count was investigated and rejected.

### 4c. The correct population, and a better source table

The expected count is confirmed and reconciled. Against `dw-main-gold.reporting.lift__ghost_bid_results` at
`stratum_type = 'overall'`:

| Gate | Campaign groups |
|---|---|
| All rows | 4,048 |
| `se > 0` | 3,532 |
| Full clean gate | 3,242 |
| Full clean gate AND `vis_holdout >= 100` | **930** |

930 is the ask's "950+". The v1 workbook's 2,215 was the artifact of gating on holdout IPs with a partial gate.
Full clean gate = `se > 0 AND has_valid_holdout AND meets_min_n AND meets_min_compliance AND NOT ghost_frac_inflated
AND NOT arm_imbalance_suspect`.

**`lift__ghost_bid_results` is the better base table than `lift__ghost_bid_rollup`** for this ticket, because it
carries per-campaign STRATA that answer the ask directly instead of by proxy:

- `stratum_type = 'score_band'`, `stratum_value` in {High, PP, Mid, MaxReach, no_score} — the sanctioned intent-band
  decomposition, per campaign, with its own lift, SE and significance. This replaces the hand-banded CIL
  `household_score` percentages entirely and removes CRITICAL 3 at the root. `data_catalog.md` explicitly warns
  against hand-banding (`eff_score` matches the documented cutpoints on only 51% of cells).
- `stratum_type = 'bid_count'`, `stratum_value` in {1, 2-3, 4-10, 11+} — lift by bid frequency, per campaign. This
  is the direct answer to the "average frequency" attribute in the ask, which v1 did not measure at all.
- `stratum_type` also carries `score_band_ivw` and `score_band_mh` pre-combined rows (3,037 campaigns each).
- Native `p_value`, `ip_compliance`, `holdout_won_rate`, `incremental_roas`, `ntb_*` (new-to-brand) columns.

**Caveat found while checking:** `treatment_spend` is populated on only **18 of 4,048** rows, so spend and any
cost-per-incremental metric still have to come from `cost_impression_log`, filtered to `objective_id = 1 AND
funnel_level = 1` to match the outcome. The window mismatch (lift from 2026-06-22, CIL attributes Jul-Aug 2026)
therefore remains and must be disclosed, not hidden.

**Also pending from `data_catalog.md`:** pooling relative lift as `IVW(abs_itt) / IVW(base_rate)` is documented as
unsound for low-baseline strata and reverses the band gradient. Pool on the **log risk ratio** instead, variance
`(1-p_t)/(p_t*n_t) + (1-p_h)/(p_h*n_h)`. Helper exists at
`tickets/incr_75_eligible_advertisers/artifacts/incr_75_lift_stats.py`.

### 4d. Second audit (2026-09-01) — v2 REJECTED, holdout depletion found

A 12-agent verification pass was run against the rebuild. It confirmed the three v1 criticals were fixed,
and found **six new criticals**, two of which are the same defect found independently by two lenses.

**CRITICAL — the estimator self-poisons as the holdout thins, and the clean gate is one-sided.**
`ghost_frac` is the observed share of a campaign's households in the holdout arm (it equals
`n_holdout / (n_holdout + n_treatment)` to a correlation of 1.000). `data_catalog.md` records the
entry-cohort ghost-bid estimator as valid only while `ghost_frac` sits in **0.09 to 0.11**. The rollup's
own `ghost_frac_inflated` flag fires **only above ~0.15** (6 rows in the entire overall stratum), so it
guards the high side and nothing guards the low side. Of the 930 v2 campaigns, **490 (53%) sat below 0.09**.

Measured lift is strongly monotone in holdout depth, which is the signature of a depleting holdout rather
than a real effect. Re-pooled with the workbook's own estimator:

| Holdout share | Campaigns | Pooled lift |
|---|---|---|
| Under 8% | 165 | +16.4% |
| 8 to 9% | 281 | +16.7% |
| 9 to 10% | 369 | +8.4% |
| 10 to 11% | 40 | +2.1% |
| Over 11% | 22 | **-13.4%** |

Spearman(ghost_frac, rel_itt) = -0.325, p = 2.8e-24. It survives inside every holdout-visit tercile and on
partner 8 alone, so it is neither a small-sample artifact nor a bidder-leg artifact. The v2 headline of
+14.7% was an artifact of pooling mostly-depleted campaigns; the in-band figure is **+7.8%**.

**CRITICAL — partner_id = 8 was dropped in the rebuild.** v1 filtered it; v2 did not. `data_catalog.md`
ghost-bid gotcha (8) records partner 79 (the Rust leg) as having no trustworthy holdout. This produced the
false headline **"Select +76.1% vs PTV +11.6%"**: 47 of 58 Select groups and 98.9% of Select's incremental
visits were partner 79. Product and leg are 91% collinear in this population, so the two cannot be
separated. On partner 8 alone Select is 6 campaigns at +6.6% against PTV +7.9%, intervals overlapping:
**there is no measurable product effect here.** The contamination also reached the vertical sheet, moving
Insurance from rank 2 to rank 22 and Household Goods from rank 3 to rank 10.

**CRITICAL — the strata sheets applied the power gate only at campaign grain**, not at stratum grain, so
individual bands under 100 holdout visits were pooled. Fixed by adding `vis_holdout >= 100` to the stratum
select in both strata queries.

**MAJOR — cost per incremental visit** divided full-campaign impression-log spend by measured-cohort
incremental visits, mixing bases. Now scaled to the measured cohort as `ip_compliance x n_treatment /
prospecting_ips` per `data_catalog.md`, and blanked where a campaign measured no incremental visits
(182 rows).

**MAJOR — a Read me count was wrong by 975**, and `% significant` was computed over a different row set
than `Pooled lift` in the same table row (the v1 defect recurring). Both fixed; `pool()` now returns the
surviving-row mask and every column in a row is computed on it.

**Refuted (1):** a claim that the period label asserts a window the base table cannot express.

### 4e. Final shape

- **Campaign detail:** 877 campaign groups. Full clean gate, 100+ holdout visits, partner 8, `ghost_frac`
  and an in-band flag carried as columns.
- **Summary sheets:** the **409** campaigns inside the 0.09 to 0.11 holdout band, 256 advertisers,
  149 significant, pooling to **+7.8%**.
- **A "Holdout depth check" sheet ships the gradient above**, so the exclusion is visible rather than asserted.

**Result that survived both audits:** lift rises monotonically with bid frequency, from **-3.9%** at one bid
per household to **+12.6%** at 11 or more. Households are not randomised into bid-count bands, so this is
a pattern and not a dose response, but it is the strongest attribute signal in the data and it held through
the partner and holdout corrections.

**Result that did NOT survive:** the v2 intent-band ordering appeared to reproduce the AUDI-1209 log-RR
result (Unscored highest). After the partner and holdout corrections the ordering is monotone in intent
instead (High +8.0%, PP +7.4%, Unscored +6.6%, Mid +4.8%, MaxReach +3.7%). The earlier corroboration claim
is withdrawn. This is a **third reading** of the band gradient and should be appended to the existing
contradiction in `experimentation.md`, not used to overwrite either prior one.

## 5. Solution

What was done to resolve the issue:
- Code changes (PRs, commits)
- Configuration changes
- Recommendations made
- Dashboards/reports created

## 6. Questions Answered
Specific questions that were resolved during this ticket:
- **Q:** {question}
  **A:** {answer}

## 7. Data Documentation Updates
What new knowledge was added to `data_catalog.md` or `data_knowledge.md` as a result of this ticket.

## 8. Open Items / Follow-ups

Checked against the AUDI-1313 description on 2026-09-02. The tables in sections 1 and 3 render as
placeholders through the REST API (no `table` node in the ADF, no attachments, no comments), so the full
34-attribute list could not be read; the gaps below come from the prose that IS retrievable plus Kirsa's
spoken asks.

**Section 1, inclusion filters**
- Minimum 75% days live in the full window: **NOT applied.** Learning-period contamination is not excluded.
  `campaign_groups.start_time`/`end_time` are carried in the base query, so this is a filter away.
- Exclude underpowered: applied (100+ holdout visits, full clean gate, holdout validity band).
- Exclude internal, test and demo accounts: **partial.** `campaign_groups.is_test = FALSE` only; no
  advertiser-level internal/demo exclusion.

**Section 2, outcome metrics.** The visit side is complete; the conversion side largely is not.

| Metric | State |
|---|---|
| Visit lift %, CI, p, significance, baseline | delivered |
| Incremental visits | delivered |
| Cost per incremental visit | delivered, cohort-scaled, media spend only |
| Attributed IVR | **missing** |
| % of attributed visits that are incremental | **missing** |
| Conversion lift % | partial: point estimate and significance on Campaign detail, no CI |
| Incremental conversions | in the base CSV, not surfaced in the workbook |
| Cost per incremental conversion | **missing** |
| Control-group baseline conversion rate | **missing** |
| Attributed CPA | **missing** |
| **Attribution inflation ratio (attributed CPA / incremental CPA)** | **missing** |
| % of attributed conversions that are incremental | **missing** |

The attribution inflation ratio is the metric that most directly serves the ticket's stated goal of surfacing
product gaps, and it is absent. It needs Reporting attributed visits and conversions joined to the ghost-bid
incremental counts. `data_catalog.md` ghost-bid gotcha (4) already documents the method: `incremental_VV =
Reporting_VV x rel_lift / (1 + rel_lift)` with `Reporting_VV = SUM(clicks + views + competing_views)` over
objective 1 for the cohort and window. That is the path for the whole conversion side.

**Section 3, attributes not delivered:** creative length (15s vs 30s, an explicit Kirsa ask), attribution
window, CRM exclusion, Display multi-touch flag, media plan flag, geography, audience size and type. None
were located in BigQuery; several likely need a PM or the campaign config service.

**Expectation gap to flag with Kirsa:** she anticipated 950+ campaigns. 930 clear the power gate and 877 the
bidder-leg gate, but only **409** sit inside the holdout validity band, and the summary sheets use those.
The other 468 ship in Campaign detail with the reason shown on the Holdout depth check sheet.

**Third reading of the intent-band gradient** (section 4e) should be appended to the existing contradiction
in `experimentation.md`, not merged over either prior reading.

**Not yet done:** self-review entry, Jira comment to Kirsa, `/capture` sweep of the durable schema facts in
section 4a (the `cost_impression_log.group_id` bridge, the `sh_device` Beeswax gap, the one-sided
`ghost_frac_inflated` flag, the `bq_run.sh` legacy-SQL and leading-comment footguns).
