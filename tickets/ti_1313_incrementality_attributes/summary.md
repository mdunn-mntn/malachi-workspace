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

### 4f. Third audit (2026-09-02) and the final shape

A 16-agent audit against every AUDI-1313 requirement returned **11 findings above minor, 0 refuted**. All
were fixed. Three mattered.

**CRITICAL — the conversion pooling used the wrong denominator, and it was my bug.** `pool_conv` fed
`conv_rate_treatment`/`conv_rate_holdout` into the pooler alongside `n_treatment`/`n_holdout`. But
`lift__ghost_bid_results.conv_rate` is **per visitor, not per household**. Verified in BigQuery over all
3,384 partner-8 overall rows:

| Candidate denominator | max abs deviation from `conv_rate_treatment` |
|---|---|
| `conv_treatment / vis_treatment` | **0.0 exactly** |
| `conv_treatment / n_treatment` | 0.9999968670157855 |

Pairing a per-visitor rate with household counts inflated each campaign's effective sample size by a median
~80x, which understated the log-RR variance by that factor and corrupted both the inverse-variance weights
and the DerSimonian-Laird estimate. Corrected to `vis_treatment`/`vis_holdout`, the conversion side
collapses: pooled conversion lift clears zero for **1 of 27** attribute levels (Education, +27.1%
[+18.8%, +35.9%], k=15) rather than 8, and three previously positive levels turn negative. The whole
headline "attribution inflation" story the v3 sheet told was an artifact of this one mis-wiring.

**CRITICAL — two incompatible cost-per-incremental-conversion columns shipped side by side.** One divided
cohort-scaled prospecting media spend by ghost-bid `incremental_conversions`; the other divided reporting
total spend (media + data + platform, 2.30x larger) by an attributed-conversion bridge, and was unguarded,
so it rendered negative dollars on 376 of 758 rows wherever `conv_rel_itt < 0`. Denominators differed by a
median factor of 3.09. Resolved by deleting the second construction, guarding the survivor, and naming
each spend basis in the column header and the Read me.

**MAJOR — an MNTN internal account was inside the population.** Advertiser 30506, "MNTN - No ENG Testing"
(`is_test = TRUE`), passed every gate. The ticket explicitly requires excluding internal, test and demo
accounts, and only `campaign_groups.is_test` was being filtered, inside a CTE that was LEFT JOINed so it
excluded nothing. Fixed with `is_test = FALSE AND deleted = FALSE` on the advertiser CTE and inner joins
on both dimensions. Population 877 to 874, primary 209 to 208.

**Also fixed:** the ranked sheet sorted on raw best-minus-worst spread, which is an order statistic biased
toward high-cardinality attributes (17 verticals will out-spread 4 creative levels by chance); it now ranks
on a between-level Cochran Q p-value. Summary sheets reverted to unscaled spend while the detail sheet used
cohort-scaled spend; `scaled_spend` is now computed once in SQL and used everywhere. The inflation sheet
silently deleted every level whose conversion lift was negative, which selected on the outcome; it now
shows every level and blanks only the derived money columns. Negative conversion counts no longer render.

**The ranking after correction** (between-level p, ascending):

| Attribute | Levels | p | Best | Worst |
|---|---|---|---|---|
| Bids per household | 4 | 4.1e-12 | 11+ (+15.2%) | 1 (-5.4%) |
| Vertical | 16 | 0.0023 | Moving (+17.8%) | Vehicles (-2.2%) |
| Creative length mix | 4 | 0.014 | Mixed 30s-led (+10.1%) | 15s only (+4.1%) |
| Average frequency | 4 | 0.024 | 2.5 to 4.1 (+11.4%) | 1.1 to 2.5 (+6.6%) |
| Intent band | 5 | 0.74 | High (+8.4%) | Max Reach (+1.1%) |
| Geographic targeting | 6 | 0.99 | DMA (+8.7%) | State (+7.5%) |

Intent band and geographic targeting do **not** separate lift once the levels are tested against each
other. Bid frequency does, by a wide margin, and it is the one result that has survived all three audits.

## 5. Solution

Delivered `AUDI-1313 Campaign Incrementality by Attribute.xlsx` to
`My Drive/Tickets/AUDI-1313/`, built by `ti_1313_build_xlsx.py` from three committed queries.

**Population.** 874 campaign groups pass power and quality: `lift__ghost_bid_results` at
`stratum_type='overall'`, full clean gate (`se > 0 AND has_valid_holdout AND meets_min_n AND
meets_min_compliance AND NOT ghost_frac_inflated AND NOT arm_imbalance_suspect`), `vis_holdout >= 100`,
`partner_id = 8`, and no internal or test advertiser or campaign group. **208** of those also sit inside the
0.09 to 0.11 `ghost_frac` holdout validity band and delivered on at least 54 of the 71 window days; those
208 feed every summary sheet. All 874 ship on Campaign detail with both flags as columns.

**Method.** Relative lift is pooled with DerSimonian-Laird random effects on the log risk ratio, variance
`(1-p_t)/(p_t n_t) + (1-p_h)/(p_h n_h)`, per `data_catalog.md` (dividing a pooled absolute effect by a
pooled base rate is documented as unsound and reverses the band gradient). Heterogeneity is reported on
every row and runs above 85% on most, which is disclosed.

**Sheets.** Ranked hypotheses (headline), By frequency, By creative length, By geography, By intent band,
By vertical, By campaign frequency, Other attributes, Conversion outcomes, Attribution inflation,
Population choices, Holdout depth check, Campaign detail, Read me, Queries.

**Headline.** 208 campaign groups, 146 advertisers, 91 significant, pooled visit lift **+8.4%**
[+7.0%, +9.9%].

## 6. Questions Answered

- **Q:** Which campaign attributes correlate with strong incrementality?
  **A:** Bid frequency, decisively (p = 4e-12): households bid on once show -5.4% lift, those bid on 11+
  times show +15.2%. Vertical (p = 0.002), creative length mix (p = 0.014) and campaign average frequency
  (p = 0.024) also separate. Intent band (p = 0.74) and geographic targeting (p = 0.99) do not. All
  observational: households are not randomised into bid-count bands, so heavier exposure also marks a
  household the system judged more promising.
- **Q:** What did Kirsa's "950+ campaigns" refer to?
  **A:** 100+ holdout **visits** (`vis_holdout`), not holdout IPs (`n_holdout`), under the full clean gate.
  That yields 930. Adding the validated bidder leg, the holdout validity band and the ticket's own
  75%-days-live filter brings it to 208.
- **Q:** Is 15s or 30s creative better?
  **A:** Not answerable as a binary; 47% of campaigns run both. On the 4-level mix, 30s-led leads (+10.1%)
  and 15s-only trails (+4.1%), and the levels do differ (p = 0.014).
- **Q:** What does the conversion side say?
  **A:** Almost nothing. Pooled conversion lift clears zero for 1 of 27 attribute levels. The conversion
  half of this ticket is close to a null result and should be reported as such.

## 7. Data Documentation Updates

Routed to `knowledge/` by `/capture`. New durable facts established here:
`cost_impression_log.group_id` is not `campaign_group_id` (bridge via `campaigns.campaign_id`);
`lift__ghost_bid_results.conv_rate` is per visitor, not per household; `ghost_frac_inflated` guards only
the high side of the validity band; `sh_device` is unusable on the Beeswax leg; `creatives.length` carries
video seconds; `sum_by_campaign_by_day` is current, not stale to 2026-05-01; `bq_run.sh` needs
`--nouse_legacy_sql` and rejects leading `--` comment lines.

## 8. Open Items / Follow-ups

**Delivered against the ticket.** Section 1 filters all applied (75% days live measured from delivery not
config dates, underpowered excluded, internal/test/demo excluded). Section 2: the visit side is complete
with point estimate, CI, p-value, significance flag and control baseline; the conversion side is complete
but is close to a null result and is labelled as such. Section 3 attributes delivered: intent bands,
frequency (two ways), creative length, geography, vertical, multi-touch share, customer-file exclusion,
multi-touch access, product, budget, advertiser size, delivered footprint.

**Genuinely not available, checked and confirmed:**
- **Attribution window** exists but does not vary across these advertisers, so it cannot correlate with
  anything. Not shipped.
- **Audience size** is not stored anywhere in BigQuery. `has_audience` is TRUE for all 874, so it
  discriminates nothing and is not shipped as an attribute.
- **Media plan flag** was not located in any config table.

**Known caveats a reader should carry:**
- Customer-file exclusion is read from live audience config, not point-in-time; 45% of these advertisers
  edited an audience mid-window.
- Multi-touch access is an **advertiser-level** setting. There is no campaign-group display multi-touch
  toggle anywhere in BigQuery.
- Geographic class is the advertiser's stored targeting choice. Delivered DMA count ships separately as an
  outcome measure, and the two agree on 96.6% of campaigns.
- Heterogeneity above 85% on most rows: a pooled number is the centre of a wide spread, not a value to
  expect from a single campaign.

**Ticket blocked upstream:** sections 1 and 3 of AUDI-1313 contain tables that render as literal
`<Table 5x2>` and `<Table 34x1>` placeholders in the Jira UI itself. The 34 named attributes have never
been readable by anyone. Malachi has asked Kirsa for them. If they arrive, re-check coverage against the
real list; the current scope came from the retrievable prose plus Kirsa's spoken asks.

**Third reading of the intent-band gradient.** Section 4f's ordering (High > PP > Unscored > Mid >
MaxReach, and not significant between levels) is a third distinct reading, after the AUDI-1209 log-RR
result and the earlier abs/base-rate one. Append to the contradiction already recorded in
`experimentation.md`; do not overwrite either prior reading.

**Remaining process work:** self-review entry, Jira comment to Kirsa, `/capture` sweep of section 7's
durable facts.

## 9. The real ticket tables (2026-09-02) and the window problem

Kirsa fixed the broken tables in AUDI-1313. The scope table and the 34-attribute list are readable for the
first time. An 18-agent hunt checked every item against BigQuery.

### 9a. The trailing-30-day window is the ticket's ask and is the WORST window available

The scope table says **Window: trailing 30 days**. The workbook computes all-time lift over the full 71-day
ghost-bid span, because `gold.reporting.lift__ghost_bid_results` has no `dt` or `period` column. A windowed
read is possible from `silver.enriched.lift__ghost_bid_visits` and was built and run.

**It costs most of the population.** Of the 874 shipped campaign groups: 669 have any entry-cohort row in
the trailing 30 days, 663 have a computable lift, **308** clear `vis_holdout >= 100`, **126** also sit
inside the `ghost_frac` band, **115** also have 23+ of 30 entry days. The 208-campaign primary set the
summary sheets are built on becomes **95**.

**And it is the dirtiest end of the table.** This corrects a hypothesis recorded earlier in this ticket
that a shorter window would be cleaner. `data_catalog.md` gotcha (7) makes the self-poisoning a function of
**calendar entry date, not window length**: later entry cohorts are progressively treatment-only. So a
short window at the START of the table is clean and a short window at the END is dirty, and the trailing
30 days is the end.

Measured, on the same cohort:

| Window | Pooled rel lift | Pooled ghost_frac |
|---|---|---|
| Trailing 30d (2026-07-27 to 08-25) | +18.4% [+17.9, +18.9] | 0.0862 |
| Documented clean band (2026-06-23 to 07-07) | +6.3% [+6.0, +6.6] | 0.0972 |
| Full 71-day span | (workbook: +8.4% on the gated 208) | 0.0918 pooled |

**A pooled `ghost_frac` is not evidence a window is clean.** The full-span 0.0918 sits in-band only because
an inflated left edge (0.118) offsets a depleted tail; two opposite biases cancel in the diagnostic. The
by-day curve is the real check. This weakens the workbook's own per-campaign `in_validity_band` flag, which
is computed on an all-time pooled `ghost_frac`: it is a useful filter but not proof any individual campaign
was measured cleanly.

### 9b. The conversion side cannot survive a 30-day window

Two independent reasons. First, power: conversion lift already cleared zero for only 1 of 27 attribute
levels on the full 71 days, and 30 days removes what little remained. Second, a **conversion data hole at
2026-08-20 to 2026-08-25** sits inside any trailing-30 window. The conversion outcome needs materially more
than 7 days to mature and the SQLMesh rebuild does not refresh uniformly, so the standard
`DATE_SUB(MAX(dt), INTERVAL 7 DAY)` guard protects visits but **not** conversions. Any conversion metric
must end its window at **2026-08-19**, a 13-day lag, and say so. A 30-day deliverable should be visit-lift
only and state that plainly rather than shipping empty conversion columns.

### 9c. Attribute coverage against the real 34

**Newly located and verified:** avg household score and the four intent-band household shares (household
collapsed, share of SCORED households, shipped beside an explicit unscored share); device % spend to TV and
to Mobile/Tablet; % spend to Display; live-advertiser status; advertiser tenure in months; fcap settings;
VV attribution window.

**Already in the workbook:** vertical, overall frequency, spend, impressions, unique households reached,
geo targeting, CRM exclusion, Display MT enabled, Select campaign, advertiser MUVs.

**Dead or useless:** `cost_impression_log.sh_device` (the previously assumed device source, confirmed
unusable); Device % Spend to Desktop; advertiser `account_health` / `company_size` / tier (all
non-discriminating across this population).

### 9d. Two corrections to earlier work in this ticket

**Device % Spend to Mobile/Tablet is OTT video on a phone or tablet screen, NOT display advertising.**
100% of those rows are `partner_ad_format = 'VIDEO'` and `publisher_type_id = 1`. It does not overlap the
separate "% spend to Display" attribute and must never be labelled as display. It is also perfectly
anti-correlated with % spend to TV (-0.999999), so the two carry one piece of information, not two.

**`objective_id` is not the stage key.** Campaigns with `objective_id = 1` sitting at `funnel_level` 2 and
3 carry **$1,804,164** inside this cohort: 70.9% of all non-prospecting spend and 9.25% of total. Reading
`objective_id` alone as the stage key would pull $1.8M of stage-2 and stage-3 CTV into the prospecting
aggregate. The workbook's `objective_id = 1 AND funnel_level = 1` is correct; the single-condition version
is not.

**The population is not stable.** Re-running the documented base gate today returns 897 rows against the
877 recorded on 2026-09-01, and 890 against 874 after the test-account inner joins.
`lift__ghost_bid_results` is all-time with no `dt` column and is rebuilt daily, so the cohort drifts. Any
number quoted from it needs its read date attached.

## 10. Fourth audit (2026-09-02) and the corrected window reasoning

12 agents, **8 findings above minor, 0 refuted**. All fixed. Three mattered.

**The window sensitivity sheet compared ungated populations, and correcting it overturns my own reasoning.**
The sheet's campaign counts were gated but its lift and holdout-share columns were ratio-of-sums over every
campaign group in the window, including unpowered and out-of-band ones. Regated:

| Window | Powered | In band | Lift, ungated | Lift, powered | Lift, powered and in band |
|---|---|---|---|---|---|
| Clean band, 23 Jun to 7 Jul | 672 | 540 | +6.3% | +6.0% | **+6.0%** |
| Full span, 23 Jun to 1 Sep | 1,023 | 552 | +12.0% | +11.3% | **+8.3%** |
| Trailing 30 days | 327 | 140 | +18.4% | +16.6% | **+8.2%** |

Once the quality gates are applied the three windows **converge**. The "later window reads high" gradient is
almost entirely a property of the ungated population. So the honest reason to prefer the full span over the
ticket's trailing 30 days is **power** (190 campaigns against 95), not bias. Section 9a overstated this and
is corrected here. The conditioning is expected: `ghost_frac` is the mediator of the depletion bias, so
gating on it removes the contrast by construction.

**The Read me denied an attribute the workbook ranks fourth.** It carried the stale section 8 line saying
attribution window does not vary. The VISIT attribution window
(`silver.public.advertisers.clickpass_acquisition_ttl`) varies over 9 levels from 1 to 45 days, is populated
on all 190, and ranks 4th of 13 at p = 0.0039. The earlier "does not vary" reading came from the wrong
column: `view_conversion_window` / `click_conversion_window` are the CONVERSION-side fields and those are
genuinely constant at 30 days for every advertiser here. `data_knowledge.md` (PS-8572) already records that
these are three independent lookback knobs that must never be conflated. Section 8 is superseded.

**14 of the ticket's 33 named attributes were silently missing, and 8 were already in hand.** Impressions,
households reached, advertiser MUVs and advertiser AOV were in the query output and had been dropped at the
detail projection. Avg HHST was located at `silver.dso.household_score_thresholds` (159 of 190). Stage-2 and
stage-3 spend shares were collapsed into a single multi-touch column. Media plan exists at
`silver.core.media_plan` keyed on campaign_group_id, which overturns section 8's "not located in any config
table". All are now columns. Four are genuinely flat in this population and now carry an explicit Read me
line rather than silence: conversion attribution window (30 days for every advertiser), desktop (under 0.03%
of spend), retargeting (no delivery at all), media plan (0 of the 190). Four are genuinely absent: audience
type, targetable audience size, advertiser CVR, advertiser sales cycle.

**Also fixed:** the ranked sheet claimed 190 campaigns for its top result when the bid-count strata cover
119; conversion counts and cost now blank wherever the pooled conversion interval includes zero, matching
the inflation sheet; the household collapse now excludes the `0.0.0.0` sentinel IP, which would otherwise
have merged millions of impressions into one fake household on 16 campaign groups; device shares now carry
an explicit unknown column (max 0.22%); all three extracts are same-day.

**Final shape:** 890 campaign groups on Campaign detail across 67 columns, 190 in the summary population
(holdout band, 75%+ days live, live advertiser), 130 advertisers, 83 significant, pooled visit lift +7.9%.
13 attributes ranked; 6 separate at p < 0.05.

## 11. Delivery (2026-09-02)

Workbook sent to Matt Brorby and Kirsa Haenebalcke for review, marked **DRAFT - NOT FINAL** on all 20
sheets. Jira comment 614657 posted to AUDI-1313 via REST v2.

**New standing convention, from the user:** every .xlsx deliverable ships marked `DRAFT - NOT FINAL` until
the work is confirmed and the ticket is closed, then switches to Final. This is now enforced rather than
remembered: `lib/mntn_xlsx.py` `MntnWorkbook.__init__` had `status: str = "Final"` and now defaults to
`status: str = "DRAFT - NOT FINAL"`. Final is the deliberate act. Marking every sheet takes three places,
not one: `status=` covers the Overview cover, `period=` propagates into every table sheet's Source footer,
and a glossary tab and a `sql_dir` tab need it in their own `intro=` and `note=`.

**Open until review comes back:** whether the 190-campaign primary population is the right cut for Kirsa's
playbook purpose, and whether Matt agrees the conversion side should be reported as a null rather than
dropped. The ticket stays open; status flips to Final only after both confirm.
