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

## 12. Kirsa's first review round (2026-09-02)

Three questions came back on Slack. All three are now answered in the workbook (22 sheets, up from 20).

### 12a. "Are the frequencies across the entire data range? 2+ months?"

**Yes.** Both frequency sheets count once over the whole 71-day span, 2026-06-22 to 2026-08-31. Nothing is
per-week or per-month. `11+` on **By frequency** means 11 or more bids on that household across ten weeks;
`avg_frequency` on **By campaign frequency** is `prospecting_impressions / prospecting_ips` over the same
span. The window is not a choice: `gold.reporting.lift__ghost_bid_results` has no `dt` or `period` column
and the ghost-bid floor is 2026-06-22 with no backfill (§9a), so the strata cannot be re-cut to a shorter
window without recomputing from silver.

Fixed by stating it rather than leaving it inferable: both sheet findings and both method lines now name
the span, and the Read me carries a "Frequency is over the whole window" entry.

### 12b. "Some tabs have cost per inc visit and some do not, especially the intent band tab"

The two strata sheets (**By frequency**, **By intent band**) had no spend columns because the strata come
from the gold lift table, which reports lift per stratum but no spend per stratum. Now added on both, plus
**Holdout depth check** which had the same gap for no reason.

**`treatment_spend` exists in the gold schema and is unusable: populated on 18 of 3,978 `overall` rows and
0 of 15,732 `bid_count` and 9,401 `score_band` rows** (verified 2026-09-02). Same for `treatment_impressions`
and `cost_per_bid`. Anything below campaign grain has to be allocated.

**First attempt, refuted by its own check.** Measure prospecting spend per (campaign group, intent band)
straight from `cost_impression_log`, banding each household on `MAX(household_score)`. This is exact and it
correctly captures that unscored delivery runs ~26% higher CPM ($13.01 vs ~$10.2 for every scored band).
It fails because **the impression log's band assignment does not agree with the platform's own strata.**
Across the 381 campaign groups in both, the mean total variation distance between the two band mixes is
**0.284**, and **55.9% of campaigns disagree by more than 10 points**. Mean mix, platform vs impression
log: High 62.8% / 36.7%, Unscored 29.8% / 48.3%. Four large campaign groups (96237, 81053, 106777, 117662)
have platform High-band strata while their impression log tops out at `household_score = 8000` exactly, so
they hold zero High-band impressions and 15% of allocated spend went missing. **The platform bands at bid
time; `cost_impression_log.household_score` is a different value.** Do not use one to weight the other.

**Shipped instead:** allocate each campaign's `scaled_spend` across its strata by the stratum's share of
`bid_count_treatment`, which is in the gold table at both grains and sums exactly to the campaign total
(verified: 3,978 of 3,978 campaign groups match `overall` to the unit, for both stratum types; allocated
spend recovers 100% of campaign spend on both sheets). The assumption is a flat cost per bid inside a
campaign, and it is stated on both sheets and in the Read me. Because the band assignment is contested,
the measured per-band CPM correction is NOT transferable and was dropped rather than applied.

Result. Bids per household: 4-10 = $35.69, 11+ = $18.11. Bands `1` and `2-3` net out to **negative**
incremental visits (-22,529 and -7,452), so their cost per incremental visit is left blank rather than
shown negative, with a Read me line saying why. Intent band: Unscored $19.32, Peak Performance $21.35,
High Intent $28.11, Mid Intent $76.78.

### 12c. "I'm not seeing any data for the audience size, and how does it relate to bids per household?"

**The Read me was wrong and is corrected.** It listed "total targetable audience size" as not stored
anywhere we could find. It is stored: **`dw-main-silver.perml.flight_cid_day_audience_sizes`**
(`campaign_id, campaign_group_id, rpt_day, funnel_audience_size, total_audience_size, tmul_*`), already
documented in `data_knowledge.md` from TI-1026. The earlier sweep missed it. Coverage is complete: **890 of
890 campaign groups, 190 of 190 in the primary population.**

Taken as the median across delivered days over prospecting campaigns, so a mid-window audience edit does
not decide the value. Median 4.1M, range 35,904 to 91.4M. Caveat carried on the sheet and in the Read me:
this is the stored user expression and **overstates the deliverable pool** (no DS14 clause, no holdout
carve-out, no retargeting exclusions), so it ranks campaigns rather than giving a level.

**Kirsa's actual question is the sharp one: bids per household is an outcome, audience size is a setting.**
It resolves cleanly. On the 190 primary campaigns, Spearman rank correlation of audience size against:

| against | rho | p |
|---|---|---|
| average frequency | **-0.487** | 1.0e-12 |
| impressions per audience member | -0.701 | 2.2e-29 |
| households reached as share of audience | -0.663 | 1.9e-25 |
| households reached (count) | +0.678 | 6.1e-27 |
| **visit lift** | **+0.045** | **0.54** |

And on the 119 campaigns that also have bid-count strata, share of treated households by band against
audience size: bid once **+0.377** (p=2.4e-05), 2-3 **+0.386**, 4-10 **-0.216**, 11+ **-0.295** (p=1.1e-03).

By audience-size quartile, share of households bid on 11+ times: 48.4% / 33.8% / 29.7% / 24.1%. Median
campaign frequency: 7.55 / 4.41 / 3.78 / 2.81. Reached as a share of audience: 45.6% / 14.2% / 6.4% / 2.3%.

**So audience size is the controllable lever behind the bid-frequency finding.** New sheet **Audience size
and frequency** carries this. It is still a correlation, not a designed test, and the confound is obvious:
a small audience is a narrow-targeting choice, so its households differ from a broad audience's households
in more than how often they were bid on.

**Audience size does not itself predict lift.** Rank correlation +0.045 (p=0.54). The between-level test
does register (p=0.030, ranks 7th of 14) but the pattern is **not monotone**: the second quartile
(975K-4.1M) is highest at +10.4% and the smallest quartile (36K-975K) is lowest at +5.1%, and those two
intervals separate. The sheet says quartile gap, not trend. Do not read "smaller audience is better" out
of the frequency result: frequency correlates with lift, audience size does not, and audience size drives
frequency. That triangle is the thing worth a designed test.

**Also added:** `audience_size`, `pct_audience_reached` and `impressions_per_audience_member` are now
columns on Campaign detail (70 columns).

**Still open from this round:** nothing Kirsa raised. The §11 open items stand.

### 12d. Column names were jargon (2026-09-02)

Flagged on the ranked sheet, and it was on ten sheets through the shared column set. Renamed everywhere:

| was | now | why it was wrong |
|---|---|---|
| Pooled lift | Lift | "pooled" is a method detail, not a column |
| CI low / CI high | Low end / High end | statistics notation in a client sheet |
| % significant | % with a clear effect | |
| Heterogeneity | Campaigns disagree | |
| Cost per inc visit | Cost per incremental visit | abbreviation saved nothing |
| Smallest level | Campaigns in the thinnest setting | read as a level NAME next to "Best level", but it is a COUNT |
| Levels | Settings compared | |
| Spread | Gap, best minus worst | |
| Intervals separate | Best and worst do not overlap | |
| Levels differ (p) | p value | was the sort key but sat last |

**The ranked sheet had a structural problem, not just naming.** The sort key was the last column, in the
least readable form, while the heat colour sat on Spread, which the method line explicitly tells the reader
NOT to rank on. Colouring the column you just told someone to ignore is the lie-factor failure in Tufte
terms. Fixed: new plain-language **Real difference?** column (Yes strong / Yes / Too close to call / No at
0.01 and 0.05) sits second with the traffic-light fill, the p-value follows it as a percentage, and the
supporting counts move to the right-hand end. The p value keeps its name: it was briefly spelled out as "chance of seeing this gap if nothing differs" and the user pulled that back, this audience reads p-values. Read me gained entries for "Low end and High end" and
"% with a clear effect".

### 12e. The naming rule is now enforced, not remembered (2026-09-02)

The rename in 12d was still a one-off, and a second read of the ranked sheet found three more defects that
the same rule should have caught. All three are now written into
`documentation/docs/xlsx_deliverable_standard.md` and, where cheap, checked by `lib/mntn_xlsx.py`.

**Remaining defects on the ranked sheet:**

1. **`Settings compared` had the same defect as `Smallest level`.** Both sat beside `Best setting`, which
   holds a NAME, and both held a COUNT. Renaming one and leaving its neighbour is not a fix. Now
   `Number of settings` and `Campaigns in smallest setting`.
2. **`Best and worst do not overlap` was a negated boolean over TRUE/FALSE**, so TRUE meant the thing did
   NOT happen. Now `Best beats worst outright`, valued Yes/No.
3. **`Real difference?` was dropped entirely.** It restated the p value sitting next to it, and it oversold:
   it read "Yes, strong" on Vertical, whose smallest setting holds 5 campaigns. The traffic light moved onto
   the p value itself, which is the column the sheet is sorted on. `Gap, best minus worst` shortened to `Gap`.

**New hard build checks in `MntnWorkbook.table()`** (a violation refuses to write the file, same as the
existing placeholder-header and ordinal-label checks):

- `_JARGON_HEADERS` — statistical notation as a header (`CI low`, `Heterogeneity`, `Pooled lift`,
  `% significant`, `SE`, `ITT`, `CPIV`, `AOV`, …).
- `_ABBREV_HEADER` — a header abbreviating a word (`inc`, `conv`, `attr`, `pct`, `freq`, `imps`, `num`).
- `_NEGATED_HEADER` — a negated header over a yes/no column.

**The check paid for itself on the first run: it caught six more headers in this same workbook** that two
manual review passes had let through, all on Conversion outcomes and Campaign detail
(`Baseline conv rate`, `Conv p value`, `Conv significant`, `Attributed per incremental conv`,
`% attr visits incremental`, `% attr conversions incremental`).

**The rule, stated generally.** A header names what the cell holds, in words this reader already uses.
Method (`pooled`, `random-effects`) belongs in the subtitle. A term the audience's own field uses is fine,
which is why `p value` stayed after being briefly spelled out. The worst header is one that could be read
as either a name or a number, because the reader never learns they misread it.

### 12f. Word breaks and tab names (2026-09-02)

**A cell wrapped `Peak Performanc / e` on the intent band sheet.** The width logic already claimed to
prevent this and had been fixed once before, but it protected only the HEADER's longest word and it decided
"is this column text?" with `df[col].dtype == object`. `bands["band"]` is a pandas **Categorical**, which is
text on the sheet and is neither object nor numeric, so it fell through to the numeric branch, got sized
from three digits, and came out 13 wide against an 11-character bold word. Two fixes: test
`pd.api.types.is_numeric_dtype` instead of `== object`, and fold the DATA's longest word into the width
alongside the header's, with three extra units for the bold first column. Column A on that sheet went 13 to
18. A sweep of all 22 sheets now finds no remaining break.

Machine identifiers are exempt: a campaign group named `acquisition_conversion_leads_CTV_M…` is 80
characters with no space and cannot fit any sane column. `_longest_real_word()` skips tokens containing an
underscore or longer than the 38-character cap, so the rule protects words a person reads, not database keys.

**Tab names drop the "By " prefix and use sentence case.** Eleven tabs read "By frequency", "By intent
band", "By vertical". The workbook already says what it is about, so the prefix was a word repeated eleven
times to say nothing. Now `Frequency`, `Intent band`, `Vertical`. Both rules are checked in `_new_sheet()`
and refuse the build, and both are written into `documentation/docs/xlsx_deliverable_standard.md`.

## 13. Matt's review round (2026-09-03) and the lift-scale problem

Matt Brorby read the delivered workbook and returned four takeaways plus one caveat:

1. Average campaign frequencies > 2.5 appear better than lower frequency.
2. No household cap, or higher caps, seems better.
3. Serving into lower intent (non High Intent) buckets is effective.
4. Audience sizes > 1M.
5. CAVEAT (his): do not read much into Bids per household, it is biased by frequency-capping selection.

Every one of these is a fair reading of what the workbook put on the page. Three of the four do not survive
re-testing, one is contradicted, and his caveat is correct and understated. The reason is a defect in the
workbook, not in his reading.

### 13a. The defect: the workbook reports only relative lift

`rel_itt` is `abs_itt / rate_holdout`. Relative lift is therefore absolute lift divided by the advertiser's
baseline visit rate, and **the baseline varies about two-fold across every cut Matt named**:

| cut | median holdout visit rate |
|---|---|
| audience > 1M | 0.897% |
| audience <= 1M | 1.776% |
| avg frequency > 2.5 | 1.289% |
| avg frequency <= 2.5 | 0.770% |
| no household cap | 2.061% |
| any cap | 1.121% |

In an advertiser-clustered weighted meta-regression of campaign log risk ratio on all three of Matt's binaries
plus log baseline rate, **only the baseline term survives**: `big` p=0.231, `hifreq` p=0.815, `nocap` p=0.059,
`log(rate_holdout)` **b = -0.0335, p = 0.0067**. Baseline visit rate alone carries roughly twice the weighted R²
of Matt's three attributes combined. Every cut he named is a baseline cut wearing an attribute label.

Consequence: a workbook that ranks attributes on relative lift alone will keep producing near-threshold results
that invite exactly this kind of reading. The two scales disagree in sign on the audience cut.

| cut | relative d (log RR) | p | absolute d (pp) | p | advertiser-clustered p (rel / abs) |
|---|---|---|---|---|---|
| audience > 1M | +0.0318 | 0.0196 | **-0.0090** | 0.655 | 0.210 / 0.794 |
| avg frequency > 2.5 | +0.0281 | 0.252 | +0.0296 | 0.0381 | 0.193 / 0.213 |
| no household cap | +0.0257 | 0.461 | +0.0203 | 0.362 | 0.615 / 0.447 |

Neither scale survives clustering by the 130 advertisers. The correct message is not "Matt was wrong about
frequency and right about audience", it is **the scale decides which of his claims looks alive, and once
campaigns are clustered by advertiser none of them is**.

### 13b. Takeaway 3 is contradicted, not merely unsupported

This is the only one that points the other way, and it is the one worth acting on.

The workbook's Intent band sheet is **not a within-campaign stratification**: 152 of 176 campaigns appear in
exactly one band, so pooling by band mostly compares different campaign sets. Non-High campaigns happen to have
higher whole-campaign lift (Mid +13.98%, Max Reach +16.27% against High +8.22%), which is what produces the
apparent parity.

Restricting to the **37 pairs across 22 campaigns where one campaign served both High and a lower band** removes
every campaign-level confound. The lower band is worse on both scales:

| scale | pooled paired difference | campaign-clustered bootstrap CI | negative pairs |
|---|---|---|---|
| absolute | **-0.2169 pp** (z = -6.51) | [-0.397, -0.082] pp, p < 0.001 | 33 / 37 |
| relative | **-5.90%** (z = -3.16) | [-12.2%, -1.1%], p = 0.008 | 25 / 37 |

Per band, negative pairs: Mid Intent **11/11**, Unscored **7/7**, Max Reach **4/4**, Peak Performance 11/15.

**"Non-High" is not one thing.** Peak Performance is the exception on every measure: it is the weakest of the
four on pair count and it has the *cheapest* median cost per incremental visit of any band ($17.82 against High
$25.69, Unscored $27.06, Max Reach $34.03, Mid $41.94). The defensible split is that Peak Performance holds up
and Mid and Max Reach do not. Mid and Max Reach together are 5.5% of measured households.

Two further defects on that sheet: Max Reach, the lowest band and the most direct test of Matt's claim, was
**dropped by the `min_k=5` floor** (k=4) and never appeared, and it does not clear zero (+6.11%, [-6.51%, +20.42%]).

### 13c. Takeaway 4 is a max-selected threshold on top of a baseline confound

The +8.51% vs +5.11% gap at 1M is the **maximum of a scan across 41 candidate cut points** (0.5M p=0.071, 1M
p=0.0196, 1.5M p=0.081, 2M p=0.264, sign reverses above 5M). A max-selected-cutpoint permutation test returns
p=0.205. 1M also sits on the 25th percentile of the population (974,916), so the contrast is bottom-quartile
against the rest. There is no size gradient (Spearman +0.045, p=0.535) and median cost per incremental visit is
flat across audience quartiles ($24.27, $24.14, $28.09, $25.60).

An audience-size by geography interaction that looked strong in a first pass (local >1M +11.02% vs <=1M +4.80%,
p=0.0026; broad geo p=0.68) **did not survive**. The formal difference-of-differences is p=0.161; the broad arm's
small cell is k=9 with a 95% CI that contains the local estimate, so its null is a power failure, not a null;
geo ranks 8th of 11 candidate moderators on interaction p; and local/broad does not carve at the joint (zip
p=0.0012 and DMA p=0.0022, but DMA is in the broad bucket, while local_radius p=0.483 and city p=0.893). This
was a difference-in-significance fallacy and it was caught by adversarial review, not by the first analysis.

### 13d. Takeaway 2 cannot be resolved by this data

Not contradicted, unmeasured. No household cap pools highest of the five common settings (+10.47%) but on 15
campaigns across 12 advertisers; against all capped campaigns the gap is +2.6% relative, p=0.46, advertiser
bootstrap CI [-3.4%, +10.1%]. Cochran Q across the five settings is p=0.554, Bonferroni-adjusted 1.00. The
smallest gap this design can detect at 80% power is about 10% relative against a 2.6% observed gap. Dropping one
two-campaign advertiser flips the sign of the continuous allowance slope. It needs a designed cap test.

### 13e. Matt's caveat is right, and the evidence is stronger than he put it

The decisive number is not the ghost-fraction imbalance, it is that **the 1-bid stratum pools to a significantly
negative lift**: -5.52% [-8.16%, -2.80%], netting -22,529 incremental visits, against 11+ at +14.21% and
+184,558. **A ghost bid cannot suppress visits.** A negative bottom stratum is therefore a direct readout of
selection rather than an effect, which is a cleaner proof of his point than anything on the sheet.

| band | k | relative | absolute | net incremental visits |
|---|---|---|---|---|
| 1 | 97 | -5.52% | -0.0620 pp | -22,529 |
| 2-3 | 92 | -1.37% | -0.0069 pp | -7,452 |
| 4-10 | 94 | +9.82% | +0.1102 pp | +24,967 |
| 11+ | 86 | +14.21% | +0.2392 pp | +184,558 |

Two extensions he should have: the sheet covers only 119 of the 190 campaigns, and **the same caveat applies in
weaker form to the Intent band sheet** (152 of 176 campaigns in a single band; treated households
over-represented among scored ones, p < 1e-60; roughly 13x milder than the bid-count imbalance).

### 13f. Multiplicity across the ranked sheet

Cochran Q per attribute, Bonferroni over the 14 attributes ranked on the same 190 campaigns: only **bids per
household** (p=4e-10, and Matt correctly discounted it), **creative length mix** (0.00027) and **vertical**
(0.00097) clear 0.05. None of Matt's four does: frequency 0.065, audience size 0.414, frequency cap 1.00, intent
band 1.00.

### 13g. What was wrong with my own first answer

Worth recording because two of the four verdicts I produced before adversarial review were themselves wrong.

- I called takeaway 3 **supported** and offered a mechanism ("intent score is assigned before the bid, so band
  membership cannot be affected by treatment"). The mechanism is **false**: within-campaign holdout share is not
  homogeneous across bands (21 of 24 testable campaigns reject at p<0.05), and treated households are
  over-represented among scored households (p=2.9e-70), consistent with exposure feeding the score.
- I called the audience-by-geography interaction a **key result**. It was a difference-in-significance fallacy.
- I over-refuted takeaway 2, asserting a negative sign on cap permissiveness from a fixed-effect fit under
  I²≈0.90 that excluded the no-cap group Matt actually named. Under random effects it is p=0.345, and coding
  no-cap as most permissive flips the sign. "Underpowered" was the honest verdict, and it favours Matt.
- Nobody, including me, checked the absolute scale until the completeness critic did. That single check inverts
  two conclusions.

The corrections came from an eight-agent adversarial pass (four per-claim skeptics, three cross-cutting, one
completeness critic), then re-verified by hand. The critic's own cost-per-incremental-visit figures were in turn
wrong (it used the stratum bid-share spend split, giving Max Reach $126.07); the median per-campaign figures
above are the ones to quote.

### 13h. What this changes about the deliverable

The workbook is not wrong, it is under-specified. Three changes would prevent the misreading:

1. Report **incremental visits per household served** (absolute lift) beside relative lift on every attribute
   sheet. This is the number that answers "where should the next impression go" and it is the one that surfaces
   the intent-band result.
2. **Cluster intervals by advertiser.** 190 campaign groups are 130 advertisers, and every headline contrast
   that clears 0.05 unclustered fails clustered.
3. State the **baseline visit rate** per level on each sheet, so a reader can see when a lift gap is a baseline
   gap.

Pending the user's call, because the workbook is in review with two stakeholders.

## 14. Matt call, 2026-09-03 (transcript `meetings/ti_1313_01_matt_attribute_review_2026_09_03.txt`)

Six-minute call to walk him through §13 before a follow-on with Kirsa. He agreed with the correction and
supplied the mechanism himself before I finished explaining it.

### 14a. He confirmed the intent-band reversal, unprompted

> "Most of the campaigns have the largest chunk of their spend in high intent... and high intent has a higher
> baseline visit rate typically than the other ones, so then it is harder to get a larger percentage in the
> high intent even though you are getting more absolute visits."

That is exactly the §13a defect stated from the other direction. **Takeaway 3 is settled**: he is not defending
it, and Peak Performance as the exception was accepted without argument.

### 14b. Cost per incremental visit is the objective, and that reframes the whole deliverable

> "It is that cost per incremental visit makes the most sense as the thing to optimize for. So you go where
> that's the best."

This answers a question that had been open since Kirsa's review round: **why she asked for CPIV on every tab.**
It is not a nice-to-have column, it is the metric the business optimizes and what a future bidding model would
be pointed at. Lift percentage is diagnostic; CPIV is the objective. Routed to `mntn_business.md`.

**But he has not validated our CPIV numbers**: "that's a tricky calculation to me... I didn't pay too close
attention to that specific metric." He has seen the sheet; that is not sign-off. Walk him through the query.

### 14c. The rebuild I proposed is mostly unnecessary

I offered to rebuild on "incremental visits per household". Matt: **that is already what the tables are.**

> "The tables that I have are all deduplicated to the IP to the household... it's just like, did we bid on this
> IP at all over this time or did we not."

So `n_treatment`/`n_holdout` are deduped IPs over the window, and `abs_itt` is *already* incremental visits per
bid-on household. **The workbook has the column and does not display it.** The change is to surface `abs_itt`
and the baseline rate beside `rel_itt`, not to recompute anything. He also drew the distinction explicitly:
"household" here is the deduped IP, **not** an identity-graph household. Routed to `data_catalog.md` (16).

### 14d. New lead: Ryan Kleck's `_experiments` tables carry spend

> "Those same lift ghost bid BigQuery tables, he has ones that are underscore experiments, and he's been adding
> additional logic there to bring in spend and stuff like that... they're not 100% live and useful, but the
> join logic and the calculations might be useful."

This bears directly on the §12 problem (no spend below campaign grain, forcing the bid-share allocation).
**Check these before hand-rolling another allocation.** Routed to `data_catalog.md` (17).

### 14e. He softened takeaway 4 himself, to a measurability claim

> "The ones that were lower than a million had a lower lift percentage, but they're also harder to measure on...
> I think it's just better to have a larger audience for incrementality purposes."

That is a different and defensible claim from the original takeaway: small audiences are noisier to measure,
not necessarily worse performing. It matches §13c (no size gradient, flat CPIV across quartiles) rather than
contradicting it. **No disagreement outstanding on audience size.**

### 14f. New caveat from him on the High Intent band

> "The most recent example I have from the experiment, which is three advertisers... there's a high intent issue
> with like the ghost bidding fraction, the holdout size, so it's hard to infer something from there."

A separate three-advertiser experiment shows a ghost-fraction / holdout-size problem specific to High Intent.
**This is a flag on the band my own analysis says wins**, so it needs following up before the intent-band
finding is presented as settled. Open item.

### 14g. Where CPIV meets the frequency question

CPM is roughly flat within an advertiser's audience, so cost is driven by how often the same household is
served. CPIV therefore trades directly against frequency, which is why §13's frequency finding and the CPIV
objective are the same question: "we need to have higher frequencies, hit them more often, but that is more
cost." Our data says CPIV worsens monotonically as frequency rises ($12.92 to $36.14 across quartiles), which
is the empirical version of his tradeoff and argues against pushing frequency up.

## 15. Workbook rebuild (2026-09-03)

Six changes, all driven by §13 and §14. Workbook stays `DRAFT - NOT FINAL`. 23 tabs (was 22).

1. **Every attribute sheet now carries `Extra visits per 1,000 households` and `Baseline visit rate`**
   beside relative lift, plus `Advertisers` and an advertiser-clustered range
   (`Low/High end allowing for advertisers`, 600 draws resampling advertisers). `abs_itt` already existed
   in the data; the workbook simply was not showing it (§14c).
2. **New sheet `Intent band like for like`** — the 37-pair within-campaign comparison. Every lower band
   is below High Intent on visits per household: Peak Performance −1.44 [−3.54, −0.01], Mid −2.12
   [−4.12, −0.90], Max Reach −1.87 [−3.62, −1.49], Unscored −4.46 [−8.37, −1.09]; pooled −2.17
   [−4.15, −0.84], 33 of 37 pairs negative. Ranges resample campaigns, the unit each pair belongs to.
3. **`Intent band` min_k lowered to 4 so Max Reach appears.** It pools to +6.1% with a range crossing
   zero, which is the honest reading of the most on-point band and was previously hidden.
4. **`Frequency` (bids per household) restated as a warning on its own sheet**, not only in the Read me:
   the single-bid band reads negative, and withholding a bid cannot reduce visits.
5. **`Ranked hypotheses` gained `Survives testing every attribute`** at a Bonferroni bar of 0.0036.
   Three of fourteen survive: bids per household (which is not a valid split), creative length mix,
   vertical. The method line now names the bid-count caveat and points at the tab.
6. **Cost per incremental visit switched to the median per campaign.** The pooled spend-weighted version
   inverts the ordering (§13g).

### 15a. Two defects in my own rebuild, caught before shipping

- `Advertisers` on the ranked sheet was `max()` across an attribute's levels, so Vertical read 13
  advertisers instead of 130. Replaced with a distinct count per attribute.
- The ranked sheet marked bids per household as surviving correction while its own tab calls it invalid.
  The method line now carries the warning. The library also blocked a method line that said "see that
  sheet" without naming the tab.

### 15b. One nuance that changes what was said to Matt

Peak Performance was described in the draft reply as "the exception, looks good." Held like for like it is
**also below High Intent on visits per household** (−1.44, range excluding zero), just least so. What holds
is the cost claim: PP is the cheapest band at **$15.36** per incremental visit against High at $26.71.
So PP is good value, not higher yield. The reply should say that rather than "looks good".

### 15c. Tab rename (Kirsa, 2026-09-03)

`Frequency` renamed to **`Bids per household`**, matching its own first column and removing the collision
with `Campaign frequency` (a different sheet, on the campaign's own average frequency). Kept sentence case
rather than the requested Title Case, per the tab-name rule enforced in `mntn_xlsx._check_tab_name`.

## 16. Kirsa and Matt call, 2026-09-03 (transcript `meetings/ti_1313_02_kirsa_matt_attribute_review_2026_09_03.txt`)

34 minutes. This is the meeting that explains the whole ticket. Kirsa opened by saying the written summary
had lost her, and by the end had accepted the lift-scale correction, named a methodology blocker, and given
two concrete asks.

### 16a. The scale correction landed

Kirsa restated it herself: "if you're comparing high intent versus mid intent and there were a hundred base
visits in mid intent, 400 in high intent, and they both cause eight extra incremental visits." Then: "there's
not really actually much to say about it." **Settled**, and she now understands why the team uses cost per
incremental visit.

### 16b. ROOT CAUSE: ghost bids are never counted, so the holdout is not frequency capped

Matt laid out the defect underneath every ghost-bid number MNTN produces. A real bid increments a counter that
drives the frequency cap and pacing; **a ghost bid increments nothing**. So a held-back household keeps being
bid on after its treated twin is capped, and pacing never slows for it: "you can get three holdout bids coming
in before you get a real bid." The holdout is exactly 10% in the audience and stops being 10% only once it
flows through the bidder.

Above ~11% the holdout has accumulated highly active IPs the cap would have removed, which raises its baseline
and drives measured lift negative. Below ~10% those IPs land in the first-day cohort and are dropped by the
washout. Matt's control-negative testing agrees: over 11% goes immediately negative, below it centres on zero.

Rogus refused to count ghost bids in Beeswax, fearing leakage into spend, budget and pacing. It exists on the
MNTN bidder (Ryan Kleck built it for frequency caps) but that bidder runs essentially only Select, so it does
not help this population. **Not retroactively fixable**, because the flaw is in the bidding mechanism.

Kirsa's separate mechanism: even with identical capping, bidding the holdout as often as the treatment makes
its bid-to-audience ratio ten times outsized. Matt partly accepted it but held that bidding less does not fix
the bias, only the percentage: "the only solution that actually works is to track the holdout group on the
bidder side, not just after the fact." Routed to `data_knowledge.md` and memory.

### 16c. Matt's "50% holdout" is high, but the drift is real and now on the sheet

Matt: "what should be 10% in the holdout can get up to like 50% in this bucket." Checked against the data.
Pooled holdout share per bid-count band is 9.67% / 8.94% / 8.71% / **10.61%**. Per campaign in the 11+ band the
median is 10.3%, but **33 of 86 exceed 11%**, the 95th percentile is 20.2% and the **maximum is 31.2%** (one
4-10 band campaign reaches 45.7%). So 50% is not observed here; 31% is. The direction and the materiality are
his, the ceiling is not. This is now four columns on the Bids per household sheet, which was his explicit ask:
"the thing that's missing from here is the number of households in the treatment and the number in the holdout.
If we had that breakdown, it would be very clear to you why you shouldn't trust this number."

### 16d. Kirsa's exclusion ask is already exceeded, and following it literally would inflate the headline

Kirsa: "can we exclude all campaign groups over 11%... I would block [under 7%] too, just to be safe." Matt,
about his own pipelines, said "I don't think I'm excluding anything." **The workbook already blocks below 9%
and at or above 11%**, which is stricter than what was asked.

| Gate | Campaigns | Advertisers | Pooled lift |
|---|---|---|---|
| Current workbook, 9 to 11% | 190 | 130 | **+7.92%** [+6.43%, +9.44%] |
| Kirsa and Matt's 7 to 11% | 426 | 294 | +10.69% [+9.56%, +11.83%] |
| No band at all | 470 | 316 | +9.97% [+8.83%, +11.11%] |

Loosening to 7-11% would add 236 campaigns and move the headline from +7.9% to +10.7%. That gain is the
holdout-thinning artifact the Holdout depth check sheet exists to document, not a real effect. **Recommend
keeping 9-11% and telling them it is already stricter.** This needs saying explicitly, because the ask was
made in the belief that nothing was excluded.

### 16e. Delivered against the two build asks

1. **Cost beside lift on Ranked hypotheses** (Kirsa: "you can have both side by side just to see"). Added
   `Cheapest setting`, `Cost per incremental visit there`, `Dearest cost per incremental visit` and
   `Best lift setting is also cheapest`. **They disagree on 9 of 14 attributes**, which is the answer to her
   question. Only visit attribution window, TV share, audience size and display multi-touch agree.
2. **Household counts on Bids per household** (Matt's ask, 16c above).
3. Tab rename to `Bids per household` was already done from her Slack message.

A defect this surfaced: switching cost per incremental visit to a median made the single-bid band look like
the *cheapest* setting at $6.33, because the median silently dropped the campaigns where that band netted
negative. Cost is now blank wherever a band's pooled extra visits are not positive, so the cheapest bid-count
band reads 4-10 at $8.20.

### 16f. Open, and bigger than this ticket

- **The ghost-bid defect blocks the incrementality reporting release.** Nick has the same stricter-gate
  guidance for his dashboard.
- Kirsa's stated concern for next week's presentation: "I am worried about presenting this data as absolute
  fact to make decisions based upon... there's kind of this big asterisk we're going to have to put on the
  data." A fix moves the measuring stick and every number changes.
- Matt wants to investigate what made TART's ghost fraction so far off, as a route to a post-hoc correction
  if the root cause cannot be fixed.
- Matt to double-check what his standard pipelines exclude; he believed it was nothing, and thought the loose
  bound was 15%.

## 17. Gate shift to 7-11%, and three of my own conclusions overturned (2026-09-03)

Applied Kirsa and Matt's instruction (§16d): ghost fraction gated at 7% to under 11%, all four queries
re-pulled. Population **190 to 433** campaign groups, 296 advertisers, pooled visit lift **+7.92% to +9.63%**.
I flagged that this loosens rather than tightens the gate; the instruction was reaffirmed, so it ships.

An eight-agent adversarial pass then overturned three things I had reported, including two that reached the
stakeholders. Each was re-verified by hand before being accepted.

### 17a. OVERTURNED: the intent-band gap is mostly arithmetic

I reported the like-for-like finding on the **absolute** scale: every lower band delivers fewer extra visits
per household, −0.227 per 1,000, 40 of 46 pairs negative. **70% of that is definitional.**

Intent bands are *defined* by household propensity, so High Intent's baseline is 1.92% against 0.58% for the
lower bands in the same campaigns. Holding relative lift exactly equal still produces a gap of **−1.59 per
1,000** from baseline arithmetic alone. Observed is −2.27, so the residual is **−0.68**, and campaign-clustered
it does **not** clear zero: CI [−2.38, +0.15], **p=0.243**.

I had it backwards. §13a is right that relative lift hides a varying baseline *across campaigns*. But for a
**within-campaign comparison across bands whose baselines differ by construction**, the absolute difference
double-counts that definition and the **relative** comparison is the clean one.

**The defensible finding is the relative one:** pooled across the 30 paired campaigns, lower bands lift their
own baseline **−6.8%** less than High Intent lifts its own, campaign-clustered CI **[−12.4%, −1.9%]**, 29 of 46
pairs negative. Per band: Unscored −10.5% [−21.5%, −1.2%], Max Reach −9.4%, Peak Performance −6.2%, Mid −3.1%,
the last three all crossing zero individually. Real, and about half the size I claimed.

The workbook sheet now leads with the lift gap and carries the visit gap beside a column naming how much of it
is the lower baseline alone.

### 17b. OVERTURNED: Peak Performance is not the cheap exception

I told the user twice, and it is in the draft reply to Matt, that Peak Performance is "good value, cheapest
band per incremental visit" ($10.03 against High's $20.07). **That compared 29 PP campaigns against 360
different High campaigns.** Held like for like, on the 35 pairs where one campaign served both:

| Band | High Intent cheaper in | Median High | Median other |
|---|---|---|---|
| Peak Performance | **11 of 16** | $10.43 | $14.98 |
| Mid Intent | 10 of 10 | $11.29 | $35.45 |
| Max Reach | 4 of 4 | $8.97 | $44.76 |
| Unscored | 5 of 5 | $6.65 | $73.05 |
| **All** | **30 of 35** | | |

**There is no cost exception.** High Intent is cheaper per incremental visit in 30 of 35 like-for-like pairs.
The draft Slack reply must not go out with the PP claim in it.

### 17c. OVERTURNED: frequency cap is measurable after all, and it now contradicts Matt

I concluded the attribute was unmeasurable because `dso.frequency_caps` is current state. A skeptic pointed at
**`dw-main-silver.archives.frequency_cap_archives`**, which is versioned. It **covers 433 of 433** campaign
groups at the window end, and **144 of 433 (33%) ran a different cap during the window than they carry today**.
So the attribute is recoverable; my verdict was wrong. New query `ti_1313_fcap_in_window.sql`; 157 of 433
labels change once the archive is used.

With the correct in-window labels the finding **reverses**:

| Cap in force during the window | Campaigns | Lift | Cost per incremental visit |
|---|---|---|---|
| 2 per 7 days | 129 | +10.76% | $23.46 |
| 1 per 14 days | 199 | +9.61% | **$8.98** |
| 4 per 3 days | 51 | +8.18% | $31.69 |
| **No household cap** | 21 | **+7.84%** | $43.90 |
| 6 per 1 days | 16 | +6.61% | $49.62 |

**No household cap is now second from bottom, not top**, and the most permissive standard cap is last. Between
levels p=0.0112, which does not survive correction for testing 14 attributes. **Matt's takeaway 2 is not
"unmeasured", it is contradicted on the corrected labels** — and the cost column runs the same way, with the
tightest cap cheapest by a factor of five.

### 17d. NOT RESOLVED: which gate is less biased

A skeptic argued forcefully that widening was right and that 9-11% is the *contaminated* band, on a
within-campaign regression of lift on realised holdout share fitted over bid-count strata: flat below 9%,
steep above. **That test does not hold up.** Bid-count strata are post-treatment (`data_catalog.md` 14), and
holdout share and lift both track the band, so the fit is partly circular. Splitting it myself:

| Design | below 9% | at or above 9% |
|---|---|---|
| bid-count strata (post-treatment) | −0.084/pp, p=0.175 | −0.022/pp, p=0.535 |
| score-band strata (assigned pre-bid) | **−0.163/pp, p=7.9e-05** | −0.019/pp, p=0.768 |

Neither half of the bid-count version is significant on its own, and the cleaner score-band design gives the
**opposite** pattern: steep below 9%, flat above. The two stratifications contradict each other about where
the artifact switches on.

The cross-campaign profile is also a baseline story rather than a clean gradient:

| Holdout share | Campaigns | Lift | Per 1,000 households | Median baseline |
|---|---|---|---|---|
| under 7% | 37 | +8.09% | +1.342 | 2.09% |
| 7 to 8% | 69 | +8.78% | +0.783 | 1.75% |
| 8 to 9% | 167 | +12.86% | +0.604 | 1.22% |
| 9 to 9.5% | 130 | +9.25% | +0.379 | 1.08% |
| 9.5 to 10% | 50 | +5.40% | +0.364 | 1.36% |
| 10 to 11% | 17 | −1.65% | +1.268 | 1.53% |
| 11% and over | 8 | −11.48% | −0.259 | 1.14% |

Relative lift peaks at 8-9% while the baseline falls monotonically across the same range, so the "clean
plateau" is largely §13a applied to the gate itself. On the absolute scale there is no plateau at all.

**Conclusion: the data cannot say which of 7-11% or 9-11% is less biased.** What is solid is that above 11% is
bad (8 campaigns, −11.48%, and Matt's control-negatives), and that the population barely reaches there anyway
(median holdout share 8.86%). Ship 7-11% as instructed, put the band profile on the sheet, and say plainly
that most of the +1.7pp move is baseline composition.

### 17e. Corrections to carry forward

- Everything in §13 and §15 computed on 190 campaigns is superseded by the 433-campaign numbers here.
- §15b's Peak Performance cost claim is **wrong** and is corrected in 17b.
- The §13 framing that the absolute scale is always "the honest one" is **too strong**; 17a gives the
  exception. Routed to `experimentation.md`.
- Matt's takeaways now stand as: 1 unresolved and gate-dependent, 2 contradicted on corrected labels,
  3 real but half-size and on the relative scale, 4 not supported.

### 17f. Stale claims the gate change broke, found on a sweep

Three sheets still asserted things the re-pulled data no longer supports. All corrected.

- **Holdout depth check** said "measured lift climbs as the holdout thins." It does not. Re-binned finer:
  under 7% +13.04%, 7-8% +10.40%, 8-9% **+14.79%**, 9-10% +7.91%, 10-11% −0.63%, 11%+ −7.22%. The peak is in
  the middle, and baseline visit rate falls from 1.78% to 1.14% across the same rows, so much of the shape is
  the baseline. New finding line says lift falls away above 9% and turns negative past 11%, without claiming a
  monotone thinning gradient.
- **Window sensitivity** said "the three windows agree once the quality gates are applied." They no longer do:
  gated lift is 6.98% / 10.93% / 13.37% across the three windows while the holdout thins 9.77% to 8.79%. The
  sheet now says the window is a real choice and the full span is used for campaign count, not neutrality.
- **Read me and Population choices** both described a "documented reliable 9 to 11%" band. Replaced with the
  band the workbook actually uses and the reason the top of it matters.

`ti_1313_fcap_stability.sql` deleted: it was the intermediate step that established `update_time` is a refresh
stamp rather than an edit marker, and it is superseded by `ti_1313_fcap_in_window.sql`. The finding it produced
is recorded in `data_catalog.md` (18).

## 18. Kirsa's second review round (2026-09-03)

### 18a. "% spend to TV looks very high, is it ad type rather than device?"

**No, it is the device the impression served to.** `pct_spend_tv` reads `summarydata.spend_facts.device_type`
in `('SET_TOP_BOX','CONNECTED_TV','GAMES_CONSOLE','CONNECTED_DEVICE')` over the window. Actual mix of
prospecting media spend:

| Device type | Share |
|---|---|
| SET_TOP_BOX | 54.8% |
| CONNECTED_TV | 36.4% |
| MOBILE | 5.7% |
| GAMES_CONSOLE | 1.6% |
| TABLET | 1.4% |
| PHONE, PC, PERSONAL_COMPUTER, CONNECTED_DEVICE | under 0.1% each |

So TV devices are **92.9%** and phone/tablet **7.1%**. `CONNECTED_DEVICE`, the only catch-all in the TV list,
is 0.00% of spend, so it is not padding the number. The 80-100% range Kirsa saw is real: within this
population `pct_spend_tv` runs 0.776 to 0.992, median 0.921, and **99.5% of campaign groups are at or above
80%**.

### 18b. Why that does not contradict the display multi-touch result

**The two columns have different denominators**, which is the whole reconciliation.

- `pct_spend_tv` is computed over **prospecting campaigns only** (`objective_id = 1 AND funnel_level = 1`),
  because that is the leg the ghost-bid holdout measures.
- `pct_spend_display` and `runs_display` are computed over **every campaign in the group**, filtered on
  `channel_id = 1`, with no objective or funnel restriction.

So a group can be 92% TV *on its prospecting leg* and separately run display multi-touch elsewhere. The data
confirms they are unrelated: **correlation between the two is −0.021**, median `pct_spend_display` is 0.0000,
171 of 433 groups run any display, and **56 groups both run display and are at or above 95% TV**. Median
`pct_spend_tv` is 0.9205 for groups that run display and 0.9211 for those that do not — indistinguishable.

Kirsa's underlying intuition is right that MNTN serves plenty of video off-TV; that volume just sits outside
the prospecting denominator this ticket measures. Added to the Read me so the next reader does not have to ask.

### 18c. "Significance is on lift, not cost per incremental visit. Can that be revised?"

**She is right, and it is now revised.** The ranked sheet's `p value` was the between-level Cochran Q on lift
only; the cost columns added earlier carried no test at all.

Cost per incremental visit is a ratio of two pooled sums with no closed-form error, so the test is an
**advertiser-clustered bootstrap** (600 draws) of the cheapest-to-dearest cost ratio. The sheet now leads with
cost and keeps the lift test beside it:

| Attribute | Cheapest | Dearest | Ratio | Low end | Lift p |
|---|---|---|---|---|---|
| Vertical | $2.60 | $54.36 | 13.1x | 8.20 | 0.000 |
| Frequency cap | $8.98 | $49.62 | 7.7x | 4.46 | 0.011 |
| Average frequency | $5.21 | $31.80 | 5.6x | 3.36 | 0.031 |
| Intent band | $6.95 | $44.76 | 5.2x | 2.59 | 0.783 |
| Visit attribution window | $6.73 | $43.09 | 8.2x | 2.21 | 0.000 |
| Geographic targeting | $12.88 | $29.48 | 2.5x | 1.89 | 0.579 |
| Average household score | $15.81 | $28.10 | 2.5x | 1.59 | 0.000 |
| High-intent share | $15.13 | $24.52 | 2.0x | 1.35 | 0.002 |
| Creative length mix | $16.95 | $26.81 | 2.6x | 1.34 | 0.066 |
| Bids per household | $2.93 | $16.29 | 2.2x | 1.18 | 0.000 |
| Audience size | $15.36 | $21.38 | 1.2x | 1.18 | 0.158 |
| Advertiser tenure | $14.92 | $20.84 | 1.1x | 1.14 | 0.581 |
| TV share of spend | $18.58 | $20.77 | 1.7x | 1.12 | 0.000 |
| Display multi-touch | $18.74 | $19.84 | 1.1x | 1.01 | 0.367 |

**The two rankings disagree sharply**, which is the point of showing both. Intent band is 4th on cost spread
and last but one on lift (p=0.783). TV share of spend clears the lift bar (p<0.001) but is second from bottom
on cost, a 1.12 low end that is effectively no separation. Vertical leads on both.

I dropped a "Cost settings differ" yes/no column before shipping: it read Yes for all fourteen, because the
cheapest and dearest settings are **picked after seeing the data**, so the low end is optimistic by
construction. The ordered ratio carries the information honestly and the method line says the pair is
data-picked.

### 18d. The reply took three rewrites, and why

Worth recording because the failure was not the analysis, it was the answer shape.

Draft 1 explained the denominator difference correctly and was flagged **"too generalized"**. Draft 2 added
the specific counts and was flagged for not directly answering. What worked was answering **her four
questions in her order**, each opening with a plain yes or no, and quoting the figures off the tabs she named
rather than the ones I found interesting.

The load-bearing move was locating the single broken link in her reasoning. Her chain was sound apart from one
parenthetical, "display MT (aka lower % of TV spend)". Those are not the low-TV campaigns: **112 of the 171
display multi-touch groups sit in the 90 to 99% TV bucket and only 59 are under 90%**, and median TV share is
**92.0% for display runners against 92.1% for prospecting only**. Quoting that clause and showing it false
answers the whole question in one stroke, where re-deriving the denominators did not.

The comparison she was making, in her own numbers: the TV tab reads **+12.10% lift for 90 to 99% TV against
+5.21% under 90%**, and the display tab reads **+10.25% against +9.21%** on a group that is mostly high-TV
anyway. Routed to memory `feedback_slack_reply_voice`.
