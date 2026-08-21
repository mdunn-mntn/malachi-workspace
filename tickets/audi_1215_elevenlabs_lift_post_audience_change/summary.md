---
doc_type: ticket
title: "AUDI-1215: ElevenLabs lift pre/post 6/30 audience change (CGID 122748)"
status: done
date: 2026-08-21
summary: "Did ElevenLabs incrementality lift change after the 2026-06-30 audience change on CGID 122748?"
result: "Visit lift +11.1% pre vs +16.5% post (change n.s.); incremental volume fell ~4x; powered conversion instrument shows lift fell 36%"
question: "Did incrementality lift for CGID 122748 change after the 2026-06-30 audience change?"
framing_state: locked
---

# AUDI-1215: ElevenLabs lift pre/post 6/30 audience change (CGID 122748)

**Jira:** https://mntn.atlassian.net/browse/AUDI-1215
**Status:** Done
**Date Started:** 2026-08-21
**Date Completed:** 2026-08-21
**Assignee:** Malachi

---
## 0. Framing
- **Question (the unknown):** Did incremental lift (visits, conversions) for ElevenLabs CGID 122748 change after the 2026-06-30 audience change, comparing pre (≤2026-06-30) vs post (≥2026-07-11) with Matt Brorby's 2026-07-01..07-10 blackout excluded?
- **Goal (why / the decision):** Mike Dolt's urgent P0 Slack ask (2026-08-21). Escalation context: ElevenLabs paused a $770K campaign 2026-08-20 citing "no incremental lift" ($10-12M annual account); this analysis feeds the customer response and recommendation. BER-2250 incrementality is the north-star priority.
- **Objective (done-when):** A pre/post lift table (point, CI, p on both periods, plus the delta) from the ghost-bid ITT with a corroborating instrument, posted back to Mike. Binary: the table exists with uncertainty quantified, or it doesn't.
- **Approach (how):** Ghost-bid randomized ITT from `silver.enriched.lift__ghost_bid_visits` (entry-cohort anchor, ghost_frac gate 0.09–0.11, partner 8 only) as primary; the fixed-membership holdout lineage (`lift__holdout_*`, `v_lift__conversions`, `v_lift__results_by_month`) as corroboration; attributed panel (all_facts) as context only. Verify the audience change from the archives. Adversarial verify before reporting.
- **What would change the answer:** Post-period ghost_frac outside band or instrument disagreement demotes the verdict to a caveated directional read; thin pre window (silver floor 06-22) means the honest answer may be the MDE, not a point claim. (Both materialized: see §4.5 power resolution.)

## 1. Introduction
ElevenLabs (AID 51660) is a B2B advertiser on US CTV (Beeswax leg). TI-1044 (June 2026) established: clean ghost-bid ITT lift ≈ 0 on conversions (n.s.), large attributed/ATT numbers are attribution + win-selection bias, CVR unpowered at the 0.062% base rate. On 2026-06-30 15:58 UTC the audience on CGID 122748 ("growth_mntn_agents-priming-in-platform_english", objective 1 prospecting, PTV) was swapped. On 2026-08-20 the customer paused the campaign group ($770K budget) citing no incremental lift after their own 6-week multi-angle analysis (which predates these changes). Mike Dolt asked whether incrementality moved after the change; Matt Brorby produced a parallel read the same afternoon.

## 2. The Problem
- Pre/post incrementality for one campaign group with a mid-window treatment change; blackout 07-01..07-10 (Matt's convention — ElevenLabs made multiple campaign and audience changes in that window).
- Instrument issues: silver ghost-bid floor 2026-06-22 (8-day pre window); entry-cohort holdout depletion inflates post lift (gate ghost_frac 0.09–0.11); partner 79 rows garbage (kept partner 8); 7-day outcome window right-censors anchors past MAX(dt)−7.

## 3. Plan of Action
1. **[done]** Jira spike AUDI-1215 (P0, sprint 8270); local scaffold + framing.
2. **[done]** Verify CGID 122748 (AID 51660, prospecting, PTV, Beeswax).
3. **[done]** Workflow `wf_8b658238-b57` (8 agents, ~21 min): 3 scouts (audience-change archaeology, delivery panel, instrument coverage) → 3 measurements (ghost-bid ITT pre/post, holdout lineage, gold strata) → 2 adversarial verifiers (methodology lens: ADJUSTED; repro lens: CONFIRMED, all numbers reproduced exactly from fresh SQL).
4. **[done]** Branded workbook to Drive; Jira completion comment; Slack draft for Mike's thread.

## 4. Investigation & Findings

### 4.0 Matt Brorby's benchmark (Slack 2026-08-21, Compass-formatted)
Visits +11.14% pre → +16.46% post (sig both); conversions −0.03% pre → +15.61% post (n.s. both); NTB −4.46% pre (sig negative) → +0.20% post (n.s.). Blackout rationale (Matt): multiple campaign+audience changes 07-01..07-10. Full tables: `artifacts/audi_1215_matt_compass_results.md`. **Reconciliation: our Instrument A reproduces Matt's counts to within a few IPs (4,200,746 vs his 4,200,750 etc.) — same instrument, same numbers.** His takeaway needs two corrections (§4.5).

### 4.1 What actually changed (archives, fully recovered — `queries/audi_1215_audience_change_timeline.sql`)
- 2026-06-30 15:57: exclusion lookbacks (conversion + pageview blocks) widened 30d → 90d.
- **2026-06-30 15:58:29: audience swap 77883 "Agents Targeting - Growth" → 88532 "MNTN-suggested precision audience changes"** (created 06-26 by user 152724, executed by 146391). On prospecting campaign 608814 the include collapsed from ShareThis(4 cats) + DS19 MNTN Matched keywords(33 cats) + DS35 LiveRamp(112 segments) to **LiveRamp-only 6 AI/ML + B2B segments**; geo widened to US-wide.
- 2026-07-01: DS47 CRM identity-graph suppression added. 2026-07-09: geo restored to 28-state list (US-wide ran only inside the blackout). **2026-07-16: 3 custom ElevenLabs LiveRamp segments added. 2026-07-24: DS9 MNTN Campaigns added. 2026-07-29: expressions rewritten + DS16.** 2026-08-20 14:10: CG paused.
- Holdout (`md5("51660:"+ip)` buckets 0-99/1000) and RTC directive unchanged throughout → assignment is apples-to-apples.
- **Implication: POST averages 4 audience states; nothing isolates the 6/30 swap alone. PRE ≤06-30 includes ~16h old audience + the lookback widening (also 06-30).**

### 4.2 Instrument A — ghost-bid randomized ITT (primary; `queries/audi_1215_ghost_itt_prepost.sql`, outputs JSON/CSVs)
Entry-cohort anchor per (adv, campaign, ip), first-day 06-22 excluded, anchors capped 08-13 (MAX(dt) 08-20 − 7d), partner 8, CGID 122748. ghost_frac: pre 0.09505 / post 0.09193 (band floor; W29 0.0877 below band → post lift biased UP slightly).

| period | arm | n_ip | visit rate | conv rate |
|---|---|---|---|---|
| pre 06-23..06-30 | submitted | 4,200,746 | 0.9161% | 0.0179% |
| pre | ghost | 441,242 | 0.8243% | 0.0161% |
| post 07-11..08-13 | submitted | 6,639,322 | 0.1578% | 0.00401% |
| post | ghost | 672,152 | 0.1355% | 0.00298% |

| period | metric | abs lift | rel lift | z | p |
|---|---|---|---|---|---|
| pre | visit | +0.0918pp | **+11.14%** | 6.38 | 1.7e-10 |
| post | visit | +0.0223pp | **+16.46%** | 4.70 | 2.6e-06 |
| pre | conv | +0.0018pp | +11.25% | 0.90 | 0.37 |
| post | conv | +0.0010pp | +34.65% | 1.45 | 0.146 |

**Delta (post − pre): visit abs −0.0695pp (SE 0.0152, z −4.59, p 4.4e-06 — significant DECLINE in absolute incremental volume); visit relative on log-RR +0.047 (z 1.21, p 0.226 — NOT significant); conv deltas n.s. (p 0.47–0.72).** Weekly series: lift +7% to +23% every week, no collapse. Base-rate mechanism: new audience's submitted visit rate 6x lower (0.92% → 0.16%), new-IP inflow −63% (525K → 195K anchors/day); part of the pre base rate is left-censor composition artifact (returning-IP stock days 2-9 after table floor; attributed panel shows the slide beginning before 06-30).

### 4.3 Instrument B — fixed-holdout lineage, conversions (powered; `queries/audi_1215_holdout_prepost.sql`)
Fixed 10% MD5 holdout, membership static → no depletion. POST conversions 100% CGID 122748 (PRE 99.4%). Blackout excluded (conversion timestamps split on impression date):

| window | converter RR (95% CI) | conv-event RR |
|---|---|---|
| PRE 06-01..06-30 | **3.492** (3.304–3.690) | 3.076 |
| POST 07-11..07-31 | **2.229** (2.056–2.417) | 2.119 |
| **POST/PRE ratio** | **0.639 (0.579–0.704), z −8.98, p 2.6e-19** | 0.689 (0.639–0.742), p 1.2e-22 |

**Relative conversion lift FELL 36%.** Bias signed: 27.8% of POST conversions attach to impressions ≤06-30 and 14.5% to blackout impressions via the 43-day lookback → POST flattered → **the decline is a lower bound.** Run-grain visit RR 3.79 (Jun) → 3.02 (Jul, blackout included), ratio 0.795. No August run exists (POST capped 07-31). NOTE: this lineage's "lift" is an attributed-style multiplier (impression-attributed conversions vs holdout), far above clean ITT levels — only the RATIO over time is used, not the level.

### 4.4 Instrument C — gold strata (all-time 06-22..08-20, composition mechanism; `queries/audi_1215_gold_strata.sql`)
All 11 strata rows pass every quality flag; ghost_frac 0.094. Overall visit lift +12.92% (p 2.9e-16); conversions −8.6% n.s.; NTB −1.76% (p 0.041, marginal). Cohort is 79.3% no_score but NOT the dead-reach trap: no_score +14.78% (p 4e-9). **Frequency (bid_count strata): 1 bid +9.3% · 2-3 +17.9% · 4-10 +20.3% · 11+ −17.7% (p 6.6e-6, significantly NEGATIVE).** Pairs with Edgar's pull (70% of HHs ≤3 exposures): the 11+ band burns spend at negative lift → frequency-target rec has empirical footing for THIS campaign.

### 4.5 Adversarial verification (2 lenses)
- **Repro lens: CONFIRMED.** Every load-bearing number reproduced exactly from independently written SQL (240GB fresh scan); period boundaries, entity grain, 7-day outcome semantics all verified. One caveat-level reconciliation sentence in A's output flagged as arithmetically off (cosmetic, edge-row accounting; headline unaffected).
- **Methodology lens: ADJUSTED — the synthesis must lead with the decline evidence, not A's null.** Decisive point: **A's change test is underpowered for the effect B measured** — A conversion delta MDE ≈ RR-ratio outside [0.60, 1.67] at 95%, and B's observed 0.639 sits INSIDE A's blind spot; on visits, B's decline (0.722 lift-ratio) maps to a log-RR delta of −0.029 on A's scale, under A's SE (0.039). **A's "no significant change" = "cannot see", never "no change." The instruments do not conflict.** B credible at CG grain (impressions reconcile exactly with the delivery panel). Post ghost_frac at band floor biases post lift UP → strengthens no-improvement. A's "6x base rate" mechanistic claim partly confounded by left-censor stock (attributed slide began pre-06-30).

### 4.6 Attributed panel (context only; `queries/audi_1215_daily_panel.sql`, `outputs/audi_1215_daily_panel.csv`)
At flat delivery (~19M imps/period), attributed visitor rate per unique −63.4% (0.0329 → 0.0120), CVR per unique −62.5%, visits/1k imps −49.5%; uniques +12.6% (broader reach, lower frequency). Slide began days BEFORE 06-30, continued through blackout, still falling mid-August (visits/1k imps 3.13 → 2.42 → 1.95 across POST thirds). Concentrated in the prospecting campaign (visits −77%, conv −75%); MT campaigns fell far less. Attributed ≠ incremental; cannot separate targeting quality from attribution-match-rate change (new audience = different device/IP mix).

## 5. Solution
**Answer to Mike: the July changes did not improve incrementality, and the best-powered evidence says conversion lift declined.**
1. **Visit lift is real and significant in BOTH periods** (+11.1% pre, +16.5% post) — directly rebuts "no incremental lift" on visits.
2. **The +11 → +16.5 "improvement" is not statistically supported** (log-RR p 0.23) and the post point is slightly inflated (ghost_frac at band floor).
3. **Incremental visit VOLUME fell ~4x** (abs lift 0.092pp → 0.022pp per IP, p 4.4e-06): the new precision audience rarely visits at baseline, so the same relative lift produces far fewer incremental visits at flat spend.
4. **Conversions: clean test unpowered both periods** (TI-1044 floor: ~$2M/mo to detect 5% at 0.062% CVR — the customer's own conversion-based read is unpowered too); **the powered fixed-holdout instrument shows conversion lift fell 36%** (3.49x → 2.23x, p 2.6e-19, lower bound).
5. **Frequency is the actionable lever:** lift peaks at 2-10 exposures, significantly negative at 11+, 70% of HHs at ≤3 → supports Mike's Gruns-style frequency-target change.
6. **Attribution caveat for the response:** everything measures the BUNDLE of six changes 06-30..07-29, not the audience swap alone.

**Deliverables:** branded workbook `My Drive/Tickets/AUDI-1215/AUDI-1215 ElevenLabs Lift Pre Post Audience Change.xlsx` (10 sheets: lift tables, delta tests, holdout check, frequency, timeline, attributed panel, glossary, queries, method); this summary; Jira completion comment; Slack draft for Mike's thread.

## 6. Questions Answered
- **Q:** Was there any change in lift post 6/30? **A:** Relative visit lift statistically unchanged (+11.1% → +16.5%, p 0.23); absolute incremental volume down ~4x (p 4.4e-06); relative conversion lift down 36% on the powered instrument (p 2.6e-19, lower bound); clean conversion ITT n.s. both periods.
- **Q:** Do our numbers match Matt's? **A:** Yes, exactly — same instrument (ghost-bid entry-cohort ITT), counts match to a few IPs. His takeaway overstates two things: the visit "improvement" is n.s., and "conversions directionally improving" inverts once the powered instrument and the volume collapse are included.
- **Q:** What exactly changed on 6/30? **A:** Audience 77883 → 88532: include collapsed ShareThis+keywords+112 LiveRamp segments → 6 LiveRamp AI/ML+B2B segments; geo briefly US-wide; lookbacks 30d→90d; then 4 more changes through 07-29 (§4.1).
- **Q:** Does frequency relate to incrementality here? **A:** Yes: +9.3% (1 bid) / +17.9% (2-3) / +20.3% (4-10) / **−17.7% (11+)**.

## 7. Data Documentation Updates
Captured via /capture: holdout-lineage arm semantics + 43-day lookback carryover gotcha; the A-vs-B power-resolution pattern (a null change-test is not stability); v_lift__results_by_month grain facts; archives recover full audience/expression history (audience_x_campaign_group_archives, audience_segment_archives semantics).

## 8. Open Items / Follow-ups
- **Gruns frequency spot-check (Edgar, 1:54 PM)** — live ~1 week; distinct ask, needs its own spike if pursued.
- No August run in the holdout lineage (POST capped 07-31); re-run B when the August run lands to confirm the decline out-of-window.
- Instrument B ownership/canonicality still unconfirmed with Matt (holdout lineage vs ghost-bid).
- The pre-06-30 attributed slide (§4.6) is unexplained — began before the swap; possible creative fatigue or seasonality; not needed for this verdict.
