# TI-837: Ghost Bidding Incrementality Experiment — Implementation Plan

**Jira:** https://mntn.atlassian.net/browse/TI-837
**Status:** In Progress (pivoted from shuffling to ghost bidding, 2026-04-17)
**Date Started:** 2026-04-17
**Date Completed:**
**Assignee:** Malachi
**Parent:** [BER-2250](https://mntn.atlassian.net/browse/BER-2250) — Incrementality Overhaul

---

## 1. Introduction

Design and implement a ghost bidding analysis framework to measure the incremental lift of MNTN's intent-tier targeting, using Average Treatment on the Treated (ATT) methodology applied to the existing 10% holdout group. This replaces the original intent score shuffling approach, which was abandoned due to ITT coverage dilution showing zero signal (TI-835).

**Pivot history:** TI-835 observational analysis revealed "The Two Stories" — guid_log shows ~0% lift (total site traffic unchanged), clickpass_log shows 2-8x lift (MNTN-attributed visits). The ITT approach structurally biases toward zero when impression coverage is low (14-16% of treatment group). Ghost bidding (ATT) addresses this by comparing only IPs that would have been served.

## 2. The Problem

### What TI-835 Showed
- guid_log: ~0% lift across 9 advertisers — CTV ads do NOT increase total site traffic
- clickpass_log: 2-8x lift across all 9 advertisers — CTV ads DO increase MNTN-attributed visits
- ITT (Intent to Treat) shows zero because only 14-16% of the "treatment" group actually receives impressions
- Coverage rates even lower than meeting estimates: high-intent median 3.4%, peak 0.2%, mid 0.04% (Alex Knorr's pre-analysis)

### Why Ghost Bidding
Ghost bidding compares only IPs that *would have been served* — eliminating coverage dilution. The question shifts from "does assigning someone to a treatment group matter?" (ITT) to "does actually seeing the ad matter?" (ATT/LATE).

### Performance vs Incrementality Trade-off (Matt Brorby, 2026-04-07)
Optimizing for incrementality and optimizing for visit rate are partially opposed:
- High-intent users: high visit rate, low lift (would have visited anyway)
- Low-intent users: low visit rate, high lift (wouldn't have visited without the ad)
- "You don't want to just target things that get you higher lift — they don't push you into the visit rates you'd get on your own"

Need explicit direction from Kale/Alex Bohr on how to balance these.

### Phase 2: Lift-Optimized Model (Future)
Matt outlined a model that trains on impressions as a feature — predicting the *incremental* value of serving an impression to a household, not just intent to visit. This is the long-term solution but depends on first establishing the incrementality baseline.

## 3. Plan of Action

### Phase 1: Ghost Bidding Analysis Framework (Now — May 2026)

**Objective:** Build an ATT estimator using the existing 10% holdout, without requiring bidder-side changes.

**Methodology locked (2026-04-21), refined (2026-04-22):** Matt Brorby's event-level filter approach is the *primary* methodology. Ryan Kleck's aggregate win-rate is retained as a *secondary sanity check*. See "Methodology decision rationale" below. Key refinement from 2026-04-22 meeting: holdout IPs appear in augmentor_log but their `mntn_segments` array does NOT include the segment they are a holdout of — so the targetable audience must be reconstructed externally (DS13/DS19 overlap or `audience_segments.expression`) before intersecting with the holdout hash. See "Methodology correction" below.

**Path decision pending Alex Bloore (2026-04-22):** Zach Schoenberger and Jordan Piepkow are discussing with Alex Bloore about implementing ghost bidding at the **bidder level** (production solution, longer timeline). The augmentor_log analysis described here is the **stopgap** that gets us an incrementality estimate for the TI-855 April 30 deliverable without waiting on bidder work. Malachi's sync with Alex Bloore (afternoon of 2026-04-22) is the decision point: commit to the stopgap, or wait for the bidder implementation. Ryan does not build any pipeline until this is resolved.

Matt's framing (from Slack, 2026-04-21): *"use augmentor_log to see if any IPs in the holdout group appear and if we would have bid on them at the time (what was their intent score and what was the HHST?). This will get us candidate IPs from the holdout to build our comparison group. Next steps are possibly post-processing where we sample from these candidates in order to make sure the treated and control groups have similar distributions. The 'distribution' is a bit vague here, but could initially just be trying to match on prospecting intent scores."* Alex Knorr already has the structure started.

### Methodology decision rationale (2026-04-21)

Two candidate methodologies surfaced after Matt's Slack clarification and Ryan's meeting on 2026-04-21:

1. **Event-level filter + propensity matching (Matt):** for each holdout IP appearance in augmentor_log, keep the event if intent score ≥ campaign threshold AND HHST cleared; propensity-match candidate-holdout distribution to served-treatment distribution on intent score.
2. **Aggregate win-rate sampling (Ryan):** compute win_rate = cost_impression_rows / augmentor_rows per campaign-day; apply that rate as inclusion probability to holdout augmentor rows.

**Decision: Matt's event-level filter is the primary methodology.** Rationale:

- **Academic:** matches Johnson-Lewis-Nubbemeyer (2017 JMR) canonical ghost-ad design, which conditions on the deterministic ad-serving rule to identify ghost-exposed controls.
- **Statistical:** Ryan's aggregate approach assumes uniform win rate across intent bands. Almost certainly false — high-intent IPs win auctions at different rates than low-intent IPs. Applying a marginal win rate biases the holdout candidate pool toward mean-intent, underestimating ATT in exactly the intent-tier breakouts we need.
- **Pragmatic:** Intent-tier heterogeneity is the whole point. TI-884 (power per tier), TI-885 (mid-intent), and TI-886 (uplift ranking) all depend on per-tier measurement. Aggregate win-rate collapses that.

**Ryan's aggregate is retained as a secondary estimator / sanity check.** If the two estimates agree, we gain robustness. If they disagree, the disagreement is itself diagnostic of heterogeneity and should be reported.

**Storage is a separate design problem.** Matt's methodology does not require full per-event storage — IP-day aggregation with intent-score distribution bins + qualifying-event counts is sufficient. That design is Open (Q4 in the Slack thread).

#### Step 1 (revised 2026-04-22): Holdout Candidate Extraction from augmentor_log
**Important:** holdout IPs appear in augmentor_log, but their `mntn_segments` array does NOT include the segment the IP is a holdout of (confirmed empirically by Matt Brorby and Alex Knorr, 2026-04-22 meeting). The audience therefore cannot be reconstructed from `mntn_segments` on the holdout rows. Instead:

1. **Pick the target audience externally.** For a given advertiser and campaign, reconstruct the targetable IP universe from `prospecting_scores` / `advertiser_scores`, or from the DS13 × DS19 overlap that `audience_segments.expression` would resolve to at campaign time. This is the "who would have been eligible" set, independent of holdout status.
2. **Apply the holdout hash.** Compute `MD5('{advertiser_id}:{ip}')` → 64-bit unsigned → mod 1000 on the targetable IP set. IPs in buckets 0-99 are holdouts for that advertiser.
3. **Look up in augmentor_log during the campaign window.** For each holdout IP, verify at least one appearance in `augmentor_log` inside the campaign's active window. An appearance proves the IP was biddable (the augmentor evaluated it for *some* bid request) even though `mntn_segments` does not attest to audience membership.
4. **Attach event-level signals at the appearance.** Retain intent score, HHST (Household Signal Threshold — semantics still to confirm with Ryan), geo, inventory_source, time.
5. **Filter to events where MNTN would have bid** given the campaign's active intent threshold and HHST gate at that moment.

Output: per-advertiser candidate-holdout table of (ip, event_time, intent_score, hhst, etc.).

**PSA advertiser exclusion:** `advertiser_id = 90` is the MNTN PSA advertiser. PSA impressions are intentionally served to holdout IPs. Exclude AID 90 from the treatment side of the analysis.

**Scope (2026-04-22 meeting decision):** This is analysis-only, not pipeline. Pick 1-2 advertisers with an active 14-day campaign window. Run in BQ SQL. Ryan does NOT build a pipeline version until the analysis produces meaningful results AND the Alex Bloore decision on bidder-level ghost bidding is resolved.

#### Step 2 (revised): Propensity Matching to Build Comparison Group
- For each advertiser, pair the candidate-holdout table with the actually-served (treatment) table from `cost_impression_log`.
- Match on distribution of prospecting intent scores (and any other covariates Alex's pre-analysis surfaces) so the comparison group mirrors the treated group.
- This is a one-to-one or stratified sampling step — not a win-rate multiplication.
- Academic basis: Johnson, Lewis & Nubbemeyer (2017 JMR, SSRN 2620078) — ghost ads canonical design; propensity matching is a MNTN-specific adaptation using the targeting signal we already log.

#### Step 3: ATT Estimation
- Compare visit rates: actually-exposed treatment IPs vs propensity-matched holdout IPs.
- **ATT estimator:** Difference in visit rates between these two groups.
- **LATE estimator (if needed):** tau_LATE = ITT / first-stage exposure rate (Imbens-Angrist Wald estimator).
- Break down by intent tier (high/mid/peak performance) and by advertiser.

#### Original win-rate approach (ARCHIVED, 2026-04-21)
The pre-2026-04-21 plan proposed: calculate campaign-level win rate from `cost_impression_log / augmentor_log`, then apply that rate as a sampling probability on holdout augmentor appearances. Matt noted this is unnecessary approximation when we have the per-event targeting signal. The win-rate number may still be useful as a sanity check (Q: does the fraction of candidates we keep ≈ the win rate?) but not as the core ghost-bidding mechanism.

#### Step 4: Variance Reduction
- **CUPED** on pre-period visit history (20-50% SE reduction)
- Ghost-ad conditioning itself provides ~25% SE reduction (Johnson-Lewis-Reiley 2017, "When Less Is More")
- Stratified randomization on pre-period sales (10-20% SE reduction)
- Combined: ~40% SE reduction, equivalent to 2.7x the sample

#### Step 5: Power Analysis
- Apply Lewis-Rao formula with MNTN-specific sigma calibration per advertiser
- Flag campaigns where MDE > 15% — these cannot produce reliable point estimates
- **Rule of thumb:** Do not report iROAS without +/-50 pp CI for campaigns below 5M impressions
- Pre-compute MDE for each advertiser using 52 weeks of historical revenue

#### Step 6: Mid-Intent Treatment Campaign
- Set up mid-intent-only treatment campaign with experiments team (Kirsa, Nick)
- **Target:** April 30th deadline for experiment setup
- Purpose: stronger signal — pure mid-intent focus rather than shuffled high/mid mix
- The ghost bidding framework analyzes BOTH existing campaigns AND this new mid-intent campaign

**Deliverable:** ATT estimates with confidence intervals for 6-10 advertisers, broken down by intent tier.

### Future Work: Triangulation Roadmap

Ghost bidding is the immediate step. The full roadmap (from the iROAS Measurement Playbook — see `artifacts/iroas_measurement_playbook.md`):

1. **Next: DMA-Randomized Geo Holdout Pilot**
   - 6-10 performance advertisers, >=$500k/month CTV spend
   - 4-week pre-period, 4-week treatment, conformal inference via GeoLift (ASCM)
   - Feasible today without bidder changes — geo is identity-agnostic
   - Commuting-zone aggregation where spillover is a risk (NY-NJ-CT, DC-VA-MD)
   - GeoLift (R) or PyMC-Marketing (Python) on Databricks

2. **Then: Calibrated Bayesian MMM (Meridian)**
   - Feed geo lift + ghost bidding results as LogNormal priors on CTV channel ROI
   - Weekly MMM data mart in BigQuery (advertiser x geo x week x channel x KPI)
   - Reported iROAS = posterior mean with 80% credible interval per advertiser
   - Connects to Kale's 5 external vendor experiment initiative

3. **End State: Unified Measurement**
   - Bayesian blending of geo, household RCT, switchback, clean-room, and MMM
   - Explicit uncertainty per advertiser (wider intervals for small campaigns)
   - PIE-style meta-model for non-lift-tested campaigns (Gordon et al. 2023/2026)

**11 open decisions** needed from leadership to unblock the full rollout — documented in the playbook Part 4 and `knowledge/experimentation.md` (Open Decisions section).

## 4. Investigation & Findings

### 1-day smoke test on Zazzle (2026-04-27)

**Run:** advertiser 37775 (Zazzle), window 2026-04-24 (single day), pipeline = `queries/ti_837_lift_analysis.sql` + `artifacts/ti_837_compute_att.py`.

**Cost:** ~10 min wall, 18.1 TB processed (~$90). The 1-day BQ dry-run estimated 610 GB — federated-table cost estimation is unreliable; actual was ~30× higher. Logged.

**Pivot from WGU:** WGU (31357) is not in `household_scoring__prospecting_intent__v1` — it's a keyword-only (DS19) advertiser, not vertical/DS13. We pivoted to Zazzle (37775) which has 12 active campaigns, ~74M unique IPs in the prospecting feed, and was already in Alex Knorr's TI-835 sample.

**Per-tier results:**

| Tier | Outcome | Treated rate | Holdout-biddable rate | Lift (pp) | p |
|------|---------|--------------|-----------------------|-----------|---|
| high | clickpass | 1.51% | 0.022% | **+1.49** | <0.0001 |
| high | guid | 1.82% | 0.53% | **+1.30** | <0.0001 |
| peak | clickpass | 0.316% | 0.011% | **+0.306** | <0.0001 |
| peak | guid | 0.456% | 0.708% | **−0.252** | <0.0001 |
| mid | clickpass | 0.0073% | 0% | +0.007 | <0.0001 |
| mid | guid | 0.0073% | 0.0058% | +0.001 | 0.42 |

**Weighted ATT (ATT-stratified, treatment-count weights):**
- clickpass: +0.92pp
- guid: +0.65pp

**No `max_reach` tier** — Zazzle's prospecting only scores high/peak/mid.

### Three findings from the smoke test

1. **Ghost-bidding ATT recovers a real guid signal that ITT couldn't see.** TI-835 ITT-on-guid showed ~0% lift across all 9 advertisers because 86% of "targeted" never got served and diluted the signal. ATT conditioning on actually-served vs actually-biddable shows **+1.30pp lift on high-intent guid for Zazzle**. This is the answer to the question TI-835 couldn't answer: *yes, MNTN ads do drive new high-intent traffic*. Confirmation in one advertiser, one day — replication needed.

2. **Peak intent has negative guid-ATT** (−0.25pp, p<0.0001). Three explanations, in rough plausibility order:
   - **Selection bias on the control side**: today's biddable-holdout filter is the loosest possible (any augmentor appearance counts). For peak (vertical-only matches, broad audience), this brings in disproportionate "online but not Zazzle-relevant" IPs whose visit baseline differs from Zazzle's actual targeting envelope.
   - **Real null/negative incrementality at the intermediate tier.** Peak audience is "users who like education content" — broader, less keyword-anchored. Maybe targeting at this tier really doesn't add value for total traffic.
   - **Sample noise.** Unlikely given n_treated=380K, n_control=947K.
   - **Decision (per user, 2026-04-27):** don't chase as a separate investigation. Let it surface naturally when we replicate across advertisers; if it persists, dig in then.

3. **Clickpass overstates attribution ~20× vs real-traffic effect for high-intent.** High-intent clickpass lift is 70×, guid lift is 3.4×. Both are real, but the wedge between them is "attribution capture" — visits MNTN takes credit for that would have happened anyway through search, direct, or other channels. This is exactly the gap LiftLab and Kochava pick up when they tell advertisers MNTN's incrementality is overstated. Worth highlighting prominently in the deck.

### Augmentor_log holdout verification (2026-04-20)

**Question:** Alex Knorr (Apr 17) said holdout IPs appear in augmentor_log; Ryan Kleck
(Apr 20) said they don't. Which is correct? This decides whether the ghost bidding
pipeline can be built from existing data (Alex's read) or requires an ETL + bidder
change (Ryan's read).

**Method:** For advertiser 31357 (WGU), hash each IP in `bronze.raw.augmentor_log`
for 1 hour on 2026-04-19 using the production formula
`MD5(advertiser_id:ip) → first 64 bits (unsigned) mod 1000`, ported to BQ via a
Chinese-remainder split over two 32-bit halves (2^32 mod 1000 = 296). Count rows and
unique IPs where bucket is in 0-99 (the holdout range).

**Result:**
| in_holdout_bucket | n_rows        | unique_ips  |
|-------------------|---------------|-------------|
| false             | 1,246,877,127 | 16,465,297  |
| true              |   114,582,413 |  1,826,814  |

- Unique IPs in holdout bucket: **1,826,814 / 18,292,111 = 10.0%** — exactly the
  uniform expectation. Confirms augmentor_log is advertiser-agnostic and
  IP-complete: holdouts for any advertiser pass through at the expected 10% rate.
- Rows in holdout bucket: **8.4%** (slightly below 10%). Possible causes: holdout
  IPs have slightly fewer augmentor events per IP than non-holdout IPs, frequency
  cap interaction, or a downstream re-augmentation that treats holdouts differently.
  Worth flagging but does not block the methodology.

**Decision:** Alex Knorr's read is correct. The ghost bidding pipeline can be built
using `augmentor_log` (pseudo-exposure) × `cost_impression_log` (actual exposure)
with no ETL change from Zach/Jordan and no bidder-side change from Kevaughn. The
multi-party meeting scheduled for Apr 22 becomes a walkthrough-and-align session
rather than a scoping session.

*Ghost bidding framework under construction. TI-835 findings inform this work (see TI-835 summary).*

## 5. Solution

*In progress — ghost bidding ATT estimator.*

## 6. Questions Answered

- **Can we measure incrementality with ITT on the existing holdout?** No — coverage dilution (14-16% actual impression coverage) structurally biases ITT toward zero. Ghost bidding (ATT) addresses this.
- **Is shuffling still the right approach?** No — replaced by ghost bidding (April 17, 2026). Ghost bidding is more methodologically sound and doesn't require operational changes to the targeting system.
- **Do holdout IPs appear in augmentor_log?** Yes (empirically confirmed 2026-04-20, advertiser 31357, 1 hour of data). 10.0% of unique IPs fall in hash buckets 0-99 — the uniform expectation — so augmentor_log captures all IPs regardless of holdout status. The ghost bidding pipeline can proceed using existing data without ETL or bidder-side changes.

## 7. Data Documentation Updates

- Added "Incrementality Measurement — Causal Method Reference" section to `knowledge/experimentation.md` (2026-04-19)
- Includes: ranked method reference, Lewis-Rao power constraints, ghost bidding academic foundation, co-viewing bias, triangulation architecture, vendor assessments, reading list
- Full iROAS Measurement Playbook stored at `artifacts/iroas_measurement_playbook.md`

### Decisions locked 2026-04-27 (brainstorm with user)

**Goal:** A statistically significant incrementality estimate at three levels:
1. **Overall MNTN** — single number across a representative sample of advertisers, with CI.
2. **By intent tier** — high / peak / mid / max_reach where applicable.
3. **By advertiser** — only where N is sufficient for significance.

**Audience:** TI team. Final artifact is a presentation. Technical detail is OK.

**Validation:** None — internal consumption, no external benchmarking.

**Methodology:**
- Ghost-bidding ATT (locked).
- Biddable-holdout filter stays loose (augmentor appearance for any reason). Tightening deferred — the treated side has equivalent bias under the loose filter, and we want speed first.
- Variance reduction (CUPED, stratified randomization checks) deferred to Phase 2 if signal warrants.

**Iteration discipline (per user direction):**
- Speed first. Test on small samples. Optimize and log learnings before scaling.
- Don't waste compute during the planning phase.
- Converge on a method that works, then scale up.
- Don't worry about iteration cost ceiling — focus on reducing per-query cost.

**Scope of advertiser pool:**
- Source: Alex Knorr's TI-835 9-advertiser list. 7 of 9 are in `household_scoring__prospecting_intent__v1`: Ferguson Home (31276), Ancient Nutrition (31455), First Watch (34143), HexClad (34611), Clayton Homes (34838), Zazzle (37775), Northern Tool (40563). Missing: Angi (32766), REVOLVE (53308) — likely keyword-only or behind a different scoring pipeline, same as WGU.
- "Enough advertisers to estimate MNTN-overall without going massive" — final pick TBD by next chat. Probably 5-7 from the 7 available, weighted/selected for size + vertical diversity.

## 8. Open Items / Follow-ups

- [x] ~~Wait for TI-835 results~~ (complete — "The Two Stories" finding)
- [x] ~~Verify holdout IPs appear in augmentor_log~~ (complete 2026-04-20 — yes, at 10.0% of unique IPs. Caveat: segment is NOT in mntn_segments array on those rows, confirmed 2026-04-22)
- [ ] **Sync with Alex Bloore (2026-04-22 afternoon): commit to augmentor_log stopgap vs wait for bidder-level ghost bidding (Zach/Jordan path)** — blocking decision for TI-855 April 30
- [ ] Follow up on 8.4% row-level discrepancy with Ryan/Zach — is there a downstream dedupe/freq-cap that treats holdouts differently?
- [ ] Pick 1-2 advertisers with active 14-day campaign windows (post Alex-Bloore sync)
- [ ] Reconstruct target audiences from prospecting_scores / DS13 × DS19 overlap for picked advertisers
- [ ] Intersect target IPs with holdout hash (buckets 0-99) and look up in augmentor_log
- [ ] Propensity-match candidate-holdout distribution to served-treatment distribution on intent score
- [ ] Compute ATT estimates with CI by intent tier
- [ ] Implement CUPED variance reduction on pre-period visit history
- [ ] Run power analysis per advertiser (Lewis-Rao formula — TI-884)
- [ ] Set up mid-intent-only treatment campaign with Kirsa/Nick — TI-885 (deadline: April 30th)
- [ ] Get leadership direction on performance vs incrementality trade-off (Kale/Alex Bohr)
- [ ] Present ghost bidding methodology and initial results to Kale/Alex Knorr
- [ ] Exclude AID 90 (MNTN PSA advertiser) from analysis — PSA impressions are served to holdouts intentionally

## 9. Execution Progress (2026-04-27+)

Working from `artifacts/ti_837_execution_plan.md` — 7-advertiser × 7-day primary
analysis with IVW meta-analysis aggregation and a 0.5pp guid-CI N-gate per cell.

| Stage | Status | Notes |
|---|---|---|
| **Stage 1** — 7-adv × 1-day smoke (window 04-23) | ✅ Complete (2026-04-27) | 18.2 TB / 18.2 TB billed / 10.6 min wall — single-query batching held perfectly (matched the 1-advertiser smoke-test cost; ~7× reduction over naive linear scan). 42 cells, 41 pass the 0.5pp guid N-gate (Clayton mid is the lone fail — zero visitors both arms). Zazzle 04-23 high guid +1.36pp reproduces the 04-24 smoke +1.30pp within 0.07pp drift. Local output: `outputs/ti_837_lift_7adv_1day_2026_04_23.json` (gitignored). |
| **Stage 2** — 7-adv × 7-day primary (window 04-20→04-26, +3-day visit post-period to 04-29) | ✅ Complete (2026-04-27) | 126.7 TB / 74 min wall / 560 slot-hours. Single-query batching held — augmentor scan amortized 7×. 26 cells output (peak/mid empty for HexClad/First Watch/Zazzle/Northern Tool — MAX(score)/week absorbed them into high). Local output: `outputs/ti_837_lift_7adv_7day_2026_04_20_to_26.json` (gitignored). |
| **Stage 3** — IVW meta-analysis + N-gate + sensitivity | ✅ Complete (2026-04-27) | All 26 cells pass the 0.5pp guid CI half-width gate. High-tier IVW pool: clickpass +4.17pp / guid +3.36pp (1.24× over-credit). Peak-tier pool (3 advertisers): clickpass +0.55pp / guid +0.88pp (0.62× UNDER-credit — wedge inverts). Mid-tier essentially zero. Leave-one-out flagged Ancient Nutrition swinging the all-cells pool by 1.17pp — confirms IVW dominance pathology in mid-rate cells; per-tier numbers are stable. |
| **Stage 4** — diagnostic re-runs | Skipped (not needed) | All cells pass the N-gate. No window extensions required. |
| **Stage 5** — Tufte charts + RevealJS deck + presentation critique | ✅ Complete (2026-04-27, polished revision) | 4 PNG charts (`artifacts/ti_837_chart_*.png`), narrative `artifacts/ti_837_presentation.md`, self-contained RevealJS deck `artifacts/ti_837_presentation_deck.html` (527 KB). Critique applied + user-requested polish: title/subtitle z-order fixed in 3 charts (explicit positioning replaces `set_title` + `ax.text(y=1.05)`), Northern Tool's negative bar label moved left of bar, mid-intent dropped from wedge-ratio chart (uninterpretable noise floor), wedge-ratio annotation centered between bars. Methodology slide stripped of cost claims (BQ bytes-billed isn't true USD cost). Bug/feature framing removed in favor of "two different questions, both worth answering" — attribution and incrementality are inherently different metrics by design. **Shared (final): https://gist.githack.com/mdunn-mntn/ef648cae0ba1c6ac769df652f2de4615/raw/ti_837_presentation_deck.html** |

### Chart cosmetic issues — RESOLVED 2026-04-27 (kept here for context)

User reviewed the rendered RevealJS deck on 2026-04-27 and flagged four matplotlib/HTML layout bugs. All resolved in commit immediately following. Data correctness was unaffected throughout. Notes preserved for future similar work.

1. **Title/subtitle z-ordering inverted in 3 charts.** In `chart_money_per_tier_wedge`, `chart_per_advertiser_high_intent`, and `chart_wedge_ratio` the subtitle (gray, written via `ax.text(0, 1.05, ..., transform=ax.transAxes)`) renders **above** the bold-navy title (written via `ax.set_title(..., pad=8)`). Reading order should be title → subtitle, not subtitle → title.
   - **Fix.** Either (a) move the subtitle below the title with `ax.text(0, 1.02, ..., va='top', transform=ax.transAxes)` and bump the title pad up, (b) use `fig.suptitle()` for the main title and reserve `ax.set_title()` for the subtitle, or (c) concatenate into one multi-line title with markdown-style first line bold via `ax.text` only.
   - **Files.** `artifacts/generate_charts.py` lines invoking `ax.text(0, 1.05, ..., transform=ax.transAxes)` immediately after `ax.set_title(...)` in three chart functions.

2. **Northern Tool `-0.05pp` label collides with error-bar caps.** In `chart_per_advertiser_high_intent`, the bar value `v = -0.05` and the text x-position `v + 0.05 = 0.0` puts the label "−0.05pp" right next to the zero line. The error-bar left-cap (drawn at `att - 1.96*se`) renders *over* the leading "−" character, so the rendered label looks like "0.05pp" with a stray cap glyph next to it.
   - **Fix.** For negative bars, place the label to the LEFT of the bar end (not the right): `if v < 0: x_pos = v - 0.05; ha = 'right'`. Apply the same logic in the per-advertiser chart and any future negative-ATT chart.
   - **File.** `artifacts/generate_charts.py` `chart_per_advertiser_high_intent` function, the `for b, v in zip(bars, atts)` text loop.

3. **Wedge-ratio mid-tier bar overlaps the `1× = clickpass matches guid` annotation.** In `chart_wedge_ratio`, the mid-intent ratio (2.34×) draws a bar that intersects the dashed reference line and its text annotation, which sits at `y=1.02` near the right edge. The annotation text gets occluded by the mid bar.
   - **Fix.** Either (a) move the annotation to the left edge (`ha='left'` at `x=0`), (b) drop mid-intent from this chart entirely (it's flagged as noise floor in the text and the 2.34× number is statistically meaningless given the underlying ATTs are ~0.01pp), or (c) raise the annotation y-position above the tallest bar.
   - **Recommended.** Drop mid-intent from the wedge-ratio chart. Mathematically: when both numerator and denominator are at the noise floor, the ratio is uninterpretable, and visually the bar dominates a chart that's meant to be about the high-vs-peak inversion.
   - **File.** `artifacts/generate_charts.py` `chart_wedge_ratio` function.

4. **(Optional) Subtitle text wraps off the right edge** on the per-advertiser chart in widescreen viewport (≥2560px). The full subtitle "Per-advertiser high-intent guid-visit ATT (95% CI), 7-day window. Six of seven show real positive lift; Northern Tool is indistinguishable from holdout." extends past the chart's right margin. Either shorten it or wrap.

**To re-render after fixing:**

```bash
python3 tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/artifacts/generate_charts.py \
  --csv tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/outputs/ti_837_per_cell_table.csv \
  --meta tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/outputs/ti_837_meta_analysis_2026_04_20_to_26.json \
  --out-dir tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/artifacts/

# rebuild self-contained HTML deck (re-base64s the PNGs):
cd tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/artifacts/
python3 -c "
import base64, pathlib, json
charts = {f: base64.b64encode(pathlib.Path(f).read_bytes()).decode()
          for f in ['ti_837_chart_mntn_overall_headline.png',
                    'ti_837_chart_money_per_tier_with_wedge.png',
                    'ti_837_chart_per_advertiser_high_intent.png',
                    'ti_837_chart_wedge_ratio_per_tier.png']}
pathlib.Path('/tmp/ti_837_charts_b64.json').write_text(json.dumps(charts))"
# then re-run the deck-build python (the HTML body is parameterized on /tmp/ti_837_charts_b64.json)

# share the new deck:
bash .claude/scripts/share_deck.sh tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/artifacts/ti_837_presentation_deck.html
```

Note: Stage 2/3 outputs (`ti_837_per_cell_table.csv`, `ti_837_meta_analysis_2026_04_20_to_26.json`) are gitignored, so the next session needs to re-run `ti_837_compute_att.py` against the local Stage 2 BQ output (`outputs/ti_837_lift_7adv_7day_2026_04_20_to_26.json`) before regenerating charts. If that local file has been deleted, re-run Stage 2 — but note augmentor_log TTL: by 2026-05-04 the 04-20 partition will be purged, slide window forward.

### Lessons surfaced during execution (append-only)

- **2026-04-27 — bq CLI flag-parser RecursionError on SQL strings starting with `--`.**
  When passing SQL as a positional argument to `bq query`, if the first character is `--` (a SQL block-leading comment), absl's flag parser interprets the whole SQL as an unknown flag and tries to compute Levenshtein distance suggestions, blowing Python's recursion limit. The query never dispatches. Workaround: pipe SQL via stdin to `bq query` (works through `bq_run.sh` wrapper too). Documented in `knowledge/data_knowledge.md`.
- **2026-04-27 — augmentor_log partition coverage confirmed for 04-20 onward.**
  Verified via `INFORMATION_SCHEMA.PARTITIONS` before Stage 1 — partitions present back to 04-19 (1-day buffer to TTL). Window 04-20→04-26 is safe inside the 10-day TTL given today is 04-27.
- **2026-04-27 — Single-query batching is essentially free.** Stage 1 7-advertiser scan cost 18.2 TB, the same as the 1-advertiser smoke. The augmentor_log scan dominates total bytes; doing it once and inner-joining holdouts/targets per advertiser scales linearly in IPs (cheap) rather than in augmentor rows (expensive). Confirms the plan §4 optimization #1 was correctly identified as the dominant lever.
- **2026-04-27 — Per-advertiser high-intent guid-ATT spans an order of magnitude.** Stage 1 1-day window: Northern Tool +0.21pp at the low end, Ferguson Home +6.96pp at the high end. The plan's "+0.3pp to +3pp" plausible range is too tight — Ferguson's lift is real and reflects vertical (Home Goods / Plumbing) where MNTN's intent signal is most differentiated. Don't drop Ferguson as an outlier; it's the strongest evidence in the cohort.
- **2026-04-27 — MAX(household_score) over a 7-day window collapses peak/mid into high for advertisers with stable scoring.** For HexClad, First Watch, Zazzle, Northern Tool, virtually every targetable IP in their prospecting feed scored 10000 on at least one day in the week — so per-tier peak/mid pools came back empty for those four. Per-tier peak IVW pool only reflects Ferguson, Ancient Nutrition, Clayton Homes. The methodology trade-off is between using max-tier (matches how the bidder treats the IP) vs daily-tier (preserves stratification at cost of subject-per-day duplication). For Phase 2 we should consider per-(advertiser, IP, day) subjects to retain peak/mid breakouts.
- **2026-04-27 — IVW pool over all cells is dominated by mid-tier near-zero rates.** The plan's "MNTN-overall IVW across all cells" gives +0.16pp guid; leave-one-out swings to +1.33pp dropping Ancient Nutrition. The dominance is mathematical: mid-tier rates ~0.005% have variance ~5e-9 per IP, giving them inverse-variance weights orders of magnitude above high-tier cells. Per-tier pools are stable and defensible. We led the deck with the high-tier pool (+3.36pp) and noted the all-cells pathology in caveats. For Phase 2: consider sample-size-weighted or arithmetic-mean-of-advertiser-ATTs alternatives.
- **2026-04-27 — The clickpass-vs-guid wedge inverts at peak intent.** At high intent: clickpass +4.17pp > guid +3.36pp (over-credit by 24%). At peak: clickpass +0.55pp < guid +0.88pp (under-credit by 38%). The asymmetry is funnel-dependent: high-intent IPs trigger clean clickpass attribution chains; peak-intent IPs visit but don't always fire clickpass events. Aggregate hides both errors because they roughly cancel. Worth surfacing to the broader team as a structural finding about attribution capture rates by intent tier — not just "clickpass overstates," but "clickpass overstates AND understates depending on where in the funnel you look."

## 11. Phase 2 cohort selection (2026-04-27)

Phase 1 used 7 advertisers inherited from TI-835's sufficiency screen
(convenience selection). Three weaknesses surfaced: tier collapse on 4 of 7
(MAX-tier construction absorbed peak/mid into high), Ancient Nutrition
dominance in IVW pooling, no defense against "you cherry-picked 7."

Built a fresh stratified cohort of **30 advertisers** for the same 7-day
window (2026-04-20 → 2026-04-26). Selection methodology:

1. **Stage A — empirical universe characterization** (read-only BQ):
   - A.1d (1-day HLL on 2026-04-23): per-advertiser distinct IPs and
     per-tier IP counts (`ips_ever_high/peak/mid/max_reach`). Dropped to
     1-day after 7-day attempts (full COUNT DISTINCT, simpler MAX, HLL
     on 7 days) all stalled at 30+ min wall / 800B+ slot-ms with no
     bytes reported. The external prospecting Parquet has 20K parallel
     inputs per partition — the SCAN dominates regardless of
     aggregation strategy.
   - A.3 (cost_impression_log, 7-day): 1,687 active advertisers with
     served_distinct_ips per advertiser.
   - A.4 (agg__daily_sum_by_campaign, March 2026): spend +
     funnel/objective/channel mix. **Discovered table is stale at
     2026-03-31 — switched stratification window to March; documented
     in `knowledge/data_knowledge.md`.**
   - A.5 (fpa_advertiser_verticals): vertical category per advertiser.
   - A.2 SKIPPED: full augmentor scan would cost $250-500. By
     hash-symmetry of the holdout MD5 construction, biddable_rate ≈
     win_rate × O(10); used `holdouts × 0.30` as a conservative
     biddable proxy.

2. **Stage B — empirical selection criteria** (`artifacts/ti_837_cohort_selection_criteria.md`):
   - Inclusion: in prospecting feed, active in window (≥100 served IPs),
     per-tier biddable_holdouts ≥ 5,000 in any of {high, peak, mid}
     (power calc: 95% CI ≤ 0.5pp at p ∈ [0.005, 0.05]),
     `frac_high_only = max_high / total ≤ 0.95` (tier-collapse
     prevention), March spend ≥ $5,000.
   - Stratification: spend tercile × top-vertical, with composite
     scoring on (high-tier biddable_holdouts × tier-diversity).

3. **Stage C — final 30-advertiser cohort** (`artifacts/ti_837_phase2_cohort.md`):
   - 13 high / 7 mid / 10 low spend.
   - 20 distinct verticals (Apparel ×3, Home Improvement ×3, etc.).
   - Phase 1 anchors retained: **Ancient Nutrition + Ferguson Home**
     (the two NOT tier-collapsed). The 4 collapsed Phase 1 advertisers
     (First Watch, HexClad, Zazzle, Northern Tool) are correctly
     excluded by the new tier-diversity gate; their Phase 1 results
     stand as a "high-only" validation cohort.
   - Sister-company audience dedup (Re-Bath Oslund and Re-Bath Horney
     had identical (high, peak, mid) signatures): kept one, replaced
     the other with Fiji Airways.
   - Largest single advertiser is 8% of pooled high-tier biddable
     holdouts (vs Phase 1 where Ancient was ~40%) — IVW dominance
     fragility bounded.

**Artifacts (all committed):**
- `queries/cohort_selection/a1c_universe_hll.sql`, `a3_cost_impression_treatment.sql`,
  `a4_spend_funnel_mix.sql`, `a5_vertical.sql`
- `outputs/cohort_selection/a*.csv`, `cohort_scored.csv`, `cohort_final.csv`
- `artifacts/ti_837_cohort_scorer.py`, `ti_837_cohort_builder.py`,
  `ti_837_cohort_selection_criteria.md`, `ti_837_phase2_cohort.md`

**Caveats reported in cohort doc:** 1-day prospecting proxy (not 7-day),
biddability via served-treatment proxy (not augmentor), HLL approximation
(~1-3% error). Actual ATT run validates all three.

**Next session:** drop the 30-advertiser list into the Phase 1 lift SQL
and run the pipeline. Expected cost ≈ $200-400 (proportional to advertiser
count; was $90 for 7).

### Status — 2026-04-28 (CANONICAL: v5)

**v5 is the canonical run.** Multi-segment analysis splitting lift across
4 campaign cuts (all / prospecting-all-stages / Stage 1 only / retargeting
only) using same hash, same cohort, same window. Surfaces that lift is
heavily concentrated in retargeting, with Stage 1 prospecting alone
showing zero incremental lift at high intent.

| Run | Status | Notes |
|---|---|---|
| v1 (no fixes) | Superseded | Both bugs present. |
| v2 (win-rate fix only) | Cancelled at ~2h | Half-fix; slow query graph. |
| v3 (incomplete) | Cancelled at ~18 min | Internal inconsistency. |
| v4 (prospecting-only + consistent win_rates) | Superseded by v5 | First clean run. Reported +0.77pp guid IVW high-intent. v5 reveals that's the prospecting-all-stages average — masks Stage 1's near-zero lift and Stage 2/3 multi-touch's contribution. |
| **v5 (4-segment multi-segment)** | **CANONICAL** | 4-segment analysis. ~6 hr wall (139 stages — 4-segment UNION ALL inflated graph), 4.5T slot-ms, 126.7 TB billed. |

### v5 lift run — CANONICAL RESULTS (2026-04-28)

**The 4-segment headline (high-intent guid IVW):**

| Segment | guid IVW | guid sample-wt | clickpass IVW | wedge | Cells pos |
|---|---|---|---|---|---|
| **Retargeting only** | **+21.07pp** | +28.89pp | +13.97pp | 0.66× | 8/8 |
| All campaigns combined | +3.12pp | +5.44pp | +2.88pp | 0.92× | 25/27 |
| Prospecting (all stages) | +0.78pp | +0.46pp | +1.24pp | 1.58× | 20/26 |
| **Stage 1 only** | **−0.06pp** | −1.03pp | +0.47pp | −8.5× | 12/25 |

**Three findings:**

1. **Retargeting drives the bulk of measured incremental lift.** +21pp at high
   intent, +17pp at peak. Real causal effect AND likely selection bias
   (bidder preferentially bids on visit-prone IPs; our random hash subsample
   doesn't replicate this). True causal effect is bounded between zero and
   +21pp; refining requires bidder-level ghost bidding (Phase 2b).

2. **Stage 1 prospecting alone shows zero incremental lift at high intent.**
   guid-ATT IVW = −0.06pp, sample-weighted = −1.03pp, only 12 of 25
   advertisers (48%) positive. High-intent shoppers were going to convert
   anyway. Validates Alex Bloore's "movable middle" hypothesis — the
   opportunity isn't at high intent.

3. **The "+3.12pp combined" view is misleading.** Mixed-segment denominators
   conflate retargeting's +21pp with Stage 1's zero. The arithmetic averages
   to +3.12pp but obscures both stories. Earlier internal incrementality
   reports used this conflated view.

**Per-segment win_rates** (median):
- All campaigns: 1.00%
- Prospecting all stages: 0.84%
- Stage 1 only: 0.69%
- Retargeting only: 0.10%

Retargeting has tiny win_rate (highly competitive bidding, very specific
target IPs). The retargeting holdout subsample is small (~84k high-tier IPs
across 8 cells); statistical noise is correspondingly larger.

**Output artifacts:**
- `outputs/ti_837_lift_30adv_7day_v5_2026_04_20_to_26.json` (gitignored, 7,328 lines)
- `outputs/ti_837_meta_analysis_30adv_v5_segment_*.json` (4 files, gitignored)
- `outputs/ti_837_per_cell_table_30adv_v5_segment_*.csv` (4 files, gitignored)
- `queries/ti_837_lift_analysis_30adv_7day_v5_segments.sql` (committed)
- `artifacts/ti_837_chart_segment_*_v5.png` (3 charts, gitignored)
- `artifacts/ti_837_phase2_presentation_deck.html` (committed; rebuilt)
- Standalone deck shared:
  https://gist.githack.com/mdunn-mntn/4e934200ec2cce7886f9e5bea93d75fd/raw/ti_837_phase2_presentation_deck_standalone.html

**Power Line:**

> **Retargeting drives the lift.<br>
>  Pure prospecting drives almost none.<br>
>  Combined views hide both.**

### v4 lift run — RESULTS (2026-04-28, superseded by v5)

**126.7 TB billed, 113 min wall, 575 slot-hours** — same byte profile
as v1 (augmentor scan is advertiser-agnostic and dominates).

#### Sample-weighted high-intent (the headline)

For every 1,000 high-intent prospecting IPs MNTN serves:
- Holdout visit rate: **2.31%** (23 visits per 1,000 IPs, organic)
- Treated visit rate: **2.76%** (28 visits per 1,000 IPs)
- **Lift: +0.44pp absolute / +19% relative** (≈ 5 incremental visits per 1,000)

#### Per-tier IVW (×100 for pp)

| Tier | Clickpass | Guid (truth) | Wedge | Verdict |
|---|---|---|---|---|
| **High** | +1.22pp ±0.0084 | **+0.77pp** ±0.022 | **1.59×** | clickpass over-credits 60% |
| Peak | +0.12pp ±0.0054 | −0.02pp ±0.010 | undefined | no real lift |
| Mid | +0.02pp ±0.0024 | +0.00pp ±0.0037 | noise | noise floor |

#### Robustness — high-intent guid across 4 pooling methods

| Method | guid-ATT | clickpass-ATT | Wedge |
|---|---|---|---|
| IVW (default) | +0.77pp | +1.22pp | 1.59× |
| Median | +0.56pp | +1.62pp | 2.91× |
| Arithmetic mean | +0.98pp | +2.17pp | 2.21× |
| Sample-weighted | +0.44pp | +2.33pp | 5.29× |

**All 4 methods agree:** real positive lift at high intent + clickpass over-credits.

#### Per-advertiser distribution (high-intent guid)

- **27 of 29 cells** pass the 0.5pp N-gate
- **21 of 27 (78%)** positive
- Range: **−3.30pp (Ferguson Home) to +6.88pp (TurboTenant)**
- Median: **+0.56pp**
- Largest leave-one-out swing: **<±0.05pp** — no advertiser drives the result

#### Two methodology fixes (vs v1)

1. **Prospecting filter on cost_impression and clickpass** (`objective_id IN 1, 5, 6`). v1 included retargeting, conflating two strategies. Removing it dropped served_treatment dramatically for some advertisers (e.g., Ferguson Home went from +10.55pp lift in v1 to −3.30pp in v4 — Ferguson's "lift" was retargeting, not prospecting).
2. **Win-rate-corrected biddable_holdouts** (Alex Knorr, 2026-04-28). Subsample biddable_holdouts at per-advertiser empirical win_rate so the holdout denominator matches treated arm's "actually-served" condition. Per-advertiser win_rates 0.07% to 12.5% (median 0.8%). Implemented as deterministic hash (`MD5(advertiser_id||':wr:'||ip)`) — independent of the original holdout assignment hash.

Win_rates pre-computed from a small upstream query and hardcoded as
STRUCT literal in the lift SQL — avoids v2's slow CTE materialization.

#### Output artifacts (canonical)

- `outputs/ti_837_lift_30adv_7day_v4_2026_04_20_to_26.json` (gitignored, BQ raw output)
- `outputs/ti_837_per_cell_table_30adv_v4.csv` (gitignored)
- `outputs/ti_837_meta_analysis_30adv_v4_2026_04_20_to_26.json` (gitignored)
- `queries/ti_837_lift_analysis_30adv_7day_v4.sql` (committed)
- `artifacts/ti_837_chart_*_v4.png` (4 charts, gitignored)
- `artifacts/ti_837_phase2_presentation_deck.html` (committed)
- Standalone deck shared:
  https://gist.githack.com/mdunn-mntn/94c9375ffc952c58c0ff623daa640065/raw/ti_837_phase2_presentation_deck_standalone.html

#### Power Line

> **Targeting causes real but modest lift.<br>
>  Attribution shows it 60% larger than reality.**

Supports Alex Bloore's strategic hypothesis: high-intent shoppers were
going to convert anyway, so the room for incremental lift is small.
Validates the "movable middle" framing for Mountain-Match AI roadmap —
mid-intent and uplift modeling should be the next experimental tracks.

---

## 10. Key References

- **Full playbook:** `artifacts/iroas_measurement_playbook.md` — 10 ranked methods, feasibility scorecards, phased rollout, 11 open decisions, reading list
- **Johnson, Lewis & Nubbemeyer (2017) JMR** — Ghost ads canonical design (SSRN 2620078)
- **Johnson, Lewis & Reiley (2017) Marketing Science** — Exposure conditioning adds 31% precision
- **Lewis & Rao (2015) QJE** — Power analysis ground truth for ad measurement
- **Gordon et al. (2023) Marketing Science** — DML with 5,000+ features fails on 663 RCTs
- **Alex Knorr's pre-analysis:** SteelHouse/databricks_targeting, branch TI-835, `dw-main-bronze.external.TI_835_prospecting_scores`
- **Knowledge base:** `knowledge/experimentation.md` — extracted MNTN-relevant insights from playbook
