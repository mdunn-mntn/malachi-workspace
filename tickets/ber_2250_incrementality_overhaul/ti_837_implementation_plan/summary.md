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

**Methodology update (Matt Brorby, 2026-04-21):** The original Step 1–3 framing below (compute campaign win rate, apply as sampling probability to holdout IPs, compare visit rates) has been superseded by a cleaner approach. Rather than approximating "would-have-been-served" via an aggregate win rate, we use the per-event targeting signal already in `augmentor_log` — intent score and HHST at the moment of the bid opportunity — to filter holdout IPs directly. Matt's exact framing: *"use augmentor_log to see if any IPs in the holdout group appear and if we would have bid on them at the time (what was their intent score and what was the HHST?). This will get us candidate IPs from the holdout to build our comparison group. Next steps are possibly post-processing where we sample from these candidates in order to make sure the treated and control groups have similar distributions. The 'distribution' is a bit vague here, but could initially just be trying to match on prospecting intent scores."* Alex Knorr already has the structure started.

#### Step 1 (revised): Holdout Candidate Extraction from augmentor_log
- For each advertiser, pull all holdout IP appearances in `augmentor_log` during the analysis window (hash bucket 0-99 using `MD5(advertiser_id:ip)` → 64-bit unsigned mod 1000).
- At each appearance, retain the event-level targeting signals: intent score, HHST (Household Signal Threshold — confirm exact semantics with Ryan), mntn_segments, geo, inventory_source, time.
- Filter to events where **MNTN would have bid** given the campaign's active intent threshold and HHST gate at that moment. Events that don't clear the targeting gate are not candidates.
- Output: per-advertiser candidate-holdout table of (ip, event_time, intent_score, hhst, etc.)

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

## 8. Open Items / Follow-ups

- [x] ~~Wait for TI-835 results~~ (complete — "The Two Stories" finding)
- [x] ~~Verify holdout IPs appear in augmentor_log~~ (complete 2026-04-20 — yes, at 10.0% of unique IPs)
- [ ] Follow up on 8.4% row-level discrepancy with Ryan/Zach — is there a downstream dedupe/freq-cap that treats holdouts differently?
- [ ] Build win-rate calculation pipeline (augmentor_log + cost_impression_log)
- [ ] Implement pseudo-exposure assignment for holdout IPs
- [ ] Compute ATT estimates for 6-10 advertisers by intent tier
- [ ] Implement CUPED variance reduction on pre-period visit history
- [ ] Run power analysis per advertiser (Lewis-Rao formula)
- [ ] Set up mid-intent-only treatment campaign with Kirsa/Nick (deadline: April 30th)
- [ ] Get leadership direction on performance vs incrementality trade-off (Kale/Alex Bohr)
- [ ] Present ghost bidding methodology and initial results to Kale/Alex Knorr

## 9. Key References

- **Full playbook:** `artifacts/iroas_measurement_playbook.md` — 10 ranked methods, feasibility scorecards, phased rollout, 11 open decisions, reading list
- **Johnson, Lewis & Nubbemeyer (2017) JMR** — Ghost ads canonical design (SSRN 2620078)
- **Johnson, Lewis & Reiley (2017) Marketing Science** — Exposure conditioning adds 31% precision
- **Lewis & Rao (2015) QJE** — Power analysis ground truth for ad measurement
- **Gordon et al. (2023) Marketing Science** — DML with 5,000+ features fails on 663 RCTs
- **Alex Knorr's pre-analysis:** SteelHouse/databricks_targeting, branch TI-835, `dw-main-bronze.external.TI_835_prospecting_scores`
- **Knowledge base:** `knowledge/experimentation.md` — extracted MNTN-relevant insights from playbook
