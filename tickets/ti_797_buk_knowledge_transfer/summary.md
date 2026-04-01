# TI-797: Bottoms Up Keywords (BUK) — Knowledge Transfer & Action Plan

**Jira:** https://mntn.atlassian.net/browse/TI-797
**Parent Initiative:** https://mntn.atlassian.net/browse/TI-273
**Status:** In Progress
**Date Started:** 2026-03-31
**Date Completed:**
**Assignee:** Malachi

---

## 1. Introduction

Bottoms Up Keywords (BUK) is MNTN's initiative to replace the current top-down, LLM-only keyword generation system (Mountain Match V2) with a data-driven approach using an Alternating Least Squares (ALS) collaborative filtering model. The initiative lives under Jira initiative TI-273 ("Dynamic Attribute Recommendations"), currently status: Paused, with 22 related tickets.

This ticket captures a full knowledge transfer from Alex Knorr (BUK lead) on 2026-03-31, consolidating all artifacts, experiment history, and identifying the highest-leverage next actions.

### Current System: Mountain Match V2

1. Scrape advertiser's homepage (Common Crawl)
2. LLM: describe products/services from HTML
3. LLM: describe customers who would be interested
4. LLM: generate 20 parent keywords (what customer sees in UI)
5. LLM: for each of the 20, generate 10 products/services as search terms → 200 child keywords
6. Embedding alignment: map 200 LLM keywords to closest DS19 `data_source_category_id` → collapses to some subset (<200 unique DS19 keywords)
7. These DS19 keywords become the audience expression (targetable keywords)

**Three core problems with V2:**
1. **Small audiences** — fixed 20 parent / ~200 child keywords, no way to scale without regenerating
2. **Hard to iterate** — no concept of keyword importance, can't add/remove intelligently
3. **No pixel data used** — recommendations based solely on homepage scrape, not actual customer behavior (visits, conversions, seasonality)

### BUK Solution: ALS Recommendation Model

**Model**: Implicit ALS collaborative filtering (matrix factorization)
- **Users** = `advertiser_id`
- **Items** = `data_source_category_id` (DS19 targetable keywords)
- **Training data**: 30-day window of `good_log` + `conversion_log` activity per advertiser × DS19 keyword
- **Confidence signal**: Weighted blend of:
  - Distinct IP count
  - Conversion count
  - Cart volume
  - Average daily IPs
  - Average daily conversions
  - (all log1p transformed, configurable weights per signal)
- **Output**: ~6,000 advertisers × ~20,000 keywords → ranked recommendations per advertiser

**Score adjustments** (post-model):
1. **Popularity penalty** — log odds ratio of keyword rarity. Suppresses generic keywords (e.g., "accessories", "web services"). Web services is especially problematic because Google Tag Manager URLs get classified there, associating it with nearly every advertiser.
2. **Advertiser lift** — how popular a keyword is for this specific advertiser relative to the global average for that keyword. Boosts uniquely strong signals.

**Threshold**: Single percentile-based cutoff on model scores (~top 42% of scores), replacing the fixed 200-keyword rule. Advertisers with stronger signals naturally get more keywords; niche advertisers get fewer but more focused ones.

**Parent keyword generation** (user-facing):
1. N recommended DS19 child keywords per advertiser
2. Cluster into ~20 groups based on embedding similarity (k-means)
3. LLM generates parent keyword label + description per cluster, contextualized to the advertiser
4. Parent keywords = what customers see in UI. Child keywords = what goes into audience expression.
5. **Problem**: LLM outputs are non-deterministic — same inputs produce different parent labels each run, making incremental updates hard.

**Cold start problem**: ALS cannot recommend for advertisers/keywords not in training data. Current fallback = vertical averages (generic, poor quality). Planned fix = fall back to MM V2 keywords.

### Continuous Scoring Methodology

Combines Fangorn intent score (s) with BUK keyword evidence score (K) into a single 0-1 score per (advertiser, IP). [Full RFD](https://mntn.atlassian.net/wiki/spaces/TAR/pages/3414917161).

**Step A — Keyword evidence (K):** Sum DCG discount weights of matched keywords: `W = Σ 1/log2(rank+1)`. Saturate: `K = 1 - exp(-β * W)`. β = 1.863 (p90 DCG → 0.9 adjusted).

**Step B — Intent score (s):** Fangorn model, 0-1 per (advertiser, IP).

**Step C — Blend:** Two options:
- Geometric: `F = s^(1-γ) · K^γ` — rewards only when BOTH are strong
- Linear: `F = (1-γ)·s + γ·K` — simpler (Matt's preferred). γ = 0.25 (intent-dominant)
- Missing scores: `COALESCE(fangorn_score, keywords_score, 0)`

**Score → bidder mapping:** F ∈ [0,1] → [0, 10000]: <0.6 = Max Reach, 0.6-0.8 = Mid Intent, 0.8+ = High Intent.

**Proposed rollout** (Alex, 2026-03-31):
1. Fangorn release (backbone) — released end Feb 2026
2. Continuous scoring with Fangorn + existing MM V2 (all keywords equal rank — needed for cold-start)
3. Wire in BUK keyword rankings to continuous scoring framework

**Key PRD details** ([Continuous Scoring PRD](https://mntn.atlassian.net/wiki/spaces/TAR/pages/3398828035)):
- Keywords are **supporting evidence, NOT hard filters** — contribute to score but don't gate inclusion
- When ranked keywords unavailable, fallback to "any match" (yes/no keyword match) — this is the Step 2 equal-rank approach
- **New audiences only** — doesn't retroactively change existing
- **Offline evaluation required before rollout** (FR-12): compare Fangorn-only, keywords-only, current production, and unified scoring
- Timeline: Fangorn released end Feb → 3-4 week pause → continuous scoring experimentation end Q1 → launch Q2
- Dependencies: PER squad (pacing/thresholding), Experimentation squad (A/B design)

**TI-704 — Offline Evaluation with Fangorn Experiment** (Backlog):
- Score treatment and control IPs from the current Fangorn experiment with BUK DCG
- Evaluate against impressions/visits from the experiment
- Also validate two assumptions: (1) diminishing returns to additional keywords, (2) frequency doesn't matter
- This maps directly to PRD FR-12 (offline evaluation required before rollout)

---

## 2. The Problem

BUK has shown promise but lacks a clean performance signal to drive organizational buy-in:

- **Experiment 1** (Sep 2025): Fixed 200 keywords per advertiser blew up audience to 80-88% vertical coverage → control outperformed treatment across all advertisers
- **Experiment 2** (Nov-Dec 2025): Percentile threshold + score adjustments → +27% avg visit rate lift, but results confounded by audience size differences between treatment and control
- **Internal feedback**: Results viewed as inconsistent — per-advertiser performance varied widely due to audience size confounding
- **Beta** (ongoing): 3 live campaigns all outperforming comparables, but anecdotal

The initiative is **Paused** pending a clearer performance story.

---

## 3. Plan of Action

### Prioritized Roadmap (highest to lowest leverage)

#### Priority 1: Design Offline BUK Performance Evaluation
**Why highest leverage**: The #1 blocker to BUK adoption is lack of clean performance signal. Audience size confounding makes online A/B experiments ambiguous. Experiment team pushback on size-controlled experiment: "we want to validate how things will work in production." Need an **offline** approach that sidesteps this.

**Approach** (explore alternatives to online A/B):
- Offline counterfactual: for existing advertisers, compare BUK vs MM V2 keyword overlap with observed visit patterns
- Matched-pair analysis: find advertisers where BUK and MM V2 happen to produce similar audience sizes, compare visit rates retrospectively
- DCG score validation (already done — see Section 4) provides strong evidence that rankings are predictive
- Per-advertiser keyword-level visit rate comparison: for overlapping keywords, compare BUK rank ordering vs actual visit rate ordering

**Draft Jira ticket**: "Design Offline BUK Performance Evaluation Methodology" — 5 SP

#### Priority 2: Continuous Scoring Validation (DCG)
**Why**: Sidesteps the size problem entirely. If we can show that DCG-scored IPs have monotonically increasing visit rates by score bucket, that's a compelling story independent of audience size. This is how BUK and Fangorn converge.

**Approach**:
- Validate Alex's DCG chart with fresh data across multiple advertisers
- Quantify the visit-rate lift curve by score bucket
- Per IP: sum DCG weights of all visited keywords (rank-weighted) → normalized 0-1 score
- Currently all high-intent IPs scored at flat 10,000 — this would replace with granular ordering
- Final score = blend of BUK keyword score + Fangorn IP score

**Draft Jira ticket**: "Validate BUK Continuous Scoring Signal (DCG) Across Advertisers" — 3 SP
**Related**: TI-607 (BUK IP-Level Scoring Methodology, in Development)

#### Priority 3: Cold Start — MM V2 Fallback
**Why**: Required for any production rollout. New advertisers currently get generic vertical averages.

**Approach**:
- In Shopper Graph API: check if BUK recommendations exist → if not, serve MM V2 instead of vertical averages
- Logic change in API response layer

**Draft Jira ticket**: "Implement MM V2 Fallback for BUK Cold-Start Advertisers" — 3 SP
**Related**: TI-557 (Backlog)

#### Priority 4: DAG Keyword Persistence (Idempotent Updates)
**Why**: Required for production. Currently every model retrain overwrites ALL advertiser keywords.

**Approach**:
- Once an advertiser gets BUK keywords, keep them fixed
- Each retrain only generates for net-new advertisers
- Phase 2: periodic refresh (e.g., every 3 months) for seasonal changes
- Challenge: parent keyword LLM non-determinism makes incremental updates hard

**Draft Jira ticket**: "Make BUK DAG Idempotent — Only Update Net-New Advertisers" — 5 SP

#### Priority 5: Parent Keyword Prompting Improvements
**Why**: Beta customer feedback indicates parent keyword descriptions could be more intuitive.

**Approach**: Iterate LLM prompts for parent keyword labels and descriptions. Test with existing beta customers for qualitative feedback.

**Draft Jira ticket**: "Improve BUK Parent Keyword LLM Prompts Based on Beta Feedback" — 2 SP

#### ~~Priority 6: Incorporate DDP Site Visit Signals~~ — COMPLETE
Per Alex (2026-03-31): DDP site visit signals are already incorporated into the BUK model training data. Not discussed in meeting but confirmed afterward.

#### Priority 7: Threshold Logic Refinement
**Why**: Further refinement based on experiment results.

**Approach**: Incorporate vertical size into threshold decisions (larger verticals → more keywords). Use sub-threshold keywords as "next best" expansion recommendations for customers wanting to grow their audience.

**Draft Jira ticket**: "Refine BUK Keyword Threshold Logic by Vertical Size" — 3 SP
**Related**: TI-700 (Backlog)

---

## 4. Investigation & Findings

### Experiment 1 (DPL Part 1, Sep 2025)
- **Advertisers**: iQAir, K9 Ballistics, Merrell, Teachable, No Makeup Makeup
- **Duration**: 1 week (9/13–9/22)
- **Split method**: Old audience isolation blocking (had IP contamination)
- **Treatment**: Fixed 200 BUK keywords per advertiser
- **Result**: Control (MM V2) outperformed across ALL advertisers
  - Aggregate: IVR 1.53% control vs 0.67% treatment, CPV $5.07 vs $9.08
- **Root cause**: 200 keywords pushed vertical coverage to 80-88%, massively diluting audiences
- **Key finding**: Within treatment, IPs overlapping with control keywords performed 5-10x better than non-overlapping IPs → validates need for a threshold, not a fixed count

| Advertiser | Control Coverage | Treatment Coverage | Control Overlap | Vertical Size |
|---|---|---|---|---|
| Teachable | 66.51% | 85.27% | 99.89% | 7,523,998 |
| K9 Ballistics | 11.15% | 86.06% | 99.78% | 27,401,020 |
| iQAir | 37.43% | 52.74% | 61.76% | 5,895,614 |
| Merrell | 29.74% | 80.61% | 99.21% | 19,813,065 |
| No Makeup Makeup | 36.57% | 88.77% | 99.92% | 14,639,475 |

### Experiment 2 (DPL Part 2, Nov-Dec 2025)
- **Advertisers**: Gozney, Hatch, Hello Molly, Lynch Creek Farm, Teachable
- **Duration**: 2 weeks (11/21–12/4)
- **Split method**: True A/B via MD5 hash on IP (deterministic, no bleed)
- **Treatment**: Percentile-based threshold (~top 42%) + popularity penalty + advertiser lift
- **Improvements tested**: (1) Global score threshold replaces fixed 200, (2) Popularity penalty downweights generic keywords, (3) Advertiser lift upweights uniquely strong keywords
- **Result**: +26.84% average relative lift in visit rate, but inconsistent across advertisers

| Advertiser | IVR % Change | Audience Size % Change | CPA % Change | MM Keywords | BUK Keywords | Vertical Size | MM Audience | BUK Audience |
|---|---|---|---|---|---|---|---|---|
| Gozney | +3.35% | -13.60% | -20.91% | 83 | 40 | 9,700,574 | 4,164,456 | 3,609,584 |
| Hatch | -24.83% | +93.45% | +61.09% | 61 | 98 | 17,426,611 | 4,719,126 | 9,520,158 |
| Hello Molly | +137.13% | -83.31% | -67.74% | 96 | 94 | 48,300,712 | 31,665,947 | 4,839,731 |
| Lynch Creek Farm | -9.73% | +32.50% | +16.26% | 61 | 40 | 21,790,539 | 6,909,780 | 9,152,026 |
| Teachable | +28.26% | -32.24% | 0.00% | 167 | 48 | 4,916,250 | 4,308,110 | 2,794,397 |
| **Average** | **+26.84%** | **-0.64%** | **-2.26%** | | | | | |

**Rank-performance correlation**: All 5 advertisers show negative correlation between keyword rank and visit rate (higher rank = better performance). Magnitudes modest (-0.04 to -0.3) but consistent — rankings are directionally aligned with true performance.

| Advertiser | Correlation |
|---|---|
| Gozney | -0.04 |
| Hatch | -0.20 |
| Hello Molly | -0.12 |
| Lynch Creek Farm | -0.30 |
| Teachable | -0.13 |

### K9 Ballistics Worked Example (advertiser_id=32434)
- **Training data**: 28 DS19 keywords observed from pixel data over 30 days
  - Top: Dog Beds (69,545 IPs), Pet Accessories (55,727), Pet Supplies (48,013)
  - Bottom: Pet Safety (5), Pet Health (5)
- **Predictions**: 65 ranked keywords with adjusted scores
  - Top: Pet Accessories (score_adj=1.27), Dog Accessories (1.26), Dog Beds (1.24)
  - Explore (not in training): Pet Apparel (rank 25), Fashion (rank 54), Home And Garden (rank 55)
- **Clustered output**: 20 parent keyword groups (e.g., "K-9 Facility Managers & Equipment Buyers", "K-9 Protective Gear & Apparel Buyers")
- **Shopper Graph API**: Returns both MM V2 (19 LLM keywords like "Durable Dog Toys") and BUK (20 parent groups with child keyword IDs + model version hash)

### Beta Release (ongoing)
- Feature flag enabled, customer conversations completed with 7 advertisers
- Beta customers talked to (per Alex 2026-03-31):
  - **40279 West Bend Insurance** — campaign live with beta audience
  - **45594 Samy's Camera** — campaign live with beta audience
  - **33129 Apollo.io** — talked, status TBD
  - **37336 Global Rescue** — planned to use soon, not yet live
  - **33610 Amsterdam Printing** — talked
  - **48687 Apolla** — talked
  - **35374 Experience Scottsdale** — talked
- Live campaigns outperforming best comparable campaign (anecdotal, per Michelle)
- [AI feedback tracking doc](https://docs.google.com/document/d/1KB2A5kEOb2ms7J47sxdlXOEKmw0N6GshHeuboUaqODQ/edit?tab=t.0)
- [Customer tracking sheet](https://docs.google.com/spreadsheets/d/1QFgjrn3L7u1ciZy2PzrVepS198-Oca826MGP2xh1e1A/edit?gid=0#gid=0)

### Continuous Scoring — DCG Implementation (from Databricks notebooks)

**Step 1: DCG per IP** (`ti_620_dcg_logic.py`)
- Read BUK model predictions (ranked DS19 keywords per advertiser) from GCS: `gs://targeting-infra-vertex-pipelines-prod/bottom-up-keywords/batch-predictions/dt={date}/`
- Read ipdsc DS19 data (30-day window) from GCS: `gs://mntn-data-archive-prod/ipdsc/`
- For each keyword the IP visited that's in the advertiser's recommendation list, compute discount: `1 / log2(rank + 1)`
- Sum discounts per (advertiser, IP) → raw DCG score. Also count `n_hit_cats` (number of matched keywords)
- **Normalize via exponential transform**: `adjusted_keyword_score = 1 - exp(-beta * dcg)`
  - Beta calibrated so 90th percentile of DCG → 0.9 adjusted score
  - Current beta = 1.863 (derived from DCG distribution: p50=0.30, p75=0.63, p90=1.24, p95=1.86, p99=3.99)
- Output written to: `gs://mntn-data-archive-dev/alex.knorr/test_keyword_ip_scoring`

**Step 2: Beta calibration** (`ti_620_beta_eval.py`)
- Solve: `beta = -ln(1 - target_score) / dcg_at_percentile`
- For target=0.9 at p90: `beta = -ln(0.1) / 1.2357 = 1.863`
- Visualizes: DCG→adjusted score mapping curve, DCG distribution, adjusted score distribution, binned density heatmap

**Step 3: Visit rate validation** (`ti_688_ip_score_eval.py`)
- Sample 1,000 advertisers, join scores with visits from Greenplum (`ui_visits`) over 10-day post-period
- Also joins impressions from `cost_impression_log` (30-day window before reference date)
- Bin IPs by adjusted_keyword_score (0.05 bins), compute visit rate per bin
- **Key output**: Plot showing visit propensity vs. adjusted score with 95% CIs — monotonically increasing, confirming score is predictive
- Note: queries Greenplum directly via JDBC (not BQ) for visits and impressions

**Key details from code:**
- Does NOT account for visit frequency (distinct keyword visits only, not count)
- Score range: 0-1 (exponential saturation). Most mass at low scores; ~p50 maps to ~0.43 adjusted
- The validation joins to Greenplum `summarydata.ui_visits` and `logdata.cost_impression_log` — could be replicated in BQ with our tables

### Independent BQ Replication of DCG Validation (2026-03-31)

Replicated Alex's full DCG pipeline in BigQuery using:
- BUK predictions from GCS (`dt=2026-03-16`, 5,699 advertisers, 363K rows)
- ipdsc DS19 (30-day window, 2026-02-15 to 2026-03-15)
- `ui_visits` (10-day post-period, 2026-03-16 to 2026-03-26)
- Sample: 50 advertisers (deterministic hash sample)

**Results — visit rate by adjusted keyword score bin:**

| Score Bin | N IPs | N Visitors | Visit Rate | vs Lowest |
|-----------|-------|------------|------------|-----------|
| 0.20 | 53.9M | 1 | 1.85e-08 | 1x |
| 0.35 | 42.4M | 34 | 8.01e-07 | 43x |
| 0.50 | 60.3M | 67 | 1.11e-06 | 60x |
| 0.65 | 86.2M | 194 | 2.25e-06 | 121x |
| 0.80 | 112.0M | 450 | 4.02e-06 | 217x |
| 0.90 | 133.8M | 1,417 | 1.06e-05 | 571x |
| 0.95 | 236.8M | 52,789 | 2.23e-04 | **12,028x** |

**Interpretation:**
- Signal is monotonically increasing from score 0.65 upward, with massive jump at 0.95
- Top score bin (0.95) has visit rate 12,000x higher than bottom — statistically robust (52,789 visitors in 236.8M IPs)
- Minor dips at 0.45 and 0.60 likely noise from 50-advertiser sample
- Low absolute visit rates (1e-8 to 2e-4) expected because scoring ALL ipdsc IPs, most of whom will never visit any given advertiser
- **Confirms Alex's finding independently**: DCG-based keyword scoring is strongly predictive of visit behavior

**Query:** `queries/ti_797_dcg_scoring_sample.sql`
**Output:** `outputs/ti_797_dcg_visit_rate_by_score.csv`
**Cost:** 118 GB processed, ~6 min wall time

### Scaled-Up Validation: 500 Advertisers (2026-03-31)

Ran the same query with 500 advertisers (10x the original sample). Full-scale (5,699) exceeded BQ resource limits — this is why Alex uses Databricks.

**Result: PERFECTLY MONOTONIC across all 16 bins.** The dips at 0.45 and 0.60 from the 50-advertiser sample were confirmed as sample noise.

| Score Bin | N IPs | N Visitors | Visit Rate | vs Lowest |
|-----------|-------|------------|------------|-----------|
| 0.20 | 541M | 169 | 3.14e-07 | 1x |
| 0.35 | 426M | 360 | 8.46e-07 | 3x |
| 0.50 | 606M | 1,180 | 1.95e-06 | 6x |
| 0.65 | 866M | 3,009 | 3.47e-06 | 11x |
| 0.80 | 1.1B | 8,589 | 7.64e-06 | 24x |
| 0.90 | 1.3B | 24,213 | 1.80e-05 | 57x |
| 0.95 | 2.4B | 575,134 | 2.42e-04 | **771x** |

**Output:** `outputs/ti_797_dcg_visit_rate_500advs.csv`

---

## 5. Solution

Work in progress. See Plan of Action (Section 3) for prioritized roadmap and draft Jira tickets.

---

## 6. Questions Answered

- **Q:** How does the current keyword system (MM V2) work?
  **A:** Scrape homepage → LLM generates 20 parent keywords → expand to 200 child keywords → embed and align to DS19 universe. Entirely LLM-driven, no pixel data used.

- **Q:** What is BUK and how does it differ?
  **A:** BUK uses an ALS collaborative filtering model trained on 30-day pixel data (good_log + conversion_log). It generates ranked DS19 keyword recommendations per advertiser based on actual visitor behavior across all advertisers, not just the advertiser's homepage.

- **Q:** Why did Experiment 1 fail?
  **A:** Fixed 200 keywords per advertiser covered 80-88% of the vertical, massively diluting the audience. IPs overlapping with control keywords performed 5-10x better — validating the approach but showing a threshold is needed.

- **Q:** Why are Experiment 2 results "inconsistent"?
  **A:** Performance is confounded by audience size changes. When BUK shrinks the audience (Hello Molly: -83%), IVR jumps (+137%). When it grows the audience (Hatch: +93%), IVR drops (-25%). Size and performance are inversely correlated in these experiments.

- **Q:** What is continuous scoring?
  **A:** Using DCG-weighted keyword ranks to create a 0-1 score per IP per advertiser, replacing the flat 10,000 score. This + Fangorn IP scores would let us have larger audiences that still perform, because bidding prioritizes the highest-scored IPs.

- **Q:** What is the cold start problem?
  **A:** ALS can only recommend for advertisers/keywords in the training data. New advertisers without pixel data get generic vertical averages today. Planned fix: fall back to MM V2 keywords.

- **Q:** How are recommendations served?
  **A:** Via the Shopper Graph API (`shopper-graph.in.mountain.com/autopilot?advertiser_id={id}`). It returns both MM V2 and BUK keyword payloads per advertiser. Requires Tailscale VPN.

- **Q:** How does the ALS training DAG work?
  **A:** Airflow pipeline in `airflow-ti` repo. Currently overwrites all advertiser keywords on each retrain (not idempotent). Runs on Databricks job compute (1/4 cost of interactive). Local dev via Astronomer (`astro dev start`).

---

## 7. Data Documentation Updates

- Added BUK/targeting section to `knowledge/data_knowledge.md`
- Added BUK experiment lessons to `knowledge/experimentation.md`
- Added BUK terminology and people to `knowledge/mntn_business.md`

---

## 8. Open Items / Follow-ups

### Completed This Session (2026-03-31)
1. ~~**Continuous scoring validation**~~ — **DONE.** Independently reproduced DCG visit-rate curve in BQ. Perfectly monotonic at 500 advertisers. 771x lift at top score bin.
2. ~~**Scale DCG validation**~~ — **DONE.** 500 advertisers confirmed perfectly monotonic. Full-scale (5,699) exceeds BQ resource limits (need Databricks). 500 is sufficient — 575K visitors in top bin.
3. ~~**Review TI-704**~~ — **DONE.** Offline eval using Fangorn experiment data. Maps to PRD FR-12.
4. ~~**Review Continuous Scoring PRD**~~ — **DONE.** Keywords as supporting evidence (not filters), "any match" fallback, new audiences only, Q2 launch target.
5. ~~**All Alex questions answered**~~ — dips (sample noise, confirmed), blending methodology, rollout sequence, beta customers, campaign IDs (pending Michelle), equal-rank logic.

### Next Phase — What We Can Do

**Phase A: Beta Advertiser Performance Analysis** (partially done — campaign IDs still needed from Michelle)

DCG scoring completed for all 7 beta advertisers. Visit rate lift for IPs scored >= 0.9 vs below:

| Advertiser | ID | Scored IPs | Visitors | Score>=0.9 Lift |
|---|---|---|---|---|
| Experience Scottsdale | 35374 | 36.0M | 16,338 | **129x** |
| Global Rescue | 37336 | 37.5M | 5,488 | **73x** |
| Samy's Camera | 45594 | 14.2M | 2,145 | **50x** |
| West Bend Insurance | 40279 | 81.3M | 1,890 | **65x** |
| Amsterdam Printing | 33610 | 37.1M | 713 | **101x** |
| Apollo.io | 33129 | 66.7M | 369 | **1,152x** |
| Apolla | 48687 | 73.9M | 78 | **inf** (0 visits below 0.9) |

**Every beta advertiser shows massive lift.** Signal holds per-advertiser, not just in aggregate.
**Output:** `outputs/ti_797_dcg_beta_advertisers.csv`

Still needed: campaign IDs from Michelle to compare BUK campaign performance vs comparable non-BUK campaigns for West Bend and Samy's Camera

**Phase B: TI-704 — Offline BUK Evaluation via Fangorn Experiment** (actionable now)
- Score treatment and control IPs from the current Fangorn experiment with BUK DCG
- Evaluate whether BUK keyword scores predict visit behavior within the controlled experiment context
- Test the two open assumptions: (1) diminishing returns to keywords, (2) frequency doesn't matter
- Need from Alex: which Fangorn experiment advertisers to use, experiment date range, treatment/control split definition

**Phase C: Code Review of BUK Pipeline** (actionable now)
- Alex requested a second set of eyes on the pipeline code
- Review the Airflow DAG, ALS model training, prediction generation, clustering, LLM parent keyword generation
- Identify opportunities for the idempotency fix (Priority 4) and cold-start fallback (Priority 3)

**Phase D: Incrementality Integration** (future — depends on Kale's plan)
- Kale is pivoting TI team focus toward incrementality prediction (2026-03-31 1x1)
- BUK keywords are NOT dead — Kale sees them as a valid feature in the predictive model
- The interface (exposing keywords as a separate audience mechanism) is the bigger concern, not the underlying signal
- Continuous scoring blending (Fangorn + keywords) aligns with this direction — keywords contribute to unified intent score, not as separate UI mechanism

### Blocked — Waiting On Others
- **Beta campaign IDs** — Alex finding from Michelle's tracking (Phase A)
- **Fangorn experiment details** — need advertiser list, date range, split definition from Alex (Phase B)
- **Kale's incrementality plan** — forthcoming, will inform how BUK fits into the broader strategy (Phase D)
- **Continuous scoring implementation** — depends on Fangorn release + PER squad pacing compatibility (PRD dependencies)

### Questions for Alex — All Answered
1. ~~Visit-rate dips at 0.45/0.60~~ → Confirmed as sample noise. 500-advertiser run is perfectly monotonic.
2. ~~Blending methodology~~ → Geometric (`s^(1-γ)·K^γ`) or linear (`(1-γ)s + γK`), γ=0.25.
3. ~~Path to bidder/pacing~~ → Fangorn first → Fangorn + MM V2 equal-rank → wire in BUK.
4. ~~Beta customer IDs~~ → 7 advertisers, 2 confirmed live.
5. ~~Beta campaign IDs~~ → Michelle tracked. Alex will find.
6. ~~Offline eval approach~~ → TI-704, also see Continuous Scoring PRD FR-12.
7. ~~Equal-rank logic~~ → All keywords discount=1.0, but still differentiate "visited keyword" vs "just vertical." See PRD.

---

## 9. Key Discussion Points from Meeting (2026-03-31)

- **Malachi's Etsy experience**: Removing low-performing products always decreased total ROI. There's an optimal cutoff between include-all and exclude-bad — not a binary decision. Directly applicable to BUK keyword thresholding.
- **Campaign ramp-up**: ~1 month after campaign changes before Beeswax bidding stabilizes. Can't reliably compare experiments in the first month.
- **Cost vs. performance**: Cost difference between intent tiers is negligible (few %) but visit rate difference is 10-50x. Should always bid on highest-value IPs first.
- **Publisher targeting**: ~130-170 publishers per campaign currently; separate initiative to shrink to ~16.
- **Most valuable targeting signals** (Malachi's view): (1) keywords, (2) household income proxies (device type, etc.)
- **Advocacy**: Alex needs more voices pushing BUK priority. Allison and Mike see value but the team needs a clearer performance story. Malachi committed to advocating and finding a way to demonstrate value that aligns with current OKRs.

---

## 10. Jira Ticket History (TI-273 Initiative)

| Key | Summary | Status |
|-----|---------|--------|
| TI-12 | Dynamic Attribute Recommendations | Released |
| TI-211 | Rerun existing ALS Pipeline for baseline evaluation | Done |
| TI-223 | Validate bottoms up approach against production | Done |
| TI-224 | Grid search bottoms up approach | Done |
| TI-273 | Dynamic Attribute Recommendations (DAR) — Initiative | Paused |
| TI-394 | Hook BUK pipeline pieces, CICD, deployment rigor | Done |
| TI-440 | Schedule Runs of ALS Fitting | Done |
| TI-454 | Monitor for #monitor-targeting Slack Channel on BUK | Done |
| TI-458 | Fast Follow: Reoptimize BUK for performance | Released |
| TI-478 | Build necessary tables for BUK storage | Done |
| TI-480 | Shopper Graph API — Add new BUK payload | Done |
| TI-538 | RFD on BUK + Fangorn — MNTN Matched vX | Done |
| TI-555 | Add Ranking to Child Keywords Model Output | Done |
| TI-557 | Change Shopper Graph API BUK Response (parent keywords using child ranks) | Backlog |
| TI-599 | BUK QA Environment Postgres Fix | Done |
| TI-607 | BUK IP-Level Scoring Methodology | Development |
| TI-623 | POC — Move BUK Features to Airflow VS | Done |
| TI-625 | QA BUK Airflow VS Features | Done |
| TI-663 | Further input data cleanup for BUK Model | Done |
| TI-700 | BUK Model Improvements Based on Rank Evaluation | Backlog |
| TI-720 | [SPIKE] BUK-style target for reach/impressions | Done |
| TI-722 | RFD on BUK vs. Fangorn options | Done |

---

## 11. Reference Links & Tools

| Resource | URL / Path |
|----------|------------|
| Shopper Graph API UI | `https://shopper-graph.in.mountain.com/autopilot?advertiser_id={id}` (Tailscale VPN) |
| Feature store code template | `https://github.com/SteelHouse/airflow-ti/blob/main/models/feature_store/feature_group_1_source/guid_log_advertiser_id_dsc_id.py` |
| Local Airflow guide | `https://mntn.atlassian.net/wiki/spaces/DP/pages/3032645677/Guide+Spark+on+Google+Dataproc+with+local+Astronomer` |
| Local Airflow commands | `astro dev start` / `astro dev stop`, uv venv with python 3.11 |
| DCG notebooks (Databricks) | `https://1262887251702944.4.gcp.databricks.com/editor/notebooks/3415949300800788` (Alex shared full folder access) |
| Continuous Scoring RFD | `https://mntn.atlassian.net/wiki/spaces/TAR/pages/3414917161/Fangorn+Keywords+Continuous+Scoring+Methodology` |
| TI-688 Scoring Investigation | `https://mntn.atlassian.net/wiki/spaces/TAR/pages/3503948693/Keyword+Continuous+Scoring+Investigation+TI-688` |
| Beta feedback doc (Google Doc) | `https://docs.google.com/document/d/1KB2A5kEOb2ms7J47sxdlXOEKmw0N6GshHeuboUaqODQ/edit` |
| Beta customer tracker (Google Sheet) | `https://docs.google.com/spreadsheets/d/1QFgjrn3L7u1ciZy2PzrVepS198-Oca826MGP2xh1e1A/edit` |
| Continuous Scoring PRD | `https://mntn.atlassian.net/wiki/spaces/TAR/pages/3398828035/Continuous+Scoring+PRD` |
| BUK predictions GCS | `gs://targeting-infra-vertex-pipelines-prod/bottom-up-keywords/batch-predictions/dt={date}/` |
| DCG scores GCS (dev) | `gs://mntn-data-archive-dev/alex.knorr/test_keyword_ip_scoring` |
| IPDSC GCS archive | `gs://mntn-data-archive-prod/ipdsc/dt={date}/data_source_id={id}/` |

---

## 12. Artifacts

| File | Description |
|------|-------------|
| `artifacts/buk_als_deep_dive.pdf/.pptx` | ALS math walkthrough, step-by-step example, model architecture |
| `artifacts/buk_dpl_part_1.pdf/.pptx` | Experiment 1 DPL: 200 fixed keywords, original results (Sep 2025) |
| `artifacts/buk_dpl_part_2.pdf/.pptx` | Experiment 2 DPL: percentile threshold, IP-level split, +27% visit rate (Nov-Dec 2025) |
| `artifacts/buk_model_comparisons.xlsx` | Per-advertiser model output comparisons (multiple sheets) |
| `artifacts/buk_model_comparisons_k9_ballistics.csv` | K9 Ballistics: training data, predictions, 20 clustered parent keywords |
| `artifacts/buk_shopper_graph_example.json` | Shopper Graph API response for K9 Ballistics (32434) — MM V2 + BUK payloads |
| `artifacts/buk_local_airflow_setup.txt` | Steps for local Airflow/Astronomer dev environment |
| `artifacts/ti_620_dcg_logic.py` | DCG scoring notebook — builds DCG scores per IP/advertiser from BUK predictions + ipdsc DS19 |
| `artifacts/ti_620_beta_eval.py` | Beta calibration notebook — solves optimal exponential scaling factor, visualizes score distributions |
| `artifacts/ti_688_ip_score_eval.py` | Visit rate validation notebook — joins DCG scores with Greenplum visits, plots visit propensity by score bucket |
| `artifacts/buk_continuous_scoring_methodology.pdf` | Fangorn + Keywords Continuous Scoring RFD — blending formula, score-to-bidder mapping, rollout plan |
| `artifacts/buk_ti688_scoring_investigation.pdf` | TI-688 write-up — beta calibration, distribution diagnostics, visit propensity validation results |
| `artifacts/buk_beta_feedback_tracking.docx` | ML model beta feedback tracking from customer conversations (Michelle) |
| `meetings/2026-03-31_100808_malachi_alex_-_project_discussion.txt` | Full meeting transcript |
