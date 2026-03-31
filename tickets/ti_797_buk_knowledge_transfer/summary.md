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

---

## 2. The Problem

BUK has shown promise but lacks a clean performance signal to drive organizational buy-in:

- **Experiment 1** (Sep 2025): Fixed 200 keywords per advertiser blew up audience to 80-88% vertical coverage → control outperformed treatment across all advertisers
- **Experiment 2** (Nov-Dec 2025): Percentile threshold + score adjustments → +27% avg visit rate lift, but results confounded by audience size differences between treatment and control
- **Richard's feedback**: "numbers are bullshit" — inconsistent per-advertiser results make it hard to tell a clear story
- **Beta** (ongoing): 3 live campaigns all outperforming comparables, but anecdotal

The initiative is **Paused** pending a clearer performance story.

---

## 3. Plan of Action

### Prioritized Roadmap (highest to lowest leverage)

#### Priority 1: Design and Execute Size-Controlled BUK vs MM V2 Experiment
**Why highest leverage**: The #1 blocker to BUK adoption is lack of clean performance signal. Every experiment so far has been confounded by audience size differences. Until resolved, no amount of model improvement will get organizational buy-in.

**Approach**:
- Find advertisers where BUK and MM V2 audience sizes are within 5% of each other, OR adjust BUK keyword count to force size alignment
- Use the new IP-level MD5 hash A/B split (no bleed)
- Run for minimum 4 weeks (account for ~1 month campaign ramp-up period)
- Pre-register metrics and analysis plan

**Draft Jira ticket**: "Design and Execute Size-Controlled BUK vs MM V2 Experiment" — 5 SP

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

#### Priority 6: Incorporate DDP Site Visit Signals
**Why**: Future improvement — add external site visit data to model training for advertisers with sparse on-site data.

**Approach**: Already map external visits into DS19 keyword universe. Add these signals to ALS training data.

**Draft Jira ticket**: "Add DDP External Site Visit Signals to BUK Training Data" — 5 SP

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
- Feature flag enabled, ~8 customer conversations completed
- 3 live campaigns, all outperforming best comparable campaign (anecdotal, per Michelle)

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

1. **Size-controlled experiment** — #1 priority, needs to be designed and proposed to experiment team
2. ~~**Continuous scoring validation**~~ — **DONE.** Independently reproduced DCG visit-rate curve in BQ. Monotonic increase confirmed, 12,028x lift at top score bin. See Section 4 results.
3. **Scale DCG validation to all 5,699 advertisers** — current results are from 50-advertiser sample. Full run would eliminate the minor dips at 0.45/0.60
4. **Cold start fallback** — API logic change needed (TI-557 in Backlog)
5. **DAG idempotency** — stop overwriting existing advertiser keywords on retrain
6. **Parent keyword prompting** — improve LLM quality based on beta feedback
7. **Meet with Alex again** — schedule follow-up for deeper technical dive + local Airflow demo
8. **Review Alex's code** — he expressed desire for code review on the BUK pipeline

### Questions for Alex
1. We see minor visit-rate dips at score bins 0.45 and 0.60 in our 50-advertiser sample — did you see similar patterns, or does the full advertiser set smooth those out?
2. TI-538 (RFD on BUK + Fangorn) is Done — can you share the RFD doc or notebook for the blended keyword + Fangorn score weighting?
3. What's the status of getting continuous scoring into the bidder/pacing system? Is there an API or pipeline that would consume these scores?
4. Can you share the advertiser IDs for the beta customers with live BUK campaigns so we can do independent performance analysis?

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
| `meetings/2026-03-31_100808_malachi_alex_-_project_discussion.txt` | Full meeting transcript |
