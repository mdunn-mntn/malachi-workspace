---
doc_type: ticket
title: "TI-835: Observational Incrementality via Existing 10% Holdout"
status: in_progress
date: 2026-04-17
summary: "Baseline CTV incrementality using the existing per-advertiser 10% holdout (ITT)"
result: "CTV ads: ~0% net-new traffic lift (guid), but 2-8x lift in MNTN-attributed visits"
---

# TI-835: Observational Incrementality Analysis Using Existing 10% Holdout

**Jira:** https://mntn.atlassian.net/browse/TI-835
**Status:** In Progress
**Date Started:** 2026-04-07
**Date Completed:**
**Assignee:** Malachi
**Parent:** [BER-2250](https://mntn.atlassian.net/browse/BER-2250) — Incrementality Overhaul

---

## 1. Introduction

Measure baseline incrementality using the **existing 10% holdout group** on every campaign. This gives us the first empirical signal on whether CTV ads produce incremental lift — before running any shuffling experiment.

## 2. The Problem

We don't know whether our CTV ad targeting generates incremental lift. Before designing an experiment (shuffling), we should first look at the data we already have.

### Key Insight (Matt Brorby, 2026-04-07)
Every campaign already has a **10% holdout group**:
- IPs are hashed; bucket 0-99 out of 0-999 → never receive impressions
- Pure random assignment by IP, per-advertiser
- Should have same intent tier distribution as the targeted 90%
- This IS the counterfactual — no new experiment needed for the initial analysis

### ITT Methodology
Use Intent to Treat: compare ALL IPs in the 90% targeted group (whether or not they actually received an impression) vs the 10% holdout. This avoids selection bias from the fact that only a fraction of the 90% actually get impressions served.

## 3. Plan of Action

1. ~~Get holdout identification query from Nick~~ → Got hash function from Zach
2. ~~Check with Kristen~~ → Pending (need to check #chapter-data-analytics)
3. ~~Pick a set of advertisers with sufficient volume~~ → 10 selected (9 with sufficient data)
4. ~~For each advertiser: pull visit rates for 10% holdout vs 90% targeted~~ → Done for both guid_log and clickpass_log
5. ~~Calculate incremental lift~~ → Done, with statistical testing
6. ~~Break down by intent tier~~ → NOT POSSIBLE (all scored IPs get flat 10000)
7. Document findings and present to Kale/Alex Bloore → In progress

## 4. Investigation & Findings

### HHST Distribution Reality (2026-04-08)
Checked actual `advertiser_household_score` distribution across all impressions (1 day):
- **10000 (RTC/flat):** 69.9% — all Fangorn-scored IPs get flat 10000
- **-1 (unscored):** 28.7% — no Fangorn score
- **3333-6665 (MI range):** 1.4% — only scored range with real variation
- **HI (>=6666):** 0% — NO impressions in this range
- **PP (1-3332):** 0% — NO impressions in this range

**Conclusion:** Per-tier analysis (HI vs MI vs PP) is NOT possible with current data. All scored IPs get flat 10000. Differentiated tiers will only exist after continuous scoring rollout. **Aggregate analysis (holdout vs targeted, all tiers pooled) is the correct and only viable approach.**

### PP (8000) Investigation (2026-04-08)
Peak Performance at HHST=8000 WAS active during January-February 2026 for some advertisers, but volume is minimal in the current 30-day window. Not enough for per-tier analysis.

### Visit Source Table Comparison (2026-04-08)
For WGU (31357), 7-day window:
- **guid_log:** 1,889,820 unique IPs (all pixel-detected visits — broadest)
- **clickpass_log:** 776,315 unique IPs (VV redirects only — subset)
- Used BOTH sources — they answer different questions

### The Two Stories: Aggregate Incrementality Results (2026-04-08)

**10 advertisers analyzed** (Ancient Nutrition, Ferguson Home, Zazzle, Angi, Function Health, Clayton Homes, Northern Tool, REVOLVE, First Watch, HexClad). Function Health excluded from statistical analysis due to negligible volume (2 holdout, 34 targeted visits in guid_log; 0 in clickpass_log). 30-day window.

#### Story 1: guid_log (ALL pixel visits) — No Incremental Lift

| Advertiser | Holdout % | Lift | 95% CI | p-adj | Sig? |
|---|---|---|---|---|---|
| Ancient Nutrition | 9.76% | +2.7% | [1.8%, 3.6%] | <0.001 | Yes |
| Ferguson Home | 9.97% | +0.4% | [0.1%, 0.7%] | 0.050 | Yes |
| Angi | 10.00% | -0.0% | [-0.3%, 0.3%] | 0.939 | No |
| First Watch | 9.98% | +0.2% | [-0.4%, 0.7%] | 0.782 | No |
| HexClad | 10.01% | -0.1% | [-0.7%, 0.5%] | 0.924 | No |
| Clayton Homes | 9.92% | +0.9% | [-0.5%, 2.3%] | 0.361 | No |
| Zazzle | 10.00% | -0.0% | [-0.3%, 0.3%] | 0.966 | No |
| Northern Tool | 9.97% | +0.4% | [-0.1%, 0.8%] | 0.181 | No |
| REVOLVE | 9.98% | +0.3% | [0.1%, 0.5%] | 0.047 | Yes |

**Interpretation:** Holdout share is ~10% across all advertisers = exactly what you'd expect if ads have NO effect on total site traffic. CTV ads do not cause people to visit the site more overall. The 3 "significant" results (Ancient Nutrition, Ferguson Home, REVOLVE) have lift <3% — functionally zero.

#### Story 2: clickpass_log (MNTN-attributed visits) — Massive Incremental Lift

| Advertiser | Holdout % | Lift (x) | 95% CI | p-adj | Sig? |
|---|---|---|---|---|---|
| Angi | 1.31% | 7.4x | [7.2x, 7.6x] | <0.001 | Yes |
| Northern Tool | 1.49% | 6.3x | [5.8x, 6.9x] | <0.001 | Yes |
| First Watch | 2.22% | 3.9x | [3.7x, 4.1x] | <0.001 | Yes |
| Zazzle | 2.61% | 3.1x | [3.1x, 3.2x] | <0.001 | Yes |
| HexClad | 3.16% | 2.4x | [2.3x, 2.5x] | <0.001 | Yes |
| REVOLVE | 3.56% | 2.0x | [2.0x, 2.1x] | <0.001 | Yes |
| Clayton Homes | 3.84% | 1.8x | [1.7x, 1.9x] | <0.001 | Yes |
| Ferguson Home | 3.91% | 1.7x | [1.7x, 1.8x] | <0.001 | Yes |
| Ancient Nutrition | 5.05% | 1.1x | [1.0x, 1.1x] | <0.001 | Yes |

**Interpretation:** Holdout share is 1.3-5.1% across all advertisers — dramatically below the expected 10%. CTV ads cause a 2-8x increase in MNTN-attributed visits. ALL 9 advertisers show highly significant lift (p < 0.001 after FDR correction). This is the incrementality signal MNTN measures and reports to clients.

#### What This Means

The two stories are not contradictory — they answer different questions:

1. **"Do CTV ads increase total site traffic?"** → **No.** guid_log (all pixel visits) shows ~0 lift. The holdout group visits the site at essentially the same rate as the targeted group. CTV ads don't generate net new traffic to advertiser websites.

2. **"Do CTV ads increase MNTN-attributed visits?"** → **Yes, dramatically.** clickpass_log (VV redirects) shows 2-8x lift. The targeted group generates far more MNTN-attributed visits than the holdout. This is the attribution signal — CTV ads trigger the visit-verification redirect flow.

**The key insight:** MNTN's incrementality story is about attribution capture, not traffic generation. Ads cause the same people who would have visited anyway to visit through the MNTN attribution path (VV redirect). The shuffling experiment needs to be designed with this understanding — what we're measuring as "incremental" depends entirely on which visit table we use.

### Holdout Architecture (Nicholas + Zach, 2026-04-07)

**How the holdout works:**
- Holdout is embedded IN the audience segment expression JSON as a where clause
- 1000 buckets — holdout = range 0-99 (10%), targeted = range 100-999 (90%)
- Hash uses a prefix (e.g., ex46) — DIFFERENT from experiment bucket hashing (which hashes on IP directly)
- The two are independent random assignments — holdout is separate from any experiment grouping
- Expression lives in `audience_segment_campaigns.expression` (filter expression_type = 2)
- Literally has "holdout" in the JSON

**BQ Hash Function (ported and validated):**
```sql
CREATE TEMP FUNCTION holdout_bucket(hex_str STRING)
RETURNS INT64
LANGUAGE js AS r"""
  var hex16 = hex_str.substring(0, 16);
  var val = BigInt("0x" + hex16);
  return Number(val % BigInt(1000));
""";

-- Usage:
SELECT holdout_bucket(TO_HEX(MD5(CONCAT(CAST(advertiser_id AS STRING), ':', ip)))) AS bucket
-- bucket 0-99 = holdout, 100-999 = targeted
```
- Uses UNSIGNED mod (matches Rust production service)
- Validated on WGU clickpass_log: 4.59% holdout (expected ~10% under null, deviation = incrementality signal)

**Hash input is `{AID}:{IP}`** — advertiser ID is prefixed, so holdout assignment is per-advertiser per-IP.

**Key tables:**
- `audience_segment_campaigns` — 1:1 with campaign_id, contains expression JSON (type 2 only)
- `audience.audiences` — just a wrapper, don't use directly

**Important:** Only analyze Stage 1 campaigns (funnel_level = 1). S2/S3 are downstream — they target people already hit by S1 ads.

**Expression JSON structure (4 AND clauses):**
1. selects — category selections
2. categories — DS19 keywords, data source filters, CRM blocks, visitor/converter lookbacks
3. geos — geography (usually US)
4. holdout/buckets — bucket range for holdout or experiment groups

**Experiment vs holdout hashing:**
- Incrementality holdout: hashes on prefix (ex46)
- Experiment groups: hashes on IP address
- These are independent — an IP in the 10% incrementality holdout can still be in any experiment bucket

## 5. Solution

### Methodology
- **Holdout identification:** MD5 bucket hash function ported from GP to BQ (unsigned JS UDF)
- **Visit sources:** guid_log (all pixel visits) and clickpass_log (VV-attributed visits)
- **Sample:** 9 advertisers with sufficient volume (30-day window, March-April 2026)
- **Statistical testing:** Binomial test (H0: targeted proportion = 0.9), bootstrap 95% CIs for lift, Benjamini-Hochberg FDR correction
- **Metric:** Unique visitor counts (holdout vs targeted), lift = (observed ratio / expected 9:1 ratio) - 1

### Key Finding
CTV ads don't increase total site traffic (guid_log: ~0% lift), but they dramatically increase MNTN-attributed visits (clickpass_log: 2-8x lift across all 9 advertisers). The incrementality signal is in the attribution path, not in net new traffic.

### Implication for Shuffling Experiment (TI-837)
The shuffling experiment must clearly define its success metric:
- If measuring total site visits → expect ~0 difference between shuffled and non-shuffled
- If measuring attributed visits → expect large differences driven by the VV redirect mechanism
- The experiment design should account for the fact that "incremental" has two very different meanings depending on the measurement table

### Strategic Context (Kale + Matt Brorby, 2026-04-08)

**Kale's direction:** Incrementality is the #1 priority. MNTN almost certainly looks bad on external platforms (LiftLab, Kochava) because everything is optimized for visits. Incremental ROAS is the top metric. Shutter internal dashboards, move to third-party vendors. OKR: run 5 experiments with external vendors.

**Matt's confirmation:** "Everyone suspects intent scoring is just capturing people who would visit anyway." This aligns with our guid_log ~0% lift finding. The baseline measurement is the critical first step.

**Matt's incremental ROAS experience (prior role, mobile):**
- Time-delta bucketing: bucket users by time from ad impression to conversion. Short windows (5s) ≈ 100% incremental. Beyond ~6.5 hours, signal barely noticeable.
- Industry benchmarks: Good advertisers ~$0.90 incremental/dollar. Poor ~$0.50. Trade Desk ~$1.15 (considered good). Claims of $8 ROAS are attributed, not incremental.
- Over $1.00 incremental ROAS is rare and "awesome."

**CTV-specific challenges (Matt):**
- Not deterministic — IP-based, not device-based like mobile
- Long conversion windows (weeks, not seconds/hours)
- Much harder to separate signal from noise at longer time intervals
- Should filter out cellular IPs (T-Mobile, etc.) via identity graph
- Time-delta bucketing may work differently for CTV — needs investigation

**LiftLab context (Matt):**
- LiftLab is paid by the advertiser → bias toward conservative measurement
- Their reports will be as conservative as possible
- MNTN is "at the mercy of these third parties" — we won't internalize incrementality measurement

**Ensemble approach (Matt + Kale):**
- No single model. IVR model for performance-focused advertisers, incremental ROAS model for incrementality-focused ones.
- Only applies to advertisers who opt into incrementality — won't tank performance metrics company-wide.
- Kale confirmed: "We'd only do this for advertisers who opt in. What we lose in performance, we gain in churn retention."

## 6. Questions Answered

- **Q:** What is the incremental visit rate lift from CTV ads?
  **A:** Depends on what you measure. guid_log (all visits): ~0% lift. clickpass_log (attributed visits): 1.1-7.4x lift (median ~2.4x).

- **Q:** Is mid-intent actually more incremental than high-intent?
  **A:** Cannot be answered with current data. All scored IPs get flat HHST=10000. Per-tier analysis requires continuous scoring rollout.

- **Q:** How much of the 90% targeted group actually receives impressions (ITT dilution)?
  **A:** Not yet measured. The clickpass_log analysis implicitly captures this — only IPs that went through VV redirect appear.

- **Q:** Do CTV ads generate net new site traffic?
  **A:** No. guid_log shows holdout share ~10% = no lift. The holdout visits at the same rate as targeted.

- **Q:** Do CTV ads increase MNTN-attributed visits?
  **A:** Yes, dramatically. clickpass_log shows holdout share 1.3-5.1% = 2-8x lift. All 9 advertisers significant (p < 0.001).

## 7. Data Documentation Updates

- **HHST distribution reality:** Added to data_knowledge.md — all scored IPs get flat 10000, no HI/PP/MI differentiation in production
- **PP (8000) status:** Was active Jan-Feb 2026, now minimal. Added to data_knowledge.md
- **Holdout hash function:** Ported to BQ, documented in data_knowledge.md
- **Audience expression structure:** Documented in data_knowledge.md
- **guid_log vs clickpass_log interpretation:** Added to experimentation.md

### Alex Knorr Pre-Analysis (Databricks, April 2026)

Alex Knorr ran a parallel pre-analysis using Databricks with more granular intent tier breakdowns.

**Repo:** SteelHouse/databricks_targeting, branch `TI-835`, path `notebooks/Incrementality_Pre_Analysis/`
**External table:** `dw-main-bronze.external.TI_835_prospecting_scores` (GCS: `gs://mntn-data-archive-dev/alex.knorr/TI_835_prospecting_scores/*.parquet`)
**Report:** `reports/TI_835_Pre_Analysis_v4.html`

**Key findings:**
- 10 advertisers across 8 verticals analyzed, 25-day post-period (Mar 21 – Apr 14)
- Coverage rates were **worse** than meeting-note estimates:
  - High intent: **3.4% median** (not 14% as initially discussed)
  - Peak: **0.2%** median
  - Mid intent: **0.04%** median
- LATE (Wald estimator) only credible above ~4-5% coverage — only high-intent barely crosses this threshold
- Intent tier thresholds used: High=10000 (vertical+keyword), Peak=7000-9999 (vertical only), Mid=3333-6999 (keyword only), Max Reach <3333
- Scoring source: `gs://household-scoring-prod/output/scoring/prospecting_intent/` (daily, 35-day retention)
- These results reinforce the pivot to ghost bidding (ATT) — ITT is structurally unable to detect incrementality at these coverage levels

## 8. Open Items / Follow-ups

- [x] Get holdout query from Nick → Zach provided hash function
- [ ] Check Kristen's work in #chapter-data-analytics (before presenting)
- [x] Identify good advertisers for the analysis → 10 selected, 9 with sufficient data
- [ ] Discuss with Kale: what does the two-stories finding mean for the incrementality OKR?
- [ ] Present findings to Kale/Alex Bloore
- [ ] Inform shuffling experiment design (TI-837) with these findings
- [ ] Per-tier analysis: blocked until continuous HHST scoring is deployed

## 9. Files

| File | Description |
|------|-------------|
| `queries/ti_835_holdout_hash_bq.sql` | BQ holdout bucket hash function (unsigned JS UDF) |
| `queries/ti_835_aggregate_incrementality.sql` | Main analysis query (guid_log version) |
| `outputs/ti_835_guid_log_results.csv` | guid_log holdout vs targeted by advertiser |
| `outputs/ti_835_clickpass_log_results.csv` | clickpass_log holdout vs targeted by advertiser |
| `outputs/ti_835_significance_results.csv` | Statistical test results (both sources) |
| `artifacts/ti_835_significance_testing.py` | Statistical testing script (binomial, bootstrap CI, FDR) |
| `artifacts/generate_charts.py` | Tufte-style chart generation |
| `artifacts/ti_835_chart_dual_story.png` | The money chart: guid_log ~10% vs clickpass <<10% |
| `artifacts/ti_835_chart_lift_by_advertiser.png` | Clickpass lift by advertiser with 95% CI |
| `artifacts/ti_835_chart_holdout_scatter.png` | Scatter: observed vs expected holdout share |

## Key People

| Person | Role |
|--------|------|
| **Matt Brorby** | Staff DS — outlined approach, wrote the lift-model doc, thinking about performance vs incrementality trade-off |
| **Alex Bloore** | Engineering lead on incrementality — clarified three workstreams under BER-2250. Shuffling experiment is priority. |
| **Nicholas** | Experimentation team — explained holdout architecture, expression JSON structure |
| **Zach** | Engineering — confirmed holdout hash function (MD5, unsigned mod 1000) |
| **Kristen** | Data analytics — may already be doing related incrementality intent analysis |
| **Kale** | Director — originated the idea, needs to see findings and decide on OKR framing |
