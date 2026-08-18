---
name: reference-prospecting-scores-gcs-monitor
description: Daily email monitor of MNTN Prospecting Scores distribution from GCS. Source of truth for Fangorn-on vs Non-Fangorn score landscape across funnel levels.
metadata: 
  node_type: memory
  type: reference
  originSessionId: 2a20d28f-2a8c-4757-a5e4-36e63bd41f18
doc_type: memory
keywords: [prospecting scores, gcs monitor, fangorn, household_score, funnel level, hhst band mapping, high intent, max reach, score distribution, targeting-infrastructure email]
domain: [audience-scoring, infra]
lifecycle: active
last_verified: 2026-08-18
---
**A daily distribution monitor emails the latest MNTN Prospecting Scores landscape** to `targeting-infrastructure@mountain.com` and `machine-learning-squad@mountain.com`.

- **Subject:** `MNTN Prospecting Scores Distribution GCS - YYYY-MM-DD (PROD)`
- **Source:** `gs://household-scoring-prod/output/scoring/prospecting_intent/`
- **Storage:** parquet stores per-(score, Fangorn flag, funnel_level 1-4) row totals + distinct advertiser/campaign/campaign-group counts (counts don't sum across rows).
- **Sample copy** saved in workspace: `tickets/ti_999_interest_segment_sizing/artifacts/ti_999_prospecting_scores_distribution_gcs_2026_05_31_PROD.pdf`

**What it tells you (locked findings, 2026-05-31 PROD snapshot):**

1. **Fangorn-on vs Non-Fangorn produce structurally different score distributions:**
   - Fangorn-on: 10,000 distinct scores at S1 (graduated full range 1-10000).
   - Non-Fangorn: 6,667 distinct scores at S1 (discrete buckets — point masses at 8000 and 10000, graduated within Max Reach 1-3332 and Mid 3333-6665).
2. **S3 funnel level flattens to a single score = 10,000** for BOTH Fangorn and Non-Fangorn. No graduated scoring at S3.
   **⚠ This flat 10,000 sits INSIDE the HI band (8001-10000), so banding `household_score` off `prospecting_intent` without a `funnel_level` filter counts every S3 campaign's whole scored IP set as High Intent — 3.8x inflation measured 2026-08-17 (AUDI-1208). Always scope to `funnel_level = 1` or `IN (1,2)`. See [[reference_prospecting_intent_query_rules]].**
3. **S4 has zero scores** in both cohorts — S4 doesn't use `household_score` at all.
4. **Fangorn rollout = ~22% of S1 advertisers** (292 of 1,352 total, 2026-05-31 snapshot).
5. **Intent tier ranges (from the monitor):**
   - High Intent = 8001-10000
   - Peak = 6666-7900 (with 8000 as a special discrete value)
   - Mid = 3333-6665
   - Max Reach = 1-3332

**Fangorn → HHST band mapping (Ryan Kleck, 2026-06-01):**
- Fangorn outputs continuous 0-1 raw scores. The scoring job applies tier mapping.
- Fangorn raw ≤ 0.8 → Max Reach / Mid bands (1-6665).
- Fangorn raw > 0.8 → "high intent" which gets split into PP (6666-7900) and HI (8001-10000) via linear mapping.
- Fangorn rarely produces raw scores above ~0.95 — this produces the downward slope on the two high-intent spikes in the Fangorn-on distribution histogram. The spikes ARE the natural shape of Fangorn raw scores.

**How to use it:**
- TI-999 / 3P-performance work: segment by Fangorn status before comparing KPIs across audience buckets — the score landscape differs structurally.
- Fangorn rollout monitoring: track the count of Fangorn-on advertisers and the FL1 distinct-score count (10,000 = full continuous; lower = degraded).
- Spot-check pipeline health: any sudden drop in score volume or distinct score count indicates a pipeline outage worth investigating.

**See also:** [[reference_prospecting_intent_query_rules]] (the funnel-scope + stale-external-table traps), [[reference_vertical_hi_sizing_baseline]], [[reference_rtc_hhst_gating]] (RTC scoring waterfall), `knowledge/data_knowledge.md` § "Daily Prospecting Scores Distribution Monitor (GCS, PROD)" for the canonical write-up.
