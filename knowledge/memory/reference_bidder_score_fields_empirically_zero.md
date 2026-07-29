---
name: reference-bidder-score-fields-empirically-zero
description: "The 0-10000 MM/Fangorn scores DON'T appear in silver.logdata.bidder_bid_events. All three score fields (household_score, advertiser_household_score, conquest_score) are 0 in 99.7% of rows and negative in 0.26%. HHST = 0 in 100% of rows. Real scores live upstream."
metadata: 
  node_type: memory
  type: reference
  originSessionId: 2a20d28f-2a8c-4757-a5e4-36e63bd41f18
doc_type: memory
keywords: [bidder_bid_events, household_score, advertiser_household_score, conquest_score, HHST, score fields zero, Fangorn score, MM score, prospecting scores monitor]
domain: [bidding, audience-scoring, bigquery]
lifecycle: active
last_verified: 2026-06-01
---
**Empirical finding (2026-06-01, ~33B-row scan of `silver.logdata.bidder_bid_events` for 2026-05-28):**

The graduated 0-10000 MM/Fangorn scores that the prospecting scoring monitor reports are **NOT stored in `bidder_bid_events`**. All three score columns have identical distributions on the same day, none of them ever positive:

| Field | = 0 | Negative | Positive |
|---|---|---|---|
| `household_score` | 99.738% | 0.262% | **0 rows** |
| `advertiser_household_score` | 99.738% | 0.262% | **0 rows** |
| `conquest_score` | 99.738% | 0.262% | **0 rows** |
| `household_score_threshold` (HHST) | 100% | 0% | 0 rows |

**Implications:**

1. **`bidder_bid_events.household_score` is NOT the MM score** despite the name. Either it's a downstream-filtered view that drops the raw value, or these fields are placeholders the bidder zeros after using the score upstream.
2. **`bidder_bid_events.household_score_threshold` is NOT the configured HHST.** The configured value (per campaign/advertiser) lives elsewhere; by the time we see it here, it's always 0. So HHST-distribution analysis CANNOT be done off bidder_bid_events.
3. **The "what does no score look like in logs?" question** (Ryan Kleck didn't know, 2026-06-01): in `bidder_bid_events` the answer is `household_score = 0` — NOT NULL. The 0.262% negative values are likely a `-1` sentinel for explicitly unscored IPs (per Malachi's expectation, unconfirmed).
4. **Nothing in TI-999 prior work depends on these fields.** The Pass 21 / Pass 26 / Pass 32 bucket math uses audience expressions (`audience.audience_segments`) and impression-level KPI tables (`summarydata.sum_by_campaign_by_day`), not bidder_bid_events scores. The GCS prospecting scores monitor and IPDSC scoring pipeline are the correct sources for 0-10000 distributions.

**Open question (worth a separate probe):** which BQ table actually carries the 0-10000 scores at bid time? Candidates ranked by likelihood:
- `silver.augmentor_log` / `silver.logdata.augmentor_log` (10-day TTL, partition filter required)
- `silver.logdata.bid_logs` (90-day TTL)
- `silver.logdata.cost_impression_log.model_params` (the `realtime_conquest_score=10000` pattern lives here per existing memory)
- Per-IP scoring snapshots in `bronze.household_scoring.prospecting_intent_daily` (canonical IPDSC output)

**Methodological caveat for future analyses:** if you find yourself querying `bidder_bid_events.household_score` for a distribution or threshold analysis, stop. Use `cost_impression_log.model_params` for IP-level RTC/MM score traces, or query the GCS prospecting score export for population-level distributions.

**See also:** [[reference_bidder_scoring_reality]] (the three-field overview — needs correction note that field names don't mean what they suggest in `bidder_bid_events`); [[reference_rtc_hhst_gating]] (HHST mechanic — but check that this isn't read off bidder_bid_events.HHST); [[reference_prospecting_scores_gcs_monitor]] (canonical 0-10000 distribution source); `knowledge/data_knowledge.md` § "Bidder-side score logging — empirical finding" for the durable write-up.
