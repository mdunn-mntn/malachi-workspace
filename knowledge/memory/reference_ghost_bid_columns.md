---
name: ghost-bid-field-locations-raw-gcs-bq-silver
description: "BQ ghost-bid query — bidder_bid_events.threshold_failure_reasons='ghost-bid' (dashed, NOT camelCase). 753K rows/day. Deployed 2026-05-27, no backfill, no ghost-win logging."
metadata: 
  node_type: memory
  type: reference
  originSessionId: 41b39540-28b4-4a8c-9009-852ce5566580
doc_type: memory
keywords: [ghost_bid_columns, ghost, bid, columns, query, bidder_bid_events.threshold_failure_reasons, dashed, camelcase]
domain: [reference]
lifecycle: active
last_verified: 2026-06-02
---
**Canonical BQ query (verified 2026-06-01):**

```sql
SELECT DATE(time) AS dt, COUNT(*) AS ghost_bids
FROM dw-main-silver.logdata.bidder_bid_events
WHERE DATE(time) >= '2026-05-27'
  AND threshold_failure_reasons = 'ghost-bid'
GROUP BY 1 ORDER BY 1
```

**Critical gotcha:** the value is `'ghost-bid'` (dashed), NOT `'ghostBid'` (camelCase). Ryan's DM listed both forms — `threshold_failure_reasons='ghostBid'` (Beeswax raw avro) and `bid_dropped_reason='ghost-bid'` (MNTN raw parquet) — but in BQ silver, the `bidder_bid_events.threshold_failure_reasons` column carries the MNTN-bidder dashed form. Filtering `LIKE '%ghostBid%'` returns zero. Always use exact `= 'ghost-bid'`.

**Observed volume (2026-05-30):** 752,981 ghost-bid rows in `bidder_bid_events`. For context, top non-null reasons same day: `dailyTermImpressionRateLimited` (971M), `metadata:ctv-blocked-by-block-list` (711M). Ghost-bid is a small minority of dropped bids; Ryan estimated ~10% of *successful* bid count (much smaller than total drops).

**Coverage caveat (confirmed 2026-06-02):** `bidder_bid_events` is **MNTN-bidder ONLY** (Rust `rtb-campaign-service`) — only 22 distinct advertisers in the whole day vs ~300-400 live. Beeswax-bidder ghost bids (camelCase `'ghostBid'` per Confluence page) land in a different BQ table or aren't ingested to silver yet — that BQ surface is the open question for Ryan. Cohort scope is capped at the MNTN-bidder advertiser set.

**Raw GCS (per Ryan Kleck DM, 2026-06-01) — for reference, BQ is preferred:**
- Beeswax bidder: avro at `gs://bidder-price-events-prod-east/topics/rtb-bid-price-events/date=YYYY-MM-DD/` — `threshold_failure_reasons = 'ghostBid'`
- MNTN bidder: parquet at `gs://bidder-bid-events-prod-east/v2/YYYY-MM-DD/HH` — `bid_dropped_reason LIKE '%ghost%'`

**`bidder_auction_events.auction_dropped_reason` is NOT the ghost-bid surface.** It carries auction-level drops (`global-allow-list-rejection`, `no-candidates-after-pacing-engine`, etc.). Zero ghost-matching values 2026-05-25 → 2026-05-31. Don't use it for ghost-bid analysis.

**Hard facts:**
- Deployed 2026-05-27. **No backfill** — data only from that date forward.
- **Ghost WINS are NOT logged.** Only ghost bids. Estimate wins via per-campaign or per-advertiser win-rate × ghost-bid count.
- Open work: [Ghost Win Simulation Discussion](https://mntn.atlassian.net/wiki/spaces/DATA/pages/3608150103/Ghost+Win+Simulation+Discussion) — Scylla push + simulation service debate.

**Why this matters:** Ghost-bid logging is the deployed mechanism that unblocks downstream BER-2250 / TI-886 incrementality work (see [[project_bidder_level_ghost_bidding_approved]]). Treated/holdout cohorts can now be derived from ghost-bid records continuously, with no augmentor-TTL constraint and no random-subsampling proxy.

**How to apply:** Lead with the BQ surface `bidder_bid_events.threshold_failure_reasons = 'ghost-bid'`. For analyses needing IP-level granularity older than the deploy date, fall back to the augmentor-log post-hoc approach (TI-837 v5).
