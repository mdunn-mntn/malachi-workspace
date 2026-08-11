---
name: reference_bidder_serving_stores
description: "Bidder serving-store architecture — Aerospike household profile (key=IP), scores in GCS not MemDB, ScyllaDB raw-wins pipeline, id-service Bigtable ID→household resolution rule, ownership/contacts"
metadata: 
  node_type: memory
  type: reference
  originSessionId: eee87269-027a-4bd9-9df4-5666d4c3fde9
doc_type: memory
keywords: [bidder serving stores, Aerospike, household-profile, GCS scores, ScyllaDB, Redis, rtb namespace, holdout_cids, MembershipDB, Eric membership consumer, id-service, Bigtable household resolution, raw_score inversion, confidence tiebreak, bidder parity]
domain: [bidding, infra]
lifecycle: active
last_verified: 2026-08-11
---
Bidder team system design (Abbas + Ryan walkthrough, 2026-06-09, TI-1016). Full detail in `knowledge/data_knowledge.md` § "Bidder System Design & Caching Architecture" and `tickets/ti_1016_memdb_bidder_cache_optimization/`.

**Serving stores:**
- **Aerospike = household profile cache.** PRIMARY KEY = IP address. ~300M IP keys, 3–5 TB, single-digit-ms, hit DIRECTLY on every bid (no in-bidder in-memory tier). Only IPs actually in use are stored; IP with empty segment list is deleted.
- **Aerospike `rtb` namespace sets (query via `aql`, creds in "camperbid" 1Password, needs VPN):** `rtb.household-profile` (PK=IP: bins `segments[]`, `holdout_cids[]` = per-IP array of campaign_ids held out, `geo_version`, `hhs:campaign` map cid→score, `hhs:advertiser` map aid→score, `timestamp`/`hhs:timestamp` in µs) — **household score is per-campaign + per-advertiser maps, not one value**; `rtb.spend` (PK=`flight_id=…[:YYYYMMDD]`, spend+count maps at CG/campaign/term grain, spend≈micro-$); `rtb.price` (`avg_cpi` by `W:H:duration`); `rtb.recency` (per-IP vast+page_view). Schema in knowledge/data_catalog.md.
- **Bid price + eligibility tables (DW):** price from `summarydata.publisher_adsize_metrics` (avg_cpi = 3-day avg win price by publisher×adsize) ×`pace_multiplier` (`sync.creative_metadata`). Thresholds in `sync.creative_metadata` (null/zero=skip); threshold tables `dso.household_score_thresholds` (=HHST), `dso.recency_score_thresholds`, `dso.viewability_score_thresholds`, `dso.cpm_thresholds`, `dso.publisher_performance_thresholds`+`dso.network_performance_threshold`. Viewability vs `logdata.publisher_adsize_metrics`.
- **Canonical source:** Confluence BP "Bidder" page (PDF at documentation/docs/bidder_platform_confluence_reference.pdf) + "Aerospike Datastore" page. Pull via Confluence API — see [[reference_confluence_api_access]]. RTB lifecycle: auction→bid→win→imp; win rate=wins/bids, use rate=imps/wins. Logs: auction logs (fka augmentor)→bidder_auction_events; bid logs (fka BPL)→bidder_bid_events.
- **Scores' system-of-record is GCS, NOT MembershipDB.** Scoring team → GCS bucket → (GCS event → PubSub → RabbitMQ) → membership consumer → Aerospike. MembershipDB emits segments + owns holdout logic. This is why bid-side BQ tables (bidder_bid_events) carry no segments/scores — see [[reference_bidder_score_fields_empirically_zero]].
- **ScyllaDB = raw wins (spend pipeline).** Notification service (HTTP webhook) → ScyllaDB (dedup) → Kafka → 3 aggregators {frequency, spend, logs→GCS}, ~1 min end-to-end. Logs aggregator's GCS output is the upstream of BQ spend/win tables.
- **Redis = slow-changing static data** (flight budgets/thresholds/weights), pulled on a 5–10 min cron (not real-time → stopped flights can spend ~10 min more).
- **Migration:** Aerospike → ScyllaDB + Redis (Aerospike expensive / poor support). Treat Aerospike = current, Scylla = future.

**id-service = ID→household resolution at auction time (Bigtable, `SteelHouse/id-service/src/bigtable.rs`,
source-verified 2026-08-11).** Distinct from the Aerospike profile cache above. Rowkey =
`id|id_type|version|experiment|confidence_score|household_id`, zero-padded confidence, read in lexicographic
order so **the first row is the best match**. Bigtable stores a **lower-is-better `raw_score`** (a sort
encoding inherited from the graph); `resolve_household_id` picks the lowest raw score across all
caller-supplied identities (strict `<`, so **first-wins on ties in the caller's identity order**) and inverts
on the way out (`confidence_score = 10_000 − raw_score`) so callers see higher-is-better 0–10000. **The graph's
own parquet `ConfidenceScore` is a probability (higher better)**, so `max(probability)` ≡ `min(raw_score)` —
same winner. **Two things the bidder does NOT do: no confidence floor, and it never reads `is_shared`** (a
confidence-0 match still resolves). **Equal-confidence tiebreak = LOWEST `household_id`** (next rowkey
segment) — airflow-ti `utils_model/household_resolution.py` takes the highest, the one real parity gap for the
Fangorn-on-MNTN-ID feature store. Per-identity timeout, partial results returned on partial timeout.
See [[project_fangorn_on_mntn_id]].

**Latency budgets:** Mountain Bidder ~200 ms; Beeswax 15 ms timeout (Beeswax = proxy/middleman in front of the exchanges). SSPs: Magnite, Index Exchange, Freewheel, Pubmatic.

**Ownership/routing:** Abbas moved to performance-pacing team. **Eric** now owns the membership consumer + next-gen rework (dedup cache, split recency/membership consumers, read GCS directly). Secondary: **Alkaif**. PCS + Campaign Metadata Service = perf-pacing team (write static pacing data). Holdout logic = MembershipDB (load-bearing for ghost bidding / BER-2250).

Related: [[reference_bidder_scoring_reality]], [[reference_rtc_hhst_gating]], [[reference_fangorn_audience_overlay]].
