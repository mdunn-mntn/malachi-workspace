---
doc_type: ticket
title: "TI-1016: MemDB → Bidder cache — bidder-inert IP storage"
status: in_progress
date: 2026-06-09
summary: "Investigate whether MemDB→Bidder caches bidder-inert 3P-OR-include IPs wastefully"
result: "Cache-shrink idea killed (key=IP); new lever: skip score writes for segmentless IPs"
keywords: [ti-1016, memdb, membership consumer, aerospike, bidder cache, 3p-or-include, mountain match, intent score, segments, hhst, max reach, zach schoenberger, abbas, ryan, eric]
---

## TL;DR

**Q:** TL;DR card for TI-1016 (MemDB to Bidder cache optimization), plus delta facts not already in the knowledge docs.

**A:** TI-1016 investigated whether the MemDB to Bidder pipeline wastefully caches bidder-inert 3P-OR-include IPs (MM adds LiveRamp interest segments via OR-include, inflating a ~1M-scored-IP audience to 20-30M cached IPs without changing the bid-eligible pool). Result: the original cache-shrink idea is largely KILLED by the Aerospike key structure (primary key = IP address); a sharper optimization replaced it. Because 3P segments are just entries in an IP's segments field, removing a segment does not drop the key, and the bidder only stores IPs actually in use (not the full LiveRamp universe), so the 1M-vs-20-30M framing does not translate into fewer keys; net footprint saving is small (confirms Zach's "segments are smaller than you think"). The membership consumer already deletes any IP whose segment list goes empty. The actionable replacement: don't write intent scores for IPs that have no active segments -- the membership-consumer intent-score write path does not currently check whether an IP has any segments before writing the score, and Abbas + Ryan both agreed it "probably should" (implemented as a conditional write; Max Reach is preserved because it only suppresses IPs with no segments at all, not unscored-but-segmented IPs). Three inputs gate a bid: (1) score (GCS to consumer), (2) segments (MembershipDB), (3) HHST threshold; the clean safe cut is no-segments then don't write intent score. Status: in_progress. Next: empirically size how many scoreless-no-segment IPs are being written, and confirm conditional-write feasibility with Eric (now owns the membership consumer; Abbas moved to perf-pacing, Alkaif secondary), and align with the Victor + Abbas "what we can filter out" follow-up. The full bidder system-design + caching architecture was captured from the Abbas/Ryan 2026-06-09 walkthrough into data_knowledge.md and data_catalog.md per section 7.

**How:** Read summary.md in full (front-matter + sections 1-8). Bidder architecture is drawn from two synced meetings referenced in the summary: Zach 2026-06-02 (section 4.1) and Abbas + Ryan 2026-06-09 (section 4.2). Solution (section 5) and Questions Answered (section 6) state the kill of the cache-shrink hypothesis and the replacement optimization. Grepped knowledge/data_catalog.md, data_knowledge.md, experimentation.md, mntn_business.md to confirm which facts are already documented (section 7 confirms bidder architecture was captured 2026-06-09; verified present at data_knowledge.md L2237-2278 and data_catalog.md L2783-2785).

**Tables:** rtb.household-profile, Aerospike, ScyllaDB, MembershipDB, MemDB

**Learned:**
- Original MM 3P-OR-include cache-shrink hypothesis is largely killed: Aerospike primary key = IP address, so removing a segment from an IP does not drop the key; footprint saving is marginal.
- Replacement optimization: don't write intent scores for IPs with no active segments; the membership-consumer intent-score write path does not currently check for segments before writing.
- Three inputs gate a bid: score (GCS to consumer), segments (MembershipDB), HHST threshold; safe drop = no-segments then don't write intent score, preserving Max Reach.
- Full bidder caching architecture (Aerospike/Redis/ScyllaDB, membership consumer, spend pipeline) already documented in data_knowledge.md (L2237-2278) and data_catalog.md (L2783-2785) per section 7.

**Reuse when:**
- question about MemDB, membership consumer, or bidder cache internals
- question about whether 3P-OR-include expands the MM bid-eligible pool
- question about Aerospike household-profile key structure or where intent scores / segments live
- question about optimizing bidder storage / suppressing scoreless IPs
- onboarding on the score + segments + HHST bidding gate

---

# TI-1016: MemDB → Bidder cache — investigate bidder-inert IP storage

**Jira:** https://mntn.atlassian.net/browse/TI-1016
**Status:** In Progress
**Date Started:** 2026-06-02
**Date Completed:** TBD
**Assignee:** Malachi

---

## 1. Introduction

Sitting at the intersection of TI-999 (interest-segment portfolio sizing) and TI-956 (per-pattern segment application), this ticket investigates whether the MemDB → Bidder pipeline is storing/caching IPs that **cannot affect bidding** under the current scoring rules.

The trigger: ~80% of Mountain Match (MM) campaigns currently use a 3P-OR-include pattern — adding LiveRamp interest segments via OR-include to inflate audience size. But MM only bids on **scored** IPs (DS13/DS19). The 3P-OR-include additions inflate the apparent audience (1M MM IPs → 20M–30M after 3P add) without changing the bid-eligible pool. Those extra IPs still propagate through MemDB to Bidder caches.

Empirical context locked from prior work (Pass 21/32/33 of TI-999):
- 3P-OR-include is bidder-inert in MM context — verified (DS46 audience overlay leaves DS13/DS19 as scoring substrate).
- Within-advertiser CVR delta for MM vs MM+3P-OR-incl is **−8.08e-4** — 3P-OR-include actually hurts CVR slightly, despite being supposedly inert.

## 2. The Problem

**Current state, as Zach Schoenberger described it (2026-06-02 sync):**

MemDB → Bidder data flow:
1. **MemDB (Membership DB)** — owned by targeting team.
   - Stores **all** audience segment definitions including the full LiveRamp catalog (≈260K interest segments), even if inactive.
   - Does **not** store IP→segment mapping itself.
   - Evaluates audience expressions when an IP-update event arrives (impression, pixel event, TPA export, etc.) and emits the resulting segment set for that IP.
   - Pushes the same recomputation downstream **every 4 hours** to absorb audience-creation / -deletion / -expression changes.
   - Pumps TPA export from BigQuery into the same stream.
2. **Bidder** — separate team, separate stack. **Does not query MemDB directly** (deliberate decoupling).
   - Receives the IP→segment state from MemDB.
   - Owns its own database/cache (Zach assumes Redis hot tier in front, with a slower store behind — ~5 ms lookup or better for the segment fetch).
   - Cache fill is "one of the more time-consuming pieces" in the bid path.

**The proposed optimization (raised with Zach, to be developed with Abbas):**

If we know an IP is being added to a campaign via a pattern that **cannot bid on it** (MM + 3P-OR-include is the canonical case), do not propagate / cache that IP for that campaign.

**Why this might matter:**
- A typical MM campaign goes from ~1M scored IPs → 20–30M cached IPs after 3P-OR-include is layered on. At 800–900 concurrent MM campaigns running this pattern, the multiplicative effect on cache footprint is non-trivial.
- The bidder cache is on the hot path. Smaller working set → fewer evictions, better hit rate, lower fetch latency on net-new IPs.

**Zach's pushbacks (must be addressed before committing to the change):**
1. **Max Reach edge case** — some campaigns occasionally open up to unscored IPs via Max Reach (the rest of the audience beyond DS13/DS19). These must remain reachable. Zach believes the % of spend touching Max Reach is small but it must be quantified.
2. **Cache warmup latency on spend bursts** — even if storage savings exist, the cost of *re-fetching / regenerating* an IP→segment mapping when an advertiser suddenly increases spend can be worse than the cost of keeping it warm. The cache exists because pulls are slow.
3. **"Segments are smaller than you think"** — Zach asserts the actual cached segment objects are smaller than expected; the savings may not be dramatic. Needs empirical sizing.
4. **Lower-hanging fruit** — Zach said there's other optimization work that beats this for current ROI. We should size this honestly before pitching it.
5. **Secondary idea (cap LiveRamp catalog at top-100 segments) is a non-starter** — customers can target any of the 260K historically. Top-100 churns month-to-month; locked audiences would lose targeting. UI-side "suggest top N" is fine; MemDB storage cuts are not.

## 3. Plan of Action

1. **Capture Zach's system design** (done — § 4.1 below).
2. **Friday 2026-06-06: meet with Abbas (bidder team)** to fill in the bidder side:
   - Cache architecture (Redis tier sizes, eviction policy, backing store, fetch latency p50/p99).
   - How segment data lands from MemDB into the cache (push vs. pull, batching, deduplication).
   - Is there a per-campaign-expression hook where we could mark IPs as cache-inert?
   - Max Reach: does the bidder hold all unscored IPs resident, or fetch on demand?
   - Quantify cost of cache miss on a hot bid path.
3. **Quantify the inert-IP footprint** empirically:
   - Cross-reference MM campaigns currently running with 3P-OR-include.
   - For each, count: scored IPs (DS13/DS19) vs. IPs added by 3P-OR-include only.
   - Aggregate cache size delta with vs. without.
4. **Quantify the Max Reach exception** — what fraction of spend / impressions actually fires on unscored IPs across the same set of campaigns?
5. **Propose** with measured cost/benefit: memory saved, expected hit-rate improvement, latency change. Include the spend-burst counterfactual Zach raised.
6. **Document outcome** — if proceed, hand to bidder team for sys-design follow-up. If not, durable docs on why (so the question doesn't get re-raised in a year).

## 4. Investigation & Findings

### 4.1 MemDB → Bidder system design (from Zach, 2026-06-02)

Transcript: [meetings/ti_1016_01_zach_memdb_optimizations_2026_06_02.txt](meetings/ti_1016_01_zach_memdb_optimizations_2026_06_02.txt)

```
   [audience expression changes]              [IP-update events: impressions, pixels, TPA]
                │                                              │
                ▼                                              ▼
   ┌──────────────────────────────────────────────────────────────┐
   │                          MemDB                                │
   │  • Stores all audience segment definitions (incl. LiveRamp    │
   │    260K, active + inactive)                                   │
   │  • Does NOT store IP→segment mapping                          │
   │  • Evaluates expressions on event arrival → emits IP state    │
   │  • Also re-emits every 4h on audience-config changes          │
   │  • TPA export from BigQuery flows through here in parallel    │
   └──────────────────────────────────────────────────────────────┘
                │
                ▼  (downstream push — segment-update stream)
   ┌──────────────────────────────────────────────────────────────┐
   │                          Bidder                               │
   │  • Separate team, separate stack — does NOT query MemDB       │
   │  • Owns its own database + cache                              │
   │    (Zach assumes Redis hot tier; ~5ms segment lookup)         │
   │  • Cache fill is one of the more expensive bid-path steps     │
   └──────────────────────────────────────────────────────────────┘
                │
                ▼
            [bid decision]
```

Key invariants:
- MemDB never holds IP→segment durably; it computes on event arrival.
- Bidder cache decoupling is intentional — neither team queries the other directly.
- Mountain Match campaigns are special-cased: only scored IPs (DS13/DS19) are bid on; 3P-OR-include expansions do not affect the bid-eligible pool. Max Reach is the only path that opens MM to unscored IPs.

### 4.2 Bidder cache internals (from Abbas + Ryan, 2026-06-09 sys-design walkthrough)

Transcript: [meetings/ti_1016_02_abbas_bidder_sys_design_caching_2026_06_09.txt](meetings/ti_1016_02_abbas_bidder_sys_design_caching_2026_06_09.txt)
Diagram: Abbas's screen-share (reproduced as ASCII below). "Might be slightly outdated but overall still correct."

**Caveat on authority:** Abbas has moved to the **performance-pacing team** and is no longer fully privy to the in-flight membership-consumer changes. For the next-gen membership consumer / Aerospike→Scylla migration detail, the owner is **Eric** (set up a follow-up). Secondary contact: **Alkaif**.

#### Bid path (left→center→SSP)

```
SSPs (Magnite, Index Exchange, Freewheel, Pubmatic) ── millions of req/sec
   │  (direct = "Mountain Bidder";  Beeswax = proxy/middleman that aggregates
   │   exchange requests, forwards to us, relays our response back to the exchange)
   ▼
Campaign service ──"do we want to bid on this IP?"──► hits MEMBERSHIP CACHE first
   │
   ▼
Aerospike (household profile)  ── PRIMARY KEY = IP address
   • value record per IP; `segments` is ONE field. Also: geo version, intent
     scores, segment scores, the segments themselves, and now holdout IPs.
   • ~300 MILLION IP keys; 3–5 TB total.
   • single-digit-millisecond latency → hit DIRECTLY on every bid (no in-bidder
     in-memory tier — 3TB too big for memory, and with 300M evenly-distributed
     keys a subset cache gets a near-zero hit rate, so it isn't worth it).
   • MUST be hit every bid — always need up-to-date membership/audience data.
```

**Latency budgets:** Mountain Bidder ~**200 ms** to respond; **Beeswax tighter — 15 ms timeout window**; Aerospike lookups single-digit ms (so the direct-hit design fits the budget).

#### What lives where (Aerospike vs Redis)

- **Aerospike (hot, per-IP):** household profile (segments + intent scores + segment scores + geo + holdout IPs), plus **spend data, recency data, frequency data (freq capping)**, and (currently) metadata.
- **Redis (slow-changing "static" data):** flight data, flight budgets, thresholds, weights — metadata that doesn't change day-to-day. Bidder pulls Redis metadata on a **scheduled cron every 5–10 min** (NOT real-time). Consequence: a customer who stops a flight keeps spending for up to ~10 min until the next pull. Roadmap = notification-based metadata updates.

#### How the caches get populated (loaders)

1. **Python cache loaders (simplest):** read CoreDB (Postgres) or BigQuery → copy JSON blob almost verbatim into Redis/Aerospike. Minimal transforms (live schedules e.g. World Cup do a little).
2. **Membership consumer (the path TI scores travel):**
   - Scoring team writes scores to a **GCS bucket** → GCS event trigger → **PubSub** → **RabbitMQ**. Membership consumer consumes RabbitMQ messages, each carrying a **GCS file URL**.
   - Downloads the GCS file locally, processes the (large) file, writes to **Aerospike**.
   - Internal logic: **intent scores grouped + written in one batch** (not line-by-line); **if an IP's segment list is empty → the IP is deleted from Aerospike** ("no longer important"); special write handling for holdout IPs.
   - Handles **anything household-profile related**. Lives in **Kubernetes** (scalable).
   - **Scores are NOT stored in MembershipDB** — they go GCS → membership consumer → Aerospike. MembershipDB emits the **segments**; membership consumer writes those too. (Reconciles the open question of "where do scores live": GCS is the durable source, Aerospike is the serving copy.)
3. **PCS (Pacing Controller Service) + Campaign Metadata Service (CMS):** performance-pacing team. Write the **static pacing data** (flight budgets/thresholds/weights) via a separate service. Currently → Aerospike, moving to **Redis** soon.

#### Spend pipeline (right side) — source of spend/win data

```
SSP/Beeswax win notification ──► Notification service (standalone HTTP webhook)
   │  writes RAW WINS ──► ScyllaDB cluster  (DEDUP: each win processed once,
   │                                          prevents double-counting spend)
   ▼  via Kafka
3 aggregators (within ~1 MINUTE end-to-end):
   • frequency aggregator   ─┐
   • spend aggregator        ├─► write to Aerospike (→ Redis soon)
   • logs aggregator ──► GCS ─┘  (for downstream consumers/teams)
```

All spend-pipeline services run in **Kubernetes** (scalable for spiky load — World Cup, Black Friday).

**VAST / bid response:** Mountain Bidder returns **VAST markup** in either the bid response or the win-notification response. Beeswax instead hits the **ad-markup service** directly (proxy).

#### In-flight changes / future state (per Abbas, low-confidence — confirm w/ Eric)

- **Aerospike → ScyllaDB** ("our future is Scylla"). Driver: Aerospike is **expensive, poor support**; Scylla cheaper. Decision made above Abbas.
- Adding a **deduplication cache** on the membership-consumer write path (don't write every time).
- **Splitting the membership consumer** into separate **recency** and **membership** consumers (currently one).
- Likely **read directly from GCS** instead of via RabbitMQ (skip a hop, faster load) if PubSub limits allow.
- PCS/CMS static pacing data → Redis (from Aerospike).

### 4.3 Empirical sizing (TBD)

Open. Queries to be added to `queries/`. Target outputs in `outputs/`:
- Cache-footprint estimate per MM campaign with/without 3P-OR-include.
- Max Reach impression / spend share by campaign.

### 4.4 Eric Salinger's written proposal: "Reducing segment write load and moving the bidder to Kafka" (2026-08-25)

Filed verbatim at [ti_1016_eric_segment_path_consolidation.md](ti_1016_eric_segment_path_consolidation.md) (source: `~/Downloads/segment-path-consolidation.md`; Eric also shared it as a claude.ai artifact link in the meeting chat). Status per the doc: investigation complete, proposal for review. Owner: esalinger. This section extracts what answers the six AUDI-1016 clarifying questions; the doc itself is the authority for file:line evidence.

**Q1 — which pathway carries the junk:** the **4-hourly GCS full-snapshot dump**, not the Kafka topic. `rtb-membership-consumer-service` consumes segment data over two paths from the same producer (`membership-db/etl`): Kafka topic `segment-updates-burnin-proto` (activity-driven) and the GCS dump (GCS event → Pub/Sub `GCS_SEGMENT_SUBSCRIPTION` → consumer). The GCS path is 92.9% empty records. The sweep that produces it runs on the VM ETL deployment (ansible/systemd, `membership-db-prod-c4hm32-[0:63]`, `refresh_enabled: true`); the k8s ETL deployment (40 replicas) runs the Kafka activity path. Sole producer confirmed org-wide across 742 non-archived repos.

**Q2 — what counts as "duplicate empty" and how the 92% was measured:** measured over 24h in namespace `bidder`: 10.60B GCS segment records processed, 9.85B entirely empty (**92.9%**), peak **402,907 records/s**, duty cycle 43.8% (~1.75 of every 4 hours). "Empty" = a household record whose segment list is empty; the sweep emits every household every sweep unconditionally, so a household with no memberships is re-sent as empty every 4 hours. Every empty record still executes a full `REPLACE` to east and west with collection tombstones (`gcs.rs:929-936` — counter increments, record still sent, no `continue`). Root cause: prod refresh strategy `ParallelDiskSingleEvalNoUpdate` (`server/config/config-prod-c4hm32.yml:31`) has **no emission filter at all**; the sibling `SingleDiskSingleEvalNoUpdate`'s filter (`!response.update_deltas.is_empty()`, `refresh.rs:288`) is dead code because `update_deltas` always gets a single entry when `default_advertiser_id = Some(1)` (`query.rs:142`, `refresh.rs:102`).

**Q3 — full-snapshot today, and preferred fix:** yes, full snapshot every 4h on the GCS path. Preferred fix = **diff-to-Kafka, additive**: (1) membership-db keeps writing the GCS dump exactly as today (it is a shared data product feeding attribution/incrementality/reporting across four teams, ~8 consumers — deleting it is not a bidder-team decision and not necessary); (2) add change-only emission to the existing Kafka topic; (3) the consumer deletes its GCS segment consumer and gets full coverage from Kafka. Result: ~93% Scylla write reduction, 402k/s peak eliminated, dormant-household coverage retained, no downstream breakage. Change detection = digest compare in the ETL sidecar (there is NO stored membership state to diff against in prod — MCRocks `membership_map` never populated, `apply_segments` persistence born commented-out in `cbd5d52b` 2025-02-19, PR #312); digest advances only on confirmed publish (no TTL means a lost message with an advanced digest would never re-emit).

**Q4 — delete semantics under suppression:** the proposal removes the TTL entirely (finite TTL is incompatible with change-only emission — stable households never refresh it and the longest-tenured members get deleted first) and replaces expiry with **push-based delete-on-empty per path**: segment path deletes its own columns (`segments`, `geo_version`, `segment_scores`, `holdout_campaign_ids`, `timestamp_segments`); intent path deletes its own (`advertiser_scores`, `campaign_scores`, `timestamp_intent_scores`); the row disappears once both have deleted. A just-became-empty household DOES still emit (the eval always produces the empty-delta record), so the delete flows naturally — send-once-then-suppress falls out of change-only emission. **The one gap:** MDB's daily garbage collector (09:00, every env) hard-deletes emptied households by removing only the index key, invisibly to the sweep, with no Kafka message — under no-TTL such a household strands its Scylla row forever. Fix is small: emit a final empty `UpdateResponse` before `entry.delete()` (and publish gRPC `_delete` through the ETL producer), or stop deleting the index key. Verified: no GDPR/CCPA deletion path exists; `full_delete`/`delete_cf_key` have zero callers. Campaign deactivation needs no mechanism (dead segment id = lookup miss via Redis `rtb:campaigns:mntn`; `segment_scores` can only suppress, never create a bid). Holdout handling: the Kafka mapper filters holdout-tagged segments (`segment_update.rs:40-63`) — see the flip-flop bug below.

**Q5 — does the consumer change:** yes, this is NOT producer-only. Bidder-team work: Kafka broker timestamp as `cql_timestamp` (split from `timestamp_segments`, which keeps meaning activity time); delete `run_segment_batch_consumer` + `GCS_SEGMENT_SUBSCRIPTION` + the bucket notification; replace the Pub/Sub freshness page (3h warn / 3.5h page) with Kafka-lag alerting; add the segment-path DLQ; remove the TTL (shared-crate change in `rtb-scylla-models`, used by other services) and implement delete-on-empty. Membership-db-team work: digest change detection in the ETL sidecar; a Kafka producer handle on `ServiceInner` (none today); rate-limit the emission path; producer retries + producer DLQ; close the GC gap. Cutover: deploy Kafka emission → deploy consumer changes (no TTL, delete-on-empty) → **let exactly one full GCS sweep run** (rewrites every household to clear the inherited 7-day per-cell TTL — required, not optional) → then delete the GCS consumer. Rollback is trivial: re-enable the Pub/Sub subscription (the dump keeps being produced).

**Q6 — validation:** the doc's quantitative anchors are the consumer-side counters that produced the 24h measurement (records processed / empty / peak rate) — post-change: 402k/s peak gone, ~93% Scylla write reduction, Kafka messages ≈ today's 1.03B/day plus change-only additions. Alerting shifts from Pub/Sub freshness to Kafka lag. (Meeting transcript to confirm the specific dashboard and any staging path.)

**Transition hazards (both flagged in the doc):** (1) Kafka broker timestamps are **milliseconds**, `cql_timestamp` is **microseconds** — passing millis puts every write 1000x below existing cells and they silently drop (the exact failure documented at `household_profiles.rs:29-33`); (2) per-cell TTL means partially-populated rows during transition (segment vs intent columns on different clocks — already true today).

**Independent bugs the investigation surfaced (not this project, worth tracking):**
- **Live today:** no `epoch == 0` guard in the consumer — a proto3-absent `uint32` decodes to 0, `cql_timestamp = 0` is permanently below every stored cell, every subsequent write for that IP silently drops until TTL expiry. Producer guards one path only (`kafka.rs:210-212`).
- The two segment paths disagree on holdouts: GCS mapper does NOT filter holdout-tagged segments, Kafka mapper does; both full-replace, so any household with a holdout segment flip-flops on a 4-hour cycle. Self-resolves once GCS consumption stops.
- `segment_scores` freshness gate reads `timestamp_intent_scores`, not `timestamp_segments` — segment data gated by the daily intent pipeline's clock.
- Likely-dead DAG (not bidder-owned): `airflow:dags/targeting/tpa_membership_log_kafka.py` reads an S3 path whose sink moved to GCS `mntn-analytics-raw` on 2025-08-21 — probably skipping every run. Unverified at runtime.

**Why change-only Kafka must be additive (analytics constraint):** `tpa_membership_update_log_uber.sql` and the other roster jobs read the GCS dump as a daily audience roster (explode `in_segments` per `dt`); under change-only semantics each partition becomes "IPs that changed that day" and every population/coverage/reach figure silently collapses. Only `airflow-ti:materialize_mntn_first_party` is Kafka-fed. This is directly relevant to the sizing work in §4.3/§8: the BQ copy of the dump is the roster, and whether empty records survive the importer is an open empirical check.

**Membership-db context:** prod on `ParallelDiskSingleEvalNoUpdate` since 2023-02-16 (PR #131); `update_membership_entry: Some(false)` since 2022-06-03 (PR #85, migration-era). Active branches are heavily **SlateDB** (a storage-engine migration in flight) — worth asking whether that lands first, since it would moot MCRocks-level design and might restore `membership_map` persistence. No reverse index exists (households keyed only by IP, `MD5(ip) % 64` across 64 shards) — per-campaign recompute would need a full 64-shard scan, which is why the proposal reuses the existing sweep.

### 4.5 Meeting 03: Eric Salinger + Matt Brorby, "Discuss Empty Segments" (2026-08-25)

Transcript: [meetings/ti_1016_03_eric_matt_empty_segments_2026_08_25.txt](meetings/ti_1016_03_eric_matt_empty_segments_2026_08_25.txt)

*(Transcription in progress — meeting-sourced confirmations, divergences from the doc, owners, and timing to be folded in here.)*

## 5. Solution

**The original hypothesis is largely KILLED by the Aerospike key structure (Abbas, 2026-06-09); a sharper optimization replaces it.**

The original idea — "don't propagate the bidder-inert 3P-OR-include IPs to shrink the cache" — assumed cache size scales with (IP × segment) pairs. It does not:

- **Aerospike primary key = IP address.** The 3P segments are just entries in the `segments` field of an IP's record. Removing a 3P segment from an IP **does not drop the key** — "it won't decrease the keys they have in Aerospike."
- The bidder **only stores IPs actually being used**, not the full LiveRamp universe. Unused 3P-segment IPs aren't written to Aerospike until a campaign references them.
- The membership consumer **already deletes any IP whose segment list goes empty.**

So the per-IP record gets marginally smaller (fewer segment entries), but the 39M-vs-1M framing doesn't translate into 39M fewer keys — most of those IPs are stored anyway (they're real IPs used elsewhere). Net footprint saving is small. This matches Zach's "segments are smaller than you think" pushback.

**Sharper optimization that DID surface (the actionable thread):**

> **Don't write intent scores for IPs that have no (active) segments.** The membership consumer's intent-score write path **does not currently check whether the IP has any segments** before writing the score. Abbas + Ryan both agreed it "probably should" — an IP with no segments effectively doesn't exist for bidding, so writing a score for it is wasted storage/writes.

- **Where:** membership-consumer write path, as a **conditional write** — "check if IP has content in the segments field; if so write, if not don't." Easy on the TI/scoring side (just don't dump scoreless-segment IPs to GCS); needs feasibility check on the bidder side.
- **Three inputs gate a bid:** (1) score (GCS→consumer), (2) segments (MembershipDB), (3) HHST threshold. To safely drop data you need: no threshold AND no score, OR no segments. The clean, safe cut is **no-segments → don't write intent score**.
- **Max Reach is preserved** — this only suppresses scores for IPs that have *no segments at all*, not unscored-but-segmented IPs that Max Reach can still open up.

**Status:** Victor + Abbas had a follow-up call immediately after this meeting specifically about "what we can filter out." Next step is to size this empirically (how many scoreless-no-segment IPs are we writing?) and confirm feasibility of the conditional write with Eric (owns the membership consumer now).

## 6. Questions Answered

- **Q:** Does adding a 3P interest segment via OR-include to an MM campaign expand the bid-eligible pool?
  **A:** No. MM still only bids on scored IPs (DS13/DS19). The 3P-OR-include is bidder-inert. Peak Performance is the supported path to expand scorable IPs. (Zach, 2026-06-02; matches Pass 21/32/33 of TI-999.)

- **Q:** Does MemDB store the IP→segment mapping?
  **A:** No. It stores segment definitions and evaluates expressions on event arrival, emitting downstream. (Zach, 2026-06-02.)

- **Q:** Does the bidder query MemDB directly?
  **A:** No. MemDB pushes state downstream; the bidder maintains its own cache. (Zach, 2026-06-02.)

- **Q:** Is the LiveRamp catalog (~260K segments) actually all held in MemDB even if unused?
  **A:** Yes — full state is stored regardless of activity. Cutting to "top N" is a non-starter because customer audiences reference specific segments historically. (Zach, 2026-06-02.)

- **Q:** What is the "bidder cache" Zach could only speculate about?
  **A:** **Aerospike**, the household-profile store. Primary **key = IP address**; the record holds segments, intent scores, segment scores, geo, and holdout IPs. ~300M IP keys, 3–5 TB, single-digit-ms latency, hit directly on every bid (no in-bidder in-memory tier). (Abbas, 2026-06-09; matches Ryan's chat note + the Aerospike Household-Profile Confluence page.)

- **Q:** Where do intent scores actually live — MembershipDB?
  **A:** **No.** Scoring team writes scores to **GCS**; the membership consumer picks them up (GCS→PubSub→RabbitMQ→consumer) and writes them into **Aerospike**. MembershipDB emits the **segments** only. GCS is the durable source of scores; Aerospike is the serving copy. (Abbas + Ryan, 2026-06-09.)

- **Q:** Does dropping the bidder-inert 3P-OR-include IPs shrink the bidder cache?
  **A:** **Largely no.** Key = IP, so removing a segment from an IP doesn't drop the key, and the bidder only stores IPs actually in use (not the full LiveRamp universe). The footprint win is marginal — confirms Zach's "smaller than you think." (Abbas, 2026-06-09.) See §5 for the replacement optimization.

- **Q:** Where does the spend/win data originate?
  **A:** Win notifications (SSP/Beeswax) → **Notification service** (HTTP webhook) → raw wins into **ScyllaDB** (dedup, no double-count) → Kafka → 3 aggregators (frequency, spend, logs→GCS), all within ~1 min. Aggregators currently write Aerospike (→ Redis soon). (Abbas, 2026-06-09.)

- **Q:** Where does holdout logic live (relevant to ghost bidding / BER-2250)?
  **A:** **MembershipDB.** Holdout IPs are also mirrored into the Aerospike household profile. Ryan flagged that moving geo-radius targeting into the bidder would force ghost-bid holdout logic to move there too. (Ryan, 2026-06-09.)

## 7. Data Documentation Updates

Done (2026-06-09, from the Abbas sys-design walkthrough + Confluence BP pages):
- `knowledge/data_knowledge.md` — added **Bidder System Design & Caching Architecture** (Abbas walkthrough) **and** a **CANONICAL reference** section from the Confluence BP "Bidder" page: RTB lifecycle terms, all service repos, the dual Beeswax→MNTN-Bidder architecture, price/threshold logic, GCS log lineage. Corrected the `hhs:*` household-profile bin names and resolved the `holdout_cids` grain (per-IP array of campaign_ids).
- `knowledge/data_catalog.md` — **Aerospike `rtb` namespace** set schemas (`household-profile` / `spend` / `price` / `recency`) + `aql` access; **bidder price + threshold DW tables** (`summarydata.publisher_adsize_metrics`, `sync.creative_metadata`, `dso.*_thresholds`); **GCS log buckets + BQ lineage** (auction logs→`bidder_auction_events`, bid logs→`bidder_bid_events`); ScyllaDB raw-wins.
- Archived the Confluence "Bidder" page PDF at `documentation/docs/bidder_platform_confluence_reference.pdf`; pulled the "Aerospike Datastore" page via the Confluence REST API.

## 8. Open Items / Follow-ups

- [x] ~~Sync with Abbas (bidder team)~~ — done 2026-06-09, transcript filed as `ti_1016_02_abbas_bidder_sys_design_caching_2026_06_09.txt`, §4.2 filled.
- [x] ~~Extract canonical Confluence pages~~ — BP "Bidder" + "Aerospike Datastore" pages captured into `knowledge/data_knowledge.md` + `data_catalog.md` (2026-06-09); PDF archived at `documentation/docs/bidder_platform_confluence_reference.pdf`.
- [ ] **Follow-up meeting pending** on the new bidder infra/architecture (Aerospike→ScyllaDB migration, next-gen membership-consumer split into cse/oracle/recency) — owner **Eric** (perf-pacing). When it happens: transcribe → extract into the knowledge docs (the "current vs future state" notes there are the baseline to update).
- [ ] **New primary thread:** size the "don't write intent scores for IPs with no segments" optimization — how many scoreless-no-segment IPs are we currently writing to GCS/Aerospike? (TI side: query the score dump vs segment membership.)
- [ ] Confirm conditional-write feasibility on the bidder side with **Eric** (now owns the membership consumer; Abbas moved to perf-pacing). Alkaif is secondary.
- [ ] Cross-check with the **Victor + Abbas "what we can filter out" follow-up** that happened immediately after this meeting — align so we're not duplicating.
- [ ] (De-prioritized) Original MM-OR-include footprint quantification — Abbas confirmed marginal because key=IP; keep only as a sizing sanity check, not the headline.
- [ ] Decision: package the no-segment-score-suppression as a sys-design RFC for the bidder team if sizing justifies it.

**2026-08-25 Eric Salinger (owns membership consumer, store is ScyllaDB not Aerospike now):** ~92% of segment writes are empty; filtering would cut ~400k tps to ~40k. Scylla-side conditional writes REJECTED (LWT-class cost; a cache to make them performant ~$20k/mo, deemed too expensive). Agreed fix: upstream, do not supply duplicate empty-segment records. Supersedes the June consumer-side conditional-write framing (Abbas/Ryan). Next: identify the feed emitting duplicate empties, filter TI-side.
**2026-08-25 addendum:** Eric confirmed the upstream filter is what they'd want; his named alternative is switching the supply to diff-based delivery on the Kafka pathway (only changed records flow, empties/dupes disappear structurally). Two design options for the build: (a) filter the existing snapshot feed, (b) diffs-to-Kafka.
