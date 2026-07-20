---
doc_type: ticket
title: "TI-1016: MemDB → Bidder cache — bidder-inert IP storage"
status: in_progress
date: 2026-06-09
summary: "Investigate whether MemDB→Bidder caches bidder-inert 3P-OR-include IPs wastefully"
result: "Cache-shrink idea killed (key=IP); new lever: skip score writes for segmentless IPs"
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
