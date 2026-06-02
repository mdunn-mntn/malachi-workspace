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

### 4.2 Bidder cache internals (TBD — Abbas, Friday 2026-06-06)

Open. To be filled in after the Abbas sync.

### 4.3 Empirical sizing (TBD)

Open. Queries to be added to `queries/`. Target outputs in `outputs/`:
- Cache-footprint estimate per MM campaign with/without 3P-OR-include.
- Max Reach impression / spend share by campaign.

## 5. Solution

TBD pending § 4.2 and § 4.3.

Working hypothesis (to validate or kill):
> Filter the MemDB → Bidder push so that IPs newly entering a campaign's universe **only** via a bidder-inert pattern (MM + 3P-OR-include) are not propagated to the bidder cache for that campaign. Max Reach reopens those IPs on demand via the existing unscored-IP path.

Expected wins (rough, must be measured):
- **Cache footprint:** ~20× → ~1× per affected MM campaign (1M scored vs. 20M+ post-3P-add). Multiplied by ~800–900 concurrent MM-OR-include campaigns → potentially order-of-magnitude reduction in working-set size.
- **Bidder cache hit rate:** smaller working set → fewer evictions of hot scored IPs.
- **MemDB→Bidder network volume:** scales with the same factor, since the inert IPs are currently part of the 4h re-push.

Expected risks (must be sized):
- Max Reach cold-fetch latency if Max Reach kicks in faster than the inert-IP repopulation path. Zach's guidance: Max Reach % of spend is small.
- Cache warmup pain on spend bursts that suddenly require the previously-inert pool.

## 6. Questions Answered

- **Q:** Does adding a 3P interest segment via OR-include to an MM campaign expand the bid-eligible pool?
  **A:** No. MM still only bids on scored IPs (DS13/DS19). The 3P-OR-include is bidder-inert. Peak Performance is the supported path to expand scorable IPs. (Zach, 2026-06-02; matches Pass 21/32/33 of TI-999.)

- **Q:** Does MemDB store the IP→segment mapping?
  **A:** No. It stores segment definitions and evaluates expressions on event arrival, emitting downstream. (Zach, 2026-06-02.)

- **Q:** Does the bidder query MemDB directly?
  **A:** No. MemDB pushes state downstream; the bidder maintains its own cache. (Zach, 2026-06-02.)

- **Q:** Is the LiveRamp catalog (~260K segments) actually all held in MemDB even if unused?
  **A:** Yes — full state is stored regardless of activity. Cutting to "top N" is a non-starter because customer audiences reference specific segments historically. (Zach, 2026-06-02.)

## 7. Data Documentation Updates

Pending — once Abbas fills in the bidder side, update:
- `knowledge/data_knowledge.md` — formalize MemDB → Bidder data flow (currently scattered across Zach references).
- `knowledge/mntn_business.md` — confirm/extend the MemDB ownership and decoupling note.

## 8. Open Items / Follow-ups

- [ ] Friday 2026-06-06 sync with Abbas (bidder team). File transcript as `meetings/ti_1016_02_abbas_bidder_caching_2026_06_06.txt`.
- [ ] Quantify MM-OR-include inert-IP cache footprint (queries → `queries/`, outputs → `outputs/`).
- [ ] Quantify Max Reach spend / impression share to bound the cold-fetch risk.
- [ ] Empirically size the LiveRamp catalog footprint in MemDB (Zach's "smaller than you think" claim).
- [ ] Decision: proceed with proposal, kill it, or hand to bidder team as a sys-design RFC.
