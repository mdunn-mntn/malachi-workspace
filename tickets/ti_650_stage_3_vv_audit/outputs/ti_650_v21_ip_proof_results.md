# TI-650 v21: Exhaustive IP Proof — Results

**Date:** 2026-03-16
**IP:** 216.126.34.185
**ad_served_id:** 80207c6e-1fb9-427b-b019-29e15fb3323c
**Campaign:** 450300 (S3, CTV, cg 93957, adv 37775)

---

## Phase 0: Context

### Q0.1 — Campaign group 93957 roster

| campaign_id | name | funnel_level | stage | channel_id | objective_id |
|---|---|---|---|---|---|
| 450305 | Beeswax Television Prospecting | 1 | S1 | 8 (CTV) | 1 |
| 450303 | Multi-Touch | 2 | S2 | 1 (Display) | 5 |
| 450301 | Beeswax Television Multi-Touch | 2 | S2 | 8 (CTV) | 1 |
| 450304 | Multi-Touch - Plus | 3 | S3 | 1 (Display) | 6 |
| 450300 | Beeswax Television Multi-Touch Plus | 3 | S3 | 8 (CTV) | 1 |
| 450302 | Beeswax Television Prospecting - Ego | 4 | Ego | 8 (CTV) | 7 |

All active (deleted=false, is_test=false). S1 has 1 campaign (450305). S2 has 2 (450301, 450303).

### Q0.2 — The VV itself (clickpass_log)

| field | value |
|---|---|
| ad_served_id | 80207c6e-1fb9-427b-b019-29e15fb3323c |
| ip | 216.126.34.185 |
| ip_raw | 216.126.34.185 |
| campaign_id | 450300 |
| time | 2026-02-04 00:06:14 |
| guid | 80f0805e-6153-3fbc-810a-9f9bd2e718c4 |

---

## Phase 4: ad_served_id Pipeline Trace

Every table that contains this ad_served_id shows **identical IP = 216.126.34.185, campaign = 450300 (S3)**:

| stage | detail | ip | bid/partner_ip | campaign_id | time |
|---|---|---|---|---|---|
| cost_impression_log | — | 216.126.34.185 | 216.126.34.185 | 450300 | 2026-01-27 14:52:20 |
| event_log | vast_start | 216.126.34.185 | 216.126.34.185 | 450300 | 2026-01-27 14:53:39 |
| event_log | vast_impression | 216.126.34.185 | 216.126.34.185 | 450300 | 2026-01-27 14:53:39 |
| event_log | vast_firstQuartile | 216.126.34.185 | 216.126.34.185 | 450300 | 2026-01-27 14:53:47 |
| event_log | vast_midpoint | 216.126.34.185 | 216.126.34.185 | 450300 | 2026-01-27 14:53:55 |
| event_log | vast_thirdQuartile | 216.126.34.185 | 216.126.34.185 | 450300 | 2026-01-27 14:54:02 |
| event_log | vast_complete | 216.126.34.185 | 216.126.34.185 | 450300 | 2026-01-27 14:54:08 |
| impression_log | — | 216.126.34.185 | 216.126.34.185 | 450300 | 2026-01-27 14:52:20 |
| clickpass_log | — | 216.126.34.185 | — | 450300 | 2026-02-04 00:06:14 |

Timeline: bid won (14:52:20) → ad served + VAST start/impression (14:53:39) → quartiles → complete (14:54:08) → VV redirect (Feb 4, 00:06:14, ~7 days later).

IP is **100% consistent** across all pipeline stages. No mutation.

---

## Phase 1: Physical Base Table Search — Advertiser 37775

Searched all 6 physical tables (history + raw for each of event_log, impression_log, viewability_log) for IP 216.126.34.185 across ALL campaigns for advertiser 37775.

*Results pending — queries running...*

---

## Phase 3: All Physical Tables — Campaign Group 93957 Only

Searched all 6 physical tables filtered to campaign_group 93957 only.

*Results pending — queries running...*

---

## Table Architecture Reference

```
silver VIEW (logdata.event_log)
  └─ sqlmesh VIEW (sqlmesh__logdata.logdata__event_log__314628680)
       └─ UNION of:
            ├─ bronze HISTORY TABLE (sqlmesh__history.history__event_log__1601996237)  ← no TTL
            └─ bronze RAW TABLE (sqlmesh__raw.raw__event_log__2961306213)             ← 365d TTL
```

### Data retention (verified 2026-03-16)

| Physical table | Earliest data | TTL |
|---|---|---|
| history__event_log | 2025-01-01 | none |
| history__impression_log | 2025-01-01 | none |
| history__viewability_log | 2025-04-08 | none |
| raw__event_log | 2026-01-01 | 365d |
| raw__impression_log | 2025-08-25 | 90d |
| raw__viewability_log | 2025-12-31 | 90d |

**No BQ table at any layer has data before 2025-01-01.**
