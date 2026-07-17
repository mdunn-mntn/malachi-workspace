# AUDI-1116: RTC × free logs — feed, timing, hourly-grain check

**Jira:** https://mntn.atlassian.net/browse/AUDI-1116
**Status:** In Progress
**Date Started:** 2026-07-16
**Assignee:** Malachi

---

## 1. Introduction

From the 2026-07-16 AUDI-1089 stakeholder readout (Sean's callout): RTC consumes
site_visit_signal near-real-time, so per-day coverage analysis understates vendor timing
effects. Parent epic: AUDI-1111.

## 2. The Problem

RTC = two pipelines (confirmed on the call): (1) guid_log via Kafka streaming (~real-time,
Zach S.), (2) TI-run HOURLY batch over svs-minus-guid. Vendors therefore CAN drive RTC
firings. Unknowns: how much realized RTC volume actually depends on vendors, and whether
vendor delivery is fresh enough to matter at RTC's hourly cadence.

## 3. Plan of Action

1. Ingest-latency instrument: svs `uid` is a ULID → first 10 Crockford-base32 chars = ms
   mint timestamp; `time` = event timestamp; difference = delivery lag. (Validated 2026-07-16.)
2. `audi_1116_hourly_arrival.sql` — full day (2026-07-01), per source × event-hour: rows,
   IPv4 IPs, ingest-lag med/p10/p90.
3. `audi_1116_rtc_vendor_share.sql` — RTC-fired imps (valuation week) × 37d svs membership
   masks → path split (guid_realtime / hourly_batch_only / no_svs_membership) + renewal
   split (free_covered / vendor_only) + per-source touched.

## 4. Investigation & Findings

### 4a. Ingest latency probe (MEASURED, hh=12 slice 2026-07-01, sampled)

| Source | Median ingest lag |
|---|---|
| guid_log (23) | **0.0 min (streaming)** |
| augmentor_log (30) | **0.0 min (streaming)** |
| Cybba (36) | 141 min |
| Predactiv (26) | 162 min |
| Justuno (24) | 171 min |
| Klickly (39) | 171 min |
| 33Across API (40) | 172 min |
| Sovrn (33) | 173 min |
| 5x5 (25) | 331 min |
| 33Across (28) | **516 min (8.6 h)** |

**The free logs are the only real-time sources; every vendor delivers hours late.** For the
RTC hourly batch, a vendor row arriving 8.6h post-visit has lost most of its conquest value.

### 4b. Full-day hourly profile (MEASURED, dt=2026-07-01 — `audi_1116_hourly_arrival.csv`)

Hour coverage note (verify-pass): 5x5's event timestamps are 2-hour-bucketed before ~14:00
(only even hh partitions exist 00–13, each carrying ~2 hours of volume; hourly after) — rows
are complete, binned coarsely. Cybba has zero rows in hh=19 (1.5M/day source, plausible
empty hour). All other sources cover all 24 hours.

| Source | Rows/day | Median ingest lag (min–max across event hours) | Pattern |
|---|---|---|---|
| 33Across | 1,081,178,668 | 515.6 – 521.4 | flat ~8.6h delay, all day |
| augmentor_log | 835,062,598 | **0.0 – 0.0** | streaming |
| 33Across API | 368,193,962 | 172.6 – 180.1 | flat ~2.9h |
| guid_log | 323,819,051 | **0.0 – 0.0** | streaming |
| 5x5 | 122,923,712 | 330.9 – 335.0 | flat ~5.5h (event times 2h-bucketed pre-14:00) |
| Predactiv | 63,195,691 | 157.1 – 724.9 | two interleaved streams: ~2.5h continuous feed + a batched drop; per-hour medians swing 2.6h–12.1h (worst: evening hh18–23) |
| Sovrn | 52,455,805 | 170.6 – 179.6 | flat ~2.9h |
| Justuno | 18,964,906 | 171.0 – 178.7 | flat ~2.9h |
| Klickly | 4,297,562 | 169.9 – 180.4 | flat ~2.9h |
| Cybba | 1,484,434 | 142.3 – 175.3 | flat ~2.4–2.9h |

Lags are constant by event hour for every vendor EXCEPT Predactiv (its batched stream makes
per-hour medians swing) → continuous-but-delayed vendor pipelines, not once-daily batches.
Implication: **vendor rows reach the RTC hourly batch 2.4–8.6h stale typically, up to ~12.1h
for Predactiv's evening buckets; free-log rows reach it within the hour.** "Real-time
conquest" on vendor signal is structurally impossible at current delivery cadences — a
renegotiation point (freshness SLA) as much as a drop argument.
ULID semantics caveat (verify-pass): the free logs' 0.0 proves the uid is minted at EVENT
capture (streaming corroborated by the confirmed guid Kafka path; augmentor inferred); for
vendors, the ULID lag is a LOWER bound on RTC-visible staleness whether uids are minted
vendor-side or at MNTN ingest.
Config corroboration (data_knowledge §svs): the ingestion DAG `fpa_site_visit_batch_serverless`
carries CONFIGURED per-DS lag hours — 5x5=5h, aug/guid=1h, 33across=8h — matching the measured
ULID lags almost exactly. The pipeline deliberately waits for vendor delivery completeness;
the staleness is structural, not incidental.

### 4c. RTC vendor-dependence (MEASURED — `audi_1116_rtc_vendor_share.csv`, week 2026-07-02..08)

Total RTC-fired: 30,604,353 imps on 4,004,751 IPs (IPv4 only, house convention).

| Split | Share of RTC imps |
|---|---|
| free_covered (guid or aug delivered the IP, 37d) | **99.99%** |
| vendor_only | **0.01%** (3,040 imps / 2,184 IPs) |
| guid-covered (Kafka real-time path could qualify) | 99.59% |
| hourly-batch-only reachable (svs member, no guid) | 0.41% |
| no svs membership | 0 |

**RTC is effectively vendor-independent.** Dropping all 8 vendors risks ~0.01% of realized
RTC volume — consistent with the latency finding (vendor rows arrive hours stale — 2.4–8.6h
typical, up to ~12h — while guid streams in real time, so free logs virtually always qualify
the IP first or equally).
Per-source "touched" rows are non-additive (RTC IPs are heavily multi-held: 33Across touches
99.89%, but so does augmentor at 99.87%).

Caveat: free_covered proves the free logs DELIVERED the IP in-window, not that they were
first for the specific qualifying visit — and the membership window runs through 07-08 while
impressions start 07-02, so rows dated AFTER an impression also count toward coverage. Both
splits therefore measure in-window coverage, not pre-impression causal qualification;
vendor_only 0.01% is a coverage-based (not strictly causal) bound, and it is the
renewal-relevant number. Intra-day priority effects (Sean's timing point) are second-order
given 99.59% guid-real-time coverage.

## 5. Solution

*(pending)*

## 6. Questions Answered

- **Q:** What feeds RTC?
  **A:** Two pipelines — guid_log Kafka streaming (real-time) + TI hourly batch over
  svs-minus-guid (2026-07-16 readout, documented in data_knowledge §RTC).

## 7. Data Documentation Updates

- data_knowledge §RTC: two-pipeline architecture (committed 2026-07-16).
- Pending: ULID latency instrument → data_catalog svs entry once full-day run lands.

## 8. Open Items / Follow-ups

- [ ] Where exactly does the hourly batch job live (Airflow DAG name)? Ask Sean/Zach if
      needed after empirical pass.
