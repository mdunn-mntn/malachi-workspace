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
Tight p10–p90 bands (±25 min) suggest fixed batch schedules. Full-day profile in flight.

### 4b. Scans in flight

- hourly arrival (launched 2026-07-16 ~17:00)
- RTC vendor share (launched 2026-07-16 ~17:00; CIL week ~123GB + svs 37d)

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
