---
doc_type: ticket
title: "AUDI-1116: RTC x Free Logs — Feed, Timing, Hourly-Grain Check"
status: done
date: 2026-07-17
summary: "Whether vendor delivery timing lets RTC firings depend on paid vendor feeds"
result: "RTC vendor-independent — vendors 2.4-8.6h stale, only 0.01% of RTC volume vendor-only; Jira Done 2026-08-24"
keywords: [rtc, real-time conquest, vendor-independent, ulid, ingest latency, site_visit_signal, svs, guid_log, augmentor_log, fpa_site_visit_batch_serverless, 33across, 5x5, predactiv, audi-1116, hourly batch]
---

## TL;DR

**Q:** Does vendor delivery timing let RTC firings depend on paid vendor feeds? (AUDI-1116)

**A:** No, RTC is effectively vendor-independent. RTC runs on two pipelines: (1) guid_log Kafka streaming (~real-time, Zach S.) and (2) a TI-run HOURLY batch over svs-minus-guid, so vendors CAN in principle drive RTC firings via the batch path, but measured dependence is negligible. Using a ULID ingest-latency instrument (svs uid first 10 Crockford-base32 chars = ms mint timestamp; mint minus time = delivery lag), the free logs (guid_log, augmentor_log) are the only real-time sources (0.0 min median), while every vendor delivers hours late: 33Across ~8.6h flat, 5x5 ~5.5h, most others ~2.9h, Predactiv bimodal (per-hour medians 2.6-12.1h). These match the CONFIGURED per-DS lag hours in the fpa_site_visit_batch_serverless DAG (5x5=5h, aug/guid=1h, 33across=8h), so the staleness is structural, not incidental. For RTC-fired impressions in valuation week 2026-07-02..08 (30,604,353 imps on 4,004,751 IPv4 IPs): free_covered = 99.99%, vendor_only = 0.01% (3,040 imps / 2,184 IPs); 99.59% is on guid-delivered (real-time Kafka) IPs. Dropping all 8 vendors risks ~0.01% of realized RTC volume. Caveats (verify-pass): vendor_only 0.01% is a coverage-based (not strictly causal) bound; the ULID lag is a lower bound on RTC-visible staleness; free_covered proves in-window delivery, not pre-impression causal qualification. Status: done — 5-agent adversarial verify pass complete, results posted as Jira comment 596106 (2026-07-16), transitioned Done 2026-08-24 (backlog audit).

**How:** ULID latency probe: decode svs uid first 10 chars as ms mint timestamp, subtract event time for delivery lag; per-source medians on 2026-07-01 via audi_1116_hourly_arrival.sql. RTC vendor-dependence via audi_1116_rtc_vendor_share.sql: RTC-fired imps (valuation week) intersected with 37d svs membership masks, split into guid_realtime/hourly_batch_only/no_svs and free_covered/vendor_only. IPv4-only.

**Tables:** `site_visit_signal (svs)`, `guid_log`, `augmentor_log`, `fpa_site_visit_batch_serverless`

**Learned:**
- RTC = two pipelines: guid_log Kafka streaming (real-time) + TI-run hourly batch over svs-minus-guid; vendors can drive RTC only via the batch path
- svs uid is a ULID -> free ingest-latency instrument: first 10 Crockford-base32 chars = ms mint timestamp, mint minus time = delivery lag
- Free logs (guid_log, augmentor_log) mint at event capture = 0.0 min lag (streaming); vendors 2.4-8.6h stale typically, Predactiv up to ~12.1h evenings
- Measured vendor lags match CONFIGURED per-DS lag hours in fpa_site_visit_batch_serverless (5x5=5h, aug/guid=1h, 33across=8h); staleness is structural/deliberate
- RTC vendor-independence: 99.99% of RTC-fired imps free_covered, vendor_only 0.01% (3,040 imps/2,184 IPs); 99.59% on guid-delivered real-time IPs
- 5x5 event timestamps are 2-hour-bucketed before ~14:00 (only even hh partitions exist morning); rows complete, binned coarsely
- vendor_only 0.01% is a coverage-based (not strictly causal) bound; ULID lag is a lower bound on RTC-visible staleness

**Reuse when:**
- Evaluating whether dropping a paid 3P vendor affects RTC / conquest volume
- Questions about RTC feed architecture or real-time vs batch qualification
- Measuring svs ingestion / delivery latency by source
- Vendor freshness SLA or renewal discussions
- AUDI-1111 vendor-quality epic work

# AUDI-1116: RTC × free logs — feed, timing, hourly-grain check

**Jira:** https://mntn.atlassian.net/browse/AUDI-1116
**Status:** Done (Jira transitioned 2026-08-24; results posted 2026-07-16 as comment 596106)
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

**Chart:** `artifacts/audi_1116_ingest_latency.png` (regenerate via
`artifacts/audi_1116_generate_charts.py`). Also in `../outputs/audi_1111_findings.xlsx`.

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

**RTC is effectively vendor-independent — vendor feeds cannot be a reason to keep paying.**
Free logs qualify IPs in real time (0.0 min median lag) while every vendor arrives hours
late (2.4–8.6h typical, Predactiv up to ~12.1h), matching the CONFIGURED per-DS lag hours in
`fpa_site_visit_batch_serverless` — the staleness is structural. Measured on valuation week
2026-07-02..08: 99.99% of RTC-fired imps are free_covered, vendor_only = 0.01% (3,040 imps /
2,184 IPs). Dropping all 8 vendors risks ~0.01% of realized RTC volume; the freshness SLA is
a renegotiation lever. Results posted as Jira comment 596106 (2026-07-16); Jira transitioned
Done 2026-08-24.

## 6. Questions Answered

- **Q:** What feeds RTC?
  **A:** Two pipelines — guid_log Kafka streaming (real-time) + TI hourly batch over
  svs-minus-guid (2026-07-16 readout, documented in data_knowledge §RTC).
- **Q:** Does vendor delivery timing let RTC firings depend on paid vendor feeds?
  **A:** No — vendors are 2.4–8.6h stale vs free logs at 0 min; vendor_only = 0.01% of
  RTC-fired imps (coverage-based, not strictly causal, bound).

## 7. Data Documentation Updates

- data_knowledge §RTC: two-pipeline architecture (committed 2026-07-16).
- data_knowledge §svs: ULID ingest-latency instrument (uid first 10 Crockford-base32 chars =
  ms mint timestamp) + per-source lag findings (committed 2026-07-16).

## 8. Open Items / Follow-ups

- [x] Where the hourly batch job lives — `fpa_site_visit_batch_serverless` (the DAG whose
      configured per-DS lag hours the measured lags reproduce).
- 2026-08-24: Jira closed Done (backlog audit), completion comment citing comment 596106.
