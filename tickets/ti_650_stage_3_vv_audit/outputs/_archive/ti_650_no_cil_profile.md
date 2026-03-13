# TI-650: Profile of 1,074 "No CIL" S3 VVs

**Query:** `queries/ti_650_no_cil_profile.sql`
**Run:** 2026-03-12 | 143s wall | 2,966 GB processed
**Advertiser:** 37775 | **Trace:** Feb 4–11 | **Lookback:** 90 days (CIL) / 180 days (event_log)

## Summary

| Dimension | Count | % |
|-----------|-------|---|
| **Total no-CIL** | **1,074** | 100% |
| Has event_log record | 1,074 | **100%** |
| No event_log record | 0 | 0% |
| Cross-device = true | 541 | 50.4% |
| Cross-device = false | 533 | 49.6% |
| Primary attribution (models 1-3) | 291 | 27.1% |
| Competing attribution (models 9-11) | 783 | 72.9% |
| Has first_touch_ad_served_id | 976 | 90.9% |

## Attribution Model Detail

| Model | Count | Description |
|-------|-------|-------------|
| 1 (GUID) | 108 | Primary GUID-based |
| 2 (IP) | 137 | Primary IP-based |
| 3 (GA Client ID) | 46 | Primary GA-based |
| 9 (Competing GUID) | 425 | Competing GUID-based |
| 10 (Competing IP) | 306 | Competing IP-based |
| 11 (Competing GA) | 52 | Competing GA-based |

## Impression Age Distribution

| Age Bucket | Count |
|------------|-------|
| Older than 90 days | 0 |
| 60–90 days | 0 |
| 30–60 days | 0 |
| **Under 30 days** | **1,074 (100%)** |

| Metric | Days |
|--------|------|
| Average | 2.1 |
| Minimum | 0.0 |
| Maximum | 19.5 |

## Key Finding: CIL TTL Hypothesis DISPROVEN

**The hypothesis that CIL record expiration (90-day TTL) causes the gap is definitively disproven.**

- 100% of no-CIL VVs have event_log records (vast_start, vast_impression events exist)
- 100% of those impressions are under 30 days old — well within CIL's 90-day window
- The impression is real and recent — it simply was not written to `cost_impression_log`

## Root Cause: Pipeline Gap

The 1,074 VVs have impressions in `event_log` but NOT in `cost_impression_log`. This suggests:

1. **CIL write failure** — the impression was served (event_log confirms delivery) but the cost/billing record was not created or was dropped
2. **Display-only impressions** — some impression types may bypass the CIL pipeline entirely
3. **Pipeline timing** — the CIL record may have been written but to a different partition or with a lag that places it outside the lookback window

Since the impression exists in event_log, these VVs could potentially be resolved using event_log bid_ip directly (bypassing CIL entirely). The current audit only uses CIL for the bid_ip lookup — event_log bid_ip is an untested resolution path for this cohort.

## Impact Assessment

- 1,074 = 4.5% of 23,844 S3 VVs
- 291 primary attribution = 1.2% of total
- These are NOT lost data — they have event_log records and could be resolved via event_log bid_ip
- The audit should add event_log as a fallback bid_ip source when CIL is missing
