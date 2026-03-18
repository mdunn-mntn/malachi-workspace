# TI-650 v16 Step 2: Cross-Stage IP Trace Results

**Query:** `queries/ti_650_ip_funnel_trace_cross_stage.sql`
**Date:** 2026-03-12
**Runtime:** ~137s

## Summary

Successfully linked an S3 VV to a prior-funnel S1 impression via IP match within the same campaign_group_id.

## S3 VV (within-stage trace)

| Field | Value |
|-------|-------|
| ad_served_id | `13cc841f-7dd4-4e88-a649-ea37c4b6ab93` |
| advertiser_id | 55387 |
| campaign_id | 560284 |
| campaign_name | Beeswax Television Multi-Touch Plus |
| campaign_group_id | 113222 |
| objective_id | 1 |
| funnel_level | 3 |

### IP at each pipeline stage

| Stage | IP | Timestamp |
|-------|----|-----------|
| Bid (bid_logs) | `172.59.153.228` | 2026-03-12 03:34:43 |
| Win (win_logs) | `172.59.153.228` | 2026-03-12 03:34:43 |
| Serve (impression_log) | `172.59.153.228` | 2026-03-12 03:34:43 |
| VAST impression (event_log) | `172.59.153.228` | 2026-03-12 03:35:50 |
| VAST start (event_log) | `172.59.153.228` | 2026-03-12 03:35:50 |
| Verified visit (clickpass_log) | `136.60.130.233` | 2026-03-12 17:40:05 |

IP is 100% consistent across bid → win → serve → VAST. Clickpass differs (cross-device: CTV impression → phone redirect).

## Cross-Stage Link (S3 → S1)

| Field | Value |
|-------|-------|
| prior_ad_served_id | `325b0aae-3b05-45a3-8cfc-d8982896b5d8` |
| prior_campaign_id | 560283 |
| prior_campaign_name | Beeswax Television Prospecting |
| prior_campaign_group_id | **113222** (matches S3) |
| prior_objective_id | 1 |
| prior_funnel_level | **1** (S1) |
| prior_event_type | vast_start + vast_impression (both hit) |
| prior_ip | `172.59.153.228` (exact match to S3 bid_ip) |
| prior_timestamp | 2026-03-11 06:25:32 |
| gap to S3 bid | **0.9 days** (76,151 seconds) |

## Cross-Stage Link Validation

```
S1 impression (2026-03-11 06:25:32)
  campaign: "Beeswax Television Prospecting" (funnel_level=1)
  IP: 172.59.153.228 (vast_start + vast_impression)
    │
    ├── IP enters S3 targeting segment
    │
    ▼
S3 VV (2026-03-12 03:34:43 bid → 2026-03-12 17:40:05 visit)
  campaign: "Beeswax Television Multi-Touch Plus" (funnel_level=3)
  bid_ip: 172.59.153.228 ← MATCHES S1 VAST IP
  clickpass_ip: 136.60.130.233 (cross-device visit)

campaign_group_id: 113222 (SAME for both S1 and S3)
Time between S1 impression and S3 bid: 0.9 days
```

This proves the IP was exposed to an S1 (prospecting) ad, entered the S3 targeting segment, and generated a Stage 3 verified visit — all within the same campaign group.
