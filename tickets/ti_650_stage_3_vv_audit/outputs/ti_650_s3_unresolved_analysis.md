# TI-650: S3 Unresolved VV Analysis

**Advertiser:** 31357 (WGU)
**VV Window:** 2026-02-04 to 2026-02-11
**Lookback:** 180d (2025-08-08)

## The 6 Unresolved VVs

At 180d lookback, the S3 resolution query reports:

| Metric | Count | % |
|--------|-------|---|
| Total S3 VVs | 589,630 | 100% |
| Resolved via VV path (T1+T2) | 589,626 | 99.9993% |
| **Unresolved via VV path** | **4** | 0.0007% |
| Resolved via all tiers (T1+T2+T3) | 589,628 | 99.9997% |
| **Truly unresolved** | **2** | 0.0003% |

### Breakdown of the 6

| Category | Count | Meaning |
|----------|-------|---------|
| **VV-only unresolved, T3 recovers** | 2 | S3.bid_ip not in any S1/S2 VV clickpass pool, but IS in S1 impression pool |
| **Truly unresolved** | 2 | S3.bid_ip not in any S1/S2 pool (VV or impression) within 180d |
| **Total "unresolved"** | **4** (VV path) / **2** (all paths) | |

Note: The user's "6 unresolved" counts T1+T2 misses (4) + T3 misses (2) as separate items. Technically it's 4 VVs unresolved by VV-only, of which 2 are recovered by T3 impression fallback, leaving 2 truly unresolved.

## Hypotheses for the 4 VV-Path Unresolved

### Most likely: Lookback boundary (>180d gap)

The 180d lookback window starts at 2025-08-08 for a VV window ending 2026-02-11. If a user:
1. Had an S1/S2 VV **before** 2025-08-08
2. Then triggered an S3 VV in the Feb 4-11 window

...the prior VV would fall outside our 180d lookback. This is the most plausible explanation because:
- WGU's max observed VV-to-VV gap is **152 days** (from lookback analysis)
- 180d covers the full observed distribution
- But the lookback analysis measured P99=89d and max=152d from the **same 180d pool** — there could be gaps >180d that are unobservable by definition

### Less likely: Cross-device/CGNAT IP rotation

If a user's IP changed between S1/S2 VV and S3 VV:
- CGNAT: ISP rotated the shared IP address
- Cross-device: user moved between networks (home WiFi → mobile → office)
- VPN: user was on VPN during one visit but not the other

This would make the IP unmatchable regardless of lookback window.

### Least likely: Data quality

- Missing clickpass_log rows (pipeline gaps)
- CIDR stripping inconsistency (already applied everywhere)
- Campaign misconfiguration (wrong funnel_level/objective_id)

## What the Diagnostic Query Will Show

**Query:** `ti_650_s3_unresolved_simple.sql`
**Status:** Running (started 2026-03-18 23:17, concurrent slot contention with correlated diagnostic)

When complete, we'll have for each unresolved VV:
- `ad_served_id` — unique VV identifier
- `clickpass_ip` — the S3 VV IP
- `vv_time` — when the S3 VV happened
- `campaign_id` / `campaign_group_id` — scoping
- `resolved_ip` (bid_ip) — the IP we're trying to find in prior pools
- `t1_resolved` / `t2_resolved` — which tiers failed

### Follow-up queries (once we have the ad_served_ids)

1. **Extended lookback search**: Search clickpass_log with NO time constraint for `ip = resolved_ip AND campaign_group_id = X` to check if a prior VV exists at ANY time (confirms lookback hypothesis)
2. **IP history**: Check all logs for the resolved_ip to see when it first appeared for this advertiser
3. **Campaign inspection**: Verify the campaign's funnel_level and objective_id are correct

## Performance Notes

### Query execution times

| Query | Concurrency | Runtime | Notes |
|-------|-------------|---------|-------|
| S3 resolution (optimized, 180d) | Solo | **1:17:03** | First run, `perf_20260318_161548_26940` |
| S3 resolution (pre-optimization, 180d) | Solo | **1:43:28** | Baseline, `bqjob_r3eaa2fec2525504c` |
| S3 diagnostic (correlated, 180d) | Solo | **3:56:14** | `perf_20260318_185039_54664` |
| S3 resolution re-run | 2x concurrent | **2:02:09** | `bqjob_r3798e03d40a7950c` |
| S3 diagnostic re-run (correlated) | 2x concurrent | 12h+ (running) | `bqjob_r22c2706a0e6cc80f` |
| S3 unresolved simple | 2x concurrent | 12h+ (running) | `bqjob_r22e11fde7493b786` |

**Lesson:** Never run two 18TB queries concurrently on the adhoc reservation. Slot contention causes 3-5x runtime inflation.

### Optimized vs pre-optimization

| Metric | Pre-optimization | Optimized | Delta |
|--------|-----------------|-----------|-------|
| Runtime (solo) | 1:43:28 | 1:17:03 | **-25% (-26 min)** |
| Bytes processed | 18.22 TB | 16.98 TB | **-7%** |
| Logical table scans | 16 | 10 | **-37%** |

The 7% byte reduction is modest because the CTE reuse savings are at execution time (fewer physical scans), not at the partition estimation level. The 25% runtime improvement is significant.

## Lookback Recommendation Update

Pending diagnostic results, the working recommendation is:

| Scenario | Lookback | Justification |
|----------|----------|---------------|
| Most advertisers | **90d** | 98-99% resolution |
| WGU (31357) | **180d** | 99.9993% resolution (4 of 589,630 unresolved via VV) |
| Production default | **120d** | Covers P99 for all advertisers |
| Theoretical max | **365d** | Would catch any remaining lookback-related misses |

If the diagnostic confirms the 4 are lookback issues (prior VV exists but >180d ago), the choice is:
- Accept 99.9993% at 180d (recommended — 4 VVs is negligible)
- Extend to 365d for WGU specifically (expensive, diminishing returns)
