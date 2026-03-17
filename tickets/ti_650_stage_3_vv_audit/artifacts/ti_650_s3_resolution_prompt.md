# TI-650: S3 Cross-Stage Resolution — Bottom-Up Validation Prompt

## Context

We're building `audit.vv_ip_lineage` — one row per verified visit (VV) tracing IP through the full funnel. We're validating resolution rates bottom-up: S1 (100% ✅), S2 (100% ✅), now S3.

**S3 is the real problem.** v20 multi-advertiser results show adv 31357 at 74.54% S3 resolution — worst of 10 tested advertisers. Most others are 98-99%.

## What's Been Proven

### S1: 100% resolved (within-stage)
All S1 VVs resolve via `ad_served_id` — deterministic, no IP matching needed.

### S2: 100% resolved (cross-stage, adv 31357)
S2 VVs trace back to S1 impressions via IP matching within `campaign_group_id`:
- **Method:** S2 VV → get bid_ip from source tables (bid_logs/win_logs/impression_log via ad_served_id + auction_id bridge) → match against S1 pool (event_log, viewability_log, impression_log, clickpass_log)
- **Two fixes needed for 100%:** (1) `strip_cidr()` on event_log.ip (pre-2026 has /32 suffix), (2) expanded S1 pool (all 4 source tables + clickpass_log)
- **Lookback CORRECTED:** Initial MIN-based analysis showed 186d max, but using MOST RECENT match: max 69d, median 6d, P95 29d. 90d lookback is sufficient (zero IPs have latest S1 match >90d before VV).
- Query: `queries/ti_650_s2_resolution_31357.sql` (v8)

### S3 architecture (Zach breakthrough, v20)
S3 targeting is **VV-based, not impression-based.** You can't enter S3 without a prior verified visit (S1 or S2). The cross-stage link is:
- `S3.bid_ip → clickpass_log.ip` (prior S1 or S2 VV in same campaign_group_id)

This is different from S2→S1 (impression-based: `S2.bid_ip → event_log/viewability_log/impression_log.ip`).

In cross-device scenarios, the VV clickpass IP is completely different from the impression bid IP (e.g., CTV impression on `172.59.117.71` → iPhone VV clickpass on `216.126.34.185` → S3 targets `216.126.34.185`).

## Task: Build S3 Resolution Query for Advertiser 31357

### Approach
Same bottom-up pattern as the S2 investigation. Single-advertiser deep dive, same VV window (Feb 4-11, 2026).

### Step 1: Get S3 VVs and their bid_ip
- Source: `clickpass_log` WHERE `funnel_level = 3` AND `objective_id IN (1, 5, 6)` AND `advertiser_id = 31357`
- Get bid_ip: trace via `ad_served_id` through impression_log → `ttd_impression_id` → bid_logs/win_logs `auction_id`
- Also get clickpass_log.ip directly (this IS the VV IP)
- Apply `strip_cidr()` on any event_log.ip references
- COALESCE priority: bid_logs.ip > win_logs.ip > impression_log.ip > viewability_log.ip > event_log.ip

### Step 2: Build S1+S2 VV pool (clickpass_log.ip)
This is the PRIMARY resolver. S3 targeting requires a prior VV.
- Source: `clickpass_log` WHERE `funnel_level IN (1, 2)` AND `objective_id IN (1, 5, 6)` AND same `campaign_group_id`
- Match: `S3_bid_ip = S1_or_S2_clickpass_ip` WHERE `S1/S2 VV time < S3 VV time`
- **Lookback: 90 days** from VV start date (sufficient per corrected analysis; 180d safe but wasteful)

### Step 3: Build S1+S2 impression pool (fallback)
Only check this for VVs that DON'T resolve via clickpass_log:
- event_log (CTV VAST): `strip_cidr(el.ip)`, `event_type_raw IN ('vast_start', 'vast_impression')`
- viewability_log (viewable display)
- impression_log (non-viewable display)
- All scoped to same `campaign_group_id`, `funnel_level IN (1, 2)`, 90d lookback

### Expected Output
```
total_s3_vvs
has_bid_ip          -- Step 1: IP coverage
s3_via_clickpass    -- Step 2: resolved via prior VV clickpass_log.ip (PRIMARY)
s3_via_event_log    -- Step 3: resolved via impression (fallback)
s3_via_viewability
s3_via_impression
resolved_vv_only    -- clickpass_log only
resolved_vv_only_pct
resolved_all        -- clickpass + impression fallback
resolved_all_pct
unresolved_with_ip  -- has bid_ip but no S1/S2 match
unresolved_total
```

### Key Constraints
- **campaign_group_id scoping** — all matches within same campaign_group_id (Zach directive)
- **Prospecting only** — `objective_id IN (1, 5, 6)` (NOT retargeting obj=4 or ego obj=7)
- **funnel_level is authoritative for stage** — don't rely on objective_id for stage identification (48,934 S3 campaigns have obj=1 instead of 6, UI migration bug)
- **90-day lookback** — from VV start date for S1/S2 pools (corrected: 90d covers 100% using most recent match; 180d is safe but unnecessary)
- **strip_cidr()** — on all event_log.ip references: `CREATE TEMP FUNCTION strip_cidr(ip STRING) AS (SPLIT(ip, '/')[SAFE_OFFSET(0)])`
- **Temporal ordering** — S1/S2 pool event must be BEFORE S3 VV time

### Step 4: Measure full chain depth (critical for lookback tuning)
For every resolved S3 VV, compute `S3_vv_time - MIN(earliest_matching_event_time)` across the full chain. This answers: "for a given week/month of S3 VVs, how far back do we need to look within the same campaign_group_id to catch 100% of traces?"

The chain can stack multiple gaps:
```
S3 VV (Feb 4-11) → prior S2 VV (day -A) → S2 impression (day -B) → S1 impression (day -C)
                                                                      C = total lookback
```

For S2→S1, corrected analysis shows max 69 days (median 6d) using most recent match. The initial 186-day figure was biased by MIN(impression_time). S3 adds another hop via the VV bridge, but the most recent match principle applies here too. Measure the distribution (median, P95, P99, max) to determine the minimum lookback. **IMPORTANT:** Always use MAX(impression_time) WHERE time < vv_time, not MIN. The question is: "what's the MOST RECENT prior S1/S2 event that matches this S3 VV's bid_ip within the same campaign_group_id?"

### Hypothesis
S3 should be resolvable near-100% via `clickpass_log.ip` since you can't enter S3 without a prior VV. The 74.54% rate for adv 31357 in v20 may be low due to:
1. Insufficient lookback (less likely after S2 correction — 90d covers S2→S1; but S3 may have different patterns)
2. CIDR mismatch on event_log.ip in the impression fallback
3. Possibly some S3 VVs entered via identity graph paths not traceable through clickpass_log

Start with clickpass_log as the primary resolver. If it doesn't reach near-100%, investigate what's different about the unresolved VVs before expanding to other tables.

### Reference Files
- `queries/ti_650_s2_resolution_31357.sql` — S2 query (v8), use as template
- `queries/ti_650_resolution_rate_v21.sql` — Multi-advertiser v21, has the full tier structure
- `queries/ti_650_s1_resolution_31357.sql` — S1 within-stage (reference only)
- `summary.md` — Full findings history

### Impression Trace Paths (for bid_ip extraction)
```
CTV:                clickpass → event_log(vast) → win_logs → impression_log → bid_logs
Viewable Display:   clickpass → viewability_log → win_logs → bid_logs
Non-Viewable Disp:  clickpass → impression_log → win_logs → bid_logs
```
Join keys: MNTN tables use `ad_served_id`; Beeswax tables use `auction_id` (bridged via `impression_log.ttd_impression_id`).
