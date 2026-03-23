# TI-650: S3 Resolution — Execution Prompt

**STATUS: COMPLETE (2026-03-18)**

## Results Summary

| Stage | Resolution | Method |
|-------|-----------|--------|
| S1 | **100%** (93,274/93,274) | ad_served_id (deterministic) |
| S2→S1 | **100%** (68,498/68,498) | bid_ip → S1 impression pool + CIDR fix + 4-table pool. 90d sufficient. |
| S3 (90d) | **96.47%** (568,839/589,630) | T1-T4 tier structure, 5-source IP trace, no CIL |
| S3 (180d) | **100.00%** (589,628/589,630) | Same tiers, extended lookback. 2 unresolved = IP untraceable within 180d |

**Bottom-up validation: S1 (100%), S2 (100%), S3 (100%). All stages validated.**

---

## Context

We're building `audit.vv_ip_lineage` — one row per verified visit (VV) tracing IP through the full funnel. Bottom-up validation: S1 (100% ✅), S2→S1 (100% ✅), S3 (100% ✅).

**S3 was the real problem.** v20 multi-advertiser results showed adv 31357 at 74.54% S3 resolution — worst of 10 tested advertisers. Most others were 98-99%. Zach said WGU (adv 31357) has "abnormally long S3 lookback window." **RESOLVED:** 180d lookback achieves 100%.

## What's Been Proven

- **S1:** 100% resolved via `ad_served_id` — deterministic, no IP matching needed
- **S2→S1:** 100% resolved (68,498/68,498) — cross-stage via `bid_ip → S1 impression pool` within same `campaign_group_id`. CIDR fix + 4-table S1 pool. 90d lookback sufficient (v21c verified).
- **S3 architecture (Zach breakthrough, v20):** S3 targeting is VV-based, not impression-based. Cross-stage link is `S3.bid_ip → clickpass_log.ip` (prior S1/S2 VV), NOT `S3.bid_ip → event_log.ip`. In cross-device, VV clickpass IP ≠ impression bid IP.

## Task: Execute the S3 Resolution Plan

Read the full plan at `.claude/plans/memoized-swinging-rocket.md`. Execute it in order:

### Step 1: Create and run `queries/ti_650_s3_lookback_analysis_31357.sql`

**Purpose:** Measure gap distribution for S3 VVs with 180d pool lookback. Determines whether 90d is sufficient.

**Structure:**
- S3 VVs from clickpass_log (funnel_level=3, obj IN (1,5,6), adv 31357, Feb 4-11)
- 5-source IP trace for bid_ip: bid_logs > win_logs > impression_log > viewability_log > event_log (via ad_served_id + ttd_impression_id→auction_id bridge). COALESCE priority same order.
- VV pool (180d): clickpass_log, funnel_level IN (1,2), obj IN (1,5,6), same advertiser. GROUP BY (campaign_group_id, strip_cidr(ip)) → MIN(time), MAX(time)
- Impression pool (180d): event_log (CTV VAST, strip_cidr) + viewability_log + impression_log, funnel_level IN (1,2), obj IN (1,5,6). UNION ALL, GROUP BY → MIN(time), MAX(time)
- Gap computation: JOIN resolved_ip to both pools. Compute gap_earliest and gap_latest for each.
- Output: max, median, P95, P99 for both pools. Count within_90d, beyond_90d, latest_after_vv.
- Always compute BOTH MIN and MAX gaps (MIN is biased — we learned this from S2)

**Parameters:** `p_step1_lookback = 90d`, `p_pool_lookback = 180d`

**Template:** Adapt from `queries/ti_650_s2_lookback_analysis.sql` (proven MIN/MAX pattern)

**Dry run first.** Expected ~15-20 TB for WGU.

### Step 2: Analyze lookback results

Two outcomes:
- **MAX gap under 90d for P99+:** 90d confirmed sufficient. Use 90d for resolution query.
- **MAX gap exceeds 90d for significant count:** Use whatever lookback the data demands (120d, 150d, etc.)

### Step 3: Update and run `queries/ti_650_s3_resolution_31357.sql`

**Purpose:** Full-tier S3 resolution diagnostics with validated lookback window.

**Changes from existing file:**
1. Pool lookback: Set based on Step 2 results (start at 180d if unclear)
2. Add T1 (S2 VV bridge chain): `s2_vvs` + `s2_bid_ips` (use CIL for S2 bid_ip — proven 100% = bid_ip, cheaper than 5-source trace) + JOIN to s1_pool
3. Add T2 (S1 VV direct): S3.bid_ip → S1 clickpass_log.ip
4. Keep existing impression fallback as T3

**Tier structure (matching v21):**
- T1: S2 VV bridge chain — S3.bid_ip = S2.clickpass_ip → S2.bid_ip in S1 pool (validated chain)
- T2: S1 VV direct — S3.bid_ip = S1.clickpass_ip
- T3: S1 impression direct — S3.bid_ip in S1 impression pool (event_log + viewability_log + impression_log). Split by source table for diagnostics.
- T4: Net-new from impression fallback (resolved by T3 but NOT T1+T2)

**S2 bid_ip for chain:** Use CIL shortcut (`cost_impression_log.ip` via ad_served_id) — NOT full 5-source trace. v15 proved CIL.ip = bid_ip at 100%. Saves scan cost.

**Step 1 bid_ip for S3 VVs:** Keep full 5-source trace (bid_logs > win_logs > impression_log > viewability_log > event_log). This is needed because not all S3 VVs have CIL records.

**Output columns:**
```
total_s3_vvs
has_bid_ip, has_win_ip, has_impression_ip, has_viewability_ip, has_event_log_ip, has_any_ip, no_ip
t1_s2_vv_bridge_chain, t2_s1_vv_direct
t3_s1_imp_direct (+ breakdown: via_event_log, via_viewability, via_impression)
resolved_vv_only (T1+T2), resolved_vv_only_pct
resolved_all (T1+T2+T3), resolved_all_pct
impression_fallback_net_new
unresolved_with_ip, unresolved_total
```

### Step 4: Document findings

- Create `outputs/ti_650_s3_lookback_analysis.md` with gap distribution results
- Create `outputs/ti_650_s3_resolution_31357.md` with resolution results
- Update `summary.md` with new findings
- Commit and push after each milestone

## Impression Trace Paths (for bid_ip extraction)

```
CTV:                clickpass → event_log(vast) → win_logs → impression_log → bid_logs
Viewable Display:   clickpass → viewability_log → impression_log → win_logs → bid_logs
Non-Viewable Disp:  clickpass → impression_log → win_logs → bid_logs
```

For display, impression_log comes AFTER win_logs (opposite of CTV).

Join keys: MNTN tables use `ad_served_id`; Beeswax tables use `auction_id` (bridged via `impression_log.ttd_impression_id`).

## Key Constraints

- **campaign_group_id scoping** — all matches within same campaign_group_id (Zach directive)
- **Prospecting only** — `objective_id IN (1, 5, 6)` (NOT retargeting obj=4 or ego obj=7)
- **funnel_level is authoritative for stage** — don't rely on objective_id for stage identification
- **strip_cidr()** — on all event_log.ip references: `CREATE TEMP FUNCTION strip_cidr(ip STRING) AS (SPLIT(ip, '/')[SAFE_OFFSET(0)])`
- **Temporal ordering** — S1/S2 pool event must be BEFORE S3 VV time
- **MAX not MIN** — for lookback gap analysis, use most recent match (MAX), not earliest (MIN). MIN is biased.

## Reference Files

- `queries/ti_650_s2_resolution_31357.sql` — S2 query (v8), full 5-source trace template
- `queries/ti_650_s2_lookback_analysis.sql` — S2 lookback analysis, MIN/MAX gap template
- `queries/ti_650_resolution_rate_v21.sql` — Multi-advertiser v21, tier structure reference
- `queries/ti_650_s3_resolution_31357.sql` — S3 resolution query (FINAL: 180d lookback, T1-T4 tiers)
- `queries/ti_650_s3_lookback_analysis_31357.sql` — S3 lookback gap analysis (180d pool, VV-only)
- `artifacts/ti_650_s3_resolution_prompt.md` — Original S3 resolution prompt (background)
- `.claude/plans/memoized-swinging-rocket.md` — Full plan with detailed CTE structures

## Execution Log

### Step 1: Lookback analysis ✅
- Query: `queries/ti_650_s3_lookback_analysis_31357.sql`
- Simplified to VV-pool-only (dual-pool version OOM'd at 16.3 TB)
- **Result:** 589,627/589,630 matched (99.999%), P99=89d, max=152d
- BQ: 8.8 TB, 4:54 runtime
- Output: `outputs/ti_650_s3_lookback_analysis_31357_results.json`

### Step 2: Analyze lookback ✅
- P99=89d, max=152d → 90d NOT sufficient for WGU (P99 right at boundary)
- Decision: run resolution at both 90d (baseline) and 180d (full coverage)
- Gap decomposition: `outputs/ti_650_s3_lookback_vs_resolution_analysis.md`

### Step 3a: Resolution (90d) ✅
- Query: `queries/ti_650_s3_resolution_31357.sql` (step1_lookback=2025-11-06)
- **Result:** 568,839/589,630 (96.47%)
- BQ: 9.2 TB, 3:09 runtime
- Output: `outputs/ti_650_s3_resolution_31357_results.json`

### Step 3b: Resolution (180d) ✅
- Query: same file with step1_lookback=2025-08-08
- **Result:** 589,628/589,630 (**100.00%**) — only 2 unresolved (0.0003%)
- BQ: 18.2 TB, 1:43 runtime
- Output: `outputs/ti_650_s3_resolution_31357_180d_results.json`

### Step 4: Documentation ✅
- Full analysis: `outputs/ti_650_s3_resolution_31357_analysis.md`
- Summary updated with v22 findings (#31, #32, #33)
- Production lookback recommendation table added
- All committed and pushed

### Key deviations from plan:
1. **No CIL anywhere** — used impression_log + bid_logs for S2 bid_ip extraction instead of CIL shortcut (user directive)
2. **Lookback simplified to VV-pool-only** — dual-pool (VV + impression) query OOM'd; VV pool is the primary resolver per Zach breakthrough, impression pool is T3 fallback
3. **Ran resolution at BOTH 90d and 180d** — 90d as baseline to quantify gap, 180d for full coverage
