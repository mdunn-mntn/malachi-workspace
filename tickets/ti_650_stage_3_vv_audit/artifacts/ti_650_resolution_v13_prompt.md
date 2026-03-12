# TI-650: Resolution Rate v13 — Proper S3→S2→S1 Chain Resolution

## Context

Read `tickets/ti_650_stage_3_vv_audit/summary.md` for full context. This is the Stage 3 VV IP Lineage audit.

## Problem

The current resolution rate query (`queries/ti_650_resolution_rate_multi.sql`) only checks **S3→S1 direct**. It does NOT check **S3→S2→S1** (chain through S2). This means some S3 VVs are marked "unresolved" when they should resolve via the S2 chain.

## What needs to happen

Build a resolution rate query that checks BOTH paths for S3 VVs:

1. **S3→S2 first:** Does the S3 VV's bid_ip exist as a vast_start_ip or vast_impression_ip on any S2 prospecting impression (funnel_level=2, objective_id IN 1,5,6) for the same advertiser, before the VV time?
2. **If S3→S2 matches:** Take that S2 impression's bid_ip and check if IT exists as a vast_start_ip or vast_impression_ip on any S1 prospecting impression (funnel_level=1, objective_id IN 1,5,6) for the same advertiser, before the S2 impression time.
3. **If S3→S2 does NOT match:** Fall back to S3→S1 direct (same as current query).

The key: S3.bid_ip → S2.vast_ip gets you to S2, then S2.bid_ip → S1.vast_ip gets you to S1.

## Cross-stage matching key (validated, do not change)

- **Within stage:** `ad_served_id` links VV to its impression (deterministic, 100%)
- **Cross stage:** `next_stage.bid_ip` → `prev_stage.vast_start_ip OR vast_impression_ip`
- Both vast IPs must be in the pool (they differ ~0.15% due to SSAI proxies)
- The vast IP is the IP that enters the next stage's targeting segment

## S1 pool construction (validated, do not change)

UNION ALL of:
- `event_log.ip` for `event_type_raw IN ('vast_start', 'vast_impression')` on S1 campaigns
- `cost_impression_log.ip` (= bid_ip) on S1 campaigns
Deduped by `(advertiser_id, match_ip)`, earliest impression wins.

## S2 pool construction (new — same pattern as S1)

UNION ALL of:
- `event_log.ip` for `event_type_raw IN ('vast_start', 'vast_impression')` on S2 campaigns
- `cost_impression_log.ip` on S2 campaigns
Deduped by `(advertiser_id, match_ip)`, earliest impression wins.

**CRITICAL:** For the S3→S2→S1 chain, after matching S3.bid_ip to S2 vast_ip, you need the S2 impression's **bid_ip** (NOT the vast_ip) to look up in the S1 pool. This means you need the S2 impression's `ad_served_id` to get its bid_ip from CIL or event_log.bid_ip.

## Additional links to also check (same as current)

- **imp_visit:** `ui_visits.impression_ip` → S1/S2 pool (same matching logic)

## Scoping rules

- **Prospecting only:** `objective_id IN (1, 5, 6)` at ALL funnel levels
- **funnel_level is authoritative for stage** (not objective_id — Ray confirmed obj_id is unreliable)
- **Date range:** VVs from Feb 4-11, 90-day lookback for S1/S2 pools (back to Nov 6)
- **Top 10-20 advertisers** by VV volume (parameterize LIMIT)

## Expected output

Per advertiser, per funnel_level:
- total_vvs, resolved, unresolved, resolved_pct
- Breakdown by resolution path: s3_to_s2_to_s1, s3_to_s1_direct, imp_visit
- cross_device count for unresolved

## Key files to read

- `tickets/ti_650_stage_3_vv_audit/summary.md` — full architecture and findings
- `queries/ti_650_resolution_rate_multi.sql` — current query (S3→S1 direct only, works, 87s for 10 advs)
- `queries/ti_650_resolution_rate_fast.sql` — single-advertiser baseline (adv 37775)
- `outputs/ti_650_multi_adv_resolution_rates.md` — current results (S2: 99.45%, S3: 62-97%)
- `outputs/ti_650_unresolved_ip_origin.md` — unresolved IPs only appear in retargeting campaigns

## Performance notes

- event_log 90-day scan = 27.7B rows, ~26K slot-seconds (unavoidable)
- Adding S2 pool will add another event_log scan — expect ~2x cost
- Don't try to optimize with CTE pivoting (tested, BQ re-scans CTEs per reference, 2x slower)
- Direct GROUP BY ip with UNION ALL is the optimal pattern for pool construction
- Target: <5 min wall time for 10 advertisers
