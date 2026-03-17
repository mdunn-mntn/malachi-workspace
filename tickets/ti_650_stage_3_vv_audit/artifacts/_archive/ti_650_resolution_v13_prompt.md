# TI-650: Resolution Rate v13 — Full S3→S2→S1 Chain Resolution

## Context

Read `tickets/ti_650_stage_3_vv_audit/summary.md` for full context. This is the Stage 3 VV IP Lineage audit.

## Problem

The current resolution rate query (`queries/ti_650_resolution_rate_multi.sql`) only checks **S3→S1 direct**. It skips S2 entirely. The correct resolution must try **S3→S2 first**, then chain S2→S1. Only if S3→S2 fails should it fall back to S3→S1 direct.

## The Resolution Logic

The hop pattern is always the same: **ad_served_id → bid_ip → vast_ip on previous stage**.

### S1 VV
Done. `ad_served_id` links directly to its own impression. No cross-stage hop needed.

### S2 VV
1. `ad_served_id` → get this VV's **bid_ip** (from `cost_impression_log`)
2. `bid_ip` → match to **S1 vast_ip** (event_log vast_start/vast_impression on S1 campaigns)
3. That S1 match gives you the S1 impression's `ad_served_id` → its bid_ip, IPs, timestamps

### S3 VV
1. `ad_served_id` → get this VV's **bid_ip** (from `cost_impression_log`)
2. `bid_ip` → try to match **S2 vast_ip** first (event_log vast_start/vast_impression on S2 campaigns)
3. **If S2 match found:** get that S2 impression's `ad_served_id` → its **bid_ip** → match to **S1 vast_ip** → get S1 impression details
4. **If NO S2 match:** fall back to `bid_ip` → match **S1 vast_ip** directly
5. **If neither:** unresolved / lost

Also check **imp_visit** at each hop: `ui_visits.impression_ip` can substitute for `bid_ip` as the lookup key into the previous stage's vast_ip pool.

## The Hop (repeated at each stage transition)

```
current_stage.bid_ip  →  previous_stage.vast_start_ip OR vast_impression_ip
```

- `bid_ip` comes from `cost_impression_log.ip` for the current ad_served_id
- `vast_start_ip` / `vast_impression_ip` come from `event_log.ip` filtered by event_type_raw
- Both vast IPs must be in the pool (they differ ~0.15% due to SSAI proxies)
- Within a stage, `ad_served_id` is deterministic (100% link between VV and its impression)

## Pool Construction (same pattern at each stage)

For each stage N pool (N = 1 or 2):

```sql
-- Vast IPs from event_log
SELECT advertiser_id, ip AS match_ip, MIN(time) AS impression_time
FROM event_log
JOIN campaigns ON funnel_level = N
WHERE event_type_raw IN ('vast_start', 'vast_impression')
GROUP BY advertiser_id, ip

UNION ALL

-- Bid IPs from CIL (covers display + failed vast events)
SELECT advertiser_id, ip AS match_ip, MIN(time) AS impression_time
FROM cost_impression_log
JOIN campaigns ON funnel_level = N
GROUP BY advertiser_id, ip

-- Dedup outer: GROUP BY (advertiser_id, match_ip), MIN(impression_time)
```

**CRITICAL for S3→S2→S1 chain:** After matching S3.bid_ip to an S2 vast_ip, you need that S2 impression's **bid_ip** to do the next hop into S1. The vast_ip pool alone won't give you that — you need to go back to the S2 impression (via ad_served_id or by storing bid_ip alongside the vast_ip match).

## Scoping Rules

- **Prospecting only:** `objective_id IN (1, 5, 6)` at ALL funnel levels
- **funnel_level is authoritative for stage** (not objective_id — objective_id is unreliable, 48K S3 campaigns have obj=1)
- **Retargeting (obj=4) exists at EVERY funnel_level** — must filter by objective_id, not funnel_level alone
- **Date range:** VVs from Feb 4–11 2026, 90-day lookback for pools (back to Nov 6 2025)
- **Top 10 advertisers** by VV volume (parameterize LIMIT for scaling to 20/40)
- **90-day max lookback** — Zach confirmed max window = 88 days (14+30+14+30)

## Expected Output

Per advertiser, per funnel_level (S2 and S3 only — S1 is always 100%):

| Column | Description |
|--------|-------------|
| advertiser_id | |
| funnel_level | 2 or 3 |
| total_vvs | Total VVs at this stage |
| resolved | VVs with any S1 trace found |
| unresolved | VVs with bid_ip but no S1 trace |
| resolved_pct | resolved / total for S2+S3 |
| s3_via_s2_s1 | S3 VVs resolved through S2 chain (S3→S2→S1) |
| s3_direct_s1 | S3 VVs resolved directly to S1 (S3→S1) |
| imp_direct_count | Resolved via bid_ip → vast_ip |
| imp_visit_count | Resolved via impression_ip → vast_ip |
| unresolved_xdevice | Unresolved with is_cross_device = TRUE |

## Key Files to Read

- `tickets/ti_650_stage_3_vv_audit/summary.md` — full architecture, all findings, schema
- `queries/ti_650_resolution_rate_multi.sql` — current query (S3→S1 direct only, 87s for 10 advs)
- `queries/ti_650_resolution_rate_fast.sql` — single-advertiser baseline (adv 37775)
- `outputs/ti_650_multi_adv_resolution_rates.md` — current results without S2 chain
- `outputs/ti_650_unresolved_ip_origin.md` — unresolved IPs trace to retargeting campaigns

## Performance Notes

- event_log 90-day scan = 27.7B rows, ~26K slot-seconds per scan (unavoidable)
- Adding S2 pool = second event_log scan. Expect ~2x cost (~170s for 10 advertisers)
- **Do NOT try CTE pivoting** — tested, BQ re-scans CTEs per reference, resulted in 2x slower
- Direct `GROUP BY ip` with `UNION ALL` is the optimal pattern for pool construction
- Target: <5 min wall time for 10 advertisers

## BQ Access

Authenticated as `malachi@mountain.com`. Run queries directly.

```bash
# Use the perf wrapper:
bash .claude/scripts/bq_run.sh --ticket "TI-650" --label "description" \
  --use_legacy_sql=false --format=prettyjson --max_rows=100 --project_id=dw-main-silver \
  'SQL HERE'

# Or plain bq for dry runs:
bq query --use_legacy_sql=false --dry_run --project_id=dw-main-silver 'SQL'
```

**Note:** If SQL starts with `--` comments, pipe via stdin (`< file.sql`) to avoid bq flag parsing errors.

## Safety

- LIMIT on every SELECT returning raw rows
- Date filter on every log table
- `--dry_run` before unfamiliar queries, abort if >5GB
- Read-only access — no DDL/DML
