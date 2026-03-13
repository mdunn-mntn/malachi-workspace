# TI-650 v16 Step 3: 365-Day Prior-Funnel IP Lookup for Unresolved VV

## Context

We're working on TI-650 (Stage 3 VV Audit). Read `tickets/ti_650_stage_3_vv_audit/summary.md` for full context.

**What we've built so far:**
- `queries/ti_650_ip_funnel_trace_cross_stage.sql` — parameterized cross-stage IP trace. Takes an `ad_served_id` + `vv_date` in a `params` CTE, traces it through all 5 within-stage tables (bid_logs → win_logs → impression_log → event_log → clickpass_log), then links cross-stage to prior-funnel (S1/S2) vast events within the same `campaign_group_id`. Shows the full within-stage trace even when no cross-stage match exists (prior columns are NULL).

**The problem:** We have 50 S3 VVs (from `outputs/ti_650_v15_unresolved_ids.json`) that could not be resolved to any prior-funnel impression via IP matching within the same `campaign_group_id`. We tested one — `80207c6e-1fb9-427b-b019-29e15fb3323c` — and confirmed:
- The within-stage trace works: bid_ip = `216.126.34.185`, campaign_group_id = `93957`, vv_date = `2026-02-04`
- **No cross-stage match at 90 or 180 days** within the same campaign_group_id
- Prior columns are all NULL — this IP never appeared in any S1/S2 vast event for campaign_group 93957

## What to do

1. First run the cross-stage query as-is to get the full within-stage trace (params are already set to the unresolved VV). Confirm you see the within-stage trace with NULL prior columns.

2. Then search event_log for **365 days back** to see if bid_ip `216.126.34.185` ever appeared in a `vast_impression` or `vast_start` event — **across ANY campaign, not just campaign_group_id 93957**. We want to know if this IP was ever served an MNTN ad, anywhere.

```sql
-- Search for the unresolved VV's bid_ip across ALL campaigns in event_log
SELECT
  campaign_id,
  event_type_raw,
  COUNT(*) AS event_count,
  MIN(DATE(time)) AS earliest,
  MAX(DATE(time)) AS latest
FROM `dw-main-silver.logdata.event_log`
WHERE ip = "216.126.34.185"
  AND event_type_raw IN ('vast_impression', 'vast_start')
  AND DATE(time) >= DATE_SUB(DATE("2026-02-04"), INTERVAL 365 DAY)
  AND DATE(time) < DATE("2026-02-04")
GROUP BY campaign_id, event_type_raw
ORDER BY earliest
LIMIT 20
```

3. If results come back, join to `bronze.integrationprod.campaigns` to get `campaign_group_id`, `funnel_level`, `advertiser_id`, and `name` — so we can see if this IP appeared in:
   - A different campaign_group (coincidental match, not valid funnel trace)
   - A retargeting campaign (obj=4) in the same campaign_group
   - A completely different advertiser

4. If NO results come back even at 365 days across all campaigns, that conclusively proves this IP entered S3 purely via the identity graph (tmul_daily data_source_id=3) and was never served any MNTN ad. Document this finding.

## Key facts

- **event_log data range:** History table covers 2025-01-01 to 2025-12-31 (365 day-partitions). Current SQLMesh table covers 2026-01-01 onward. Full coverage from Jan 1, 2025.
- **event_log is partitioned by `time` (DAY)** — no clustering. IP filter requires full partition scan per day.
- **Dry-run first** — 365 days of event_log will be expensive. Check bytes before running.
- `DATE(time)` for date filtering on event_log (no `dt` column — SQLMesh view).
- event_log.campaign_id is INTEGER, campaigns.campaign_id is also INTEGER (no cast needed).
- The 50 unresolved IPs are 80% T-Mobile CGNAT (172.56/58/59.x.x). IP `216.126.34.185` is NOT CGNAT — it's a static/residential IP, which makes it a more interesting test case.

## Constraints
- LIMIT required on all raw-row queries
- Dry-run unfamiliar queries first; abort if >5GB (but event_log scans are known to be large — use judgment)
- No DDL/DML — read-only

## Files
- Cross-stage query: `queries/ti_650_ip_funnel_trace_cross_stage.sql` (params set to unresolved VV)
- Unresolved IDs: `outputs/ti_650_v15_unresolved_ids.json` (50 ad_served_ids with bid_ips and campaign_group_ids)

Save any new query to `tickets/ti_650_stage_3_vv_audit/queries/` and results to `tickets/ti_650_stage_3_vv_audit/outputs/`. Update `summary.md` with findings.
