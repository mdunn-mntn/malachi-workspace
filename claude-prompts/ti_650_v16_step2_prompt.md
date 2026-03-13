# TI-650 v16 Step 2: Cross-Stage IP Linking

## Context

We're working on TI-650 (Stage 3 VV Audit). Read `tickets/ti_650_stage_3_vv_audit/summary.md` for full context.

**Step 1 is complete.** We built a within-stage IP trace query (`queries/ti_650_ip_funnel_trace.sql`) that traces a single `ad_served_id` across all 5 pipeline tables, showing the IP and timestamp at each stage:

```
bid_logs.ip → win_logs.ip → impression_log.ip → event_log.ip (vast_impression) → event_log.ip (vast_start) → clickpass_log.ip
```

**Linking architecture (within-stage):**
- `ad_served_id` links clickpass_log, event_log, and impression_log (MNTN-side tables)
- `impression_log.ttd_impression_id = win_logs.auction_id = bid_logs.auction_id` bridges to Beeswax-side tables

**Campaign context:** We join `bronze.integrationprod.campaigns` on `campaign_id` (cast to INT64) to get `campaign_group_id`, `objective_id`, `funnel_level`, and campaign `name`.

## Step 2: What to build

Take a Stage 3 VV (funnel_level = 3, objective_id = 1) and link it **cross-stage** to a prior-funnel impression.

Specifically:
1. Start with the S3 VV's `bid_ip` (from `bid_logs` via the step 1 trace)
2. Find a matching `vast_impression` or `vast_start` IP in `event_log` that:
   - Is from a campaign with `funnel_level` = 1 or 2
   - Is within the **same `campaign_group_id`** as the S3 campaign
   - Occurred **before** the S3 VV's bid timestamp
3. This proves the IP was exposed to an earlier-funnel ad before entering S3 targeting

**Cross-stage linking key (validated in prior versions):**
```
S3.bid_ip = S1_or_S2.event_log.ip (vast_impression or vast_start)
```

Within the same `campaign_group_id`. The VAST IP is what enters the next stage's targeting segment.

## Constraints
- `campaign_group_id` scoping is mandatory (Zach directive). No cross-campaign-group matching.
- Prospecting only: `objective_id IN (1, 5, 6)`. Exclude retargeting (4) and ego (7).
- `funnel_level` is authoritative for stage (not `objective_id` — 48,934 S3 campaigns have obj=1 due to UI bug).
- 90-day lookback max for the S1/S2 impression pool.
- Use `DATE(time)` for date filtering on event_log (no `dt` column — SQLMesh view).
- Always dry-run unfamiliar queries first. Date filter + LIMIT required on all log tables.

## Step 1 query for reference

```sql
WITH serve AS (
  SELECT ad_served_id, ttd_impression_id, ip AS impression_ip, time AS impression_timestamp
  FROM `dw-main-silver.logdata.impression_log`
  WHERE DATE(time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    AND ad_served_id = "13cc841f-7dd4-4e88-a649-ea37c4b6ab93"
  LIMIT 1
)
SELECT
  s.ad_served_id,
  s.ttd_impression_id                AS auction_id,
  cl.advertiser_id,
  cl.campaign_id,
  camp.name                          AS campaign_name,
  camp.campaign_group_id,
  camp.objective_id,
  camp.funnel_level,
  b.ip                               AS bid_ip,
  b.time                             AS bid_timestamp,
  w.ip                               AS win_ip,
  w.time                             AS win_timestamp,
  s.impression_ip,
  s.impression_timestamp,
  ev_imp.ip                          AS event_impression_ip,
  ev_imp.time                        AS event_impression_timestamp,
  ev_start.ip                        AS event_start_ip,
  ev_start.time                      AS event_start_timestamp,
  cl.ip                              AS clickpass_ip,
  cl.time                            AS clickpass_timestamp
FROM serve s
LEFT JOIN `dw-main-silver.logdata.clickpass_log` cl
  ON cl.ad_served_id = s.ad_served_id
  AND DATE(cl.time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
LEFT JOIN `dw-main-bronze.integrationprod.campaigns` camp
  ON camp.campaign_id = CAST(cl.campaign_id AS INT64)
  AND camp.deleted = FALSE
LEFT JOIN `dw-main-silver.logdata.event_log` ev_start
  ON ev_start.ad_served_id = s.ad_served_id
  AND ev_start.event_type_raw = "vast_start"
  AND DATE(ev_start.time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
LEFT JOIN `dw-main-silver.logdata.event_log` ev_imp
  ON ev_imp.ad_served_id = s.ad_served_id
  AND ev_imp.event_type_raw = "vast_impression"
  AND DATE(ev_imp.time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
LEFT JOIN `dw-main-silver.logdata.win_logs` w
  ON w.auction_id = s.ttd_impression_id
  AND DATE(w.time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
LEFT JOIN `dw-main-silver.logdata.bid_logs` b
  ON b.auction_id = s.ttd_impression_id
  AND DATE(b.time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
;
```

## Example ad_served_id to use

`13cc841f-7dd4-4e88-a649-ea37c4b6ab93` — Stage 3 (funnel_level=3, objective_id=1), advertiser 53308, campaign_group_id 114107. Bid IP = `172.59.153.228`, clickpass IP = `136.60.130.233`. The bid/win/serve/VAST IPs are consistent; the clickpass differs (cross-device, T-Mobile CGNAT → different ISP).

## Deliverable

A query that extends step 1 to also show the prior-funnel (S1 or S2) impression that the S3 VV's bid_ip matched, including:
- The prior-funnel `ad_served_id`, `campaign_id`, `funnel_level`, `event_type_raw`
- The prior-funnel IP and timestamp
- Confirmation that `campaign_group_id` matches

Save the query to `tickets/ti_650_stage_3_vv_audit/queries/ti_650_ip_funnel_trace_cross_stage.sql` and update `summary.md`.
