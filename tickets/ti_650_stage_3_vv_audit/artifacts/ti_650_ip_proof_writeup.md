# TI-650: IP Cross-Stage Proof — Writeup

IPs within the same stage are 100% traceable since they have the `auction_id` and `ad_served_id`.

However when going from S3 → S1 or S3 → S2 or S2 → S1, we need to link the `bid_logs.ip` of the higher stage/funnel to the previous impression that got it into this stage. Depending on the impression type, the upstream table is:

- **CTV:** `event_log.ip` (`vast_start` or `vast_impression`)
- **Display, Viewable:** `viewability_log.ip`
- **Display, Non-Viewable:** `impression_log.ip`

The match between `bid_logs.ip` and one of these three tables should be 100%. Zach says it MUST be there.

## Step 1: Find the Verified Visit

Start in `clickpass_log` — that's where verified visits land. We're tracing `ad_served_id`: `80207c6e-1fb9-427b-b019-29e15fb3323c`.

```sql
-- Always add a date filter to clickpass_log even with ad_served_id —
-- without it, BQ scans all 2,238+ partitions (110 GB).
SELECT
  ad_served_id,
  campaign_id,
  ip,
  ip_raw,
  guid,
  time,
  epoch,
  advertiser_id,
  creative_id,
  is_new,
  attribution_model_id,
  first_touch_ad_served_id,
  impression_time
FROM `dw-main-silver.logdata.clickpass_log`
WHERE ad_served_id = '80207c6e-1fb9-427b-b019-29e15fb3323c'
  AND time >= TIMESTAMP('2026-02-04') AND time < TIMESTAMP('2026-02-05')
;
```

Result (1 row):
```
ip:                        216.126.34.185
campaign_id:               450300
advertiser_id:             37775
creative_id:               6196122
attribution_model_id:      9
guid:                      80f0805e-6153-3fbc-810a-9f9bd2e718c4
is_new:                    true
first_touch_ad_served_id:  null
time:                      2026-02-04 00:06:14 UTC
impression_time:           2026-01-27 14:53:39 UTC
```

Key detail: the VV happened on **2026-02-04**, but the original impression was on **2026-01-27** (8 days earlier). The upstream tables (bid, win, serve, event_log) need to be queried on the impression date, not the clickpass date.

## Step 2: Trace the IP Through Every Pipeline Stage

Using `ad_served_id` to join across tables, and `impression_log.ttd_impression_id` to bridge to `win_logs`/`bid_logs` via `auction_id`:

```sql
-- Use TIMESTAMP filters (not DATE()) to enable partition pruning.
-- clickpass_date = 2026-02-04, impression_date = 2026-01-27
WITH cl AS (
  SELECT ad_served_id, ip, advertiser_id, campaign_id, time, impression_time,
         attribution_model_id, guid, is_new, first_touch_ad_served_id
  FROM `dw-main-silver.logdata.clickpass_log`
  WHERE time >= TIMESTAMP('2026-02-04') AND time < TIMESTAMP('2026-02-05')
    AND ad_served_id = '80207c6e-1fb9-427b-b019-29e15fb3323c'
  LIMIT 1
)
SELECT
  cl.ad_served_id,
  imp.ttd_impression_id              AS auction_id,
  cl.advertiser_id,
  cl.campaign_id,
  camp.name                          AS campaign_name,
  camp.campaign_group_id,
  camp.objective_id,
  camp.funnel_level,
  cl.attribution_model_id,
  cl.guid,
  cl.is_new,
  cl.first_touch_ad_served_id,
  b.ip                               AS bid_ip,
  b.time                             AS bid_timestamp,
  w.ip                               AS win_ip,
  w.time                             AS win_timestamp,
  imp.ip                             AS impression_ip,
  imp.time                           AS impression_timestamp,
  ev_imp.ip                          AS event_impression_ip,
  ev_imp.time                        AS event_impression_timestamp,
  ev_start.ip                        AS event_start_ip,
  ev_start.time                      AS event_start_timestamp,
  cl.ip                              AS clickpass_ip,
  cl.time                            AS clickpass_timestamp,
  cl.impression_time                 AS clickpass_impression_time,
  (b.ip != cl.ip)                    AS ip_mutated
FROM cl
LEFT JOIN `dw-main-bronze.integrationprod.campaigns` camp
  ON camp.campaign_id = CAST(cl.campaign_id AS INT64)
  AND camp.deleted = FALSE
LEFT JOIN `dw-main-silver.logdata.event_log` ev_imp
  ON ev_imp.ad_served_id = cl.ad_served_id
  AND ev_imp.event_type_raw = 'vast_impression'
  AND ev_imp.time >= TIMESTAMP('2026-01-27') AND ev_imp.time < TIMESTAMP('2026-01-28')
LEFT JOIN `dw-main-silver.logdata.event_log` ev_start
  ON ev_start.ad_served_id = cl.ad_served_id
  AND ev_start.event_type_raw = 'vast_start'
  AND ev_start.time >= TIMESTAMP('2026-01-27') AND ev_start.time < TIMESTAMP('2026-01-28')
LEFT JOIN `dw-main-silver.logdata.impression_log` imp
  ON imp.ad_served_id = cl.ad_served_id
  AND imp.time >= TIMESTAMP('2026-01-27') AND imp.time < TIMESTAMP('2026-01-28')
LEFT JOIN `dw-main-silver.logdata.win_logs` w
  ON w.auction_id = imp.ttd_impression_id
  AND w.time >= TIMESTAMP('2026-01-27') AND w.time < TIMESTAMP('2026-01-28')
LEFT JOIN `dw-main-silver.logdata.bid_logs` b
  ON b.auction_id = imp.ttd_impression_id
  AND b.time >= TIMESTAMP('2026-01-27') AND b.time < TIMESTAMP('2026-01-28')
;
```

Result — the IP is `216.126.34.185` at every single stage, zero mutation:

```
Stage                          IP                Timestamp
bid (bid_logs)                 216.126.34.185    2026-01-27 14:52:20
win (win_logs)                 216.126.34.185    2026-01-27 14:52:20
serve (impression_log)         216.126.34.185    2026-01-27 14:52:20
vast_impression (event_log)    216.126.34.185    2026-01-27 14:53:39
vast_start (event_log)         216.126.34.185    2026-01-27 14:53:39
verified visit (clickpass)     216.126.34.185    2026-02-04 00:06:14
ip_mutated:                    false
```

This confirms the within-stage trace is clean. The IP never changes from bid through to verified visit.

## Step 3: Identify the Campaign Group

The `campaign_id` for this impression is **450300** — "Beeswax Television Multi-Touch Plus". Its `campaign_group_id` is **93957**, `funnel_level = 3` (S3), `objective_id = 1`.

```sql
SELECT campaign_group_id, name, advertiser_id, create_time, first_launch_time
FROM `dw-main-bronze.integrationprod.campaign_groups`
WHERE campaign_group_id = 93957;
```

Result: campaign group **"7 2025 Wedding CRM"**, advertiser 37775, created **2025-07-11 16:38:43**, first launched **2025-07-11 16:42:58**.

All campaigns in this group:

```sql
SELECT campaign_id, name, campaign_group_id, funnel_level, channel_id, objective_id,
       create_time, deleted, is_test
FROM `dw-main-bronze.integrationprod.campaigns`
WHERE campaign_group_id = 93957
ORDER BY campaign_id;
```

```
campaign_id  name                                    funnel  stage  channel
450300       Beeswax Television Multi-Touch Plus      3       S3     CTV
450301       Beeswax Television Multi-Touch           2       S2     CTV
450302       Beeswax Television Prospecting - Ego     4       Ego    CTV
450303       Multi-Touch                              2       S2     Display
450304       Multi-Touch - Plus                       3       S3     Display
450305       Beeswax Television Prospecting           1       S1     CTV
```

6 campaigns, all created 2025-07-11, none deleted, none test. The S1/S2 campaigns we need to find a prior impression in: **450305** (S1 CTV), **450301** (S2 CTV), **450303** (S2 Display).

## Step 4: Cross-Stage IP Search

This is the critical step. The verified visit was attributed to an S3 campaign (450300). For this to be legitimate, IP `216.126.34.185` must have a prior S1 or S2 impression within the same campaign group (93957) that got it into S3 targeting.

I searched all three upstream impression tables (`event_log`, `viewability_log`, `impression_log`) for this IP across **all** campaigns for advertiser 37775 — not just cg 93957. I checked both `ip` and `bid_ip` columns using exact match. The search window is **2025-07-11** (campaign group creation) through **2026-02-04** (VV date) — no impression for this campaign group can exist outside that range. BQ silver tables go back to 2025-01-01 at earliest, so this covers the full available history.

I did NOT filter by `campaign_group_id` because I wanted to see if this IP linked back to **any** campaign — proving the methodology works when S1/S2 records exist.

### event_log — 207 rows found

The IP appears across 11 campaign groups. For **cg 93957**, it has **10 rows — all S3** (campaign 450300 only). **Zero S1/S2.**

Breakdown by campaign group:

```
cg       campaign  funnel  rows   note
78903    311966    S3      102    ← same IP, different campaign group
78903    311968    S1       8     ← S1 EXISTS in other cg
78903    311965    S2       2     ← S2 EXISTS in other cg
78893    311900    S1       9     ← S1 EXISTS in other cg
78904    311974    S1       2
69778    260986    S2      12
92884    443862    S2      20
92884    443866    S3       2
92881    443844    S2       6
92881    443848    S3       2
92876    443815    S2       6
92876    443816    S3       4
93961    450324    S3      14
93957    450300    S3      10     ← OUR CG — S3 only, ZERO S1/S2
96071    462967    S1       2
96071    462965    S3       2
84697    394577    S2       2
84697    394578    S3       2
```

### impression_log — 384 rows found

Same pattern. For **cg 93957: only campaign 450300 (S3, 7 rows) — zero S1/S2.**

```
cg       campaign  funnel  rows
78903    311966    S3      207
78903    311968    S1       6
78903    311965    S2       2
78893    311900    S1      21
78904    311974    S1       8
69778    260986    S2      19
69778    260988    S3       3
92884    443862    S2      31
92884    443866    S3       6
92881    443844    S2      23
92881    443848    S3       3
92876    443815    S2       6
92876    443816    S3      15
93961    450324    S3      19
93957    450300    S3       7    ← OUR CG — S3 only, ZERO S1/S2
96071    462965    S3       1
96071    462967    S1       1
84697    394577    S2       2
84697    394578    S3       4
```

### viewability_log — 0 rows

IP never appeared in viewability_log for any campaign for this advertiser.

### Physical table verification

I also searched the raw physical tables directly (not just the silver views), across all 6 physical tables (history + raw for each of event_log, impression_log, viewability_log):

```
Physical table              cg 93957 rows    cg 93957 S1/S2 rows
history__event_log          0                0
raw__event_log              10 (all S3)      0
history__impression_log     0                0
raw__impression_log         7 (all S3)       0
history__viewability_log    0                0
raw__viewability_log        0                0
TOTAL                       17 (all S3)      0
```

### Key observation

The IP has S1/S2 records in **10 other campaign groups** for this same advertiser — so the join logic works and the IP is findable when it exists. But it has **zero** S1 or S2 records within cg 93957, despite being credited with an S3 verified visit.

## Conclusion

This means the IP entered S3 targeting via the identity graph (LiveRamp/CRM), not via a prior MNTN S1/S2 impression within this campaign group.

## Verification Query — Cross-Table Search

Run each table separately for better slot allocation. Use TIMESTAMP filters (not DATE()) to enable partition pruning, and exact IP match (no LIKE wildcards — IPs in silver log tables do not carry CIDR suffixes):

```sql
-- Part 1: event_log (CTV impressions — vast_impression, vast_start)
SELECT
  'event_log' AS source_table,
  ev.campaign_id,
  c.name AS campaign_name,
  c.campaign_group_id,
  c.funnel_level,
  c.channel_id,
  c.objective_id,
  ev.event_type_raw,
  ev.ip,
  ev.bid_ip,
  ev.ad_served_id,
  ev.time
FROM `dw-main-silver.logdata.event_log` ev
JOIN `dw-main-bronze.integrationprod.campaigns` c
  ON c.campaign_id = ev.campaign_id
  AND c.advertiser_id = 37775
WHERE ev.event_type_raw IN ('vast_impression', 'vast_start')
  AND (ev.ip = '216.126.34.185' OR ev.bid_ip = '216.126.34.185')
  AND ev.time >= TIMESTAMP('2025-07-11')
  AND ev.time <  TIMESTAMP('2026-02-05')
ORDER BY time
;

-- Part 2: viewability_log (viewable display impressions)
SELECT
  'viewability_log' AS source_table,
  vl.campaign_id,
  c.name AS campaign_name,
  c.campaign_group_id,
  c.funnel_level,
  c.channel_id,
  c.objective_id,
  CAST(NULL AS STRING) AS event_type_raw,
  vl.ip,
  vl.bid_ip,
  vl.ad_served_id,
  vl.time
FROM `dw-main-silver.logdata.viewability_log` vl
JOIN `dw-main-bronze.integrationprod.campaigns` c
  ON c.campaign_id = vl.campaign_id
  AND c.advertiser_id = 37775
WHERE (vl.ip = '216.126.34.185' OR vl.bid_ip = '216.126.34.185')
  AND vl.time >= TIMESTAMP('2025-07-11')
  AND vl.time <  TIMESTAMP('2026-02-05')
ORDER BY time
;

-- Part 3: impression_log (non-viewable display + CTV serve records)
SELECT
  'impression_log' AS source_table,
  il.campaign_id,
  c.name AS campaign_name,
  c.campaign_group_id,
  c.funnel_level,
  c.channel_id,
  c.objective_id,
  CAST(NULL AS STRING) AS event_type_raw,
  il.ip,
  il.bid_ip,
  il.ad_served_id,
  il.time
FROM `dw-main-silver.logdata.impression_log` il
JOIN `dw-main-bronze.integrationprod.campaigns` c
  ON c.campaign_id = il.campaign_id
  AND c.advertiser_id = 37775
WHERE (il.ip = '216.126.34.185' OR il.bid_ip = '216.126.34.185')
  AND il.time >= TIMESTAMP('2025-07-11')
  AND il.time <  TIMESTAMP('2026-02-05')
ORDER BY time
;
```
