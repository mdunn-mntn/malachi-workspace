# TI-650: IP Cross-Stage Proof — Writeup

IPs within the same stage are 100% traceable since they have the `auction_id` and `ad_served_id`.

However when going from S3 → S1 or S3 → S2 or S2 → S1, we need to link the `bid_logs.ip` of the higher stage/funnel to the previous impression that got it into this stage. Depending on the impression type, the upstream table is:

- **CTV:** `event_log.ip` (`vast_start` or `vast_impression`)
- **Display, Viewable:** `viewability_log.ip`
- **Display, Non-Viewable:** `impression_log.ip`

The match between `bid_logs.ip` and one of these three tables should be 100%. Zach says it MUST be there.

## Example

In `clickpass_log`, there's an `ad_served_id`: `80207c6e-1fb9-427b-b019-29e15fb3323c`.

If you trace this ad_served_id back through the full pipeline, the IP is `216.126.34.185` at every single stage — zero mutation:

```
Stage                          IP                Timestamp
bid (bid_logs)                 216.126.34.185    2026-01-27 14:52:20
win (win_logs)                 216.126.34.185    2026-01-27 14:52:20
serve (impression_log)         216.126.34.185    2026-01-27 14:52:20
vast_impression (event_log)    216.126.34.185    2026-01-27 14:53:39
vast_start (event_log)         216.126.34.185    2026-01-27 14:53:39
vast_firstQuartile → complete  216.126.34.185    14:53:47 → 14:54:08
verified visit (clickpass)     216.126.34.185    2026-02-04 00:06:14
```

You can verify that with this query:

```sql
WITH cl AS (
  SELECT ad_served_id, ip, advertiser_id, campaign_id, time, impression_time,
         attribution_model_id, guid, is_new, first_touch_ad_served_id
  FROM `dw-main-silver.logdata.clickpass_log`
  WHERE DATE(time) = '2026-02-04'
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
  AND DATE(ev_imp.time) = '2026-01-27'
LEFT JOIN `dw-main-silver.logdata.event_log` ev_start
  ON ev_start.ad_served_id = cl.ad_served_id
  AND ev_start.event_type_raw = 'vast_start'
  AND DATE(ev_start.time) = '2026-01-27'
LEFT JOIN `dw-main-silver.logdata.impression_log` imp
  ON imp.ad_served_id = cl.ad_served_id
  AND DATE(imp.time) = '2026-01-27'
LEFT JOIN `dw-main-silver.logdata.win_logs` w
  ON w.auction_id = imp.ttd_impression_id
  AND DATE(w.time) = '2026-01-27'
LEFT JOIN `dw-main-silver.logdata.bid_logs` b
  ON b.auction_id = imp.ttd_impression_id
  AND DATE(b.time) = '2026-01-27'
;
```

## Campaign Group Context

The `campaign_id` for this impression is **450300** — "Beeswax Television Multi-Touch Plus". Its `campaign_group_id` is **93957**, `funnel_level = 3` (S3), `objective_id = 1` (prospecting).

If you look up that campaign_group:

```sql
SELECT *
FROM `dw-main-bronze.integrationprod.campaigns`
WHERE campaign_group_id = 93957
ORDER BY campaign_id
LIMIT 10;
```

You get 6 campaigns, all created 2025-07-11, none are test campaigns:

```
campaign_id  name                                    funnel  stage  channel
450300       Beeswax Television Multi-Touch Plus      3       S3     CTV
450301       Beeswax Television Multi-Touch           2       S2     CTV
450302       Beeswax Television Prospecting - Ego     4       Ego    CTV
450303       Multi-Touch                              2       S2     Display
450304       Multi-Touch - Plus                       3       S3     Display
450305       Beeswax Television Prospecting           1       S1     CTV
```

The S1/S2 campaigns we need to find a prior impression in: **450305** (S1), **450301** (S2 CTV), **450303** (S2 Display).

## Cross-Stage IP Search

The `bid_ip` for this verified visit is `216.126.34.185`. To find the cross-stage link, I searched all three upstream impression tables (`event_log`, `viewability_log`, `impression_log`) for this IP across **all** campaigns for advertiser 37775 — not just cg 93957. I checked both `ip` and `bid_ip` columns and added a `%` wildcard in case a CIDR suffix like `/32` or `/24` was appended. I went all the way back to **2025-01-01**, which is as far as GCP tables go (~14 months of history). The campaign group was created 2025-07-11 so no impression should be earlier than that, but I searched the full range to be thorough.

I did NOT filter by `campaign_group_id` because I wanted to see if this IP linked back to **any** campaign. Here's what came back:

**event_log — 173 rows found.** The IP shows up in S1/S2 impressions for other campaign groups:
- cg 78903: 311966 (S3, 102 rows), 311968 (S1, 8 rows), 311965 (S2, 2 rows)
- cg 78893: 311900 (S1, 9 rows)
- cg 78904: 311974 (S1, 2 rows)
- cg 69778: 260986 (S2, 12 rows)
- cg 92881: 443844 (S2, 4 rows), 443848 (S2, 2 rows)
- cg 92876: 443816 (S2, 4 rows), 443815 (S2, 2 rows)
- cg 92884: 443866 (S2, 2 rows), 443862 (S2, 2 rows)
- cg 96071: 462967 (S1, 2 rows), 462965 (S3, 2 rows)
- cg 84697: 394578 (S3, 2 rows), 394577 (S2, 2 rows)
- cg 93961: 450324 (S3, 14 rows)
- **cg 93957: ZERO rows**

**impression_log — 200+ rows found.** Same pattern — IP shows up across multiple campaign groups. **cg 93957: only campaign 450300 (S3, 7 rows) — zero S1/S2.**

**viewability_log — 0 rows.** IP never appeared in viewability_log for any campaign.

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

The IP has S1/S2 records in **10 other campaign groups** for this same advertiser — so the join logic works and the IP is findable when it exists. But it has **zero** S1 or S2 records within cg 93957, despite being credited with an S3 verified visit.

## Conclusion

This means the IP entered S3 targeting via the identity graph (LiveRamp/CRM), not via a prior MNTN S1/S2 impression within this campaign group.

## Verification Query — Cross-Table Search

```sql
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
  AND (ev.ip = '216.126.34.185' OR ev.ip LIKE '216.126.34.185%'
       OR ev.bid_ip = '216.126.34.185' OR ev.bid_ip LIKE '216.126.34.185%')
  AND DATE(ev.time) BETWEEN '2025-01-01' AND '2026-12-31'

UNION ALL

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
WHERE (vl.ip = '216.126.34.185' OR vl.ip LIKE '216.126.34.185%'
       OR vl.bid_ip = '216.126.34.185' OR vl.bid_ip LIKE '216.126.34.185%')
  AND DATE(vl.time) BETWEEN '2025-01-01' AND '2026-12-31'

UNION ALL

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
WHERE (il.ip = '216.126.34.185' OR il.ip LIKE '216.126.34.185%'
       OR il.bid_ip = '216.126.34.185' OR il.bid_ip LIKE '216.126.34.185%')
  AND DATE(il.time) BETWEEN '2025-01-01' AND '2026-12-31'
ORDER BY source_table, time
LIMIT 200
;
```
