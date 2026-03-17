-- TI-650: 365-Day Prior-Funnel IP Lookup (v16)
-- For unresolved VVs: search event_log across ALL campaigns (not just campaign_group_id)
-- to determine if the bid_ip was EVER served an MNTN ad.
--
-- Purpose: Confirm that unresolved VVs entered S3 via identity graph (not via a prior MNTN impression).
-- If the IP appears in other campaign_groups but NOT the VV's own campaign_group, that proves
-- the S3 entry was identity-graph-driven, not IP-funnel-driven.
--
-- USAGE: Change ip_address and vv_date in the params CTE.

WITH params AS (
  SELECT
    "216.126.34.185" AS ip_address,
    DATE("2026-02-04")  AS vv_date
)

SELECT
  ev.campaign_id,
  c.name                AS campaign_name,
  c.campaign_group_id,
  c.funnel_level,
  c.objective_id,
  c.advertiser_id,
  ev.event_type_raw,
  ev.event_count,
  ev.earliest,
  ev.latest
FROM (
  SELECT
    campaign_id,
    event_type_raw,
    COUNT(*)        AS event_count,
    MIN(DATE(time)) AS earliest,
    MAX(DATE(time)) AS latest
  FROM `dw-main-silver.logdata.event_log`, params p
  WHERE ip = p.ip_address
    AND event_type_raw IN ('vast_impression', 'vast_start')
    AND DATE(time) >= DATE_SUB(p.vv_date, INTERVAL 365 DAY)
    AND DATE(time) < p.vv_date
  GROUP BY campaign_id, event_type_raw
) ev
LEFT JOIN `dw-main-bronze.integrationprod.campaigns` c
  ON c.campaign_id = ev.campaign_id
  AND c.deleted = FALSE
ORDER BY ev.event_count DESC
LIMIT 30
;
