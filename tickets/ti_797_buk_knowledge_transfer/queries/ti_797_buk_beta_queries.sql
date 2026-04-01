-- ============================================================
-- TI-797: BUK Beta Customer Analysis — Complete Query Set (BQ)
-- ============================================================
-- BQ equivalents of Alex's Greenplum queries from the
-- Customer Audience Tracking spreadsheet.
-- All queries use dw-main-silver / dw-main-bronze.
-- ============================================================


-- ============================================================
-- 1. Find Audience ID for an advertiser
-- ============================================================
-- Shows all audiences and their linked campaigns/segments
-- Replace advertiser_id as needed

SELECT a.advertiser_id, a.audience_id,
       b.campaign_id, b.segment_id,
       a.name AS audience_name, b.expression, b.create_time
FROM `dw-main-bronze.integrationprod.audience_audiences` a
JOIN `dw-main-bronze.integrationprod.audience_audience_segments` b
  ON a.audience_id = b.audience_id
WHERE a.is_test = FALSE
  AND a.advertiser_id = 40279;  -- change per advertiser


-- ============================================================
-- 2. Find Campaign Group ID for an audience
-- ============================================================
-- Maps audience → campaigns → campaign groups with funnel level

SELECT a.audience_id, a.name AS audience_name,
       b.campaign_id, c.name AS campaign_name,
       cg.campaign_group_id, cg.name AS campaign_group_name,
       c.funnel_level
FROM `dw-main-bronze.integrationprod.audience_audiences` a
JOIN `dw-main-bronze.integrationprod.audience_audience_segments` b
  ON a.audience_id = b.audience_id
JOIN `dw-main-bronze.integrationprod.campaigns` c
  ON b.campaign_id = c.campaign_id
JOIN `dw-main-bronze.integrationprod.campaign_groups` cg
  ON c.campaign_group_id = cg.campaign_group_id
WHERE a.advertiser_id = 40279
  AND a.audience_id = 63331
  AND a.is_test = FALSE
  AND c.deleted = FALSE;


-- ============================================================
-- 3. Get Campaign Audience Change History (pre/post swap)
-- ============================================================
-- Shows which audiences were attached to a campaign over time.
-- The archive table stores previous audience versions.
-- Use this to find the switch date and previous audience.
--
-- BQ table: dw-main-bronze.integrationprod.audience_segment_archives
-- Greenplum: archives.audience_segment_archives

SELECT 'current' AS source,
       b.audience_id,
       a.name AS audience_name,
       b.campaign_id,
       b.create_time
FROM `dw-main-bronze.integrationprod.audience_audience_segments` b
JOIN `dw-main-bronze.integrationprod.audience_audiences` a
  ON b.audience_id = a.audience_id
WHERE b.campaign_id = 523410  -- change per campaign
UNION ALL
SELECT 'archive_v' || CAST(sa.version AS STRING),
       sa.audience_id,
       aa.name AS audience_name,
       sa.campaign_id,
       sa.create_time
FROM `dw-main-bronze.integrationprod.audience_segment_archives` sa
JOIN `dw-main-bronze.integrationprod.audience_audiences` aa
  ON sa.audience_id = aa.audience_id
WHERE sa.campaign_id = 523410  -- change per campaign
ORDER BY create_time DESC;


-- ============================================================
-- 4. Find all audience swaps for beta advertisers
-- ============================================================
-- Finds the previous audience on the same campaign group
-- before the BUK swap. Best for pre/post comparison.

WITH beta_cgs AS (
  SELECT campaign_group_id FROM UNNEST([107024, 104020, 48733]) AS campaign_group_id
),
cg_campaigns AS (
  SELECT c.campaign_id, c.campaign_group_id, c.advertiser_id
  FROM `dw-main-bronze.integrationprod.campaigns` c
  WHERE c.campaign_group_id IN (SELECT campaign_group_id FROM beta_cgs)
    AND c.deleted = FALSE
)
SELECT DISTINCT
  cc.advertiser_id,
  adv.company_name,
  cc.campaign_group_id,
  cg.name AS campaign_group_name,
  cur.audience_id AS current_audience_id,
  ca.name AS current_audience_name,
  sa.audience_id AS prev_audience_id,
  aa.name AS prev_audience_name,
  MAX(sa.create_time) AS switch_date
FROM cg_campaigns cc
JOIN `dw-main-bronze.integrationprod.audience_audience_segments` cur
  ON cc.campaign_id = cur.campaign_id
JOIN `dw-main-bronze.integrationprod.audience_audiences` ca
  ON cur.audience_id = ca.audience_id
JOIN `dw-main-bronze.integrationprod.audience_segment_archives` sa
  ON cc.campaign_id = sa.campaign_id
JOIN `dw-main-bronze.integrationprod.audience_audiences` aa
  ON sa.audience_id = aa.audience_id
JOIN `dw-main-bronze.integrationprod.campaign_groups` cg
  ON cc.campaign_group_id = cg.campaign_group_id
JOIN `dw-main-bronze.integrationprod.advertisers` adv
  ON cc.advertiser_id = adv.advertiser_id
WHERE sa.audience_id != cur.audience_id
GROUP BY 1,2,3,4,5,6,7,8
ORDER BY cc.advertiser_id;


-- ============================================================
-- 5. Performance Tracking — Pre/Post BUK comparison
-- ============================================================
-- BQ equivalent of Alex's Greenplum performance query.
-- Uses sum_by_campaign_by_day (BQ) instead of
-- sum_by_campaign_group_by_day (Greenplum).
-- Visit definition matches Alex's: clicks + views + competing_views

WITH cg_campaigns AS (
  SELECT c.campaign_id, c.campaign_group_id
  FROM `dw-main-bronze.integrationprod.campaigns` c
  WHERE c.campaign_group_id IN (107024, 104020)
    AND c.deleted = FALSE
)
SELECT
  CASE cc.campaign_group_id
    WHEN 107024 THEN 'West Bend Insurance'
    ELSE 'Samys Camera'
  END AS advertiser_name,
  cc.campaign_group_id,
  CASE
    WHEN cc.campaign_group_id = 107024 AND d.day < DATE('2026-02-27') THEN 'pre_buk'
    WHEN cc.campaign_group_id = 104020 AND d.day < DATE('2026-03-04') THEN 'pre_buk'
    ELSE 'post_buk'
  END AS period,
  MIN(d.day) AS period_start,
  MAX(d.day) AS period_end,
  COUNT(DISTINCT d.day) AS days,
  SUM(d.impressions) AS impressions,
  SUM(d.clicks + d.views + COALESCE(d.competing_views, 0)) AS visits,
  SAFE_DIVIDE(
    SUM(d.clicks + d.views + COALESCE(d.competing_views, 0)),
    SUM(d.impressions)
  ) AS ivr,
  SUM(d.media_spend + d.data_spend + d.platform_spend) / 1e9 AS spend_usd
FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` d
JOIN cg_campaigns cc ON d.campaign_id = cc.campaign_id
WHERE d.day BETWEEN DATE('2026-01-01') AND CURRENT_DATE()
GROUP BY 1, 2, 3
ORDER BY 1, 3;


-- ============================================================
-- 6. Daily performance detail (for spreadsheet import)
-- ============================================================
-- Produces the same daily grain as Alex's West Bend / Samy's
-- Camera data sheets. Can be filtered per CG.

WITH cg_campaigns AS (
  SELECT c.campaign_id, c.campaign_group_id, c.advertiser_id
  FROM `dw-main-bronze.integrationprod.campaigns` c
  WHERE c.campaign_group_id = 107024  -- change: 107024 or 104020
    AND c.deleted = FALSE
)
SELECT
  cc.advertiser_id,
  cc.campaign_group_id,
  d.day,
  SUM(d.media_spend + d.data_spend + d.platform_spend) / 1e9 AS total_spend,
  SUM(d.impressions) AS impressions,
  SUM(d.uniques) AS households_reached,
  SUM(d.clicks + d.views + COALESCE(d.competing_views, 0)) AS visits,
  SUM(d.click_conversions + d.view_conversions
    + COALESCE(d.competing_view_conversions, 0)) AS conversions,
  SUM(d.click_order_value + d.view_order_value
    + COALESCE(d.competing_view_order_value, 0)) AS revenue
FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` d
JOIN cg_campaigns cc ON d.campaign_id = cc.campaign_id
WHERE d.day BETWEEN DATE('2025-01-01') AND CURRENT_DATE()
GROUP BY 1, 2, 3
ORDER BY d.day;
