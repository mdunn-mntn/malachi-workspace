-- AUDI-1215 Instrument A, single scan of lift__ghost_bid_visits for CGID 122748
-- Output rows: metric='weekly_arm' (row_ct/distinct_ips per week x partner x arm)
--              metric='cohort_ghost_frac' (entry-cohort ghost/total IPs per week; partner='all' or anchor partner)
WITH base AS (
  SELECT dt, partner_id, arm, ip, advertiser_id, campaign_id
  FROM `dw-main-silver.enriched.lift__ghost_bid_visits`
  WHERE dt BETWEEN '2026-06-01' AND '2026-08-21'
    AND campaign_group_id = 122748
),
weekly AS (
  SELECT 'weekly_arm' AS metric,
         FORMAT_DATE('%G-W%V', dt) AS iso_week,
         CAST(partner_id AS STRING) AS partner,
         arm,
         COUNT(*) AS row_ct,
         COUNT(DISTINCT ip) AS distinct_ips,
         CAST(NULL AS INT64) AS ghost_ips,
         CAST(NULL AS INT64) AS total_ips,
         CAST(NULL AS FLOAT64) AS ghost_frac,
         CAST(MIN(dt) AS STRING) AS min_dt,
         CAST(MAX(dt) AS STRING) AS max_dt
  FROM base
  GROUP BY 2, 3, 4
),
anchors AS (
  SELECT *
  FROM (
    SELECT dt, partner_id, arm, ip,
           ROW_NUMBER() OVER (PARTITION BY advertiser_id, campaign_id, ip ORDER BY dt) AS rn
    FROM base
  )
  WHERE rn = 1
),
cohort AS (
  SELECT 'cohort_ghost_frac' AS metric,
         FORMAT_DATE('%G-W%V', dt) AS iso_week,
         IF(GROUPING(partner_id) = 1, 'all', IFNULL(CAST(partner_id AS STRING), 'null')) AS partner,
         CAST(NULL AS STRING) AS arm,
         CAST(NULL AS INT64) AS row_ct,
         CAST(NULL AS INT64) AS distinct_ips,
         COUNT(DISTINCT IF(arm = 'ghost', ip, NULL)) AS ghost_ips,
         COUNT(DISTINCT ip) AS total_ips,
         ROUND(SAFE_DIVIDE(COUNT(DISTINCT IF(arm = 'ghost', ip, NULL)), COUNT(DISTINCT ip)), 4) AS ghost_frac,
         CAST(MIN(dt) AS STRING) AS min_dt,
         CAST(MAX(dt) AS STRING) AS max_dt
  FROM anchors
  GROUP BY GROUPING SETS ((iso_week), (iso_week, partner_id))
)
SELECT * FROM weekly
UNION ALL
SELECT * FROM cohort
ORDER BY metric, iso_week, partner, arm
