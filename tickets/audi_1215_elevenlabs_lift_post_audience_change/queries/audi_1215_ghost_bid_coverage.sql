-- AUDI-1215 Instrument A coverage: dw-main-silver.enriched.lift__ghost_bid_visits, CGID 122748

-- Q1: overall dt range
SELECT MIN(dt) AS min_dt, MAX(dt) AS max_dt, COUNT(*) AS row_ct
FROM `dw-main-silver.enriched.lift__ghost_bid_visits`
WHERE dt BETWEEN '2026-06-01' AND '2026-08-21';

-- Q2: CGID 122748 rows and distinct IPs by ISO week x partner_id x arm
SELECT FORMAT_DATE('%G-W%V', dt) AS iso_week, partner_id, arm,
       COUNT(*) AS row_ct, COUNT(DISTINCT ip) AS distinct_ips,
       MIN(dt) AS wk_min_dt, MAX(dt) AS wk_max_dt
FROM `dw-main-silver.enriched.lift__ghost_bid_visits`
WHERE dt BETWEEN '2026-06-01' AND '2026-08-21'
  AND campaign_group_id = 122748
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;

-- Q3: entry-cohort ghost_frac by week (anchor = first dt per advertiser x campaign x ip), overall and by partner
WITH ranked AS (
  SELECT advertiser_id, campaign_id, ip, arm, dt, partner_id,
         ROW_NUMBER() OVER (PARTITION BY advertiser_id, campaign_id, ip ORDER BY dt, first_bid_time) AS rn
  FROM `dw-main-silver.enriched.lift__ghost_bid_visits`
  WHERE dt BETWEEN '2026-06-01' AND '2026-08-21'
    AND campaign_group_id = 122748
)
SELECT FORMAT_DATE('%G-W%V', dt) AS iso_week,
       CAST(partner_id AS STRING) AS partner,
       COUNT(DISTINCT IF(arm = 'ghost', ip, NULL)) AS ghost_ips,
       COUNT(DISTINCT ip) AS total_ips,
       ROUND(SAFE_DIVIDE(COUNT(DISTINCT IF(arm = 'ghost', ip, NULL)), COUNT(DISTINCT ip)), 4) AS ghost_frac
FROM ranked
WHERE rn = 1
GROUP BY GROUPING SETS ((iso_week), (iso_week, partner))
ORDER BY iso_week, partner NULLS FIRST;
