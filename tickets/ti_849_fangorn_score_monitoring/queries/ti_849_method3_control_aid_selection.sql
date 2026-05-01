/* ========================================================================
   TI-849 Method 3 — Control AID selection for CausalImpact synthetic control

   For each Fangorn AID, find non-Fangorn AIDs in the same sub-vertical
   (type=1) that are active in the pre-period and stable in spend profile.
   Excludes any AID currently flipped to vertical_data_source = 46.

   Treated verticals (May 1 launch):
     - 111004 Lending & Brokerage      (Biz2Credit)
     - 110001 Games & Comics            (Big Blue Bubble)
     - 107000 Colleges & Universities   (UNW Ohio)
   ======================================================================== */

DECLARE pre_start DATE DEFAULT DATE '2026-02-01';
DECLARE pre_end   DATE DEFAULT DATE '2026-04-29';

WITH
vertical_dim AS (
  SELECT advertiser_id, vertical_id, vertical_name
  FROM `dw-main-silver.fpa.advertiser_verticals`
  WHERE type = 1 AND vertical_id IN (111004, 110001, 107000)
),

fangorn_aids AS (
  SELECT DISTINCT advertiser_id
  FROM `dw-main-bronze.integrationprod.audience_advertiser_configurations`
  WHERE vertical_data_source = 46
),

daily_activity AS (
  SELECT
    i.advertiser_id,
    DATE(i.hour) AS day,
    SUM(i.display_impressions + i.ctv_impressions) AS impressions,
    SUM(i.media_cost) AS media_cost
  FROM `dw-main-silver.summarydata.impression_facts` i
  WHERE DATE(i.hour) BETWEEN pre_start AND pre_end
    AND i.advertiser_id IN (SELECT advertiser_id FROM vertical_dim)
  GROUP BY i.advertiser_id, DATE(i.hour)
),

per_aid_stats AS (
  SELECT
    advertiser_id,
    SUM(impressions) AS total_pre_impressions,
    COUNT(DISTINCT day) AS active_days,
    AVG(media_cost) AS avg_daily_spend,
    SAFE_DIVIDE(STDDEV(media_cost), AVG(media_cost)) AS spend_cv
  FROM daily_activity
  GROUP BY advertiser_id
)

SELECT
  v.vertical_id,
  v.vertical_name,
  s.advertiser_id,
  a.company_name,
  s.total_pre_impressions,
  s.active_days,
  s.avg_daily_spend,
  s.spend_cv,
  CASE
    WHEN f.advertiser_id IS NOT NULL THEN 'FANGORN_FLIPPED_EXCLUDE'
    WHEN s.active_days < 60               THEN 'low_activity'
    WHEN s.total_pre_impressions < 1000000 THEN 'low_volume'
    ELSE 'eligible_control'
  END AS eligibility
FROM per_aid_stats s
JOIN vertical_dim v ON s.advertiser_id = v.advertiser_id
JOIN `dw-main-bronze.integrationprod.advertisers` a
  ON s.advertiser_id = a.advertiser_id
  AND a.deleted = FALSE AND a.is_test = FALSE
LEFT JOIN fangorn_aids f ON s.advertiser_id = f.advertiser_id
ORDER BY v.vertical_id, s.total_pre_impressions DESC;
