-- Expanded coverage test: rule 1 ("live campaign group, or launching in <=7d,
-- or ended <=2d ago") applied per spend_day for the last 60 days.
--
-- Limitation: uses current `campaign_groups` state. Rule 1's signals
-- (start_time, end_time) are largely stable post-launch, so this is
-- approximately correct for the period. Archive table
-- `bronze.integrationprod.archives_campaign_group_archives` exists for exact
-- historical reconstruction if needed.

WITH spend_days AS (
  SELECT advertiser_id, day AS spend_day
  FROM `dw-main-silver.summarydata.sum_by_advertiser_by_day`
  WHERE day BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
                AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    AND media_spend > 0
),
cg AS (
  SELECT advertiser_id, campaign_group_id,
         DATE(start_time) AS cg_start_day,
         COALESCE(DATE(end_time), DATE '9999-12-31') AS cg_end_day
  FROM `dw-main-bronze.integrationprod.campaign_groups`
),
covered AS (
  SELECT s.advertiser_id, s.spend_day,
    MAX(CASE
      WHEN c.cg_start_day <= s.spend_day
        AND c.cg_end_day >= DATE_SUB(s.spend_day, INTERVAL 2 DAY) THEN 1
      WHEN c.cg_start_day BETWEEN s.spend_day
        AND DATE_ADD(s.spend_day, INTERVAL 7 DAY) THEN 1
      ELSE 0 END) AS rule1_hit
  FROM spend_days s LEFT JOIN cg c USING (advertiser_id)
  GROUP BY 1, 2
)
SELECT
  COUNT(*) AS total_adv_day_events,
  SUM(rule1_hit) AS rule1_caught,
  COUNT(*) - SUM(rule1_hit) AS rule1_missed_total,
  ROUND(100 * SUM(rule1_hit) / COUNT(*), 4) AS rule1_pct
FROM covered;
