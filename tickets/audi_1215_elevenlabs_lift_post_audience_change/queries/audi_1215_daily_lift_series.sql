-- audi_1215_daily_lift_series.sql
-- Daily entry-cohort ITT visit lift for CGID 122748, for the day-over-day relative-lift chart (change-date markers).
WITH anchors AS (
  SELECT dt, ip, arm, visited
  FROM (
    SELECT dt, ip, arm, visited,
           ROW_NUMBER() OVER (PARTITION BY advertiser_id, campaign_id, ip ORDER BY dt, arm) AS rn
    FROM `dw-main-silver.enriched.lift__ghost_bid_visits`
    WHERE dt BETWEEN DATE '2026-06-22' AND DATE '2026-08-20'
      AND campaign_group_id = 122748
      AND partner_id = 8
  )
  WHERE rn = 1
    AND dt BETWEEN DATE '2026-06-23' AND DATE '2026-08-13'
)
SELECT dt, arm, COUNT(*) AS n_ip, SUM(CAST(visited AS INT64)) AS visited
FROM anchors
GROUP BY dt, arm
ORDER BY dt, arm
