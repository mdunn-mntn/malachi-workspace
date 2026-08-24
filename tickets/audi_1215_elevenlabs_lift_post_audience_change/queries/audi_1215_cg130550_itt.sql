-- audi_1215_cg130550_itt.sql
-- New campaign CG 130550 (elevencreative prospecting CTV), ITT lift since 2026-08-01; anchors capped at MAX(dt) 08-23 minus 7d.
WITH anchors AS (
  SELECT dt, ip, arm, visited, converted
  FROM (
    SELECT dt, ip, arm, visited, converted,
           ROW_NUMBER() OVER (PARTITION BY advertiser_id, campaign_id, ip ORDER BY dt, arm) AS rn
    FROM `dw-main-silver.enriched.lift__ghost_bid_visits`
    WHERE dt BETWEEN DATE '2026-08-01' AND DATE '2026-08-23'
      AND campaign_group_id = 130550
      AND partner_id = 8
  )
  WHERE rn = 1 AND dt <= DATE '2026-08-16'
)
SELECT 'period' AS grain, 'post_0801_0816' AS bucket, arm, COUNT(*) AS n_ip,
       SUM(CAST(visited AS INT64)) AS visited, SUM(CAST(converted AS INT64)) AS converted
FROM anchors GROUP BY arm
UNION ALL
SELECT 'week', FORMAT_DATE('%G-W%V', dt), arm, COUNT(*), SUM(CAST(visited AS INT64)), SUM(CAST(converted AS INT64))
FROM anchors GROUP BY 2, arm
ORDER BY grain, bucket, arm
