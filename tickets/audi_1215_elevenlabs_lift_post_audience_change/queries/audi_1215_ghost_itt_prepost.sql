-- audi_1215_ghost_itt_prepost.sql
-- AUDI-1215: ghost-bid ITT lift, CGID 122748 (ElevenLabs AID 51660), pre vs post 2026-06-30 audience change.
-- Entry-cohort anchor = first dt per (advertiser_id, campaign_id, ip); arm/visited/converted taken from that row.
-- Excludes table's global first day 2026-06-22 (left-censored stock) and anchors after 2026-08-13 (= MAX(dt) 2026-08-20 minus 7d outcome window).
-- PRE = anchors 2026-06-23..2026-06-30; BLACKOUT = 2026-07-01..2026-07-10 (excluded from contrast); POST = 2026-07-11..2026-08-13.
-- partner_id = 8 (Beeswax) only; partner 79 holdout path is suspect.
WITH anchors AS (
  SELECT dt, ip, arm, visited, converted, won
  FROM (
    SELECT dt, ip, arm, visited, converted, won,
           ROW_NUMBER() OVER (PARTITION BY advertiser_id, campaign_id, ip ORDER BY dt, arm) AS rn
    FROM `dw-main-silver.enriched.lift__ghost_bid_visits`
    WHERE dt BETWEEN DATE '2026-06-22' AND DATE '2026-08-20'
      AND campaign_group_id = 122748
      AND partner_id = 8
  )
  WHERE rn = 1
    AND dt BETWEEN DATE '2026-06-23' AND DATE '2026-08-13'
),
labeled AS (
  SELECT
    CASE WHEN dt <= DATE '2026-06-30' THEN 'pre'
         WHEN dt >= DATE '2026-07-11' THEN 'post'
         ELSE 'blackout' END AS period,
    FORMAT_DATE('%G-W%V', dt) AS iso_week,
    dt, arm, visited, converted, won
  FROM anchors
)
SELECT 'period' AS grain, period AS bucket, arm,
       COUNT(*) AS n_ip,
       COUNTIF(visited) AS visited_ct,
       COUNTIF(converted) AS converted_ct,
       COUNTIF(won) AS won_ct,
       CAST(MIN(dt) AS STRING) AS anchor_min_dt,
       CAST(MAX(dt) AS STRING) AS anchor_max_dt
FROM labeled
GROUP BY 2, 3
UNION ALL
SELECT 'week', iso_week, arm,
       COUNT(*), COUNTIF(visited), COUNTIF(converted), COUNTIF(won),
       CAST(MIN(dt) AS STRING), CAST(MAX(dt) AS STRING)
FROM labeled
GROUP BY 2, 3
ORDER BY grain, bucket, arm