-- Dynamic param defaults (Mode date params are static-only, so sentinels map in SQL):
--   Period_Start = 1900-01-01 (the default) -> Jan 1 of the CURRENT year; any other date honored.
--   Period_End is CLAMPED to the first day of the current month (exclusive end ->
--   data through the last FULL month); the far-future default (2099-01-01) relies on this.
-- Module 06 -- Monthly score-bucket counts of prospecting delivery.
-- Per month, count of prospecting impressions (obj=1, funnel=1) in each MNTN household-score tier, RTC-excluded.
-- Tiers on household_score hs:
--   notlogged: hs IS NULL          (score column not written -- pre-2025-06 logging onset)
--   unscored : hs is at or below 0 (served with no usable score, e.g. gate-off)
--   MaxReach : 1..3332 | MI: 3333..6665 | PP: 6666..8000 | HI: 8001..10000
-- notlogged (NULL, pre-logging) is separated from real unscored so the full window is honest.
-- Source: logdata.cost_impression_log (retains full window; filter DATE(time)/time).
-- Bounded P1_START..P2_END (P2_END EXCLUSIVE) so the full comparison span is covered.
-- Period bounds are emitted per-row so the render can split months into Period 1 vs Period 2.
WITH sel AS (
  -- FILTERS (Nick): campaign multiselect ('ALL' keeps everything) + minimum share of
  -- window spend (whole-group basis; total computed BEFORE selection so shares are
  -- of the advertiser's full window spend, not of the kept subset)
  SELECT campaign_group_id FROM (
    SELECT c.campaign_group_id,
           SUM(s.media_spend + s.data_spend + s.platform_spend) AS gs,
           SUM(SUM(s.media_spend + s.data_spend + s.platform_spend)) OVER () AS ts
    FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` s
    JOIN `dw-main-bronze.integrationprod.campaigns` c ON c.campaign_id = s.campaign_id
    WHERE s.advertiser_id = {{ Advertiser_ID }} AND c.deleted = FALSE AND c.objective_id != 4
      AND s.day >= IF(DATE(LEFT('{{ P1_Start }}', 10)) = DATE '1900-01-01', DATE_SUB(IF(DATE(LEFT('{{ Period_Start }}', 10)) = DATE '1900-01-01', DATE_TRUNC(CURRENT_DATE(), YEAR), DATE(LEFT('{{ Period_Start }}', 10))), INTERVAL 1 YEAR), DATE(LEFT('{{ P1_Start }}', 10)))
      AND s.day <  LEAST(DATE(LEFT('{{ Period_End }}', 10)), DATE_TRUNC(CURRENT_DATE(), MONTH))
    GROUP BY 1
  )
  WHERE (CAST(campaign_group_id AS STRING) IN ({{ Campaign_Groups }}) OR (SELECT LOGICAL_AND(v = 'ALL') FROM UNNEST([{{ Campaign_Groups }}]) v))
    AND (ts <= 0 OR gs / ts >= LEAST(GREATEST(IFNULL(SAFE_CAST('{{ Min_Spend_Pct }}' AS FLOAT64), 0), 0), 100) / 100)
),
camp AS (
  SELECT campaign_id
  FROM `dw-main-bronze.integrationprod.campaigns`
  WHERE advertiser_id = {{ Advertiser_ID }} AND deleted = FALSE
    AND objective_id IN (1, 5, 6)
    AND (CAST(funnel_level AS STRING) IN ({{ Stages }}) OR (SELECT LOGICAL_AND(v = 'ALL') FROM UNNEST([{{ Stages }}]) v))
    AND campaign_group_id IN (SELECT campaign_group_id FROM sel)
),
base AS (
  SELECT FORMAT_DATE("%Y-%m", DATE(time)) AS mo, household_score AS hs
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE advertiser_id = {{ Advertiser_ID }}
    AND time >= TIMESTAMP(IF(DATE(LEFT('{{ P1_Start }}', 10)) = DATE '1900-01-01', DATE_SUB(IF(DATE(LEFT('{{ Period_Start }}', 10)) = DATE '1900-01-01', DATE_TRUNC(CURRENT_DATE(), YEAR), DATE(LEFT('{{ Period_Start }}', 10))), INTERVAL 1 YEAR), DATE(LEFT('{{ P1_Start }}', 10))))
    AND time <  TIMESTAMP(LEAST(DATE(LEFT('{{ Period_End }}', 10)), DATE_TRUNC(CURRENT_DATE(), MONTH)))
    AND campaign_id IN (SELECT campaign_id FROM camp)
    AND (model_params IS NULL OR model_params NOT LIKE "%realtime_conquest_score=10000%")
)
SELECT
  mo,
  COUNT(*)                                        AS total,
  COUNTIF(hs IS NULL)                             AS notlogged,
  COUNTIF(hs <= 0)                                AS unscored,
  COUNTIF(hs BETWEEN 1 AND 3332)                  AS maxreach,
  COUNTIF(hs BETWEEN 3333 AND 6665)               AS mi,
  COUNTIF(hs = 8000 OR hs BETWEEN 6666 AND 7999)  AS pp,
  COUNTIF(hs = 10000 OR hs BETWEEN 8001 AND 9999) AS hi,
  FORMAT_DATE("%Y-%m", IF(DATE(LEFT('{{ P1_Start }}', 10)) = DATE '1900-01-01', DATE_SUB(IF(DATE(LEFT('{{ Period_Start }}', 10)) = DATE '1900-01-01', DATE_TRUNC(CURRENT_DATE(), YEAR), DATE(LEFT('{{ Period_Start }}', 10))), INTERVAL 1 YEAR), DATE(LEFT('{{ P1_Start }}', 10)))) AS p1_start_mo,
  FORMAT_DATE("%Y-%m", DATE_SUB(IF(DATE(LEFT('{{ P1_End }}', 10)) = DATE '1900-01-01', DATE_SUB(LEAST(DATE(LEFT('{{ Period_End }}', 10)), DATE_TRUNC(CURRENT_DATE(), MONTH)), INTERVAL 1 YEAR), LEAST(DATE(LEFT('{{ P1_End }}', 10)), DATE_TRUNC(CURRENT_DATE(), MONTH))), INTERVAL 1 DAY)) AS p1_end_mo,
  FORMAT_DATE("%Y-%m", IF(DATE(LEFT('{{ Period_Start }}', 10)) = DATE '1900-01-01', DATE_TRUNC(CURRENT_DATE(), YEAR), DATE(LEFT('{{ Period_Start }}', 10)))) AS p2_start_mo,
  FORMAT_DATE("%Y-%m", DATE_SUB(LEAST(DATE(LEFT('{{ Period_End }}', 10)), DATE_TRUNC(CURRENT_DATE(), MONTH)), INTERVAL 1 DAY)) AS p2_end_mo
FROM base
GROUP BY mo, p1_start_mo, p1_end_mo, p2_start_mo, p2_end_mo
ORDER BY mo
