-- Frequency Phase-0 / Q1: reach-frequency curve at HOUSEHOLD grain (ip x advertiser), MM prospecting, 7d.
-- Per freq bucket: n_households, impressions, spend, visits, per-household visit rate, CPV.
-- Household visit = >=1 last_tv_touch visit attributed to any of the household's impressions (impression_id 1:1).
WITH cohort AS (
  SELECT campaign_id FROM `dw-main-silver.audience.mm_campaign_classifier`
  WHERE has_mm = TRUE AND objective_id = 1
),
imp AS (
  SELECT c.ip, c.advertiser_id, c.impression_id,
         (c.media_spend + c.data_spend + c.platform_spend) AS spend
  FROM `dw-main-silver.logdata.cost_impression_log` c
  JOIN cohort USING (campaign_id)
  WHERE DATE(c.time) BETWEEN '2026-07-06' AND '2026-07-12'
    AND c.advertiser_id <> 31357
    AND c.ip IS NOT NULL AND c.ip <> '0.0.0.0'
),
hh AS (  -- one row per household (ip x advertiser): total frequency + spend in window
  SELECT ip, advertiser_id, COUNT(*) AS freq, SUM(spend) AS spend
  FROM imp GROUP BY ip, advertiser_id
),
vis AS (  -- deduped prospecting visits, one row per visit event, carries its attributed impression_id
  SELECT DISTINCT advertiser_id AS v_adv, guid, epoch, impression_id
  FROM `dw-main-silver.summarydata.ui_visits`
  WHERE DATE(time) BETWEEN '2026-07-06' AND '2026-07-26'   -- +14d tail (Phase-0; long tail caveated)
    AND source_type = 'last_tv_touch_visits' AND impression_id IS NOT NULL
),
hh_vis AS (  -- visits per household via impression_id join (impression_id 1:1 to a household)
  SELECT i.ip, i.advertiser_id, COUNT(*) AS visits
  FROM imp i JOIN vis v USING (impression_id)
  GROUP BY i.ip, i.advertiser_id
),
household AS (
  SELECT hh.ip, hh.advertiser_id, hh.freq, hh.spend, COALESCE(hv.visits,0) AS visits
  FROM hh LEFT JOIN hh_vis hv USING (ip, advertiser_id)
),
bucketed AS (
  SELECT
    CASE WHEN freq=1 THEN '01_freq_1' WHEN freq<=3 THEN '02_freq_2-3'
         WHEN freq<=7 THEN '03_freq_4-7' WHEN freq<=12 THEN '04_freq_8-12'
         WHEN freq<=20 THEN '05_freq_13-20' WHEN freq<=40 THEN '06_freq_21-40'
         ELSE '07_freq_41+' END AS freq_bucket,
    freq, spend, visits, (visits>0) AS visited
  FROM household
)
SELECT freq_bucket,
  COUNT(*)                                                    AS n_households,
  SUM(freq)                                                  AS impressions,
  ROUND(100*SUM(freq)/SUM(SUM(freq)) OVER(),2)               AS imp_share_pct,
  ROUND(SUM(spend),2)                                        AS spend,
  ROUND(100*SUM(spend)/SUM(SUM(spend)) OVER(),2)             AS spend_share_pct,
  SUM(visits)                                                AS visits,
  ROUND(AVG(freq),2)                                         AS avg_freq,
  ROUND(100*COUNTIF(visited)/COUNT(*),4)                     AS hh_visit_rate_pct,
  ROUND(SAFE_DIVIDE(SUM(spend),NULLIF(SUM(visits),0)),4)     AS cpv,
  ROUND(1000*SAFE_DIVIDE(SUM(visits),SUM(freq)),4)           AS visits_per_1k_imps
FROM bucketed GROUP BY freq_bucket ORDER BY freq_bucket
