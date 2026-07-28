-- Frequency Phase-0 / Q3: reach-frequency curve for ENGAGED/RETARGETING stages (funnel_level>=2), 7d.
-- Household = (ip, advertiser_id). Visits via the retargeting branch source_type='visits'.
WITH cohort AS (
  SELECT campaign_id FROM `dw-main-bronze.integrationprod.public_campaigns`
  WHERE deleted = FALSE AND is_test = FALSE AND funnel_level >= 2
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
hh AS (
  SELECT ip, advertiser_id, COUNT(*) AS freq, SUM(spend) AS spend
  FROM imp GROUP BY ip, advertiser_id
),
vis AS (
  SELECT DISTINCT advertiser_id AS v_adv, guid, epoch, impression_id
  FROM `dw-main-silver.summarydata.ui_visits`
  WHERE DATE(time) BETWEEN '2026-07-06' AND '2026-07-26'
    AND source_type = 'visits' AND impression_id IS NOT NULL
),
hh_vis AS (
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
  COUNT(*) AS n_households, SUM(freq) AS impressions,
  ROUND(100*SUM(freq)/SUM(SUM(freq)) OVER(),2) AS imp_share_pct,
  ROUND(SUM(spend),2) AS spend,
  ROUND(100*SUM(spend)/SUM(SUM(spend)) OVER(),2) AS spend_share_pct,
  SUM(visits) AS visits, ROUND(AVG(freq),2) AS avg_freq,
  ROUND(100*COUNTIF(visited)/COUNT(*),4) AS hh_visit_rate_pct,
  ROUND(SAFE_DIVIDE(SUM(spend),NULLIF(SUM(visits),0)),4) AS cpv,
  ROUND(1000*SAFE_DIVIDE(SUM(visits),SUM(freq)),4) AS visits_per_1k_imps
FROM bucketed GROUP BY freq_bucket ORDER BY freq_bucket
