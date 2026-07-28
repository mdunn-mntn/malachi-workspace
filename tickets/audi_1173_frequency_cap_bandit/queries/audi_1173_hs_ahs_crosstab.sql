-- AUDI-1173 refined sizing / Q3: HS x AHS crosstab x freq bucket, household grain = (ip, advertiser_id), COMBINED scope.
-- Separates cold prospecting from warm revisitors:
--   household_score band (per-IP intent):  HI >=6666 / MI 3333-6665 / MaxReach 1-3332 / unscored (<=0 or NULL)
--   advertiser_household_score:             RTC (=10000, retargeting/conquest) vs non-RTC
-- Household-level HS = MAX(household_score) over its impressions; RTC = MAX(advertiser_household_score=10000)
--   (retargeting rows carry HS=-1 & AHS=10000 -> land in unscored x RTC, the warm-revisitor cell, by construction).
-- Same window / joins / purge rule / exclusions as Q1 (audi_1173_delivered_freq_curve.sql).
-- Headline metrics per cell: households, spend, TOTAL visits, hh_visit_rate, cost-per-household. Raw AND purged.
WITH camp AS (
  SELECT campaign_id, objective_id
  FROM `dw-main-bronze.integrationprod.public_campaigns`
  WHERE deleted = FALSE AND is_test = FALSE
),
imp AS (                                          -- the one CIL scan (adds household_score + advertiser_household_score)
  SELECT c.ip, c.advertiser_id, c.impression_id, c.guid,
         (c.media_spend + c.data_spend + c.platform_spend) AS spend,
         COALESCE(c.household_score, -1)                    AS hs,
         CASE WHEN c.advertiser_household_score = 10000 THEN 1 ELSE 0 END AS ahs_rtc
  FROM `dw-main-silver.logdata.cost_impression_log` c
  JOIN camp USING (campaign_id)
  WHERE DATE(c.time) BETWEEN '2026-05-15' AND '2026-06-13'
    AND c.advertiser_id NOT IN (31357, 90)
    AND c.ip IS NOT NULL AND c.ip <> '0.0.0.0'
),
vis AS (
  SELECT impression_id, COUNT(*) AS nvis
  FROM (
    SELECT DISTINCT advertiser_id, guid, epoch, impression_id
    FROM `dw-main-silver.summarydata.ui_visits`
    WHERE DATE(time) BETWEEN '2026-05-15' AND '2026-07-28'
      AND impression_id IS NOT NULL
      AND source_type IN ('last_tv_touch_visits','visits')
  )
  GROUP BY impression_id
),
impv AS (
  SELECT i.ip, i.advertiser_id, i.guid, i.spend, i.hs, i.ahs_rtc, COALESCE(v.nvis,0) AS nvis
  FROM imp i LEFT JOIN vis v USING (impression_id)
),
g AS (
  SELECT ip, advertiser_id, guid,
         COUNT(*) AS imps, SUM(spend) AS spend, SUM(nvis) AS visits,
         MAX(hs) AS hs_max, MAX(ahs_rtc) AS ahs_rtc
  FROM impv
  GROUP BY ip, advertiser_id, guid
),
ipf AS (                                          -- per-IP shared/NAT signals (integers carried, flagged at classification)
  SELECT ip, COUNT(DISTINCT guid) AS ndev, COUNT(DISTINCT advertiser_id) AS nadv, SUM(imps) AS nimp
  FROM g GROUP BY ip
),
hh AS (                                           -- combined household (ip x advertiser)
  SELECT g.ip, g.advertiser_id,
         SUM(g.imps) AS freq, SUM(g.spend) AS spend, SUM(g.visits) AS visits,
         MAX(g.hs_max) AS hs_max, MAX(g.ahs_rtc) AS ahs_rtc,
         MAX(f.ndev) AS ndev, MAX(f.nadv) AS nadv, MAX(f.nimp) AS nimp
  FROM g JOIN ipf f USING (ip)
  GROUP BY g.ip, g.advertiser_id
),
classed AS (
  SELECT
    CASE WHEN hs_max >= 6666 THEN '1_HI' WHEN hs_max >= 3333 THEN '2_MI'
         WHEN hs_max >= 1 THEN '3_MaxReach' ELSE '4_unscored' END AS hs_band,
    CASE WHEN ahs_rtc = 1 THEN '1_RTC' ELSE '2_nonRTC' END        AS ahs_class,
    CASE WHEN freq=1 THEN '01_freq_1' WHEN freq<=3 THEN '02_freq_2-3'
         WHEN freq<=7 THEN '03_freq_4-7' WHEN freq<=12 THEN '04_freq_8-12'
         WHEN freq<=20 THEN '05_freq_13-20' WHEN freq<=40 THEN '06_freq_21-40'
         ELSE '07_freq_41+' END AS freq_bucket,
    freq, spend, visits, (visits>0) AS visited,
    (ndev >= 51 OR nadv >= 121 OR nimp >= 501) AS shared_ip
  FROM hh
),
emitted AS (
  SELECT hs_band, ahs_class, freq_bucket, p.purge, freq, spend, visits, visited
  FROM classed, UNNEST([STRUCT('1_raw' AS purge, TRUE AS keep),
                        STRUCT('2_purged',       NOT shared_ip)]) AS p
  WHERE p.keep
)
SELECT purge, hs_band, ahs_class, freq_bucket,
  COUNT(*)                                   AS n_households,
  SUM(freq)                                  AS impressions,
  ROUND(SUM(spend),2)                        AS spend,
  SUM(visits)                                AS visits,
  ROUND(AVG(freq),3)                         AS avg_freq,
  ROUND(100*COUNTIF(visited)/COUNT(*),4)     AS hh_visit_rate_pct,
  ROUND(SAFE_DIVIDE(SUM(visits),COUNT(*)),5) AS visits_per_hh,
  ROUND(SAFE_DIVIDE(SUM(spend),COUNT(*)),4)  AS cost_per_hh,
  ROUND(SAFE_DIVIDE(SUM(spend),NULLIF(SUM(visits),0)),4) AS cpv
FROM emitted
GROUP BY purge, hs_band, ahs_class, freq_bucket
ORDER BY purge, hs_band, ahs_class, freq_bucket
