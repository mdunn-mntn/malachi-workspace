-- audi_1210_zero_visit_rate_advertisers.sql — advertisers spending with almost no attributed site visits,
-- split by whether their site has any traffic at all.
--
-- Two different numbers, and the gap between them is the whole point (Johnny Chen, 2026-08-19):
--   raw_visits_30d  = every visit the advertiser's pixel reported, from summarydata.sum_by_advertiser_by_day.
--                     This is their site traffic. If it is ~0 the pixel may still be fine -- the site is quiet.
--   visiting_ips_30d= served IPs we later saw visiting. MNTN attributed visits are a SUBSET of raw visits,
--                     so a low raw number caps the attributed number arithmetically.
-- An advertiser with healthy raw visits and zero attributed visits is the real defect. An advertiser with
-- 20 raw visits a month is not a pixel bug, it is a quiet site.
--
-- Grain: one row per advertiser, trailing 30 days, ranked by spend. 9090 (PSA) excluded by design.
WITH served AS (
  SELECT advertiser_id, ip,
    COUNT(*) AS impressions,
    SUM(COALESCE(media_spend,0) + COALESCE(data_spend,0) + COALESCE(platform_spend,0)) AS spend
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE DATE(time) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AND CURRENT_DATE()
    AND advertiser_id IS NOT NULL AND advertiser_id != 9090
  GROUP BY 1, 2
),
visiting AS (
  SELECT advertiser_id, ip FROM `dw-main-silver.logdata.clickpass_log`
  WHERE DATE(time) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AND CURRENT_DATE()
  GROUP BY 1, 2
),
converting AS (
  SELECT advertiser_id, ip FROM `dw-main-silver.summarydata.ui_conversions`
  WHERE DATE(time) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AND CURRENT_DATE()
  GROUP BY 1, 2
),
-- Site traffic as the advertiser's own pixel reported it, independent of anything MNTN served.
raw AS (
  SELECT advertiser_id,
    SUM(raw_visits)            AS raw_visits_30d,
    SUM(raw_conversions)       AS raw_conversions_30d,
    COUNTIF(raw_visits > 0)    AS days_with_any_visit
  FROM `dw-main-silver.summarydata.sum_by_advertiser_by_day`
  WHERE day BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AND CURRENT_DATE()
  GROUP BY 1
),
rolled AS (
  SELECT
    s.advertiser_id,
    SUM(s.spend)                    AS spend_30d,
    SUM(s.impressions)              AS impressions_30d,
    COUNT(*)                        AS served_ips_30d,
    SUM(IF(v.ip IS NOT NULL, 1, 0)) AS visiting_ips_30d,
    SUM(IF(c.ip IS NOT NULL, 1, 0)) AS converting_ips_30d
  FROM served s
  LEFT JOIN visiting   v USING (advertiser_id, ip)
  LEFT JOIN converting c USING (advertiser_id, ip)
  GROUP BY 1
  HAVING SUM(s.impressions) > 0
)
SELECT
  r.advertiser_id,
  adv.company_name AS advertiser_name,
  r.spend_30d,
  r.impressions_30d,
  r.served_ips_30d,
  r.visiting_ips_30d,
  SAFE_DIVIDE(r.visiting_ips_30d, r.served_ips_30d) AS visit_rate,
  r.converting_ips_30d,
  COALESCE(w.raw_visits_30d, 0)      AS raw_visits_30d,
  COALESCE(w.raw_conversions_30d, 0) AS raw_conversions_30d,
  COALESCE(w.days_with_any_visit, 0) AS days_with_any_visit,
  CASE
    WHEN COALESCE(w.raw_visits_30d, 0) = 0    THEN 'Pixel reported nothing'
    WHEN COALESCE(w.raw_visits_30d, 0) < 1000 THEN 'Site traffic is tiny'
    ELSE 'Traffic exists, we are not matching it'
  END AS reading
FROM rolled r
JOIN `dw-main-bronze.integrationprod.advertisers` adv USING (advertiser_id)
LEFT JOIN raw w USING (advertiser_id)
WHERE COALESCE(adv.deleted, FALSE) = FALSE
  AND COALESCE(adv.is_test, FALSE) = FALSE
  AND adv.company_name IS NOT NULL
  AND SAFE_DIVIDE(r.visiting_ips_30d, r.served_ips_30d) < 0.005
ORDER BY r.spend_30d DESC
