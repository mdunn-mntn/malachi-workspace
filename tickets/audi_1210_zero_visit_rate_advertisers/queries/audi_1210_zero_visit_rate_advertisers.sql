-- audi_1210_zero_visit_rate_advertisers.sql — advertisers spending with no measurable site visits.
--
-- For each advertiser that served an impression in the trailing 30 days: the distinct IPs it
-- served, how many of those IPs were seen visiting its site, and the resulting visit rate.
-- A visit is a row in clickpass_log, which is written by the advertiser's own site pixel.
-- So visit rate 0 with real spend means the pixel is not reporting for that advertiser --
-- either not installed, not firing, or firing under a different advertiser id.
--
-- Grain: one row per advertiser. Ranked by trailing-30d spend, largest first.
-- advertiser_id 9090 (PSA) is excluded: it serves to holdouts by design.
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
  SELECT advertiser_id, ip
  FROM `dw-main-silver.logdata.clickpass_log`
  WHERE DATE(time) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AND CURRENT_DATE()
  GROUP BY 1, 2
),
converting AS (
  SELECT advertiser_id, ip
  FROM `dw-main-silver.summarydata.ui_conversions`
  WHERE DATE(time) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AND CURRENT_DATE()
  GROUP BY 1, 2
),
rolled AS (
  SELECT
    s.advertiser_id,
    SUM(s.spend)                          AS spend_30d,
    SUM(s.impressions)                    AS impressions_30d,
    COUNT(*)                              AS served_ips_30d,
    SUM(IF(v.ip IS NOT NULL, 1, 0))       AS visiting_ips_30d,
    SUM(IF(c.ip IS NOT NULL, 1, 0))       AS converting_ips_30d
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
  r.converting_ips_30d
FROM rolled r
JOIN `dw-main-bronze.integrationprod.advertisers` adv USING (advertiser_id)
WHERE COALESCE(adv.deleted, FALSE) = FALSE
  AND COALESCE(adv.is_test, FALSE) = FALSE
  AND adv.company_name IS NOT NULL
  AND SAFE_DIVIDE(r.visiting_ips_30d, r.served_ips_30d) < 0.005   -- under half a percent
ORDER BY r.spend_30d DESC
