-- TI-1044: ElevenLabs ghost-ad lift — FULL canonical readout (matches TI-933/TI-837 outcome set).
-- Control = ghost-holdout (bid_price_log threshold_failure_reasons='ghostBid', is_ctv).
-- Treated = served (cost_impression_log; advertiser-clustered → cheap).
-- THREE outcomes (TI-933 clickpass_rate / guid_rate / conv_rate):
--   clickpass = MNTN-ATTRIBUTED visits (overstates incrementality)
--   guid      = TOTAL site traffic (organic+paid+direct) = TRUE incremental visit signal
--   conv      = conversions (conversion_log)
-- Lift = treated_rate / control_rate - 1 per outcome. (ATT; win-selection caveat applies vs ITT query.)
DECLARE d0 DATE DEFAULT '2026-06-13';
DECLARE d1 DATE DEFAULT '2026-06-22';
DECLARE o1 DATE DEFAULT '2026-06-23';

WITH
ghost AS (
  SELECT DISTINCT ip FROM `dw-main-bronze.raw.bid_price_log`
  WHERE advertiser_id=51660 AND is_ctv AND threshold_failure_reasons='ghostBid'
    AND DATE(time) BETWEEN d0 AND d1 AND ip IS NOT NULL AND ip!=''
),
served AS (
  SELECT DISTINCT ip FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE advertiser_id=51660 AND DATE(time) BETWEEN d0 AND d1 AND ip IS NOT NULL AND ip!=''
),
cohort AS (
  SELECT ip,'treated' AS grp FROM served
  UNION ALL
  SELECT g.ip,'control' FROM ghost g LEFT JOIN served s USING(ip) WHERE s.ip IS NULL
),
clickpass_v AS (
  SELECT DISTINCT ip FROM `dw-main-silver.logdata.clickpass_log`
  WHERE advertiser_id=51660 AND DATE(time) BETWEEN d0 AND o1 AND ip IS NOT NULL AND ip!=''
),
guid_v AS (
  SELECT DISTINCT ip FROM `dw-main-silver.logdata.guid_log`
  WHERE advertiser_id=51660 AND DATE(time) BETWEEN d0 AND o1 AND ip IS NOT NULL AND ip!=''
),
conv_v AS (
  SELECT DISTINCT ip FROM `dw-main-silver.logdata.conversion_log`
  WHERE advertiser_id=51660 AND DATE(time) BETWEEN d0 AND o1 AND ip IS NOT NULL AND ip!=''
)
SELECT g.grp,
  COUNT(*) AS ips,
  COUNTIF(cp.ip IS NOT NULL) AS clickpass_visitors,
  COUNTIF(gd.ip IS NOT NULL) AS guid_visitors,
  COUNTIF(cv.ip IS NOT NULL) AS converters,
  ROUND(100*SAFE_DIVIDE(COUNTIF(cp.ip IS NOT NULL),COUNT(*)),4) AS clickpass_rate_pct,
  ROUND(100*SAFE_DIVIDE(COUNTIF(gd.ip IS NOT NULL),COUNT(*)),4) AS guid_rate_pct,
  ROUND(100*SAFE_DIVIDE(COUNTIF(cv.ip IS NOT NULL),COUNT(*)),5) AS conv_rate_pct
FROM cohort g
LEFT JOIN clickpass_v cp USING(ip)
LEFT JOIN guid_v gd USING(ip)
LEFT JOIN conv_v cv USING(ip)
GROUP BY 1 ORDER BY 1;
