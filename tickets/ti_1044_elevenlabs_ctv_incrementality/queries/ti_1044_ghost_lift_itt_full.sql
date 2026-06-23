-- TI-1044: clean ITT (targeted-and-bid vs ghost-holdout, pre-auction) with ALL THREE outcomes.
-- Gives the defensible lift table: total-traffic visits (guid), attributed visits (clickpass), conversions.
DECLARE d0 DATE DEFAULT '2026-06-13';
DECLARE d1 DATE DEFAULT '2026-06-22';
DECLARE o1 DATE DEFAULT '2026-06-23';

WITH bp AS (
  SELECT ip,
         LOGICAL_OR(threshold_failure_reasons = 'ghostBid') AS ever_ghost,
         LOGICAL_OR(threshold_failure_reasons IS NULL OR threshold_failure_reasons = '') AS ever_placed
  FROM `dw-main-bronze.raw.bid_price_log`
  WHERE advertiser_id = 51660 AND is_ctv
    AND DATE(time) BETWEEN d0 AND d1 AND ip IS NOT NULL AND ip != ''
  GROUP BY ip
),
cohort AS (
  SELECT ip,'treated' AS grp FROM bp WHERE ever_placed
  UNION ALL
  SELECT ip,'control' AS grp FROM bp WHERE ever_ghost AND NOT ever_placed
),
clickpass_v AS (SELECT DISTINCT ip FROM `dw-main-silver.logdata.clickpass_log`
  WHERE advertiser_id=51660 AND DATE(time) BETWEEN d0 AND o1 AND ip IS NOT NULL AND ip!=''),
guid_v AS (SELECT DISTINCT ip FROM `dw-main-silver.logdata.guid_log`
  WHERE advertiser_id=51660 AND DATE(time) BETWEEN d0 AND o1 AND ip IS NOT NULL AND ip!=''),
conv_v AS (SELECT DISTINCT ip FROM `dw-main-silver.logdata.conversion_log`
  WHERE advertiser_id=51660 AND DATE(time) BETWEEN d0 AND o1 AND ip IS NOT NULL AND ip!='')
SELECT g.grp, COUNT(*) AS ips,
  COUNTIF(cp.ip IS NOT NULL) AS clickpass_visitors,
  COUNTIF(gd.ip IS NOT NULL) AS guid_visitors,
  COUNTIF(cv.ip IS NOT NULL) AS converters
FROM cohort g
LEFT JOIN clickpass_v cp USING(ip)
LEFT JOIN guid_v gd USING(ip)
LEFT JOIN conv_v cv USING(ip)
GROUP BY 1 ORDER BY 1;
