-- TI-1044: ElevenLabs ghost-ad lift — CLEAN ITT (removes win-selection bias).
-- Both arms are PRE-AUCTION random partitions of the eligible pool (ghost = held out at bid stage):
--   control = ghost-holdout (threshold_failure_reasons='ghostBid'), never bid-placed.
--   treated = targeted & bid-placed (bid_placed=true) — entered the auction, NOT held out.
-- Single bid_price_log scan (no spend_log). ITT lift is diluted by win-rate but UNBIASED by selection.
-- If ITT conv lift ~0 / insignificant, the served-vs-ghost +34% was win-selection, consistent w/ geo null.
DECLARE d0 DATE DEFAULT '2026-06-13';
DECLARE d1 DATE DEFAULT '2026-06-22';
DECLARE o1 DATE DEFAULT '2026-06-23';

WITH bp AS (
  SELECT ip,
         LOGICAL_OR(threshold_failure_reasons = 'ghostBid')                       AS ever_ghost,
         LOGICAL_OR(threshold_failure_reasons IS NULL OR threshold_failure_reasons = '') AS ever_placed
  FROM `dw-main-bronze.raw.bid_price_log`
  WHERE advertiser_id = 51660 AND is_ctv
    AND DATE(time) BETWEEN d0 AND d1 AND ip IS NOT NULL AND ip != ''
  GROUP BY ip
),
cohort AS (
  SELECT ip, 'treated' AS grp FROM bp WHERE ever_placed
  UNION ALL
  SELECT ip, 'control' AS grp FROM bp WHERE ever_ghost AND NOT ever_placed
),
visitors AS (
  SELECT DISTINCT ip FROM `dw-main-silver.logdata.clickpass_log`
  WHERE advertiser_id = 51660 AND DATE(time) BETWEEN d0 AND o1 AND ip IS NOT NULL AND ip != ''
),
converters AS (
  SELECT DISTINCT ip FROM `dw-main-silver.logdata.conversion_log`
  WHERE advertiser_id = 51660 AND DATE(time) BETWEEN d0 AND o1 AND ip IS NOT NULL AND ip != ''
)
SELECT g.grp, COUNT(*) AS ips,
       COUNTIF(v.ip IS NOT NULL) AS visitors,
       COUNTIF(c.ip IS NOT NULL) AS converters,
       ROUND(100*SAFE_DIVIDE(COUNTIF(v.ip IS NOT NULL),COUNT(*)),4) AS visit_rate_pct,
       ROUND(100*SAFE_DIVIDE(COUNTIF(c.ip IS NOT NULL),COUNT(*)),5) AS conv_rate_pct
FROM cohort g
LEFT JOIN visitors v USING (ip)
LEFT JOIN converters c USING (ip)
GROUP BY 1 ORDER BY 1;
