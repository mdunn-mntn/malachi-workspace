-- TI-1044: ElevenLabs (AID 51660) ghost-ad incrementality lift (TI-837 method, new bidder ghost logs).
-- Control  = ghost-holdout IPs: bid_price_log threshold_failure_reasons='ghostBid' (Beeswax), is_ctv.
-- Treated  = served IPs: spend_log won impressions (got an ad).
-- Groups disjoint (holdout is a deterministic 10% hash → a ghost IP is never served).
-- Outcome  = did the IP visit (clickpass_log) / convert (conversion_log) in the window.
-- Lift = treated_rate / control_rate - 1, for visit rate (IVR) and conversion rate (CVR).
-- Window = last ~10 days (bid_price_log TTL). National campaign is live throughout.
-- CAVEATS: (1) served won auctions, ghost didn't necessarily (win-selection); (2) ghost bids are NOT
--   frequency-capped → holdout over-represents high-frequency/high-visit IPs → lift biased DOWN (lower bound).
DECLARE d0 DATE DEFAULT '2026-06-13';
DECLARE d1 DATE DEFAULT '2026-06-22';   -- cohort window
DECLARE o1 DATE DEFAULT '2026-06-23';   -- outcome window end (+1d for visit/conv lag)

WITH
ghost AS (
  SELECT DISTINCT ip
  FROM `dw-main-bronze.raw.bid_price_log`
  WHERE advertiser_id = 51660 AND is_ctv AND threshold_failure_reasons = 'ghostBid'
    AND DATE(time) BETWEEN d0 AND d1 AND ip IS NOT NULL AND ip != ''
),
served AS (
  -- cost_impression_log is clustered on advertiser_id → prunes to just this advertiser (cheap).
  SELECT DISTINCT ip
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE advertiser_id = 51660 AND DATE(time) BETWEEN d0 AND d1
    AND ip IS NOT NULL AND ip != ''
),
cohort AS (
  SELECT ip, 'treated' AS grp FROM served
  UNION ALL
  SELECT g.ip, 'control' AS grp
  FROM ghost g LEFT JOIN served s USING (ip)
  WHERE s.ip IS NULL                     -- pure holdout: ghosted and never served
),
visitors AS (
  SELECT DISTINCT ip FROM `dw-main-silver.logdata.clickpass_log`
  WHERE advertiser_id = 51660 AND DATE(time) BETWEEN d0 AND o1 AND ip IS NOT NULL AND ip != ''
),
converters AS (
  SELECT DISTINCT ip FROM `dw-main-silver.logdata.conversion_log`
  WHERE advertiser_id = 51660 AND DATE(time) BETWEEN d0 AND o1 AND ip IS NOT NULL AND ip != ''
)
SELECT
  g.grp,
  COUNT(*)                                              AS ips,
  COUNTIF(v.ip IS NOT NULL)                             AS visitors,
  COUNTIF(c.ip IS NOT NULL)                             AS converters,
  ROUND(100 * SAFE_DIVIDE(COUNTIF(v.ip IS NOT NULL), COUNT(*)), 4) AS visit_rate_pct,
  ROUND(100 * SAFE_DIVIDE(COUNTIF(c.ip IS NOT NULL), COUNT(*)), 5) AS conv_rate_pct
FROM cohort g
LEFT JOIN visitors v USING (ip)
LEFT JOIN converters c USING (ip)
GROUP BY 1
ORDER BY 1;
