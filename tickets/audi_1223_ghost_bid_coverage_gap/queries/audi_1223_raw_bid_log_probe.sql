-- audi_1223 — THE DISCRIMINATING TEST (Matt Brorby, 2026-08-25): absentees present in
-- the raw Beeswax bid log with ghostBid rows => the sqlmesh silver model drops them.
-- 1h window because the table is ip-clustered (advertiser filter scans ~395 GB/hour).
SELECT advertiser_id, threshold_failure_reasons, COUNT(*) AS bids, COUNT(DISTINCT campaign_id) AS campaigns
FROM `dw-main-bronze.raw.bid_price_log`
WHERE time >= '2026-08-24 18:00:00' AND time < '2026-08-24 19:00:00'
  AND advertiser_id IN (32127, 31602, 32244, 44720, 42097)
GROUP BY 1, 2 ORDER BY 1, 3 DESC
