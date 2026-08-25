-- audi_1223 — ROOT CAUSE confirmation (Matt Brorby, 2026-08-25): env label is
-- per-campaign; the silver lift model keeps env='prod' only. ThirdLove's prospecting
-- is all burnin (prod rows = retargeting only); Gruns' old prospecting is prod but
-- its newer campaigns (626274-6) are burnin too.
SELECT advertiser_id, env, campaign_id,
       COUNTIF(threshold_failure_reasons = '') AS submitted,
       COUNTIF(threshold_failure_reasons = 'ghostBid') AS ghost
FROM `dw-main-bronze.raw.bid_price_log`
WHERE time >= '2026-08-24 18:00:00' AND time < '2026-08-24 19:00:00'
  AND advertiser_id IN (32127, 42097)
  AND threshold_failure_reasons IN ('', 'ghostBid')
GROUP BY 1, 2, 3 ORDER BY 1, 2, submitted DESC
