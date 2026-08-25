-- audi_1223 — IP-grain holdout integrity by env, pre-fix implications check.
-- Zero mixed-arm IPs in either env (no IP is both ghost and submitted).
-- ThirdLove 573186 (burnin) single-bid ghost share 2.3% vs 10% design in this hour;
-- Gruns 431456 (prod) 13.0%, burnin 416975 11.9% — per-campaign holdout share needs a
-- longer-window validation pass once burnin rows flow.
WITH b AS (
  SELECT campaign_id, env, ip,
         COUNTIF(threshold_failure_reasons = 'ghostBid') AS g,
         COUNTIF(threshold_failure_reasons = '') AS s,
         COUNT(*) AS n
  FROM `dw-main-bronze.raw.bid_price_log`
  WHERE time >= '2026-08-24 18:00:00' AND time < '2026-08-24 19:00:00'
    AND advertiser_id IN (32127, 44720, 32244, 42097)
    AND threshold_failure_reasons IN ('', 'ghostBid')
  GROUP BY 1, 2, 3
)
SELECT campaign_id, env, COUNT(*) AS ips,
       COUNTIF(n = 1) AS single_bid_ips,
       ROUND(SAFE_DIVIDE(COUNTIF(n = 1 AND g = 1), COUNTIF(n = 1)), 4) AS single_bid_ghost_frac,
       ROUND(SAFE_DIVIDE(COUNTIF(g > 0), COUNT(*)), 4) AS any_ghost_frac,
       COUNTIF(g > 0 AND s > 0) AS mixed_arm_ips
FROM b GROUP BY 1, 2 HAVING ips >= 500 ORDER BY 1, 2
