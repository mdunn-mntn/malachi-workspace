-- audi_1223 — presence probe with positive control: Gruns (42097) returns 23M rows
-- spanning the table's full history; ThirdLove (32127) returns none.
SELECT advertiser_id, partner_id, MIN(dt) AS first_dt, MAX(dt) AS last_dt, COUNT(*) AS rows_
FROM `dw-main-silver.enriched.lift__ghost_bid_visits`
WHERE advertiser_id IN (42097, 32127)
GROUP BY 1, 2
