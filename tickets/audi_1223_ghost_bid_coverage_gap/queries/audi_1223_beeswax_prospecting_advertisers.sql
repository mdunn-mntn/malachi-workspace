-- audi_1223 — advertisers with active Beeswax standard prospecting in the clean
-- ghost-bid measurement window (2026-06-23..07-07). Basis of the absentee list:
-- these minus advertisers present in enriched.lift__ghost_bid_visits.
SELECT cil.advertiser_id, COUNT(*) AS imps
FROM `dw-main-silver.logdata.cost_impression_log` cil
JOIN `dw-main-bronze.integrationprod.campaigns` c ON cil.campaign_id = c.campaign_id
WHERE DATE(cil.time) BETWEEN '2026-06-23' AND '2026-07-07'
  AND cil.partner_id = 8 AND c.objective_id = 1 AND c.funnel_level = 1
GROUP BY 1
HAVING imps >= 10000
ORDER BY imps DESC
