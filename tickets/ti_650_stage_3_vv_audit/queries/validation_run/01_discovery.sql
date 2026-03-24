-- TI-650 Validation Run: Step 1 — Advertiser Discovery
-- Find S3 advertisers with >= 100 VVs in Mar 16-22, 2026
-- Uses clickpass_log directly (agg table is behind)
-- Pick 10 randomly (mix of large and small)

SELECT
    cp.advertiser_id,
    adv.company_name AS advertiser_name,
    COUNT(*) AS s3_vvs
FROM `dw-main-silver.logdata.clickpass_log` cp
JOIN `dw-main-bronze.integrationprod.campaigns` c
    ON c.campaign_id = cp.campaign_id
    AND c.deleted = FALSE AND c.is_test = FALSE
    AND c.funnel_level = 3
    AND c.objective_id IN (1, 5, 6)
JOIN `dw-main-bronze.integrationprod.advertisers` adv
    ON cp.advertiser_id = adv.advertiser_id AND adv.deleted = FALSE
WHERE cp.time >= TIMESTAMP('2026-03-16')
  AND cp.time < TIMESTAMP('2026-03-23')
  AND cp.ip IS NOT NULL
GROUP BY cp.advertiser_id, adv.company_name
HAVING COUNT(*) >= 100
ORDER BY s3_vvs DESC
LIMIT 50;
