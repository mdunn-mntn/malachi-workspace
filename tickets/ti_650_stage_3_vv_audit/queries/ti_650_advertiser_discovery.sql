-- TI-650: Advertiser Discovery
-- Find S3 advertisers with VV volume in a date range. Cheap discovery query.
-- Cost: ~0.5 GB
--
-- ╔══════════════════════════════════════════════════════════════════╗
-- ║  PARAMETERS — 2 things to change (marked with ── PARAM ──)     ║
-- ╠══════════════════════════════════════════════════════════════════╣
-- ║  1. AUDIT_WINDOW  — date range for S3 VV volume                ║
-- ║  2. MIN_VVS       — minimum VV count threshold (default: 100)  ║
-- ╚══════════════════════════════════════════════════════════════════╝

SELECT
    c.advertiser_id,
    adv.company_name AS advertiser_name,
    SUM(a.views) AS s3_vvs
FROM `dw-main-silver.aggregates.agg__daily_sum_by_campaign` a
JOIN `dw-main-bronze.integrationprod.campaigns` c
    ON c.campaign_id = a.campaign_id
    AND c.deleted = FALSE AND c.is_test = FALSE
    AND c.funnel_level = 3
    AND c.objective_id IN (1, 5, 6)
JOIN `dw-main-bronze.integrationprod.advertisers` adv
    ON c.advertiser_id = adv.advertiser_id AND adv.deleted = FALSE
-- ── AUDIT_WINDOW ──
WHERE a.day BETWEEN '2026-03-10' AND '2026-03-16'
GROUP BY c.advertiser_id, adv.company_name
-- ── MIN_VVS ──
HAVING SUM(a.views) >= 100
ORDER BY s3_vvs DESC
LIMIT 50;
