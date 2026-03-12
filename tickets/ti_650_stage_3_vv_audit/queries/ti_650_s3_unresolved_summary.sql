-- TI-650: Unresolved S3 VVs — 20 random advertisers, grouped by campaign name/objective/funnel/attribution
-- Self-contained script using TEMP TABLEs. Splits S1 pool from S3 impression lookup
-- to avoid materializing the full impression_pool (the prior bottleneck).
-- Trace: Feb 4-11 | Lookback: 90 days | Prospecting only (obj 1,5,6)
-- NOTE: Must run as a SCRIPT in BQ (not single statement). Paste entire block.

-- Step 0: Pick 20 random advertisers that have S3 prospecting campaigns
CREATE TEMP TABLE sampled_advertisers AS
SELECT DISTINCT advertiser_id
FROM `dw-main-bronze.integrationprod.campaigns`
WHERE deleted = FALSE AND is_test = FALSE
  AND funnel_level = 3 AND objective_id IN (1, 5, 6)
ORDER BY FARM_FINGERPRINT(CAST(advertiser_id AS STRING))
LIMIT 20;

-- Step 1: S1 impression pool — only funnel_level=1 campaigns, much smaller scan
-- Earliest vast_start_ip per (advertiser, IP) for cross-stage matching
CREATE TEMP TABLE s1_pool AS
WITH s1_campaigns AS (
    SELECT campaign_id, advertiser_id
    FROM `dw-main-bronze.integrationprod.campaigns`
    WHERE deleted = FALSE AND is_test = FALSE
      AND funnel_level = 1 AND objective_id IN (1, 5, 6)
      AND advertiser_id IN (SELECT advertiser_id FROM sampled_advertisers)
),
el_s1 AS (
    SELECT
        MAX(CASE WHEN e.event_type_raw = 'vast_start' THEN e.ip END) AS vast_start_ip,
        MAX(e.bid_ip) AS bid_ip,
        MAX(e.campaign_id) AS campaign_id,
        MIN(e.time) AS impression_time
    FROM `dw-main-silver.logdata.event_log` e
    WHERE e.event_type_raw IN ('vast_start', 'vast_impression')
      AND e.time >= TIMESTAMP('2025-11-06') AND e.time < TIMESTAMP('2026-02-11')
      AND e.campaign_id IN (SELECT campaign_id FROM s1_campaigns)
    GROUP BY e.ad_served_id
),
cil_s1 AS (
    SELECT
        c.ip AS vast_start_ip,
        c.ip AS bid_ip,
        c.campaign_id,
        c.time AS impression_time
    FROM `dw-main-silver.logdata.cost_impression_log` c
    WHERE c.time >= TIMESTAMP('2025-11-06') AND c.time < TIMESTAMP('2026-02-11')
      AND c.advertiser_id IN (SELECT advertiser_id FROM sampled_advertisers)
      AND c.campaign_id IN (SELECT campaign_id FROM s1_campaigns)
),
combined AS (
    SELECT COALESCE(vast_start_ip, bid_ip) AS match_ip, sc.advertiser_id, impression_time
    FROM (SELECT * FROM el_s1 UNION ALL SELECT * FROM cil_s1) pool
    JOIN s1_campaigns sc USING (campaign_id)
    WHERE COALESCE(vast_start_ip, bid_ip) IS NOT NULL
)
SELECT match_ip, advertiser_id, MIN(impression_time) AS impression_time
FROM combined
GROUP BY match_ip, advertiser_id;

-- Step 2: S3 VVs — clickpass anchor rows
CREATE TEMP TABLE cp_s3 AS
SELECT
    cp.ad_served_id, cp.time AS vv_time, cp.ip AS redirect_ip,
    cp.campaign_id, cp.advertiser_id, cp.attribution_model_id, cp.is_cross_device
FROM `dw-main-silver.logdata.clickpass_log` cp
WHERE cp.time >= TIMESTAMP('2026-02-04') AND cp.time < TIMESTAMP('2026-02-11')
  AND cp.advertiser_id IN (SELECT advertiser_id FROM sampled_advertisers)
  AND cp.campaign_id IN (
      SELECT campaign_id FROM `dw-main-bronze.integrationprod.campaigns`
      WHERE deleted = FALSE AND is_test = FALSE
        AND funnel_level = 3 AND objective_id IN (1, 5, 6)
  )
QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1;

-- Step 3: S3 bid_ips — just the bid_ip for each S3 VV's ad_served_id (CIL only, small)
CREATE TEMP TABLE s3_bid_ips AS
SELECT c.ad_served_id, c.ip AS bid_ip
FROM `dw-main-silver.logdata.cost_impression_log` c
INNER JOIN cp_s3 cp ON cp.ad_served_id = c.ad_served_id
WHERE c.advertiser_id IN (SELECT advertiser_id FROM sampled_advertisers)
  AND c.time >= TIMESTAMP('2025-11-06') AND c.time < TIMESTAMP('2026-02-11')
QUALIFY ROW_NUMBER() OVER (PARTITION BY c.ad_served_id ORDER BY c.time ASC) = 1;

-- Step 4: Visit dedup — impression_ip for imp_visit fallback
CREATE TEMP TABLE v_dedup AS
SELECT CAST(v.ad_served_id AS STRING) AS ad_served_id, v.impression_ip
FROM `dw-main-silver.summarydata.ui_visits` v
INNER JOIN cp_s3 cp ON cp.ad_served_id = CAST(v.ad_served_id AS STRING)
WHERE v.from_verified_impression = TRUE
  AND v.time >= TIMESTAMP('2026-01-28') AND v.time < TIMESTAMP('2026-02-18')
QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(v.ad_served_id AS STRING) ORDER BY v.time DESC) = 1;

-- Step 5: Resolve — find unresolved and aggregate
SELECT
    c.name AS campaign_name,
    c.objective_id,
    o.name AS objective_name,
    c.funnel_level,
    cp.attribution_model_id,
    CASE
        WHEN cp.attribution_model_id IN (1, 2, 3) THEN 'primary'
        WHEN cp.attribution_model_id IN (9, 10, 11) THEN 'competing'
    END AS attribution_type,
    COUNT(*) AS vv_count,
    COUNT(DISTINCT cp.advertiser_id) AS advertiser_count,
    COUNT(DISTINCT imp.bid_ip) AS distinct_ips,
    COUNTIF(cp.is_cross_device) AS cross_device_count,
    ROUND(100.0 * COUNTIF(cp.is_cross_device) / COUNT(*), 1) AS cross_device_pct
FROM cp_s3 cp
LEFT JOIN s3_bid_ips imp ON imp.ad_served_id = cp.ad_served_id
LEFT JOIN v_dedup v ON v.ad_served_id = cp.ad_served_id
-- imp_direct: S1 impression at S3's bid_ip (same advertiser)
LEFT JOIN s1_pool s1d
    ON s1d.match_ip = imp.bid_ip
    AND s1d.advertiser_id = cp.advertiser_id
    AND s1d.impression_time < cp.vv_time
-- imp_visit: S1 impression at ui_visits.impression_ip (same advertiser, fallback)
LEFT JOIN s1_pool s1v
    ON s1v.match_ip = v.impression_ip
    AND s1v.advertiser_id = cp.advertiser_id
    AND s1v.impression_time < cp.vv_time
    AND s1d.match_ip IS NULL
JOIN `dw-main-bronze.integrationprod.campaigns` c
    ON c.campaign_id = cp.campaign_id AND c.deleted = FALSE
LEFT JOIN `dw-main-silver.core.objectives` o
    ON o.objective_id = c.objective_id
WHERE s1d.match_ip IS NULL AND s1v.match_ip IS NULL
  AND imp.bid_ip IS NOT NULL
GROUP BY c.name, c.objective_id, o.name, c.funnel_level, cp.attribution_model_id
ORDER BY vv_count DESC;
