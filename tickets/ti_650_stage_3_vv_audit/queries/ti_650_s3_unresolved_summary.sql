-- TI-650: Unresolved S3 VVs — 20 random advertisers, grouped by campaign name/objective/funnel/attribution
-- Self-contained script using TEMP TABLEs to avoid re-scanning event_log/CIL.
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

-- Step 1: Impression pool — materialized once, used 3x downstream
CREATE TEMP TABLE impression_pool AS
WITH campaigns_in_scope AS (
    SELECT campaign_id, advertiser_id, funnel_level
    FROM `dw-main-bronze.integrationprod.campaigns`
    WHERE deleted = FALSE AND is_test = FALSE
      AND funnel_level IN (1, 2, 3)
      AND objective_id IN (1, 5, 6)
      AND advertiser_id IN (SELECT advertiser_id FROM sampled_advertisers)
),
el AS (
    SELECT
        e.ad_served_id,
        MAX(CASE WHEN e.event_type_raw = 'vast_start' THEN e.ip END) AS vast_start_ip,
        MAX(e.bid_ip) AS bid_ip,
        MAX(e.campaign_id) AS campaign_id,
        MIN(e.time) AS impression_time
    FROM `dw-main-silver.logdata.event_log` e
    WHERE e.event_type_raw IN ('vast_start', 'vast_impression')
      AND e.time >= TIMESTAMP('2025-11-06') AND e.time < TIMESTAMP('2026-02-11')
      AND e.campaign_id IN (SELECT campaign_id FROM campaigns_in_scope)
    GROUP BY e.ad_served_id
),
cil AS (
    SELECT
        c.ad_served_id,
        c.ip AS vast_start_ip,
        c.ip AS bid_ip,
        c.campaign_id,
        c.time AS impression_time
    FROM `dw-main-silver.logdata.cost_impression_log` c
    WHERE c.time >= TIMESTAMP('2025-11-06') AND c.time < TIMESTAMP('2026-02-11')
      AND c.advertiser_id IN (SELECT advertiser_id FROM sampled_advertisers)
      AND c.campaign_id IN (SELECT campaign_id FROM campaigns_in_scope)
),
ranked AS (
    SELECT
        ad_served_id, vast_start_ip, bid_ip, campaign_id, impression_time,
        cs.advertiser_id, cs.funnel_level,
        ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY impression_time ASC) AS rn
    FROM (SELECT * FROM el UNION ALL SELECT * FROM cil) pool
    JOIN campaigns_in_scope cs USING (campaign_id)
)
SELECT * FROM ranked;

-- Step 2: S1 pool — earliest S1 vast_start_ip per (advertiser, IP)
CREATE TEMP TABLE s1_by_vast_start AS
SELECT vast_start_ip, advertiser_id, impression_time
FROM impression_pool
WHERE rn = 1 AND funnel_level = 1 AND vast_start_ip IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY advertiser_id, vast_start_ip
    ORDER BY impression_time
) = 1;

-- Step 3: Visit dedup — impression_ip for imp_visit fallback
CREATE TEMP TABLE v_dedup AS
SELECT CAST(ad_served_id AS STRING) AS ad_served_id, impression_ip
FROM `dw-main-silver.summarydata.ui_visits`
WHERE from_verified_impression = TRUE
  AND time >= TIMESTAMP('2026-01-28') AND time < TIMESTAMP('2026-02-18')
QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(ad_served_id AS STRING) ORDER BY time DESC) = 1;

-- Step 4: S3 VVs — clickpass anchor rows
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
LEFT JOIN impression_pool imp
    ON imp.ad_served_id = cp.ad_served_id AND imp.rn = 1
LEFT JOIN v_dedup v
    ON v.ad_served_id = cp.ad_served_id
-- imp_direct: S1 impression at S3's bid_ip (same advertiser)
LEFT JOIN s1_by_vast_start s1d
    ON s1d.vast_start_ip = imp.bid_ip
    AND s1d.advertiser_id = cp.advertiser_id
    AND s1d.impression_time < cp.vv_time
-- imp_visit: S1 impression at ui_visits.impression_ip (same advertiser, fallback)
LEFT JOIN s1_by_vast_start s1v
    ON s1v.vast_start_ip = v.impression_ip
    AND s1v.advertiser_id = cp.advertiser_id
    AND s1v.impression_time < cp.vv_time
    AND s1d.vast_start_ip IS NULL
JOIN `dw-main-bronze.integrationprod.campaigns` c
    ON c.campaign_id = cp.campaign_id AND c.deleted = FALSE
LEFT JOIN `dw-main-silver.core.objectives` o
    ON o.objective_id = c.objective_id
WHERE s1d.vast_start_ip IS NULL AND s1v.vast_start_ip IS NULL
  AND imp.bid_ip IS NOT NULL
GROUP BY c.name, c.objective_id, o.name, c.funnel_level, cp.attribution_model_id
ORDER BY vv_count DESC;
