-- TI-650: List distinct unresolved S3 IPs
-- Self-contained query. Returns the 616 IPs that cannot resolve to S1.
-- Advertiser: 37775 | Trace: Feb 4-11 | Lookback: 90 days | Prospecting only

WITH el AS (
    SELECT
        ad_served_id,
        MAX(CASE WHEN event_type_raw = 'vast_start' THEN ip END) AS vast_start_ip,
        MAX(bid_ip) AS bid_ip,
        MAX(campaign_id) AS campaign_id,
        MIN(time) AS impression_time
    FROM `dw-main-silver.logdata.event_log`
    WHERE event_type_raw IN ('vast_start', 'vast_impression')
      AND time >= TIMESTAMP('2025-11-06') AND time < TIMESTAMP('2026-02-11')
      AND campaign_id IN (
          SELECT campaign_id FROM `dw-main-bronze.integrationprod.campaigns`
          WHERE advertiser_id = 37775 AND deleted = FALSE
            AND funnel_level IN (1, 2, 3) AND objective_id IN (1, 5, 6)
      )
    GROUP BY ad_served_id
),
cil AS (
    SELECT
        ad_served_id,
        ip AS vast_start_ip,
        ip AS bid_ip,
        campaign_id,
        time AS impression_time
    FROM `dw-main-silver.logdata.cost_impression_log`
    WHERE time >= TIMESTAMP('2025-11-06') AND time < TIMESTAMP('2026-02-11')
      AND advertiser_id = 37775
      AND campaign_id IN (
          SELECT campaign_id FROM `dw-main-bronze.integrationprod.campaigns`
          WHERE advertiser_id = 37775 AND deleted = FALSE
            AND funnel_level IN (1, 2, 3) AND objective_id IN (1, 5, 6)
      )
),
impression_pool AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY impression_time ASC) AS rn
    FROM (SELECT * FROM el UNION ALL SELECT * FROM cil)
),
s1_by_vast_start AS (
    SELECT ip.vast_start_ip, ip.impression_time
    FROM impression_pool ip
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = ip.campaign_id AND c.deleted = FALSE AND c.funnel_level = 1
    WHERE ip.rn = 1 AND ip.vast_start_ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ip.vast_start_ip ORDER BY ip.impression_time) = 1
),
v_dedup AS (
    SELECT CAST(ad_served_id AS STRING) AS ad_served_id, impression_ip
    FROM `dw-main-silver.summarydata.ui_visits`
    WHERE from_verified_impression = TRUE
      AND time >= TIMESTAMP('2026-01-28') AND time < TIMESTAMP('2026-02-18')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(ad_served_id AS STRING) ORDER BY time DESC) = 1
),
cp_s3 AS (
    SELECT cp.ad_served_id, cp.time AS vv_time
    FROM `dw-main-silver.logdata.clickpass_log` cp
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = cp.campaign_id AND c.deleted = FALSE
        AND c.funnel_level = 3 AND c.objective_id IN (1, 5, 6)
    WHERE cp.time >= TIMESTAMP('2026-02-04') AND cp.time < TIMESTAMP('2026-02-11')
      AND cp.advertiser_id = 37775
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),
unresolved AS (
    SELECT imp.bid_ip AS s3_bid_ip
    FROM cp_s3 cp
    LEFT JOIN impression_pool imp ON imp.ad_served_id = cp.ad_served_id AND imp.rn = 1
    LEFT JOIN v_dedup v ON v.ad_served_id = cp.ad_served_id
    LEFT JOIN s1_by_vast_start s1d
        ON s1d.vast_start_ip = imp.bid_ip AND s1d.impression_time < cp.vv_time
    LEFT JOIN s1_by_vast_start s1v
        ON s1v.vast_start_ip = v.impression_ip AND s1v.impression_time < cp.vv_time
        AND s1d.vast_start_ip IS NULL
    WHERE s1d.vast_start_ip IS NULL AND s1v.vast_start_ip IS NULL
      AND imp.bid_ip IS NOT NULL
)
SELECT DISTINCT s3_bid_ip AS unresolved_ip
FROM unresolved
ORDER BY unresolved_ip;
