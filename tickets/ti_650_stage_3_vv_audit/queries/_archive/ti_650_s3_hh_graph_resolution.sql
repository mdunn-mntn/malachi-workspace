-- TI-650: Household IP graph resolution for 752 unresolved S3 VVs
-- Tests approach #1: graph_ips_aa_100pct_ip links IPs to households.
-- If an unresolved S3 bid_ip shares a household with an IP that has an S1 impression, that's a link.

WITH el AS (
    SELECT ad_served_id,
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
    SELECT ad_served_id, ip AS vast_start_ip, ip AS bid_ip, campaign_id, time AS impression_time
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
    FROM (SELECT ad_served_id, vast_start_ip, bid_ip, campaign_id, impression_time FROM el
          UNION ALL
          SELECT ad_served_id, vast_start_ip, bid_ip, campaign_id, impression_time FROM cil)
),
-- S1 impressions: dedup by vast_start_ip (earliest per IP)
s1_by_vast_start AS (
    SELECT ip.vast_start_ip, ip.impression_time
    FROM impression_pool ip
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = ip.campaign_id AND c.deleted = FALSE AND c.funnel_level = 1
    WHERE ip.rn = 1 AND ip.vast_start_ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ip.vast_start_ip ORDER BY ip.impression_time) = 1
),
-- S3 anchor VVs
cp_s3 AS (
    SELECT cp.ad_served_id, cp.time AS vv_time, cp.attribution_model_id, cp.is_cross_device
    FROM `dw-main-silver.logdata.clickpass_log` cp
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = cp.campaign_id AND c.deleted = FALSE
        AND c.funnel_level = 3 AND c.objective_id IN (1, 5, 6)
    WHERE cp.time >= TIMESTAMP('2026-02-04') AND cp.time < TIMESTAMP('2026-02-11')
      AND cp.advertiser_id = 37775
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),
-- Visit info for imp_visit fallback
v_dedup AS (
    SELECT CAST(ad_served_id AS STRING) AS ad_served_id, impression_ip
    FROM `dw-main-silver.summarydata.ui_visits`
    WHERE from_verified_impression = TRUE
      AND time >= TIMESTAMP('2026-01-28') AND time < TIMESTAMP('2026-02-18')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(ad_served_id AS STRING) ORDER BY time DESC) = 1
),
-- Identify unresolved S3 VVs (neither imp_direct nor imp_visit finds S1)
unresolved AS (
    SELECT cp.ad_served_id, cp.vv_time, imp.bid_ip AS s3_bid_ip,
           cp.attribution_model_id, cp.is_cross_device
    FROM cp_s3 cp
    LEFT JOIN impression_pool imp ON imp.ad_served_id = cp.ad_served_id AND imp.rn = 1
    LEFT JOIN v_dedup v ON v.ad_served_id = cp.ad_served_id
    LEFT JOIN s1_by_vast_start s1d ON s1d.vast_start_ip = imp.bid_ip AND s1d.impression_time < cp.vv_time
    LEFT JOIN s1_by_vast_start s1v ON s1v.vast_start_ip = v.impression_ip AND s1v.impression_time < cp.vv_time
        AND s1d.vast_start_ip IS NULL
    WHERE s1d.vast_start_ip IS NULL AND s1v.vast_start_ip IS NULL
      AND imp.bid_ip IS NOT NULL
),
-- Household graph: find household-linked IPs for unresolved bid_ips
hh_links AS (
    SELECT u.ad_served_id, u.s3_bid_ip, u.vv_time,
           u.attribution_model_id, u.is_cross_device,
           g2.ip AS hh_linked_ip
    FROM unresolved u
    JOIN `dw-main-bronze.tpa.graph_ips_aa_100pct_ip` g1 ON g1.ip = u.s3_bid_ip
    JOIN `dw-main-bronze.tpa.graph_ips_aa_100pct_ip` g2 ON g2.householdid = g1.householdid AND g2.ip != u.s3_bid_ip
),
-- Check which household-linked IPs have S1 impressions
hh_resolved AS (
    SELECT DISTINCT hh.ad_served_id, hh.s3_bid_ip, hh.attribution_model_id, hh.is_cross_device
    FROM hh_links hh
    JOIN s1_by_vast_start s1 ON s1.vast_start_ip = hh.hh_linked_ip AND s1.impression_time < hh.vv_time
)
SELECT
    (SELECT COUNT(*) FROM unresolved) AS total_unresolved,
    (SELECT COUNT(DISTINCT s3_bid_ip) FROM unresolved) AS distinct_unresolved_ips,
    (SELECT COUNT(DISTINCT u.s3_bid_ip) FROM unresolved u
     JOIN `dw-main-bronze.tpa.graph_ips_aa_100pct_ip` g ON g.ip = u.s3_bid_ip) AS ips_in_graph,
    (SELECT COUNT(*) FROM hh_resolved) AS vvs_resolved_by_hh,
    (SELECT COUNT(DISTINCT s3_bid_ip) FROM hh_resolved) AS distinct_ips_resolved,
    (SELECT COUNT(*) FROM hh_resolved WHERE attribution_model_id IN (1,2,3)) AS primary_resolved,
    (SELECT COUNT(*) FROM hh_resolved WHERE attribution_model_id IN (9,10,11)) AS competing_resolved;
