-- TI-650: GUID bridge resolution on the 567 irreducible unresolved S3 VVs
-- Uses ALL campaigns in S1 pool. For each unresolved VV, look up GUID in
-- guid_identity_daily, find linked IPs, check if any link to S1 pool.
-- Advertiser: 37775 | Trace: Feb 4–11 | Lookback: 90 days

WITH campaigns_all AS (
    SELECT campaign_id, funnel_level, objective_id
    FROM `dw-main-bronze.integrationprod.campaigns`
    WHERE advertiser_id = 37775 AND deleted = FALSE AND is_test = FALSE
      AND funnel_level IN (1, 2, 3)
),

s1_pool AS (
    SELECT match_ip, MIN(impression_time) AS impression_time
    FROM (
        SELECT ip AS match_ip, MIN(time) AS impression_time
        FROM `dw-main-silver.logdata.event_log`
        WHERE event_type_raw IN ('vast_start', 'vast_impression')
          AND time >= TIMESTAMP('2025-11-06') AND time < TIMESTAMP('2026-02-11')
          AND campaign_id IN (SELECT campaign_id FROM campaigns_all WHERE funnel_level = 1)
          AND ip IS NOT NULL
        GROUP BY ip
        UNION ALL
        SELECT ip AS match_ip, MIN(time) AS impression_time
        FROM `dw-main-silver.logdata.cost_impression_log`
        WHERE time >= TIMESTAMP('2025-11-06') AND time < TIMESTAMP('2026-02-11')
          AND advertiser_id = 37775
          AND campaign_id IN (SELECT campaign_id FROM campaigns_all WHERE funnel_level = 1)
        GROUP BY ip
    )
    GROUP BY match_ip
),

s1_ips AS (
    SELECT DISTINCT match_ip FROM s1_pool
),

cp_s3 AS (
    SELECT cp.ad_served_id, cp.time AS vv_time, cp.campaign_id,
           cp.attribution_model_id, cp.is_cross_device, cp.guid
    FROM `dw-main-silver.logdata.clickpass_log` cp
    JOIN campaigns_all c ON c.campaign_id = cp.campaign_id AND c.funnel_level = 3
        AND c.objective_id IN (1, 5, 6)
    WHERE cp.time >= TIMESTAMP('2026-02-04') AND cp.time < TIMESTAMP('2026-02-11')
      AND cp.advertiser_id = 37775
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),

bid_ips AS (
    SELECT cil.ad_served_id, cil.ip AS bid_ip
    FROM `dw-main-silver.logdata.cost_impression_log` cil
    WHERE cil.advertiser_id = 37775
      AND cil.time >= TIMESTAMP('2025-11-06') AND cil.time < TIMESTAMP('2026-02-11')
      AND cil.ad_served_id IN (SELECT ad_served_id FROM cp_s3)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cil.ad_served_id ORDER BY cil.time ASC) = 1
),

visit_ips AS (
    SELECT CAST(uv.ad_served_id AS STRING) AS ad_served_id, uv.impression_ip
    FROM `dw-main-silver.summarydata.ui_visits` uv
    WHERE uv.time >= TIMESTAMP('2026-01-28') AND uv.time < TIMESTAMP('2026-02-18')
      AND uv.from_verified_impression = TRUE
      AND CAST(uv.ad_served_id AS STRING) IN (SELECT ad_served_id FROM cp_s3)
      AND uv.impression_ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(uv.ad_served_id AS STRING) ORDER BY uv.time ASC) = 1
),

unresolved AS (
    SELECT cp.ad_served_id, cp.guid, cp.attribution_model_id, cp.is_cross_device,
           b.bid_ip
    FROM cp_s3 cp
    JOIN bid_ips b ON b.ad_served_id = cp.ad_served_id
    LEFT JOIN visit_ips v ON v.ad_served_id = cp.ad_served_id
    LEFT JOIN s1_pool s1d ON s1d.match_ip = b.bid_ip AND s1d.impression_time < cp.vv_time
    LEFT JOIN s1_pool s1v ON s1v.match_ip = v.impression_ip AND s1v.impression_time < cp.vv_time
    WHERE s1d.match_ip IS NULL AND s1v.match_ip IS NULL
),

guid_linked_ips AS (
    SELECT DISTINCT u.ad_served_id, u.guid, u.attribution_model_id, u.is_cross_device,
           u.bid_ip, g.ip AS guid_linked_ip
    FROM unresolved u
    JOIN `dw-main-silver.aggregates.guid_identity_daily` g
        ON g.guid = u.guid
        AND g.day >= DATE('2025-11-06') AND g.day < DATE('2026-02-11')
    WHERE g.ip IS NOT NULL AND g.ip != u.bid_ip
),

guid_resolved AS (
    SELECT DISTINCT gl.ad_served_id, gl.guid, gl.attribution_model_id, gl.is_cross_device,
           gl.bid_ip, gl.guid_linked_ip
    FROM guid_linked_ips gl
    JOIN s1_ips s1 ON s1.match_ip = gl.guid_linked_ip
)

SELECT
    (SELECT COUNT(*) FROM unresolved) AS total_unresolved,
    (SELECT COUNT(DISTINCT ad_served_id) FROM guid_linked_ips) AS vvs_with_other_ips,
    (SELECT COUNT(DISTINCT ad_served_id) FROM guid_resolved) AS guid_bridge_resolved,
    (SELECT COUNT(*) FROM unresolved WHERE ad_served_id NOT IN (SELECT ad_served_id FROM guid_resolved)) AS still_unresolved_after_guid,

    -- Breakdown of resolved by attribution model
    (SELECT COUNT(DISTINCT ad_served_id) FROM guid_resolved WHERE attribution_model_id IN (1,2,3)) AS primary_resolved,
    (SELECT COUNT(DISTINCT ad_served_id) FROM guid_resolved WHERE attribution_model_id IN (9,10,11)) AS competing_resolved,

    -- Breakdown of still-unresolved by attribution model
    (SELECT COUNTIF(attribution_model_id IN (1,2,3)) FROM unresolved
     WHERE ad_served_id NOT IN (SELECT ad_served_id FROM guid_resolved)) AS primary_still_unresolved,
    (SELECT COUNTIF(attribution_model_id IN (9,10,11)) FROM unresolved
     WHERE ad_served_id NOT IN (SELECT ad_served_id FROM guid_resolved)) AS competing_still_unresolved,

    -- Cross-device breakdown of still-unresolved
    (SELECT COUNTIF(is_cross_device = TRUE) FROM unresolved
     WHERE ad_served_id NOT IN (SELECT ad_served_id FROM guid_resolved)) AS still_unresolved_cross_device,
    (SELECT COUNTIF(is_cross_device = FALSE) FROM unresolved
     WHERE ad_served_id NOT IN (SELECT ad_served_id FROM guid_resolved)) AS still_unresolved_same_device,

    -- Distinct S1 IPs found via GUID bridge
    (SELECT COUNT(DISTINCT guid_linked_ip) FROM guid_resolved) AS distinct_s1_ips_found;
