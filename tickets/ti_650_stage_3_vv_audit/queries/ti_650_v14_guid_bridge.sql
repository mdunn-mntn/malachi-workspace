-- TI-650: GUID bridge for v14 unresolved (campaign_group_id scoped)
-- For each unresolved VV, look up GUID in guid_identity_daily,
-- find linked IPs, check if any link to S1 pool within same campaign_group_id
-- Advertiser: 37775 | Trace: Feb 4–11 | Lookback: 90 days

WITH campaigns AS (
    SELECT c.campaign_id, c.campaign_group_id, c.funnel_level
    FROM `dw-main-bronze.integrationprod.campaigns` c
    WHERE c.advertiser_id = 37775
      AND c.deleted = FALSE AND c.is_test = FALSE
      AND c.funnel_level IN (1, 2, 3) AND c.objective_id IN (1, 5, 6)
),

s1_pool AS (
    SELECT campaign_group_id, match_ip, MIN(impression_time) AS impression_time
    FROM (
        SELECT c.campaign_group_id, el.ip AS match_ip, MIN(el.time) AS impression_time
        FROM `dw-main-silver.logdata.event_log` el
        JOIN campaigns c ON c.campaign_id = el.campaign_id AND c.funnel_level = 1
        WHERE el.event_type_raw IN ('vast_start', 'vast_impression')
          AND el.time >= TIMESTAMP('2025-11-06') AND el.time < TIMESTAMP('2026-02-11')
          AND el.ip IS NOT NULL
        GROUP BY c.campaign_group_id, el.ip
        UNION ALL
        SELECT c.campaign_group_id, cil.ip AS match_ip, MIN(cil.time) AS impression_time
        FROM `dw-main-silver.logdata.cost_impression_log` cil
        JOIN campaigns c ON c.campaign_id = cil.campaign_id AND c.funnel_level = 1
        WHERE cil.time >= TIMESTAMP('2025-11-06') AND cil.time < TIMESTAMP('2026-02-11')
        GROUP BY c.campaign_group_id, cil.ip
    )
    GROUP BY campaign_group_id, match_ip
),

s2_chain_reachable AS (
    SELECT
        s2v.campaign_group_id,
        s2v.vast_ip AS chain_ip,
        MIN(s2v.vast_time) AS s2_impression_time
    FROM (
        SELECT c.campaign_group_id, el.ip AS vast_ip, el.ad_served_id, MIN(el.time) AS vast_time
        FROM `dw-main-silver.logdata.event_log` el
        JOIN campaigns c ON c.campaign_id = el.campaign_id AND c.funnel_level = 2
        WHERE el.event_type_raw IN ('vast_start', 'vast_impression')
          AND el.time >= TIMESTAMP('2025-11-06') AND el.time < TIMESTAMP('2026-02-11')
          AND el.ip IS NOT NULL
        GROUP BY c.campaign_group_id, el.ip, el.ad_served_id
    ) s2v
    JOIN (
        SELECT cil.ad_served_id, cil.ip AS bid_ip
        FROM `dw-main-silver.logdata.cost_impression_log` cil
        JOIN campaigns c ON c.campaign_id = cil.campaign_id AND c.funnel_level = 2
        WHERE cil.time >= TIMESTAMP('2025-11-06') AND cil.time < TIMESTAMP('2026-02-11')
        QUALIFY ROW_NUMBER() OVER (PARTITION BY cil.ad_served_id ORDER BY cil.time ASC) = 1
    ) s2b ON s2b.ad_served_id = s2v.ad_served_id
    JOIN s1_pool ON s1_pool.campaign_group_id = s2v.campaign_group_id
                 AND s1_pool.match_ip = s2b.bid_ip
                 AND s1_pool.impression_time < s2v.vast_time
    GROUP BY s2v.campaign_group_id, s2v.vast_ip
),

cp_s3 AS (
    SELECT cp.ad_served_id, cp.time AS vv_time, c.campaign_group_id,
           cp.attribution_model_id, cp.is_cross_device, cp.guid
    FROM `dw-main-silver.logdata.clickpass_log` cp
    JOIN campaigns c ON c.campaign_id = cp.campaign_id AND c.funnel_level = 3
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
    SELECT cp.ad_served_id, cp.guid, cp.campaign_group_id,
           cp.attribution_model_id, cp.is_cross_device, b.bid_ip
    FROM cp_s3 cp
    JOIN bid_ips b ON b.ad_served_id = cp.ad_served_id
    LEFT JOIN visit_ips v ON v.ad_served_id = cp.ad_served_id
    LEFT JOIN s1_pool s1d
        ON s1d.campaign_group_id = cp.campaign_group_id
        AND s1d.match_ip = b.bid_ip
        AND s1d.impression_time < cp.vv_time
    LEFT JOIN s1_pool s1v
        ON s1v.campaign_group_id = cp.campaign_group_id
        AND s1v.match_ip = v.impression_ip
        AND s1v.impression_time < cp.vv_time
    LEFT JOIN s2_chain_reachable s2c
        ON s2c.campaign_group_id = cp.campaign_group_id
        AND s2c.chain_ip = b.bid_ip
        AND s2c.s2_impression_time < cp.vv_time
    WHERE s1d.match_ip IS NULL AND s1v.match_ip IS NULL AND s2c.chain_ip IS NULL
),

guid_linked_ips AS (
    SELECT DISTINCT u.ad_served_id, u.guid, u.campaign_group_id,
           u.attribution_model_id, u.is_cross_device,
           u.bid_ip, g.ip AS guid_linked_ip
    FROM unresolved u
    JOIN `dw-main-silver.aggregates.guid_identity_daily` g
        ON g.guid = u.guid
        AND g.day >= DATE('2025-11-06') AND g.day < DATE('2026-02-11')
    WHERE g.ip IS NOT NULL AND g.ip != u.bid_ip
),

guid_resolved AS (
    SELECT DISTINCT gl.ad_served_id, gl.guid, gl.campaign_group_id,
           gl.attribution_model_id, gl.is_cross_device,
           gl.bid_ip, gl.guid_linked_ip
    FROM guid_linked_ips gl
    JOIN s1_pool s1 ON s1.campaign_group_id = gl.campaign_group_id
                    AND s1.match_ip = gl.guid_linked_ip
)

SELECT
    (SELECT COUNT(*) FROM unresolved) AS total_unresolved,
    (SELECT COUNT(DISTINCT ad_served_id) FROM guid_linked_ips) AS vvs_with_other_ips,
    (SELECT COUNT(DISTINCT ad_served_id) FROM guid_resolved) AS guid_bridge_resolved,
    (SELECT COUNT(*) FROM unresolved WHERE ad_served_id NOT IN (SELECT ad_served_id FROM guid_resolved)) AS still_unresolved,

    (SELECT COUNT(DISTINCT ad_served_id) FROM guid_resolved WHERE attribution_model_id IN (1,2,3)) AS primary_resolved,
    (SELECT COUNT(DISTINCT ad_served_id) FROM guid_resolved WHERE attribution_model_id IN (9,10,11)) AS competing_resolved,

    (SELECT COUNTIF(attribution_model_id IN (1,2,3)) FROM unresolved
     WHERE ad_served_id NOT IN (SELECT ad_served_id FROM guid_resolved)) AS primary_still_unresolved,
    (SELECT COUNTIF(attribution_model_id IN (9,10,11)) FROM unresolved
     WHERE ad_served_id NOT IN (SELECT ad_served_id FROM guid_resolved)) AS competing_still_unresolved,

    (SELECT COUNTIF(is_cross_device = TRUE) FROM unresolved
     WHERE ad_served_id NOT IN (SELECT ad_served_id FROM guid_resolved)) AS still_unresolved_cross_device,
    (SELECT COUNTIF(is_cross_device = FALSE) FROM unresolved
     WHERE ad_served_id NOT IN (SELECT ad_served_id FROM guid_resolved)) AS still_unresolved_same_device;
