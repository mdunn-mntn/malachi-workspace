-- TI-650: Profile the 567 irreducible unresolved S3 VVs
-- Uses ALL campaigns (incl retargeting) in S1 pool — these are the ones that remain unresolved
-- even with the widest possible pool.
-- Advertiser: 37775 | Trace: Feb 4–11 | Lookback: 90 days

WITH campaigns_all AS (
    SELECT campaign_id, funnel_level, objective_id
    FROM `dw-main-bronze.integrationprod.campaigns`
    WHERE advertiser_id = 37775 AND deleted = FALSE AND is_test = FALSE
      AND funnel_level IN (1, 2, 3)
),

-- S1 pool: ALL campaigns (widest possible)
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

-- S3 VVs (prospecting only — the VVs themselves are prospecting-scoped)
cp_s3 AS (
    SELECT cp.ad_served_id, cp.time AS vv_time, cp.ip AS redirect_ip,
           cp.campaign_id, cp.attribution_model_id, cp.is_cross_device,
           cp.guid, cp.first_touch_ad_served_id
    FROM `dw-main-silver.logdata.clickpass_log` cp
    JOIN campaigns_all c ON c.campaign_id = cp.campaign_id AND c.funnel_level = 3
        AND c.objective_id IN (1, 5, 6)
    WHERE cp.time >= TIMESTAMP('2026-02-04') AND cp.time < TIMESTAMP('2026-02-11')
      AND cp.advertiser_id = 37775
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),

-- Bid IPs from CIL for S3 VVs
bid_ips AS (
    SELECT cil.ad_served_id, cil.ip AS bid_ip, cil.time AS impression_time
    FROM `dw-main-silver.logdata.cost_impression_log` cil
    WHERE cil.advertiser_id = 37775
      AND cil.time >= TIMESTAMP('2025-11-06') AND cil.time < TIMESTAMP('2026-02-11')
      AND cil.ad_served_id IN (SELECT ad_served_id FROM cp_s3)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cil.ad_served_id ORDER BY cil.time ASC) = 1
),

-- Visit IPs from ui_visits
visit_ips AS (
    SELECT CAST(uv.ad_served_id AS STRING) AS ad_served_id, uv.impression_ip
    FROM `dw-main-silver.summarydata.ui_visits` uv
    WHERE uv.time >= TIMESTAMP('2026-01-28') AND uv.time < TIMESTAMP('2026-02-18')
      AND uv.from_verified_impression = TRUE
      AND CAST(uv.ad_served_id AS STRING) IN (SELECT ad_served_id FROM cp_s3)
      AND uv.impression_ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(uv.ad_served_id AS STRING) ORDER BY uv.time ASC) = 1
),

-- Identify the 567 unresolved: have CIL record but no S1 pool match even with all campaigns
unresolved AS (
    SELECT
        cp.ad_served_id,
        cp.vv_time,
        cp.campaign_id,
        cp.attribution_model_id,
        cp.is_cross_device,
        cp.guid,
        cp.first_touch_ad_served_id,
        b.bid_ip,
        b.impression_time AS s3_impression_time,
        v.impression_ip
    FROM cp_s3 cp
    JOIN bid_ips b ON b.ad_served_id = cp.ad_served_id  -- INNER JOIN: must have CIL record
    LEFT JOIN visit_ips v ON v.ad_served_id = cp.ad_served_id
    LEFT JOIN s1_pool s1d ON s1d.match_ip = b.bid_ip AND s1d.impression_time < cp.vv_time
    LEFT JOIN s1_pool s1v ON s1v.match_ip = v.impression_ip AND s1v.impression_time < cp.vv_time
    WHERE s1d.match_ip IS NULL AND s1v.match_ip IS NULL
),

-- Check if bid_ip exists in S1 pool at ANY time (ignoring temporal order)
ip_any_time AS (
    SELECT u.ad_served_id,
           CASE WHEN s1.match_ip IS NOT NULL THEN TRUE ELSE FALSE END AS bid_ip_in_s1_any_time
    FROM unresolved u
    LEFT JOIN s1_pool s1 ON s1.match_ip = u.bid_ip
),

-- Check if GUID exists in guid_identity_daily
guid_check AS (
    SELECT u.ad_served_id,
           CASE WHEN g.guid IS NOT NULL THEN TRUE ELSE FALSE END AS guid_in_identity_daily
    FROM unresolved u
    LEFT JOIN (
        SELECT DISTINCT guid
        FROM `dw-main-silver.aggregates.guid_identity_daily`
        WHERE day >= DATE('2025-11-06') AND day < DATE('2026-02-11')
    ) g ON g.guid = u.guid
)

SELECT
    -- Counts
    COUNT(*) AS total_unresolved,

    -- Cross-device
    COUNTIF(u.is_cross_device = TRUE) AS cross_device_true,
    COUNTIF(u.is_cross_device = FALSE) AS cross_device_false,
    ROUND(COUNTIF(u.is_cross_device = TRUE) * 100.0 / COUNT(*), 1) AS cross_device_pct,

    -- Attribution model split
    COUNTIF(u.attribution_model_id IN (1, 2, 3)) AS primary_attribution,
    COUNTIF(u.attribution_model_id IN (9, 10, 11)) AS competing_attribution,
    ROUND(COUNTIF(u.attribution_model_id IN (9, 10, 11)) * 100.0 / COUNT(*), 1) AS competing_pct,

    -- Attribution model detail
    COUNTIF(u.attribution_model_id = 1) AS model_1_guid,
    COUNTIF(u.attribution_model_id = 2) AS model_2_ip,
    COUNTIF(u.attribution_model_id = 3) AS model_3_ga,
    COUNTIF(u.attribution_model_id = 9) AS model_9_comp_guid,
    COUNTIF(u.attribution_model_id = 10) AS model_10_comp_ip,
    COUNTIF(u.attribution_model_id = 11) AS model_11_comp_ga,

    -- First touch ad_served_id availability
    COUNTIF(u.first_touch_ad_served_id IS NOT NULL) AS has_first_touch,
    COUNTIF(u.first_touch_ad_served_id IS NULL) AS no_first_touch,

    -- Bid IP prefix (carrier identification)
    COUNTIF(STARTS_WITH(u.bid_ip, '172.56.') OR STARTS_WITH(u.bid_ip, '172.57.')
         OR STARTS_WITH(u.bid_ip, '172.58.') OR STARTS_WITH(u.bid_ip, '172.59.')) AS tmobile_cgnat,
    ROUND(COUNTIF(STARTS_WITH(u.bid_ip, '172.56.') OR STARTS_WITH(u.bid_ip, '172.57.')
         OR STARTS_WITH(u.bid_ip, '172.58.') OR STARTS_WITH(u.bid_ip, '172.59.')) * 100.0 / COUNT(*), 1) AS tmobile_cgnat_pct,

    -- IP exists in S1 pool at any time (ignoring temporal order)
    COUNTIF(iat.bid_ip_in_s1_any_time = TRUE) AS ip_in_s1_any_time,
    COUNTIF(iat.bid_ip_in_s1_any_time = FALSE) AS ip_never_in_s1,
    ROUND(COUNTIF(iat.bid_ip_in_s1_any_time = FALSE) * 100.0 / COUNT(*), 1) AS ip_never_in_s1_pct,

    -- GUID bridge potential
    COUNTIF(gc.guid_in_identity_daily = TRUE) AS guid_in_identity,
    COUNTIF(gc.guid_in_identity_daily = FALSE) AS guid_not_in_identity,
    ROUND(COUNTIF(gc.guid_in_identity_daily = TRUE) * 100.0 / COUNT(*), 1) AS guid_bridge_potential_pct,

    -- Impression-to-VV time gap
    ROUND(AVG(TIMESTAMP_DIFF(u.vv_time, u.s3_impression_time, HOUR) / 24.0), 1) AS avg_imp_to_vv_days,
    ROUND(MIN(TIMESTAMP_DIFF(u.vv_time, u.s3_impression_time, HOUR) / 24.0), 1) AS min_imp_to_vv_days,
    ROUND(MAX(TIMESTAMP_DIFF(u.vv_time, u.s3_impression_time, HOUR) / 24.0), 1) AS max_imp_to_vv_days,

    -- Distinct IPs
    COUNT(DISTINCT u.bid_ip) AS distinct_bid_ips

FROM unresolved u
LEFT JOIN ip_any_time iat ON iat.ad_served_id = u.ad_served_id
LEFT JOIN guid_check gc ON gc.ad_served_id = u.ad_served_id;
