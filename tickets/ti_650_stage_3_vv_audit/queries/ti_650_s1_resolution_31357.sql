-- TI-650: S1 VV resolution test - advertiser 31357
-- Verify every S1 VV resolves to its impression via ad_served_id
-- Expected: 100% (within-stage is deterministic)

DECLARE p_advertiser_id INT64 DEFAULT 31357;
DECLARE p_vv_start TIMESTAMP DEFAULT TIMESTAMP('2026-02-04');
DECLARE p_vv_end TIMESTAMP DEFAULT TIMESTAMP('2026-02-11');
DECLARE p_lookback_start TIMESTAMP DEFAULT TIMESTAMP_SUB(TIMESTAMP('2026-02-04'), INTERVAL 90 DAY);

WITH s1_vvs AS (
    SELECT
        cp.ad_served_id,
        cp.ip AS clickpass_ip,
        cp.time AS vv_time,
        cp.campaign_id,
        c.campaign_group_id
    FROM `dw-main-silver.logdata.clickpass_log` cp
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = cp.campaign_id
        AND c.deleted = FALSE AND c.is_test = FALSE
        AND c.funnel_level = 1 AND c.objective_id IN (1, 5, 6)
    WHERE cp.time >= p_vv_start AND cp.time < p_vv_end
      AND cp.advertiser_id = p_advertiser_id
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),

-- event_log (CTV path: vast_start/vast_impression)
el_match AS (
    SELECT ad_served_id, ip AS el_ip, bid_ip AS el_bid_ip
    FROM `dw-main-silver.logdata.event_log`
    WHERE event_type_raw IN ('vast_start', 'vast_impression')
      AND time >= p_lookback_start AND time < p_vv_end
      AND advertiser_id = p_advertiser_id
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time) = 1
),

-- cost_impression_log (bid IP)
cil_match AS (
    SELECT ad_served_id, ip AS cil_ip
    FROM `dw-main-silver.logdata.cost_impression_log`
    WHERE time >= p_lookback_start AND time < p_vv_end
      AND advertiser_id = p_advertiser_id
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

-- viewability_log (display viewable path)
vl_match AS (
    SELECT ad_served_id, ip AS vl_ip
    FROM `dw-main-silver.logdata.viewability_log`
    WHERE time >= p_lookback_start AND time < p_vv_end
      AND advertiser_id = p_advertiser_id
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

-- impression_log (display non-viewable path)
il_match AS (
    SELECT ad_served_id, ip AS il_ip
    FROM `dw-main-silver.logdata.impression_log`
    WHERE time >= p_lookback_start AND time < p_vv_end
      AND advertiser_id = p_advertiser_id
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
)

SELECT
    COUNT(*) AS total_s1_vvs,
    COUNTIF(el.el_ip IS NOT NULL) AS has_event_log,
    COUNTIF(cil.cil_ip IS NOT NULL) AS has_cil,
    COUNTIF(vl.vl_ip IS NOT NULL) AS has_viewability_log,
    COUNTIF(il.il_ip IS NOT NULL) AS has_impression_log,
    COUNTIF(
        el.el_ip IS NOT NULL OR cil.cil_ip IS NOT NULL
        OR vl.vl_ip IS NOT NULL OR il.il_ip IS NOT NULL
    ) AS resolved_any,
    ROUND(100.0 * COUNTIF(
        el.el_ip IS NOT NULL OR cil.cil_ip IS NOT NULL
        OR vl.vl_ip IS NOT NULL OR il.il_ip IS NOT NULL
    ) / NULLIF(COUNT(*), 0), 2) AS resolved_pct,
    COUNTIF(
        el.el_ip IS NULL AND cil.cil_ip IS NULL
        AND vl.vl_ip IS NULL AND il.il_ip IS NULL
    ) AS no_impression_found
FROM s1_vvs v
LEFT JOIN el_match el ON el.ad_served_id = v.ad_served_id
LEFT JOIN cil_match cil ON cil.ad_served_id = v.ad_served_id
LEFT JOIN vl_match vl ON vl.ad_served_id = v.ad_served_id
LEFT JOIN il_match il ON il.ad_served_id = v.ad_served_id;
