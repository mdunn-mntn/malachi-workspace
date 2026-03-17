-- TI-650: S2 VV resolution test - advertiser 31357
-- S2 requires a VAST event to enter, so S1 impression MUST be CTV
-- S1 pool = event_log only (vast_start/vast_impression)
-- Cross-stage link: S2.bid_ip -> S1.event_log.ip (same campaign_group_id)

DECLARE p_advertiser_id INT64 DEFAULT 31357;
DECLARE p_vv_start TIMESTAMP DEFAULT TIMESTAMP('2026-02-04');
DECLARE p_vv_end TIMESTAMP DEFAULT TIMESTAMP('2026-02-11');
DECLARE p_lookback_start TIMESTAMP DEFAULT TIMESTAMP_SUB(TIMESTAMP('2026-02-04'), INTERVAL 90 DAY);

-- S1 impression pool: event_log only (CTV VAST events)
WITH s1_pool AS (
    SELECT c.campaign_group_id, el.ip AS match_ip, MIN(el.time) AS impression_time
    FROM `dw-main-silver.logdata.event_log` el
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = el.campaign_id
        AND c.deleted = FALSE AND c.is_test = FALSE
        AND c.funnel_level = 1 AND c.objective_id IN (1, 5, 6)
    WHERE el.event_type_raw IN ('vast_start', 'vast_impression')
      AND el.time >= p_lookback_start AND el.time < p_vv_end
      AND el.advertiser_id = p_advertiser_id
      AND el.ip IS NOT NULL
    GROUP BY c.campaign_group_id, el.ip
),

-- S2 VVs
s2_vvs AS (
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
        AND c.funnel_level = 2 AND c.objective_id IN (1, 5, 6)
    WHERE cp.time >= p_vv_start AND cp.time < p_vv_end
      AND cp.advertiser_id = p_advertiser_id
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),

-- Get bid_ip for each S2 VV via CIL
bid_ips AS (
    SELECT cil.ad_served_id, cil.ip AS bid_ip
    FROM `dw-main-silver.logdata.cost_impression_log` cil
    WHERE cil.time >= p_lookback_start AND cil.time < p_vv_end
      AND cil.advertiser_id = p_advertiser_id
      AND cil.ad_served_id IN (SELECT ad_served_id FROM s2_vvs)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cil.ad_served_id ORDER BY cil.time ASC) = 1
),

-- Also get impression_ip from ui_visits as fallback
visit_ips AS (
    SELECT CAST(uv.ad_served_id AS STRING) AS ad_served_id, uv.impression_ip
    FROM `dw-main-silver.summarydata.ui_visits` uv
    WHERE uv.time >= TIMESTAMP_SUB(p_vv_start, INTERVAL 7 DAY)
      AND uv.time < TIMESTAMP_ADD(p_vv_end, INTERVAL 7 DAY)
      AND uv.from_verified_impression = TRUE
      AND CAST(uv.ad_served_id AS STRING) IN (SELECT ad_served_id FROM s2_vvs)
      AND uv.impression_ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(uv.ad_served_id AS STRING) ORDER BY uv.time ASC) = 1
)

SELECT
    COUNT(*) AS total_s2_vvs,
    COUNTIF(b.bid_ip IS NOT NULL) AS has_bid_ip,
    COUNTIF(b.bid_ip IS NULL) AS no_bid_ip,

    -- Direct: S2.bid_ip in S1 event_log pool
    COUNTIF(s1d.match_ip IS NOT NULL) AS s1_direct_resolved,

    -- Visit fallback: S2.impression_ip in S1 event_log pool
    COUNTIF(s1v.match_ip IS NOT NULL) AS s1_visit_resolved,

    -- Any resolution
    COUNTIF(s1d.match_ip IS NOT NULL OR s1v.match_ip IS NOT NULL) AS resolved_any,
    ROUND(100.0 * COUNTIF(s1d.match_ip IS NOT NULL OR s1v.match_ip IS NOT NULL)
        / NULLIF(COUNT(*), 0), 2) AS resolved_pct,

    COUNTIF(s1d.match_ip IS NULL AND s1v.match_ip IS NULL
        AND b.bid_ip IS NOT NULL) AS unresolved_with_bid_ip,
    COUNTIF(s1d.match_ip IS NULL AND s1v.match_ip IS NULL) AS unresolved_total
FROM s2_vvs v
LEFT JOIN bid_ips b ON b.ad_served_id = v.ad_served_id
LEFT JOIN visit_ips vi ON vi.ad_served_id = v.ad_served_id

-- Direct: S2.bid_ip -> S1 pool
LEFT JOIN s1_pool s1d
    ON s1d.campaign_group_id = v.campaign_group_id
    AND s1d.match_ip = b.bid_ip
    AND s1d.impression_time < v.vv_time

-- Visit fallback: S2.impression_ip -> S1 pool
LEFT JOIN s1_pool s1v
    ON s1v.campaign_group_id = v.campaign_group_id
    AND s1v.match_ip = vi.impression_ip
    AND s1v.impression_time < v.vv_time;
