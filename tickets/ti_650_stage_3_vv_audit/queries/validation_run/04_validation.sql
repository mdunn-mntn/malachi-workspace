-- TI-650 Validation Run: Step 4 — Validate Trace Table (simplified)
-- Single-pass aggregation over the trace logic

WITH anchor_vvs AS (
    SELECT
        cp.ad_served_id,
        SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] AS clickpass_ip,
        cp.time AS clickpass_time,
        cp.campaign_id,
        cp.advertiser_id,
        c.campaign_group_id,
        c.funnel_level,
        c.objective_id,
        c.channel_id
    FROM `dw-main-silver.logdata.clickpass_log` cp
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = cp.campaign_id
        AND c.deleted = FALSE AND c.is_test = FALSE
        AND c.objective_id IN (1, 5, 6)
    WHERE cp.time >= TIMESTAMP('2026-03-16') AND cp.time < TIMESTAMP('2026-03-23')
      AND cp.advertiser_id IN (31276, 53308, 37775, 37056, 46104, 31455, 48866, 34838, 38101, 40236)
      AND cp.ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),

ip_vast_start AS (
    SELECT ad_served_id, SPLIT(ip, '/')[SAFE_OFFSET(0)] AS vast_start_ip
    FROM `dw-main-silver.logdata.event_log`
    WHERE event_type_raw = 'vast_start'
      AND time >= TIMESTAMP('2026-02-14') AND time < TIMESTAMP('2026-04-22')
      AND advertiser_id IN (31276, 53308, 37775, 37056, 46104, 31455, 48866, 34838, 38101, 40236)
      AND ad_served_id IN (SELECT ad_served_id FROM anchor_vvs) AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

ip_viewability AS (
    SELECT ad_served_id, SPLIT(ip, '/')[SAFE_OFFSET(0)] AS viewability_ip
    FROM `dw-main-silver.logdata.viewability_log`
    WHERE time >= TIMESTAMP('2026-02-14') AND time < TIMESTAMP('2026-04-22')
      AND advertiser_id IN (31276, 53308, 37775, 37056, 46104, 31455, 48866, 34838, 38101, 40236)
      AND ad_served_id IN (SELECT ad_served_id FROM anchor_vvs) AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

ip_impression AS (
    SELECT ad_served_id, SPLIT(ip, '/')[SAFE_OFFSET(0)] AS impression_ip, ttd_impression_id,
           NULLIF(SPLIT(bid_ip, '/')[SAFE_OFFSET(0)], '0.0.0.0') AS impression_bid_ip
    FROM `dw-main-silver.logdata.impression_log`
    WHERE time >= TIMESTAMP('2026-02-14') AND time < TIMESTAMP('2026-04-22')
      AND advertiser_id IN (31276, 53308, 37775, 37056, 46104, 31455, 48866, 34838, 38101, 40236)
      AND ad_served_id IN (SELECT ad_served_id FROM anchor_vvs) AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

-- Fallback: no time filter for impression_log.bid_ip (catches cases outside source_window)
ip_impression_bid_fallback AS (
    SELECT ad_served_id,
           NULLIF(SPLIT(bid_ip, '/')[SAFE_OFFSET(0)], '0.0.0.0') AS fallback_bid_ip
    FROM `dw-main-silver.logdata.impression_log`
    WHERE advertiser_id IN (31276, 53308, 37775, 37056, 46104, 31455, 48866, 34838, 38101, 40236)
      AND ad_served_id IN (SELECT ad_served_id FROM anchor_vvs)
      AND bid_ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

ip_bid_direct AS (
    SELECT il.ad_served_id, NULLIF(SPLIT(b.ip, '/')[SAFE_OFFSET(0)], '0.0.0.0') AS bid_ip_direct
    FROM `dw-main-silver.logdata.bid_logs` b
    JOIN ip_impression il ON b.auction_id = il.ttd_impression_id
    WHERE b.time >= TIMESTAMP('2026-02-14') AND b.time < TIMESTAMP('2026-04-22')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY il.ad_served_id ORDER BY b.time ASC) = 1
),

-- bid_ip COALESCE: bid_logs → impression_log.bid_ip (source_window) → impression_log.bid_ip (no time filter)
ip_bid AS (
    SELECT
        v.ad_served_id,
        COALESCE(
            bd.bid_ip_direct,
            imp.impression_bid_ip,
            fb.fallback_bid_ip
        ) AS bid_ip
    FROM anchor_vvs v
    LEFT JOIN ip_bid_direct bd ON bd.ad_served_id = v.ad_served_id
    LEFT JOIN ip_impression imp ON imp.ad_served_id = v.ad_served_id
    LEFT JOIN ip_impression_bid_fallback fb ON fb.ad_served_id = v.ad_served_id
),

prior_vv_pool AS (
    SELECT cp.ad_served_id AS prior_vv_ad_served_id,
        SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] AS prior_vv_clickpass_ip,
        cp.time AS prior_vv_time,
        c.campaign_group_id AS prior_vv_campaign_group_id,
        c.funnel_level AS prior_vv_funnel_level
    FROM `dw-main-silver.logdata.clickpass_log` cp
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = cp.campaign_id AND c.deleted = FALSE AND c.is_test = FALSE
        AND c.funnel_level IN (1, 2) AND c.objective_id IN (1, 5, 6)
    WHERE cp.time >= TIMESTAMP('2025-03-16') AND cp.time < TIMESTAMP('2026-03-23')
      AND cp.advertiser_id IN (31276, 53308, 37775, 37056, 46104, 31455, 48866, 34838, 38101, 40236)
      AND cp.ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),

s3_s2_match AS (
    SELECT v.ad_served_id AS s3_ad_served_id, pv.prior_vv_ad_served_id, pv.prior_vv_funnel_level
    FROM anchor_vvs v
    JOIN ip_bid b ON b.ad_served_id = v.ad_served_id
    JOIN prior_vv_pool pv
        ON pv.prior_vv_campaign_group_id = v.campaign_group_id
        AND pv.prior_vv_clickpass_ip = b.bid_ip AND pv.prior_vv_funnel_level = 2
        AND pv.prior_vv_time < v.clickpass_time
    WHERE v.funnel_level = 3 AND b.bid_ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY v.ad_served_id ORDER BY pv.prior_vv_time DESC) = 1
),

s3_s1_match AS (
    SELECT v.ad_served_id AS s3_ad_served_id, pv.prior_vv_ad_served_id, pv.prior_vv_funnel_level
    FROM anchor_vvs v
    JOIN ip_bid b ON b.ad_served_id = v.ad_served_id
    JOIN prior_vv_pool pv
        ON pv.prior_vv_campaign_group_id = v.campaign_group_id
        AND pv.prior_vv_clickpass_ip = b.bid_ip AND pv.prior_vv_funnel_level = 1
        AND pv.prior_vv_time < v.clickpass_time
    WHERE v.funnel_level = 3 AND b.bid_ip IS NOT NULL
      AND v.ad_served_id NOT IN (SELECT s3_ad_served_id FROM s3_s2_match)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY v.ad_served_id ORDER BY pv.prior_vv_time DESC) = 1
),

prior_vv_matched AS (
    SELECT * FROM s3_s2_match UNION ALL SELECT * FROM s3_s1_match
),

pv_ip_impression AS (
    SELECT ad_served_id, ttd_impression_id,
           NULLIF(SPLIT(bid_ip, '/')[SAFE_OFFSET(0)], '0.0.0.0') AS pv_impression_bid_ip
    FROM `dw-main-silver.logdata.impression_log`
    WHERE time >= TIMESTAMP('2025-03-16') AND time < TIMESTAMP('2026-04-22')
      AND ad_served_id IN (SELECT prior_vv_ad_served_id FROM prior_vv_matched) AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

pv_ip_bid_direct AS (
    SELECT il.ad_served_id, NULLIF(SPLIT(b.ip, '/')[SAFE_OFFSET(0)], '0.0.0.0') AS pv_bid_ip_direct
    FROM `dw-main-silver.logdata.bid_logs` b
    JOIN pv_ip_impression il ON b.auction_id = il.ttd_impression_id
    WHERE b.time >= TIMESTAMP('2025-03-16') AND b.time < TIMESTAMP('2026-04-22')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY il.ad_served_id ORDER BY b.time ASC) = 1
),

pv_ip_bid AS (
    SELECT
        pvimp.ad_served_id,
        COALESCE(pvbd.pv_bid_ip_direct, pvimp.pv_impression_bid_ip) AS prior_vv_bid_ip
    FROM pv_ip_impression pvimp
    LEFT JOIN pv_ip_bid_direct pvbd ON pvbd.ad_served_id = pvimp.ad_served_id
),

s1_event_pool AS (
    SELECT el.ad_served_id AS s1_event_ad_served_id,
        SPLIT(MAX(CASE WHEN el.event_type_raw = 'vast_start' THEN el.ip END), '/')[SAFE_OFFSET(0)] AS s1_event_vast_start_ip,
        MIN(el.time) AS s1_event_time
    FROM `dw-main-silver.logdata.event_log` el
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = el.campaign_id AND c.deleted = FALSE AND c.is_test = FALSE
        AND c.funnel_level = 1 AND c.objective_id IN (1, 5, 6)
    WHERE el.event_type_raw IN ('vast_start', 'vast_impression')
      AND el.time >= TIMESTAMP('2025-03-16') AND el.time < TIMESTAMP('2026-04-22')
      AND el.advertiser_id IN (31276, 53308, 37775, 37056, 46104, 31455, 48866, 34838, 38101, 40236)
      AND el.ip IS NOT NULL
    GROUP BY el.ad_served_id
),

s1_by_vast_start AS (
    SELECT * FROM s1_event_pool WHERE s1_event_vast_start_ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY s1_event_vast_start_ip ORDER BY s1_event_time ASC) = 1
),

trace AS (
    SELECT
        v.ad_served_id,
        v.funnel_level,
        CASE
            WHEN vs.vast_start_ip IS NOT NULL THEN 'CTV'
            WHEN vw.viewability_ip IS NOT NULL THEN 'Viewable Display'
            WHEN imp.impression_ip IS NOT NULL THEN 'Non-Viewable Display'
            ELSE 'No Impression Found'
        END AS impression_type,
        b.bid_ip,
        pvm.prior_vv_ad_served_id,
        pvm.prior_vv_funnel_level,
        s1_s2.s1_event_ad_served_id AS s1_via_s2,
        s1_direct.s1_event_ad_served_id AS s1_via_direct,
        CASE
            WHEN v.funnel_level = 1 THEN 'resolved'
            WHEN v.funnel_level = 2 AND s1_direct.s1_event_ad_served_id IS NOT NULL THEN 'resolved'
            WHEN v.funnel_level = 3 AND pvm.prior_vv_ad_served_id IS NOT NULL
                 AND (pvm.prior_vv_funnel_level = 1
                      OR s1_s2.s1_event_ad_served_id IS NOT NULL) THEN 'resolved'
            WHEN b.bid_ip IS NULL THEN 'no_bid_ip'
            ELSE 'unresolved'
        END AS resolution_status,
        CASE
            WHEN v.funnel_level = 1 THEN 'current_is_s1'
            WHEN v.funnel_level = 2 AND s1_direct.s1_event_ad_served_id IS NOT NULL THEN 's1_event_match'
            WHEN v.funnel_level = 3 AND pvm.prior_vv_funnel_level = 2
                 AND s1_s2.s1_event_ad_served_id IS NOT NULL THEN 's2_vv_bridge'
            WHEN v.funnel_level = 3 AND pvm.prior_vv_funnel_level = 1 THEN 's1_vv_bridge'
            ELSE NULL
        END AS resolution_method
    FROM anchor_vvs v
    LEFT JOIN ip_vast_start vs ON vs.ad_served_id = v.ad_served_id
    LEFT JOIN ip_viewability vw ON vw.ad_served_id = v.ad_served_id
    LEFT JOIN ip_impression imp ON imp.ad_served_id = v.ad_served_id
    LEFT JOIN ip_bid b ON b.ad_served_id = v.ad_served_id
    LEFT JOIN prior_vv_matched pvm ON pvm.s3_ad_served_id = v.ad_served_id
    LEFT JOIN pv_ip_bid pvbid ON pvbid.ad_served_id = pvm.prior_vv_ad_served_id
    LEFT JOIN s1_by_vast_start s1_direct
        ON s1_direct.s1_event_vast_start_ip = b.bid_ip AND v.funnel_level = 2
    LEFT JOIN s1_by_vast_start s1_s2
        ON s1_s2.s1_event_vast_start_ip = pvbid.prior_vv_bid_ip AND pvm.prior_vv_funnel_level = 2
)

SELECT
    COUNT(*) AS total_vvs,
    COUNT(DISTINCT ad_served_id) AS distinct_ad_served_ids,
    COUNT(*) - COUNT(DISTINCT ad_served_id) AS duplicates,
    COUNTIF(funnel_level = 1) AS s1_vvs,
    COUNTIF(funnel_level = 2) AS s2_vvs,
    COUNTIF(funnel_level = 3) AS s3_vvs,
    COUNTIF(funnel_level = 1 AND resolution_status != 'resolved') AS s1_not_resolved,
    COUNTIF(funnel_level = 3 AND resolution_status = 'resolved' AND prior_vv_ad_served_id IS NULL) AS s3_resolved_no_prior,
    COUNTIF(funnel_level = 3 AND prior_vv_funnel_level = 2 AND s1_via_s2 IS NULL AND resolution_status = 'resolved') AS s3_s2_prior_no_s1,
    COUNTIF(resolution_status = 'resolved' AND impression_type = 'No Impression Found') AS resolved_no_impression,
    COUNTIF(resolution_status = 'resolved' AND bid_ip IS NULL AND funnel_level != 1) AS resolved_no_bid_ip,
    COUNTIF(resolution_status = 'resolved') AS status_resolved,
    COUNTIF(resolution_status = 'unresolved') AS status_unresolved,
    COUNTIF(resolution_status = 'no_bid_ip') AS status_no_bid_ip,
    COUNTIF(resolution_method = 'current_is_s1') AS method_current_is_s1,
    COUNTIF(resolution_method = 's1_event_match') AS method_s1_event_match,
    COUNTIF(resolution_method = 's2_vv_bridge') AS method_s2_vv_bridge,
    COUNTIF(resolution_method = 's1_vv_bridge') AS method_s1_vv_bridge,
    COUNTIF(resolution_method IS NULL) AS method_null,
    COUNTIF(impression_type = 'CTV') AS type_ctv,
    COUNTIF(impression_type = 'Viewable Display') AS type_viewable_display,
    COUNTIF(impression_type = 'Non-Viewable Display') AS type_nonviewable_display,
    COUNTIF(impression_type = 'No Impression Found') AS type_no_impression
FROM trace;
