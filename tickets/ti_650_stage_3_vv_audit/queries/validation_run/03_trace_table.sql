-- TI-650 Validation Run: Step 3 — Full Trace Table
-- One row per VV, all stages, full 7-IP trace, cross-stage linking
-- 10 advertisers, Mar 16-22, 365-day lookback, ±30d source window
-- Advertiser IDs: 31276, 53308, 37775, 37056, 46104, 31455, 48866, 34838, 38101, 40236

WITH anchor_vvs AS (
    SELECT
        cp.ad_served_id,
        SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] AS clickpass_ip,
        cp.time AS clickpass_time,
        cp.campaign_id,
        cp.advertiser_id,
        cp.guid,
        cp.is_new,
        cp.is_cross_device,
        cp.attribution_model_id,
        cp.first_touch_ad_served_id,
        c.campaign_group_id,
        c.funnel_level,
        c.objective_id,
        c.channel_id,
        c.name AS campaign_name
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

-- 7-IP trace for each VV's impression
ip_vast_start AS (
    SELECT ad_served_id,
           SPLIT(ip, '/')[SAFE_OFFSET(0)] AS vast_start_ip,
           time AS vast_start_time,
           NULLIF(SPLIT(bid_ip, '/')[SAFE_OFFSET(0)], '0.0.0.0') AS vast_start_bid_ip
    FROM `dw-main-silver.logdata.event_log`
    WHERE event_type_raw = 'vast_start'
      AND time >= TIMESTAMP('2026-02-14') AND time < TIMESTAMP('2026-04-22')
      AND advertiser_id IN (31276, 53308, 37775, 37056, 46104, 31455, 48866, 34838, 38101, 40236)
      AND ad_served_id IN (SELECT ad_served_id FROM anchor_vvs)
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

ip_vast_impression AS (
    SELECT ad_served_id,
           SPLIT(ip, '/')[SAFE_OFFSET(0)] AS vast_impression_ip,
           time AS vast_impression_time,
           NULLIF(SPLIT(bid_ip, '/')[SAFE_OFFSET(0)], '0.0.0.0') AS vast_impression_bid_ip
    FROM `dw-main-silver.logdata.event_log`
    WHERE event_type_raw = 'vast_impression'
      AND time >= TIMESTAMP('2026-02-14') AND time < TIMESTAMP('2026-04-22')
      AND advertiser_id IN (31276, 53308, 37775, 37056, 46104, 31455, 48866, 34838, 38101, 40236)
      AND ad_served_id IN (SELECT ad_served_id FROM anchor_vvs)
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

ip_viewability AS (
    SELECT ad_served_id,
           SPLIT(ip, '/')[SAFE_OFFSET(0)] AS viewability_ip,
           time AS viewability_time,
           NULLIF(SPLIT(bid_ip, '/')[SAFE_OFFSET(0)], '0.0.0.0') AS viewability_bid_ip
    FROM `dw-main-silver.logdata.viewability_log`
    WHERE time >= TIMESTAMP('2026-02-14') AND time < TIMESTAMP('2026-04-22')
      AND advertiser_id IN (31276, 53308, 37775, 37056, 46104, 31455, 48866, 34838, 38101, 40236)
      AND ad_served_id IN (SELECT ad_served_id FROM anchor_vvs)
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

ip_impression AS (
    SELECT ad_served_id,
           SPLIT(ip, '/')[SAFE_OFFSET(0)] AS impression_ip,
           time AS impression_time,
           ttd_impression_id,
           NULLIF(SPLIT(bid_ip, '/')[SAFE_OFFSET(0)], '0.0.0.0') AS impression_bid_ip
    FROM `dw-main-silver.logdata.impression_log`
    WHERE time >= TIMESTAMP('2026-02-14') AND time < TIMESTAMP('2026-04-22')
      AND advertiser_id IN (31276, 53308, 37775, 37056, 46104, 31455, 48866, 34838, 38101, 40236)
      AND ad_served_id IN (SELECT ad_served_id FROM anchor_vvs)
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

ip_win AS (
    SELECT il.ad_served_id,
           SPLIT(w.ip, '/')[SAFE_OFFSET(0)] AS win_ip,
           w.time AS win_time
    FROM `dw-main-silver.logdata.win_logs` w
    JOIN ip_impression il ON w.auction_id = il.ttd_impression_id
    WHERE w.time >= TIMESTAMP('2026-02-14') AND w.time < TIMESTAMP('2026-04-22')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY il.ad_served_id ORDER BY w.time ASC) = 1
),

-- bid_ip from bid_logs (primary source, may be purged by TTL)
ip_bid_direct AS (
    SELECT il.ad_served_id,
           NULLIF(SPLIT(b.ip, '/')[SAFE_OFFSET(0)], '0.0.0.0') AS bid_ip_direct,
           b.time AS bid_time
    FROM `dw-main-silver.logdata.bid_logs` b
    JOIN ip_impression il ON b.auction_id = il.ttd_impression_id
    WHERE b.time >= TIMESTAMP('2026-02-14') AND b.time < TIMESTAMP('2026-04-22')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY il.ad_served_id ORDER BY b.time ASC) = 1
),

-- Fallback: impression_log.bid_ip with NO time filter (catches cases outside source_window)
ip_impression_bid_fallback AS (
    SELECT ad_served_id,
           NULLIF(SPLIT(bid_ip, '/')[SAFE_OFFSET(0)], '0.0.0.0') AS fallback_bid_ip
    FROM `dw-main-silver.logdata.impression_log`
    WHERE advertiser_id IN (31276, 53308, 37775, 37056, 46104, 31455, 48866, 34838, 38101, 40236)
      AND ad_served_id IN (SELECT ad_served_id FROM anchor_vvs)
      AND bid_ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

-- bid_ip COALESCE: bid_logs → impression_log.bid_ip (source_window) → event_log → viewability_log → impression_log.bid_ip (no time filter)
ip_bid AS (
    SELECT
        v.ad_served_id,
        COALESCE(
            bd.bid_ip_direct,
            imp.impression_bid_ip,
            vs.vast_start_bid_ip,
            vi.vast_impression_bid_ip,
            vw.viewability_bid_ip,
            fb.fallback_bid_ip
        ) AS bid_ip,
        bd.bid_time,
        CASE
            WHEN bd.bid_ip_direct IS NOT NULL THEN 'bid_logs'
            WHEN imp.impression_bid_ip IS NOT NULL THEN 'impression_log.bid_ip'
            WHEN vs.vast_start_bid_ip IS NOT NULL THEN 'event_log.bid_ip(vast_start)'
            WHEN vi.vast_impression_bid_ip IS NOT NULL THEN 'event_log.bid_ip(vast_impression)'
            WHEN vw.viewability_bid_ip IS NOT NULL THEN 'viewability_log.bid_ip'
            WHEN fb.fallback_bid_ip IS NOT NULL THEN 'impression_log.bid_ip(no_time_filter)'
            ELSE NULL
        END AS bid_ip_source
    FROM anchor_vvs v
    LEFT JOIN ip_impression imp ON imp.ad_served_id = v.ad_served_id
    LEFT JOIN ip_bid_direct bd ON bd.ad_served_id = v.ad_served_id
    LEFT JOIN ip_vast_start vs ON vs.ad_served_id = v.ad_served_id
    LEFT JOIN ip_vast_impression vi ON vi.ad_served_id = v.ad_served_id
    LEFT JOIN ip_viewability vw ON vw.ad_served_id = v.ad_served_id
    LEFT JOIN ip_impression_bid_fallback fb ON fb.ad_served_id = v.ad_served_id
),

-- Prior VV pool for cross-stage matching (S3 VVs)
prior_vv_pool AS (
    SELECT
        cp.ad_served_id AS prior_vv_ad_served_id,
        SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] AS prior_vv_clickpass_ip,
        cp.time AS prior_vv_time,
        cp.campaign_id AS prior_vv_campaign_id,
        c.campaign_group_id AS prior_vv_campaign_group_id,
        c.funnel_level AS prior_vv_funnel_level
    FROM `dw-main-silver.logdata.clickpass_log` cp
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = cp.campaign_id
        AND c.deleted = FALSE AND c.is_test = FALSE
        AND c.funnel_level IN (1, 2)
        AND c.objective_id IN (1, 5, 6)
    WHERE cp.time >= TIMESTAMP('2025-03-16')
      AND cp.time < TIMESTAMP('2026-03-23')
      AND cp.advertiser_id IN (31276, 53308, 37775, 37056, 46104, 31455, 48866, 34838, 38101, 40236)
      AND cp.ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),

-- Match S3 VVs to prior S2 VV (preferred)
s3_s2_match AS (
    SELECT
        v.ad_served_id AS s3_ad_served_id,
        pv.prior_vv_ad_served_id,
        pv.prior_vv_funnel_level,
        pv.prior_vv_campaign_id,
        pv.prior_vv_clickpass_ip,
        pv.prior_vv_time
    FROM anchor_vvs v
    JOIN ip_bid b ON b.ad_served_id = v.ad_served_id
    JOIN prior_vv_pool pv
        ON pv.prior_vv_campaign_group_id = v.campaign_group_id
        AND pv.prior_vv_clickpass_ip = b.bid_ip
        AND pv.prior_vv_funnel_level = 2
        AND pv.prior_vv_time < v.clickpass_time
    WHERE v.funnel_level = 3 AND b.bid_ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY v.ad_served_id ORDER BY pv.prior_vv_time DESC) = 1
),

-- Match S3 VVs to prior S1 VV (fallback)
s3_s1_match AS (
    SELECT
        v.ad_served_id AS s3_ad_served_id,
        pv.prior_vv_ad_served_id,
        pv.prior_vv_funnel_level,
        pv.prior_vv_campaign_id,
        pv.prior_vv_clickpass_ip,
        pv.prior_vv_time
    FROM anchor_vvs v
    JOIN ip_bid b ON b.ad_served_id = v.ad_served_id
    JOIN prior_vv_pool pv
        ON pv.prior_vv_campaign_group_id = v.campaign_group_id
        AND pv.prior_vv_clickpass_ip = b.bid_ip
        AND pv.prior_vv_funnel_level = 1
        AND pv.prior_vv_time < v.clickpass_time
    WHERE v.funnel_level = 3 AND b.bid_ip IS NOT NULL
      AND v.ad_served_id NOT IN (SELECT s3_ad_served_id FROM s3_s2_match)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY v.ad_served_id ORDER BY pv.prior_vv_time DESC) = 1
),

prior_vv_matched AS (
    SELECT * FROM s3_s2_match
    UNION ALL
    SELECT * FROM s3_s1_match
),

-- Prior VV's 7-IP trace
pv_ip_vast_start AS (
    SELECT ad_served_id,
           SPLIT(ip, '/')[SAFE_OFFSET(0)] AS prior_vv_vast_start_ip,
           time AS prior_vv_vast_start_time,
           NULLIF(SPLIT(bid_ip, '/')[SAFE_OFFSET(0)], '0.0.0.0') AS pv_vast_start_bid_ip
    FROM `dw-main-silver.logdata.event_log`
    WHERE event_type_raw = 'vast_start'
      AND time >= TIMESTAMP('2025-03-16') AND time < TIMESTAMP('2026-04-22')
      AND ad_served_id IN (SELECT prior_vv_ad_served_id FROM prior_vv_matched)
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

pv_ip_vast_impression AS (
    SELECT ad_served_id,
           SPLIT(ip, '/')[SAFE_OFFSET(0)] AS prior_vv_vast_impression_ip,
           time AS prior_vv_vast_impression_time,
           NULLIF(SPLIT(bid_ip, '/')[SAFE_OFFSET(0)], '0.0.0.0') AS pv_vast_impression_bid_ip
    FROM `dw-main-silver.logdata.event_log`
    WHERE event_type_raw = 'vast_impression'
      AND time >= TIMESTAMP('2025-03-16') AND time < TIMESTAMP('2026-04-22')
      AND ad_served_id IN (SELECT prior_vv_ad_served_id FROM prior_vv_matched)
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

pv_ip_viewability AS (
    SELECT ad_served_id,
           SPLIT(ip, '/')[SAFE_OFFSET(0)] AS prior_vv_viewability_ip,
           time AS prior_vv_viewability_time,
           NULLIF(SPLIT(bid_ip, '/')[SAFE_OFFSET(0)], '0.0.0.0') AS pv_viewability_bid_ip
    FROM `dw-main-silver.logdata.viewability_log`
    WHERE time >= TIMESTAMP('2025-03-16') AND time < TIMESTAMP('2026-04-22')
      AND ad_served_id IN (SELECT prior_vv_ad_served_id FROM prior_vv_matched)
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

pv_ip_impression AS (
    SELECT ad_served_id,
           SPLIT(ip, '/')[SAFE_OFFSET(0)] AS prior_vv_impression_ip,
           time AS prior_vv_impression_time,
           ttd_impression_id,
           NULLIF(SPLIT(bid_ip, '/')[SAFE_OFFSET(0)], '0.0.0.0') AS pv_impression_bid_ip
    FROM `dw-main-silver.logdata.impression_log`
    WHERE time >= TIMESTAMP('2025-03-16') AND time < TIMESTAMP('2026-04-22')
      AND ad_served_id IN (SELECT prior_vv_ad_served_id FROM prior_vv_matched)
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

pv_ip_win AS (
    SELECT il.ad_served_id,
           SPLIT(w.ip, '/')[SAFE_OFFSET(0)] AS prior_vv_win_ip,
           w.time AS prior_vv_win_time
    FROM `dw-main-silver.logdata.win_logs` w
    JOIN pv_ip_impression il ON w.auction_id = il.ttd_impression_id
    WHERE w.time >= TIMESTAMP('2025-03-16') AND w.time < TIMESTAMP('2026-04-22')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY il.ad_served_id ORDER BY w.time ASC) = 1
),

-- Prior VV bid_ip: bid_logs (primary) → COALESCE from pipeline tables
pv_ip_bid_direct AS (
    SELECT il.ad_served_id,
           NULLIF(SPLIT(b.ip, '/')[SAFE_OFFSET(0)], '0.0.0.0') AS pv_bid_ip_direct,
           b.time AS prior_vv_bid_time
    FROM `dw-main-silver.logdata.bid_logs` b
    JOIN pv_ip_impression il ON b.auction_id = il.ttd_impression_id
    WHERE b.time >= TIMESTAMP('2025-03-16') AND b.time < TIMESTAMP('2026-04-22')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY il.ad_served_id ORDER BY b.time ASC) = 1
),

pv_ip_bid AS (
    SELECT
        pvimp.ad_served_id,
        COALESCE(
            pvbd.pv_bid_ip_direct,
            pvimp.pv_impression_bid_ip,
            pvvs.pv_vast_start_bid_ip,
            pvvi.pv_vast_impression_bid_ip,
            pvvw.pv_viewability_bid_ip
        ) AS prior_vv_bid_ip,
        pvbd.prior_vv_bid_time
    FROM pv_ip_impression pvimp
    LEFT JOIN pv_ip_bid_direct pvbd ON pvbd.ad_served_id = pvimp.ad_served_id
    LEFT JOIN pv_ip_vast_start pvvs ON pvvs.ad_served_id = pvimp.ad_served_id
    LEFT JOIN pv_ip_vast_impression pvvi ON pvvi.ad_served_id = pvimp.ad_served_id
    LEFT JOIN pv_ip_viewability pvvw ON pvvw.ad_served_id = pvimp.ad_served_id
),

-- S1 event resolution (S2 bid_ip → S1 event_log.ip)
s1_event_pool AS (
    SELECT
        el.ad_served_id AS s1_event_ad_served_id,
        SPLIT(MAX(CASE WHEN el.event_type_raw = 'vast_start' THEN el.ip END), '/')[SAFE_OFFSET(0)] AS s1_event_vast_start_ip,
        SPLIT(MAX(CASE WHEN el.event_type_raw = 'vast_impression' THEN el.ip END), '/')[SAFE_OFFSET(0)] AS s1_event_vast_impression_ip,
        MIN(el.time) AS s1_event_time,
        MAX(el.campaign_id) AS s1_event_campaign_id
    FROM `dw-main-silver.logdata.event_log` el
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = el.campaign_id
        AND c.deleted = FALSE AND c.is_test = FALSE
        AND c.funnel_level = 1
        AND c.objective_id IN (1, 5, 6)
    WHERE el.event_type_raw IN ('vast_start', 'vast_impression')
      AND el.time >= TIMESTAMP('2025-03-16') AND el.time < TIMESTAMP('2026-04-22')
      AND el.advertiser_id IN (31276, 53308, 37775, 37056, 46104, 31455, 48866, 34838, 38101, 40236)
      AND el.ip IS NOT NULL
    GROUP BY el.ad_served_id
),

s1_by_vast_start AS (
    SELECT * FROM s1_event_pool
    WHERE s1_event_vast_start_ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY s1_event_vast_start_ip ORDER BY s1_event_time ASC) = 1
)

-- FINAL OUTPUT
SELECT
    -- 1. Identity
    FORMAT('%s-%s-%s-%s-%s',
        SUBSTR(TO_HEX(MD5(v.ad_served_id)), 1, 8),
        SUBSTR(TO_HEX(MD5(v.ad_served_id)), 9, 4),
        SUBSTR(TO_HEX(MD5(v.ad_served_id)), 13, 4),
        SUBSTR(TO_HEX(MD5(v.ad_served_id)), 17, 4),
        SUBSTR(TO_HEX(MD5(v.ad_served_id)), 21, 12)
    ) AS trace_uuid,
    v.ad_served_id,
    v.advertiser_id,
    adv.company_name AS advertiser_name,
    v.campaign_id,
    v.campaign_name,
    v.campaign_group_id,
    cg.name AS campaign_group_name,
    v.funnel_level,
    v.objective_id,
    v.channel_id,
    CASE
        WHEN vs.vast_start_ip IS NOT NULL THEN 'CTV'
        WHEN vw.viewability_ip IS NOT NULL THEN 'Viewable Display'
        WHEN imp.impression_ip IS NOT NULL THEN 'Non-Viewable Display'
        ELSE 'No Impression Found'
    END AS impression_type,

    -- 2. VV Details
    v.clickpass_ip,
    v.clickpass_time,
    v.guid,
    v.is_new,
    v.is_cross_device,
    v.attribution_model_id,
    v.first_touch_ad_served_id,

    -- 3. This VV's Impression Trace (7 IPs)
    vs.vast_start_ip,           vs.vast_start_time,
    vi.vast_impression_ip,      vi.vast_impression_time,
    vw.viewability_ip,          vw.viewability_time,
    imp.impression_ip,          imp.impression_time,
    w.win_ip,                   w.win_time,
    b.bid_ip,                   b.bid_time,
    b.bid_ip_source,

    -- 4. Cross-Stage: Prior VV (S3 only)
    pvm.prior_vv_ad_served_id,
    pvm.prior_vv_funnel_level,
    pvm.prior_vv_campaign_id,
    pvm.prior_vv_clickpass_ip,
    pvm.prior_vv_time,

    -- 5. Cross-Stage: Prior VV's Impression Trace (S3 only)
    pvvs.prior_vv_vast_start_ip,        pvvs.prior_vv_vast_start_time,
    pvvi.prior_vv_vast_impression_ip,    pvvi.prior_vv_vast_impression_time,
    pvvw.prior_vv_viewability_ip,        pvvw.prior_vv_viewability_time,
    pvimp.prior_vv_impression_ip,        pvimp.prior_vv_impression_time,
    pvwin.prior_vv_win_ip,              pvwin.prior_vv_win_time,
    pvbid.prior_vv_bid_ip,              pvbid.prior_vv_bid_time,

    -- 6. Cross-Stage: S1 Event Resolution
    COALESCE(s1_s2.s1_event_ad_served_id, s1_direct.s1_event_ad_served_id) AS s1_event_ad_served_id,
    COALESCE(s1_s2.s1_event_vast_start_ip, s1_direct.s1_event_vast_start_ip) AS s1_event_vast_start_ip,
    COALESCE(s1_s2.s1_event_vast_impression_ip, s1_direct.s1_event_vast_impression_ip) AS s1_event_vast_impression_ip,
    COALESCE(s1_s2.s1_event_time, s1_direct.s1_event_time) AS s1_event_time,
    COALESCE(s1_s2.s1_event_campaign_id, s1_direct.s1_event_campaign_id) AS s1_event_campaign_id,

    -- 7. Resolution Status
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
    END AS resolution_method,

    -- 8. Metadata
    DATE(v.clickpass_time) AS trace_date,
    CURRENT_TIMESTAMP() AS trace_run_timestamp

FROM anchor_vvs v

-- This VV's 7-IP trace
LEFT JOIN ip_vast_start vs ON vs.ad_served_id = v.ad_served_id
LEFT JOIN ip_vast_impression vi ON vi.ad_served_id = v.ad_served_id
LEFT JOIN ip_viewability vw ON vw.ad_served_id = v.ad_served_id
LEFT JOIN ip_impression imp ON imp.ad_served_id = v.ad_served_id
LEFT JOIN ip_win w ON w.ad_served_id = v.ad_served_id
LEFT JOIN ip_bid b ON b.ad_served_id = v.ad_served_id

-- Cross-stage: prior VV match (S3 only)
LEFT JOIN prior_vv_matched pvm ON pvm.s3_ad_served_id = v.ad_served_id

-- Cross-stage: prior VV's 7-IP trace
LEFT JOIN pv_ip_vast_start pvvs ON pvvs.ad_served_id = pvm.prior_vv_ad_served_id
LEFT JOIN pv_ip_vast_impression pvvi ON pvvi.ad_served_id = pvm.prior_vv_ad_served_id
LEFT JOIN pv_ip_viewability pvvw ON pvvw.ad_served_id = pvm.prior_vv_ad_served_id
LEFT JOIN pv_ip_impression pvimp ON pvimp.ad_served_id = pvm.prior_vv_ad_served_id
LEFT JOIN pv_ip_win pvwin ON pvwin.ad_served_id = pvm.prior_vv_ad_served_id
LEFT JOIN pv_ip_bid pvbid ON pvbid.ad_served_id = pvm.prior_vv_ad_served_id

-- S1 event resolution: S2 VVs (this VV's bid_ip → S1 event_log)
LEFT JOIN s1_by_vast_start s1_direct
    ON s1_direct.s1_event_vast_start_ip = b.bid_ip
    AND v.funnel_level = 2

-- S1 event resolution: S3 VVs with S2 prior (prior VV's bid_ip → S1 event_log)
LEFT JOIN s1_by_vast_start s1_s2
    ON s1_s2.s1_event_vast_start_ip = pvbid.prior_vv_bid_ip
    AND pvm.prior_vv_funnel_level = 2

-- Dimension lookups
LEFT JOIN `dw-main-bronze.integrationprod.advertisers` adv
    ON v.advertiser_id = adv.advertiser_id AND adv.deleted = FALSE
LEFT JOIN `dw-main-bronze.integrationprod.campaign_groups` cg
    ON v.campaign_group_id = cg.campaign_group_id AND cg.deleted = FALSE

ORDER BY v.advertiser_id, v.clickpass_time;
