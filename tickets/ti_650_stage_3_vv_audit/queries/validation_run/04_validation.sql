-- TI-650 Validation Run: Step 4 — Validate Trace Table
-- Runs the same trace logic as Step 3 but outputs validation checks only
-- Checks: row counts, resolution integrity, dedup, impression type coverage

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
    SELECT ad_served_id, SPLIT(ip, '/')[SAFE_OFFSET(0)] AS impression_ip, ttd_impression_id
    FROM `dw-main-silver.logdata.impression_log`
    WHERE time >= TIMESTAMP('2026-02-14') AND time < TIMESTAMP('2026-04-22')
      AND advertiser_id IN (31276, 53308, 37775, 37056, 46104, 31455, 48866, 34838, 38101, 40236)
      AND ad_served_id IN (SELECT ad_served_id FROM anchor_vvs) AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

ip_bid AS (
    SELECT il.ad_served_id, NULLIF(SPLIT(b.ip, '/')[SAFE_OFFSET(0)], '0.0.0.0') AS bid_ip
    FROM `dw-main-silver.logdata.bid_logs` b
    JOIN ip_impression il ON b.auction_id = il.ttd_impression_id
    WHERE b.time >= TIMESTAMP('2026-02-14') AND b.time < TIMESTAMP('2026-04-22')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY il.ad_served_id ORDER BY b.time ASC) = 1
),

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
        AND c.funnel_level IN (1, 2) AND c.objective_id IN (1, 5, 6)
    WHERE cp.time >= TIMESTAMP('2025-03-16') AND cp.time < TIMESTAMP('2026-03-23')
      AND cp.advertiser_id IN (31276, 53308, 37775, 37056, 46104, 31455, 48866, 34838, 38101, 40236)
      AND cp.ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),

s3_s2_match AS (
    SELECT v.ad_served_id AS s3_ad_served_id, pv.prior_vv_ad_served_id,
           pv.prior_vv_funnel_level, pv.prior_vv_clickpass_ip
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
    SELECT v.ad_served_id AS s3_ad_served_id, pv.prior_vv_ad_served_id,
           pv.prior_vv_funnel_level, pv.prior_vv_clickpass_ip
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
    SELECT ad_served_id, ttd_impression_id
    FROM `dw-main-silver.logdata.impression_log`
    WHERE time >= TIMESTAMP('2025-03-16') AND time < TIMESTAMP('2026-04-22')
      AND ad_served_id IN (SELECT prior_vv_ad_served_id FROM prior_vv_matched) AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

pv_ip_bid AS (
    SELECT il.ad_served_id, NULLIF(SPLIT(b.ip, '/')[SAFE_OFFSET(0)], '0.0.0.0') AS prior_vv_bid_ip
    FROM `dw-main-silver.logdata.bid_logs` b
    JOIN pv_ip_impression il ON b.auction_id = il.ttd_impression_id
    WHERE b.time >= TIMESTAMP('2025-03-16') AND b.time < TIMESTAMP('2026-04-22')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY il.ad_served_id ORDER BY b.time ASC) = 1
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

-- Build the trace with resolution status
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
    -- 4.1: Total VVs
    'total_vvs' AS check_name,
    COUNT(*) AS value,
    CAST(NULL AS STRING) AS detail
FROM trace

UNION ALL

-- 4.2: S1 VVs all resolved
SELECT
    's1_all_resolved',
    COUNTIF(funnel_level = 1 AND resolution_status != 'resolved'),
    FORMAT('S1 total: %d, not resolved: %d',
        COUNTIF(funnel_level = 1),
        COUNTIF(funnel_level = 1 AND resolution_status != 'resolved'))
FROM trace

UNION ALL

-- 4.3: Resolved S3 VVs have prior_vv_ad_served_id
SELECT
    's3_resolved_has_prior_vv',
    COUNTIF(funnel_level = 3 AND resolution_status = 'resolved' AND prior_vv_ad_served_id IS NULL),
    FORMAT('S3 resolved: %d, missing prior_vv: %d',
        COUNTIF(funnel_level = 3 AND resolution_status = 'resolved'),
        COUNTIF(funnel_level = 3 AND resolution_status = 'resolved' AND prior_vv_ad_served_id IS NULL))
FROM trace

UNION ALL

-- 4.4: S3 with S2 prior has S1 event
SELECT
    's3_s2_prior_has_s1_event',
    COUNTIF(funnel_level = 3 AND prior_vv_funnel_level = 2 AND s1_via_s2 IS NULL AND resolution_status = 'resolved'),
    FORMAT('S3 with S2 prior (resolved): %d, missing S1 event: %d',
        COUNTIF(funnel_level = 3 AND prior_vv_funnel_level = 2 AND resolution_status = 'resolved'),
        COUNTIF(funnel_level = 3 AND prior_vv_funnel_level = 2 AND s1_via_s2 IS NULL AND resolution_status = 'resolved'))
FROM trace

UNION ALL

-- 4.5: No duplicate ad_served_ids
SELECT
    'duplicate_ad_served_ids',
    COUNT(*) - COUNT(DISTINCT ad_served_id),
    FORMAT('total rows: %d, distinct ad_served_ids: %d', COUNT(*), COUNT(DISTINCT ad_served_id))
FROM trace

UNION ALL

-- 4.6: impression_type not NULL for resolved
SELECT
    'resolved_missing_impression_type',
    COUNTIF(resolution_status = 'resolved' AND impression_type = 'No Impression Found'),
    FORMAT('resolved: %d, no impression: %d',
        COUNTIF(resolution_status = 'resolved'),
        COUNTIF(resolution_status = 'resolved' AND impression_type = 'No Impression Found'))
FROM trace

UNION ALL

-- 4.7: bid_ip not NULL for resolved
SELECT
    'resolved_missing_bid_ip',
    COUNTIF(resolution_status = 'resolved' AND bid_ip IS NULL AND funnel_level != 1),
    FORMAT('resolved non-S1: %d, bid_ip NULL: %d',
        COUNTIF(resolution_status = 'resolved' AND funnel_level != 1),
        COUNTIF(resolution_status = 'resolved' AND bid_ip IS NULL AND funnel_level != 1))
FROM trace

UNION ALL

-- 4.8: Count by resolution_status
SELECT
    'count_by_resolution_status',
    0,
    STRING_AGG(FORMAT('%s: %d', resolution_status, cnt), ' | ' ORDER BY resolution_status)
FROM (SELECT resolution_status, COUNT(*) AS cnt FROM trace GROUP BY resolution_status)

UNION ALL

-- 4.9: Count by resolution_method
SELECT
    'count_by_resolution_method',
    0,
    STRING_AGG(FORMAT('%s: %d', IFNULL(resolution_method, 'NULL'), cnt), ' | ' ORDER BY resolution_method)
FROM (SELECT resolution_method, COUNT(*) AS cnt FROM trace GROUP BY resolution_method)

UNION ALL

-- 4.10: Count by impression_type
SELECT
    'count_by_impression_type',
    0,
    STRING_AGG(FORMAT('%s: %d', impression_type, cnt), ' | ' ORDER BY impression_type)
FROM (SELECT impression_type, COUNT(*) AS cnt FROM trace GROUP BY impression_type)

UNION ALL

-- Bonus: Count by funnel_level
SELECT
    'count_by_funnel_level',
    0,
    STRING_AGG(FORMAT('S%d: %d', funnel_level, cnt), ' | ' ORDER BY funnel_level)
FROM (SELECT funnel_level, COUNT(*) AS cnt FROM trace GROUP BY funnel_level)

ORDER BY check_name;
