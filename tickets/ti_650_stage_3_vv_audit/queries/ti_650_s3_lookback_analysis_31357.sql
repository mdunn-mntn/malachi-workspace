-- TI-650: S3 lookback gap analysis — advertiser 31357 (WGU)
-- Measures gap distribution between S3 VV time and nearest prior S1/S2 pool match
-- Uses 180d pool lookback to determine if 90d is sufficient for S3 resolution
--
-- Two pools tested:
--   VV pool: clickpass_log (S1/S2 VV IPs) — PRIMARY resolver per Zach breakthrough
--   Impression pool: event_log + viewability_log + impression_log (S1/S2 impressions) — FALLBACK
--
-- Key: Uses MAX (most recent match), not MIN (earliest) — MIN is biased (learned from S2)
--
-- Adapted from ti_650_s2_lookback_analysis.sql (proven MIN/MAX pattern)

DECLARE p_advertiser_id INT64 DEFAULT 31357;
DECLARE p_vv_start TIMESTAMP DEFAULT TIMESTAMP('2026-02-04');
DECLARE p_vv_end TIMESTAMP DEFAULT TIMESTAMP('2026-02-11');
-- Step 1 lookback: 90 days (within-stage bid_ip extraction, bid_logs TTL = 90d)
DECLARE p_step1_lookback TIMESTAMP DEFAULT TIMESTAMP_SUB(TIMESTAMP('2026-02-04'), INTERVAL 90 DAY);
-- Pool lookback: 180 days (wide window to measure gap distribution)
DECLARE p_pool_lookback TIMESTAMP DEFAULT TIMESTAMP_SUB(TIMESTAMP('2026-02-04'), INTERVAL 180 DAY);

CREATE TEMP FUNCTION strip_cidr(ip STRING) AS (SPLIT(ip, '/')[SAFE_OFFSET(0)]);

-- S3 VVs
WITH s3_vvs AS (
    SELECT
        cp.ad_served_id,
        strip_cidr(cp.ip) AS clickpass_ip,
        cp.time AS vv_time,
        cp.campaign_id,
        c.campaign_group_id
    FROM `dw-main-silver.logdata.clickpass_log` cp
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = cp.campaign_id
        AND c.deleted = FALSE AND c.is_test = FALSE
        AND c.funnel_level = 3 AND c.objective_id IN (1, 5, 6)
    WHERE cp.time >= p_vv_start AND cp.time < p_vv_end
      AND cp.advertiser_id = p_advertiser_id
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),

-- Step 1: Get IP at each pipeline step via ad_served_id (5-source trace)

-- event_log: CTV path (vast_start / vast_impression)
ip_event_log AS (
    SELECT ad_served_id, strip_cidr(ip) AS event_log_ip
    FROM `dw-main-silver.logdata.event_log`
    WHERE event_type_raw IN ('vast_start', 'vast_impression')
      AND time >= p_step1_lookback AND time < p_vv_end
      AND advertiser_id = p_advertiser_id
      AND ad_served_id IN (SELECT ad_served_id FROM s3_vvs)
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

-- viewability_log: viewable display path
ip_viewability AS (
    SELECT ad_served_id, strip_cidr(ip) AS viewability_ip
    FROM `dw-main-silver.logdata.viewability_log`
    WHERE time >= p_step1_lookback AND time < p_vv_end
      AND advertiser_id = p_advertiser_id
      AND ad_served_id IN (SELECT ad_served_id FROM s3_vvs)
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

-- impression_log: all display paths (also gives ttd_impression_id for Beeswax bridge)
ip_impression AS (
    SELECT ad_served_id, strip_cidr(ip) AS impression_ip, ttd_impression_id
    FROM `dw-main-silver.logdata.impression_log`
    WHERE time >= p_step1_lookback AND time < p_vv_end
      AND advertiser_id = p_advertiser_id
      AND ad_served_id IN (SELECT ad_served_id FROM s3_vvs)
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

-- win_logs: via auction_id bridge from impression_log
ip_win AS (
    SELECT il.ad_served_id, strip_cidr(w.ip) AS win_ip
    FROM `dw-main-silver.logdata.win_logs` w
    JOIN ip_impression il ON w.auction_id = il.ttd_impression_id
    WHERE w.time >= p_step1_lookback AND w.time < p_vv_end
    QUALIFY ROW_NUMBER() OVER (PARTITION BY il.ad_served_id ORDER BY w.time ASC) = 1
),

-- bid_logs: via auction_id bridge from impression_log
ip_bid AS (
    SELECT il.ad_served_id, strip_cidr(b.ip) AS bid_ip
    FROM `dw-main-silver.logdata.bid_logs` b
    JOIN ip_impression il ON b.auction_id = il.ttd_impression_id
    WHERE b.time >= p_step1_lookback AND b.time < p_vv_end
    QUALIFY ROW_NUMBER() OVER (PARTITION BY il.ad_served_id ORDER BY b.time ASC) = 1
),

-- Coalesce: bid > win > impression > viewability > event_log
resolved_ip AS (
    SELECT
        v.ad_served_id,
        v.vv_time,
        v.campaign_group_id,
        COALESCE(bid.bid_ip, win.win_ip, imp.impression_ip, vw.viewability_ip, el.event_log_ip) AS resolved_ip
    FROM s3_vvs v
    LEFT JOIN ip_bid bid ON bid.ad_served_id = v.ad_served_id
    LEFT JOIN ip_win win ON win.ad_served_id = v.ad_served_id
    LEFT JOIN ip_impression imp ON imp.ad_served_id = v.ad_served_id
    LEFT JOIN ip_viewability vw ON vw.ad_served_id = v.ad_served_id
    LEFT JOIN ip_event_log el ON el.ad_served_id = v.ad_served_id
),

-- VV pool (180d): S1/S2 clickpass_log VV IPs — PRIMARY resolver
-- GROUP BY (campaign_group_id, ip) → MIN(time), MAX(time)
vv_pool AS (
    SELECT
        c.campaign_group_id,
        strip_cidr(cp.ip) AS match_ip,
        MIN(cp.time) AS earliest_time,
        MAX(cp.time) AS latest_time
    FROM `dw-main-silver.logdata.clickpass_log` cp
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = cp.campaign_id
        AND c.deleted = FALSE AND c.is_test = FALSE
        AND c.funnel_level IN (1, 2) AND c.objective_id IN (1, 5, 6)
    WHERE cp.time >= p_pool_lookback AND cp.time < p_vv_end
      AND cp.advertiser_id = p_advertiser_id
      AND cp.ip IS NOT NULL
    GROUP BY c.campaign_group_id, strip_cidr(cp.ip)
),

-- Impression pool (180d): S1/S2 event_log + viewability_log + impression_log
-- UNION ALL, GROUP BY (campaign_group_id, ip) → MIN(time), MAX(time)
imp_pool AS (
    SELECT campaign_group_id, match_ip,
           MIN(impression_time) AS earliest_time,
           MAX(impression_time) AS latest_time
    FROM (
        -- event_log: CTV VAST events
        SELECT c.campaign_group_id, strip_cidr(el.ip) AS match_ip, el.time AS impression_time
        FROM `dw-main-silver.logdata.event_log` el
        JOIN `dw-main-bronze.integrationprod.campaigns` c
            ON c.campaign_id = el.campaign_id
            AND c.deleted = FALSE AND c.is_test = FALSE
            AND c.funnel_level IN (1, 2) AND c.objective_id IN (1, 5, 6)
        WHERE el.event_type_raw IN ('vast_start', 'vast_impression')
          AND el.time >= p_pool_lookback AND el.time < p_vv_end
          AND el.advertiser_id = p_advertiser_id AND el.ip IS NOT NULL
        UNION ALL
        -- viewability_log: viewable display
        SELECT c.campaign_group_id, strip_cidr(vl.ip) AS match_ip, vl.time AS impression_time
        FROM `dw-main-silver.logdata.viewability_log` vl
        JOIN `dw-main-bronze.integrationprod.campaigns` c
            ON c.campaign_id = vl.campaign_id
            AND c.deleted = FALSE AND c.is_test = FALSE
            AND c.funnel_level IN (1, 2) AND c.objective_id IN (1, 5, 6)
        WHERE vl.time >= p_pool_lookback AND vl.time < p_vv_end
          AND vl.advertiser_id = p_advertiser_id AND vl.ip IS NOT NULL
        UNION ALL
        -- impression_log: non-viewable display
        SELECT c.campaign_group_id, strip_cidr(il.ip) AS match_ip, il.time AS impression_time
        FROM `dw-main-silver.logdata.impression_log` il
        JOIN `dw-main-bronze.integrationprod.campaigns` c
            ON c.campaign_id = il.campaign_id
            AND c.deleted = FALSE AND c.is_test = FALSE
            AND c.funnel_level IN (1, 2) AND c.objective_id IN (1, 5, 6)
        WHERE il.time >= p_pool_lookback AND il.time < p_vv_end
          AND il.advertiser_id = p_advertiser_id AND il.ip IS NOT NULL
    )
    GROUP BY campaign_group_id, match_ip
),

-- Gap computation: JOIN resolved_ip to both pools
vv_gaps AS (
    SELECT
        r.ad_served_id,
        r.vv_time,
        vp.earliest_time,
        vp.latest_time,
        TIMESTAMP_DIFF(r.vv_time, vp.earliest_time, DAY) AS gap_earliest_days,
        TIMESTAMP_DIFF(r.vv_time, vp.latest_time, DAY) AS gap_latest_days
    FROM resolved_ip r
    JOIN vv_pool vp
        ON vp.campaign_group_id = r.campaign_group_id
        AND vp.match_ip = r.resolved_ip
        AND vp.earliest_time < r.vv_time
    WHERE r.resolved_ip IS NOT NULL
),

imp_gaps AS (
    SELECT
        r.ad_served_id,
        r.vv_time,
        ip.earliest_time,
        ip.latest_time,
        TIMESTAMP_DIFF(r.vv_time, ip.earliest_time, DAY) AS gap_earliest_days,
        TIMESTAMP_DIFF(r.vv_time, ip.latest_time, DAY) AS gap_latest_days
    FROM resolved_ip r
    JOIN imp_pool ip
        ON ip.campaign_group_id = r.campaign_group_id
        AND ip.match_ip = r.resolved_ip
        AND ip.earliest_time < r.vv_time
    WHERE r.resolved_ip IS NOT NULL
),

-- Baseline counts
base AS (
    SELECT
        COUNT(*) AS total_s3_vvs,
        COUNTIF(resolved_ip IS NOT NULL) AS has_ip,
        COUNTIF(resolved_ip IS NULL) AS no_ip
    FROM resolved_ip
)

SELECT
    b.total_s3_vvs,
    b.has_ip,
    b.no_ip,

    -- VV pool gap stats (using MAX = most recent match)
    (SELECT COUNT(*) FROM vv_gaps) AS vv_pool_matched,
    (SELECT MAX(gap_latest_days) FROM vv_gaps) AS vv_max_gap_latest,
    (SELECT APPROX_QUANTILES(gap_latest_days, 100)[OFFSET(50)] FROM vv_gaps) AS vv_median_gap_latest,
    (SELECT APPROX_QUANTILES(gap_latest_days, 100)[OFFSET(95)] FROM vv_gaps) AS vv_p95_gap_latest,
    (SELECT APPROX_QUANTILES(gap_latest_days, 100)[OFFSET(99)] FROM vv_gaps) AS vv_p99_gap_latest,
    -- VV pool gap stats (using MIN = earliest match, for comparison)
    (SELECT MAX(gap_earliest_days) FROM vv_gaps) AS vv_max_gap_earliest,
    (SELECT APPROX_QUANTILES(gap_earliest_days, 100)[OFFSET(50)] FROM vv_gaps) AS vv_median_gap_earliest,
    (SELECT APPROX_QUANTILES(gap_earliest_days, 100)[OFFSET(95)] FROM vv_gaps) AS vv_p95_gap_earliest,
    (SELECT APPROX_QUANTILES(gap_earliest_days, 100)[OFFSET(99)] FROM vv_gaps) AS vv_p99_gap_earliest,
    -- VV pool bucket counts
    (SELECT COUNTIF(gap_latest_days < 0) FROM vv_gaps) AS vv_latest_after_vv,
    (SELECT COUNTIF(gap_latest_days >= 0 AND gap_latest_days <= 90) FROM vv_gaps) AS vv_latest_within_90d,
    (SELECT COUNTIF(gap_latest_days > 90) FROM vv_gaps) AS vv_latest_beyond_90d,

    -- Impression pool gap stats (using MAX = most recent match)
    (SELECT COUNT(*) FROM imp_gaps) AS imp_pool_matched,
    (SELECT MAX(gap_latest_days) FROM imp_gaps) AS imp_max_gap_latest,
    (SELECT APPROX_QUANTILES(gap_latest_days, 100)[OFFSET(50)] FROM imp_gaps) AS imp_median_gap_latest,
    (SELECT APPROX_QUANTILES(gap_latest_days, 100)[OFFSET(95)] FROM imp_gaps) AS imp_p95_gap_latest,
    (SELECT APPROX_QUANTILES(gap_latest_days, 100)[OFFSET(99)] FROM imp_gaps) AS imp_p99_gap_latest,
    -- Impression pool gap stats (using MIN = earliest match, for comparison)
    (SELECT MAX(gap_earliest_days) FROM imp_gaps) AS imp_max_gap_earliest,
    (SELECT APPROX_QUANTILES(gap_earliest_days, 100)[OFFSET(50)] FROM imp_gaps) AS imp_median_gap_earliest,
    (SELECT APPROX_QUANTILES(gap_earliest_days, 100)[OFFSET(95)] FROM imp_gaps) AS imp_p95_gap_earliest,
    (SELECT APPROX_QUANTILES(gap_earliest_days, 100)[OFFSET(99)] FROM imp_gaps) AS imp_p99_gap_earliest,
    -- Impression pool bucket counts
    (SELECT COUNTIF(gap_latest_days < 0) FROM imp_gaps) AS imp_latest_after_vv,
    (SELECT COUNTIF(gap_latest_days >= 0 AND gap_latest_days <= 90) FROM imp_gaps) AS imp_latest_within_90d,
    (SELECT COUNTIF(gap_latest_days > 90) FROM imp_gaps) AS imp_latest_beyond_90d

FROM base b;
