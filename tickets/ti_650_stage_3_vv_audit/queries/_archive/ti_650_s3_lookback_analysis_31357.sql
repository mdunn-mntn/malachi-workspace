-- TI-650: S3 lookback gap analysis — advertiser 31357 (WGU)
-- VV POOL ONLY (impression pool removed to avoid OOM — prior version hit 101% memory limit)
-- Measures gap distribution between S3 VV time and nearest prior S1/S2 VV pool match
-- Uses 180d pool lookback to determine if 90d is sufficient for S3 resolution
--
-- VV pool = clickpass_log (S1/S2 VV IPs) — PRIMARY resolver per Zach breakthrough
-- Key: Uses MAX (most recent match), not MIN (earliest) — MIN is biased (learned from S2)
--
-- Parameters (inlined to avoid scripted query overhead):
--   advertiser_id = 31357
--   vv_start = 2026-02-04, vv_end = 2026-02-11
--   step1_lookback = 2025-11-06 (90d before vv_start)
--   pool_lookback = 2025-08-08 (180d before vv_start)

WITH s3_vvs AS (
    SELECT
        cp.ad_served_id,
        SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] AS clickpass_ip,
        cp.time AS vv_time,
        cp.campaign_id,
        c.campaign_group_id
    FROM `dw-main-silver.logdata.clickpass_log` cp
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = cp.campaign_id
        AND c.deleted = FALSE AND c.is_test = FALSE
        AND c.funnel_level = 3 AND c.objective_id IN (1, 5, 6)
    WHERE cp.time >= TIMESTAMP('2026-02-04') AND cp.time < TIMESTAMP('2026-02-11')
      AND cp.advertiser_id = 31357
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),

-- 5-source IP trace for S3 VVs

ip_event_log AS (
    SELECT ad_served_id, SPLIT(ip, '/')[SAFE_OFFSET(0)] AS event_log_ip
    FROM `dw-main-silver.logdata.event_log`
    WHERE event_type_raw IN ('vast_start', 'vast_impression')
      AND time >= TIMESTAMP('2025-11-06') AND time < TIMESTAMP('2026-02-11')
      AND advertiser_id = 31357
      AND ad_served_id IN (SELECT ad_served_id FROM s3_vvs)
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

ip_viewability AS (
    SELECT ad_served_id, SPLIT(ip, '/')[SAFE_OFFSET(0)] AS viewability_ip
    FROM `dw-main-silver.logdata.viewability_log`
    WHERE time >= TIMESTAMP('2025-11-06') AND time < TIMESTAMP('2026-02-11')
      AND advertiser_id = 31357
      AND ad_served_id IN (SELECT ad_served_id FROM s3_vvs)
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

ip_impression AS (
    SELECT ad_served_id, SPLIT(ip, '/')[SAFE_OFFSET(0)] AS impression_ip, ttd_impression_id
    FROM `dw-main-silver.logdata.impression_log`
    WHERE time >= TIMESTAMP('2025-11-06') AND time < TIMESTAMP('2026-02-11')
      AND advertiser_id = 31357
      AND ad_served_id IN (SELECT ad_served_id FROM s3_vvs)
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

ip_win AS (
    SELECT il.ad_served_id, SPLIT(w.ip, '/')[SAFE_OFFSET(0)] AS win_ip
    FROM `dw-main-silver.logdata.win_logs` w
    JOIN ip_impression il ON w.auction_id = il.ttd_impression_id
    WHERE w.time >= TIMESTAMP('2025-11-06') AND w.time < TIMESTAMP('2026-02-11')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY il.ad_served_id ORDER BY w.time ASC) = 1
),

ip_bid AS (
    SELECT il.ad_served_id, SPLIT(b.ip, '/')[SAFE_OFFSET(0)] AS bid_ip
    FROM `dw-main-silver.logdata.bid_logs` b
    JOIN ip_impression il ON b.auction_id = il.ttd_impression_id
    WHERE b.time >= TIMESTAMP('2025-11-06') AND b.time < TIMESTAMP('2026-02-11')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY il.ad_served_id ORDER BY b.time ASC) = 1
),

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

-- VV pool (180d): S1/S2 clickpass_log VV IPs
vv_pool AS (
    SELECT
        c.campaign_group_id,
        SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] AS match_ip,
        MIN(cp.time) AS earliest_time,
        MAX(cp.time) AS latest_time
    FROM `dw-main-silver.logdata.clickpass_log` cp
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = cp.campaign_id
        AND c.deleted = FALSE AND c.is_test = FALSE
        AND c.funnel_level IN (1, 2) AND c.objective_id IN (1, 5, 6)
    WHERE cp.time >= TIMESTAMP('2025-08-08') AND cp.time < TIMESTAMP('2026-02-11')
      AND cp.advertiser_id = 31357
      AND cp.ip IS NOT NULL
    GROUP BY c.campaign_group_id, SPLIT(cp.ip, '/')[SAFE_OFFSET(0)]
),

-- Gap computation
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
    -- VV pool gap stats (using MIN = earliest match)
    (SELECT MAX(gap_earliest_days) FROM vv_gaps) AS vv_max_gap_earliest,
    (SELECT APPROX_QUANTILES(gap_earliest_days, 100)[OFFSET(50)] FROM vv_gaps) AS vv_median_gap_earliest,
    (SELECT APPROX_QUANTILES(gap_earliest_days, 100)[OFFSET(95)] FROM vv_gaps) AS vv_p95_gap_earliest,
    (SELECT APPROX_QUANTILES(gap_earliest_days, 100)[OFFSET(99)] FROM vv_gaps) AS vv_p99_gap_earliest,
    -- VV pool bucket counts
    (SELECT COUNTIF(gap_latest_days < 0) FROM vv_gaps) AS vv_latest_after_vv,
    (SELECT COUNTIF(gap_latest_days >= 0 AND gap_latest_days <= 90) FROM vv_gaps) AS vv_latest_within_90d,
    (SELECT COUNTIF(gap_latest_days > 90) FROM vv_gaps) AS vv_latest_beyond_90d

FROM base b;
