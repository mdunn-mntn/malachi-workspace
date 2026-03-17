-- TI-650: S2→S1 lookback gap analysis — earliest vs latest S1 match
-- Answers: "for a week of S2 VVs, how far back is the nearest S1 match within same campaign_group_id?"
--
-- Key finding: Using MIN(impression_time) = biased (max 186d). Using MAX = realistic (max 69d, median 6d).
-- Zero IPs have their latest S1 match >90d before the VV. 90d lookback is sufficient.
--
-- Run time: ~30 min (180d S1 pool across 4 tables)

DECLARE p_advertiser_id INT64 DEFAULT 31357;
DECLARE p_vv_start TIMESTAMP DEFAULT TIMESTAMP('2026-02-04');
DECLARE p_vv_end TIMESTAMP DEFAULT TIMESTAMP('2026-02-11');
DECLARE p_s1_lookback TIMESTAMP DEFAULT TIMESTAMP_SUB(TIMESTAMP('2026-02-04'), INTERVAL 180 DAY);

CREATE TEMP FUNCTION strip_cidr(ip STRING) AS (SPLIT(ip, '/')[SAFE_OFFSET(0)]);

-- S2 VVs
WITH s2_vvs AS (
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
        AND c.funnel_level = 2 AND c.objective_id IN (1, 5, 6)
    WHERE cp.time >= p_vv_start AND cp.time < p_vv_end
      AND cp.advertiser_id = p_advertiser_id
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),

-- Get resolved IP (Step 1: within-S2 via ad_served_id)
ip_impression AS (
    SELECT ad_served_id, strip_cidr(ip) AS impression_ip, ttd_impression_id
    FROM `dw-main-silver.logdata.impression_log`
    WHERE time >= TIMESTAMP_SUB(p_vv_start, INTERVAL 90 DAY) AND time < p_vv_end
      AND advertiser_id = p_advertiser_id
      AND ad_served_id IN (SELECT ad_served_id FROM s2_vvs)
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),
ip_bid AS (
    SELECT il.ad_served_id, strip_cidr(b.ip) AS bid_ip
    FROM `dw-main-silver.logdata.bid_logs` b
    JOIN ip_impression il ON b.auction_id = il.ttd_impression_id
    WHERE b.time >= TIMESTAMP_SUB(p_vv_start, INTERVAL 90 DAY) AND b.time < p_vv_end
    QUALIFY ROW_NUMBER() OVER (PARTITION BY il.ad_served_id ORDER BY b.time ASC) = 1
),
resolved_ip AS (
    SELECT v.ad_served_id, v.vv_time, v.campaign_group_id,
           COALESCE(b.bid_ip, i.impression_ip) AS resolved_ip
    FROM s2_vvs v
    LEFT JOIN ip_bid b ON b.ad_served_id = v.ad_served_id
    LEFT JOIN ip_impression i ON i.ad_served_id = v.ad_served_id
),

-- S1 pool with BOTH min and max impression time per (cg, ip)
s1_pool AS (
    SELECT campaign_group_id, match_ip,
           MIN(impression_time) AS earliest_impression,
           MAX(impression_time) AS latest_impression
    FROM (
        SELECT c.campaign_group_id, strip_cidr(el.ip) AS match_ip, el.time AS impression_time
        FROM `dw-main-silver.logdata.event_log` el
        JOIN `dw-main-bronze.integrationprod.campaigns` c
            ON c.campaign_id = el.campaign_id AND c.deleted = FALSE AND c.is_test = FALSE
            AND c.funnel_level = 1 AND c.objective_id IN (1, 5, 6)
        WHERE el.event_type_raw IN ('vast_start', 'vast_impression')
          AND el.time >= p_s1_lookback AND el.time < p_vv_end
          AND el.advertiser_id = p_advertiser_id AND el.ip IS NOT NULL
        UNION ALL
        SELECT c.campaign_group_id, strip_cidr(vl.ip) AS match_ip, vl.time AS impression_time
        FROM `dw-main-silver.logdata.viewability_log` vl
        JOIN `dw-main-bronze.integrationprod.campaigns` c
            ON c.campaign_id = vl.campaign_id AND c.deleted = FALSE AND c.is_test = FALSE
            AND c.funnel_level = 1 AND c.objective_id IN (1, 5, 6)
        WHERE vl.time >= p_s1_lookback AND vl.time < p_vv_end
          AND vl.advertiser_id = p_advertiser_id AND vl.ip IS NOT NULL
        UNION ALL
        SELECT c.campaign_group_id, strip_cidr(il.ip) AS match_ip, il.time AS impression_time
        FROM `dw-main-silver.logdata.impression_log` il
        JOIN `dw-main-bronze.integrationprod.campaigns` c
            ON c.campaign_id = il.campaign_id AND c.deleted = FALSE AND c.is_test = FALSE
            AND c.funnel_level = 1 AND c.objective_id IN (1, 5, 6)
        WHERE il.time >= p_s1_lookback AND il.time < p_vv_end
          AND il.advertiser_id = p_advertiser_id AND il.ip IS NOT NULL
        UNION ALL
        SELECT c.campaign_group_id, strip_cidr(cp.ip) AS match_ip, cp.time AS impression_time
        FROM `dw-main-silver.logdata.clickpass_log` cp
        JOIN `dw-main-bronze.integrationprod.campaigns` c
            ON c.campaign_id = cp.campaign_id AND c.deleted = FALSE AND c.is_test = FALSE
            AND c.funnel_level = 1 AND c.objective_id IN (1, 5, 6)
        WHERE cp.time >= p_s1_lookback AND cp.time < p_vv_end
          AND cp.advertiser_id = p_advertiser_id AND cp.ip IS NOT NULL
    )
    GROUP BY campaign_group_id, match_ip
),

-- Join: compute gap using EARLIEST vs LATEST S1 match
gaps AS (
    SELECT
        r.ad_served_id,
        r.vv_time,
        s1.earliest_impression,
        s1.latest_impression,
        TIMESTAMP_DIFF(r.vv_time, s1.earliest_impression, DAY) AS gap_earliest_days,
        TIMESTAMP_DIFF(r.vv_time, s1.latest_impression, DAY) AS gap_latest_days
    FROM resolved_ip r
    JOIN s1_pool s1
        ON s1.campaign_group_id = r.campaign_group_id
        AND s1.match_ip = r.resolved_ip
        AND s1.earliest_impression < r.vv_time
    WHERE r.resolved_ip IS NOT NULL
)

SELECT
    COUNT(*) AS total_matched,
    -- Using EARLIEST S1 match (biased — MIN)
    MAX(gap_earliest_days) AS max_gap_earliest,
    APPROX_QUANTILES(gap_earliest_days, 100)[OFFSET(50)] AS median_gap_earliest,
    APPROX_QUANTILES(gap_earliest_days, 100)[OFFSET(95)] AS p95_gap_earliest,
    APPROX_QUANTILES(gap_earliest_days, 100)[OFFSET(99)] AS p99_gap_earliest,
    -- Using LATEST S1 match (most recent — MAX)
    MAX(gap_latest_days) AS max_gap_latest,
    APPROX_QUANTILES(gap_latest_days, 100)[OFFSET(50)] AS median_gap_latest,
    APPROX_QUANTILES(gap_latest_days, 100)[OFFSET(95)] AS p95_gap_latest,
    APPROX_QUANTILES(gap_latest_days, 100)[OFFSET(99)] AS p99_gap_latest,
    -- Distribution buckets
    COUNTIF(gap_latest_days < 0) AS latest_after_vv,
    COUNTIF(gap_latest_days >= 0 AND gap_latest_days <= 90) AS latest_within_90d,
    COUNTIF(gap_latest_days > 90) AS latest_beyond_90d,
    COUNTIF(gap_earliest_days <= 90) AS earliest_within_90d,
    COUNTIF(gap_earliest_days > 90) AS earliest_beyond_90d
FROM gaps;
