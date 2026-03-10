-- How many S3 VVs find a prior VV depending on which vast_ip we use as join key?
WITH s3_vvs AS (
  SELECT
    cp.ad_served_id,
    cp.advertiser_id,
    cp.time AS vv_time,
    b.bid_ip AS s3_bid_ip
  FROM `dw-main-silver.logdata.clickpass_log` cp
  JOIN `dw-main-bronze.integrationprod.campaigns` c
    ON cp.campaign_id = c.campaign_id AND c.deleted = FALSE
  JOIN (
    SELECT ad_served_id, bid_ip
    FROM `dw-main-silver.logdata.event_log`
    WHERE DATE(time) BETWEEN "2026-03-01" AND "2026-03-09"
      AND bid_ip IS NOT NULL
      AND event_type_raw = "vast_impression"
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
  ) b ON cp.ad_served_id = b.ad_served_id
  WHERE DATE(cp.time) BETWEEN "2026-03-03" AND "2026-03-09"
    AND c.funnel_level = 3
),
prior_pool AS (
  SELECT
    cp.advertiser_id,
    cp.time AS pv_time,
    vs.ip AS vs_ip,
    vi.ip AS vi_ip
  FROM `dw-main-silver.logdata.clickpass_log` cp
  JOIN `dw-main-bronze.integrationprod.campaigns` c
    ON cp.campaign_id = c.campaign_id AND c.deleted = FALSE
  LEFT JOIN (
    SELECT ad_served_id, ip
    FROM `dw-main-silver.logdata.event_log`
    WHERE DATE(time) BETWEEN "2026-02-01" AND "2026-03-09"
      AND event_type_raw = "vast_start"
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
  ) vs ON cp.ad_served_id = vs.ad_served_id
  LEFT JOIN (
    SELECT ad_served_id, ip
    FROM `dw-main-silver.logdata.event_log`
    WHERE DATE(time) BETWEEN "2026-02-01" AND "2026-03-09"
      AND event_type_raw = "vast_impression"
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
  ) vi ON cp.ad_served_id = vi.ad_served_id
  WHERE DATE(cp.time) BETWEEN "2026-02-01" AND "2026-03-09"
    AND c.funnel_level IN (1, 2)
),
-- Try matching on vast_impression only
match_imp AS (
  SELECT s3.ad_served_id
  FROM s3_vvs s3
  JOIN prior_pool pp
    ON s3.advertiser_id = pp.advertiser_id
    AND pp.pv_time < s3.vv_time
    AND pp.pv_time > TIMESTAMP_SUB(s3.vv_time, INTERVAL 180 DAY)
    AND pp.vi_ip = s3.s3_bid_ip
  GROUP BY 1
),
-- Try matching on vast_start only
match_start AS (
  SELECT s3.ad_served_id
  FROM s3_vvs s3
  JOIN prior_pool pp
    ON s3.advertiser_id = pp.advertiser_id
    AND pp.pv_time < s3.vv_time
    AND pp.pv_time > TIMESTAMP_SUB(s3.vv_time, INTERVAL 180 DAY)
    AND pp.vs_ip = s3.s3_bid_ip
  GROUP BY 1
),
-- Try matching on either
match_either AS (
  SELECT s3.ad_served_id
  FROM s3_vvs s3
  JOIN prior_pool pp
    ON s3.advertiser_id = pp.advertiser_id
    AND pp.pv_time < s3.vv_time
    AND pp.pv_time > TIMESTAMP_SUB(s3.vv_time, INTERVAL 180 DAY)
    AND (pp.vi_ip = s3.s3_bid_ip OR pp.vs_ip = s3.s3_bid_ip)
  GROUP BY 1
)
SELECT
  (SELECT COUNT(*) FROM s3_vvs) AS total_s3_vvs,
  (SELECT COUNT(*) FROM match_imp) AS found_via_imp,
  (SELECT COUNT(*) FROM match_start) AS found_via_start,
  (SELECT COUNT(*) FROM match_either) AS found_via_either
