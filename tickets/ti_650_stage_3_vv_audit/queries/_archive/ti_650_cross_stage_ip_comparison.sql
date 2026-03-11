-- Simplified cross-stage: S3 VVs from 1 week, prior VVs from same 30 days
-- Just need to know: which prior vast_ip matches s3 bid_ip more often?
WITH s3_vvs AS (
  SELECT
    cp.ad_served_id,
    cp.advertiser_id,
    cp.time AS vv_time
  FROM `dw-main-silver.logdata.clickpass_log` cp
  JOIN `dw-main-bronze.integrationprod.campaigns` c
    ON cp.campaign_id = c.campaign_id AND c.deleted = FALSE
  WHERE DATE(cp.time) BETWEEN "2026-03-03" AND "2026-03-09"
    AND c.funnel_level = 3
),
s3_bid AS (
  SELECT e.ad_served_id, e.bid_ip
  FROM `dw-main-silver.logdata.event_log` e
  WHERE DATE(e.time) BETWEEN "2026-03-01" AND "2026-03-09"
    AND e.bid_ip IS NOT NULL
    AND e.event_type_raw = "vast_impression"
  QUALIFY ROW_NUMBER() OVER (PARTITION BY e.ad_served_id ORDER BY e.time ASC) = 1
),
prior_pool AS (
  -- Prior S1/S2 VVs with both vast IPs
  SELECT
    cp.ad_served_id AS pv_asid,
    cp.advertiser_id,
    cp.time AS pv_time,
    vs_ip,
    vi_ip
  FROM `dw-main-silver.logdata.clickpass_log` cp
  JOIN `dw-main-bronze.integrationprod.campaigns` c
    ON cp.campaign_id = c.campaign_id AND c.deleted = FALSE
  LEFT JOIN (
    SELECT ad_served_id, ip AS vs_ip
    FROM `dw-main-silver.logdata.event_log`
    WHERE DATE(time) BETWEEN "2026-02-01" AND "2026-03-09"
      AND event_type_raw = "vast_start"
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
  ) vs ON cp.ad_served_id = vs.ad_served_id
  LEFT JOIN (
    SELECT ad_served_id, ip AS vi_ip
    FROM `dw-main-silver.logdata.event_log`
    WHERE DATE(time) BETWEEN "2026-02-01" AND "2026-03-09"
      AND event_type_raw = "vast_impression"
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
  ) vi ON cp.ad_served_id = vi.ad_served_id
  WHERE DATE(cp.time) BETWEEN "2026-02-01" AND "2026-03-09"
    AND c.funnel_level IN (1, 2)
    AND (vs_ip IS NOT NULL OR vi_ip IS NOT NULL)
),
pairs AS (
  SELECT
    s3.ad_served_id,
    b.bid_ip AS s3_bid_ip,
    pp.vs_ip AS prior_vast_start_ip,
    pp.vi_ip AS prior_vast_impression_ip,
    ROW_NUMBER() OVER (
      PARTITION BY s3.ad_served_id
      ORDER BY pp.pv_time DESC
    ) AS rn
  FROM s3_vvs s3
  JOIN s3_bid b ON s3.ad_served_id = b.ad_served_id
  JOIN prior_pool pp
    ON s3.advertiser_id = pp.advertiser_id
    AND pp.pv_time < s3.vv_time
    AND pp.pv_time > TIMESTAMP_SUB(s3.vv_time, INTERVAL 180 DAY)
    AND (pp.vi_ip = b.bid_ip OR pp.vs_ip = b.bid_ip)
)
SELECT
  COUNT(*) AS total_pairs,
  COUNTIF(s3_bid_ip = prior_vast_start_ip) AS bid_eq_prior_start,
  COUNTIF(s3_bid_ip = prior_vast_impression_ip) AS bid_eq_prior_imp,
  COUNTIF(s3_bid_ip = prior_vast_start_ip AND s3_bid_ip = prior_vast_impression_ip) AS bid_eq_both,
  COUNTIF(s3_bid_ip = prior_vast_start_ip AND (s3_bid_ip != prior_vast_impression_ip OR prior_vast_impression_ip IS NULL)) AS bid_eq_start_only,
  COUNTIF(s3_bid_ip = prior_vast_impression_ip AND (s3_bid_ip != prior_vast_start_ip OR prior_vast_start_ip IS NULL)) AS bid_eq_imp_only,
  COUNTIF((s3_bid_ip != prior_vast_start_ip OR prior_vast_start_ip IS NULL) AND (s3_bid_ip != prior_vast_impression_ip OR prior_vast_impression_ip IS NULL)) AS bid_eq_neither
FROM pairs
WHERE rn = 1
