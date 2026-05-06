/*
  TI-933 Phase 2: validate that Select bids flow through augmentor_log.
  augmentor_log has no advertiser_id/campaign_id, so we test biddability via
  IP intersection: for one recent day, overlap Select-served IPs (cost_impression_log)
  with augmentor IPs. >50% overlap means the biddability filter applies to Select.
*/
WITH select_groups AS (
  SELECT campaign_group_id
  FROM `dw-main-bronze.integrationprod.campaign_groups`
  WHERE product_id = 2 AND deleted = FALSE AND is_test = FALSE
),
select_campaigns AS (
  SELECT c.campaign_id
  FROM `dw-main-bronze.integrationprod.campaigns` c
  INNER JOIN select_groups USING (campaign_group_id)
  WHERE c.deleted = FALSE AND c.is_test = FALSE
),
select_ips AS (
  SELECT DISTINCT ci.ip
  FROM `dw-main-silver.logdata.cost_impression_log` ci
  INNER JOIN select_campaigns USING (campaign_id)
  WHERE DATE(ci.time) = DATE '2026-05-04'
    AND ci.ip IS NOT NULL AND ci.ip != '0.0.0.0'
),
aug AS (
  SELECT DISTINCT ip
  FROM `dw-main-bronze.raw.augmentor_log`
  WHERE DATE(time) = DATE '2026-05-04'
    AND ip IS NOT NULL AND ip != '0.0.0.0'
)
SELECT
  (SELECT COUNT(*) FROM select_ips)                                          AS select_ips,
  (SELECT COUNT(*) FROM aug)                                                 AS augmentor_ips,
  (SELECT COUNT(*) FROM select_ips s INNER JOIN aug a USING (ip))            AS overlap_ips,
  ROUND(SAFE_DIVIDE(
    (SELECT COUNT(*) FROM select_ips s INNER JOIN aug a USING (ip)),
    (SELECT COUNT(*) FROM select_ips)
  ) * 100, 2)                                                                AS select_in_augmentor_pct;
