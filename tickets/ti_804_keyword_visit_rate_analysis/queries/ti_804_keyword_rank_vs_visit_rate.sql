-- TI-804: Per-keyword rank vs visit rate (for scatter plot)
-- Shows the relationship between BUK rank and actual visit rate
-- Aggregated across advertisers by rank bucket

WITH sample_advs AS (
  SELECT DISTINCT advertiser_id
  FROM `dw-main-silver.logdata.buk_predictions_20260316`
  ORDER BY FARM_FINGERPRINT(CAST(advertiser_id AS STRING))
  LIMIT 50
),

preds AS (
  SELECT p.advertiser_id, p.data_source_category_id, p.rank
  FROM `dw-main-silver.logdata.buk_predictions_20260316` p
  JOIN sample_advs sa USING (advertiser_id)
),

pred_dscids AS (
  SELECT DISTINCT data_source_category_id FROM preds
),

ip_keywords AS (
  SELECT DISTINCT ip, dscid.element AS dscid
  FROM `dw-main-bronze.external.ipdsc__v1`,
    UNNEST(data_source_category_ids.list) AS dscid
  WHERE data_source_id = 19
    AND dt BETWEEN '2026-03-01' AND '2026-03-15'
    AND dscid.element IN (SELECT data_source_category_id FROM pred_dscids)
),

visits AS (
  SELECT advertiser_id, impression_ip AS ip
  FROM `dw-main-silver.summarydata.ui_visits`
  WHERE DATE(time) BETWEEN '2026-03-16' AND '2026-03-26'
    AND advertiser_id IN (SELECT advertiser_id FROM sample_advs)
  GROUP BY 1, 2
),

per_keyword AS (
  SELECT
    p.advertiser_id,
    p.data_source_category_id,
    p.rank,
    COUNT(DISTINCT ik.ip) AS ips_with_keyword,
    COUNT(DISTINCT CASE WHEN v.ip IS NOT NULL THEN ik.ip END) AS ips_who_visited,
    SAFE_DIVIDE(
      COUNT(DISTINCT CASE WHEN v.ip IS NOT NULL THEN ik.ip END),
      COUNT(DISTINCT ik.ip)
    ) AS keyword_visit_rate
  FROM preds p
  JOIN ip_keywords ik ON p.data_source_category_id = ik.dscid
  LEFT JOIN visits v ON p.advertiser_id = v.advertiser_id AND ik.ip = v.ip
  GROUP BY 1, 2, 3
),

rank_buckets AS (
  SELECT
    CASE
      WHEN rank <= 5 THEN 'rank_01_05'
      WHEN rank <= 10 THEN 'rank_06_10'
      WHEN rank <= 20 THEN 'rank_11_20'
      WHEN rank <= 30 THEN 'rank_21_30'
      WHEN rank <= 50 THEN 'rank_31_50'
      ELSE 'rank_51_plus'
    END AS rank_bucket,
    rank,
    keyword_visit_rate,
    ips_with_keyword,
    ips_who_visited
  FROM per_keyword
)

SELECT
  rank_bucket,
  COUNT(*) AS n_pairs,
  AVG(keyword_visit_rate) AS avg_visit_rate,
  APPROX_QUANTILES(keyword_visit_rate, 4)[OFFSET(2)] AS median_visit_rate,
  SUM(ips_who_visited) AS total_visitors,
  SUM(ips_with_keyword) AS total_ips,
  SAFE_DIVIDE(SUM(ips_who_visited), SUM(ips_with_keyword)) AS pooled_visit_rate
FROM rank_buckets
GROUP BY 1
ORDER BY 1
