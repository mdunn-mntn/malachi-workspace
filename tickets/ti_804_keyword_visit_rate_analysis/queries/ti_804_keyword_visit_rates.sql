-- TI-804: Per-keyword visit rate analysis
-- Goal: Show that keyword selection matters — top BUK-ranked keywords
-- have dramatically higher visit rates than low-ranked ones.
--
-- Approach:
-- 1. For 50 sample advertisers, get BUK keyword rankings
-- 2. From ipdsc DS19, find IPs matched to each keyword (15-day window)
-- 3. From ui_visits, check which of those IPs visited the advertiser (10-day post-period)
-- 4. Compute per-keyword visit rate per advertiser
-- 5. Aggregate: top-10 vs middle vs bottom keywords by BUK rank

WITH sample_advs AS (
  SELECT DISTINCT advertiser_id
  FROM `dw-main-silver.logdata.buk_predictions_20260316`
  ORDER BY FARM_FINGERPRINT(CAST(advertiser_id AS STRING))
  LIMIT 50
),

preds AS (
  SELECT p.advertiser_id, p.data_source_category_id, p.product_category, p.rank,
    NTILE(3) OVER (PARTITION BY p.advertiser_id ORDER BY p.rank) AS rank_tier
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
    p.product_category,
    p.rank,
    p.rank_tier,
    COUNT(DISTINCT ik.ip) AS ips_with_keyword,
    COUNT(DISTINCT CASE WHEN v.ip IS NOT NULL THEN ik.ip END) AS ips_who_visited,
    SAFE_DIVIDE(
      COUNT(DISTINCT CASE WHEN v.ip IS NOT NULL THEN ik.ip END),
      COUNT(DISTINCT ik.ip)
    ) AS keyword_visit_rate
  FROM preds p
  JOIN ip_keywords ik ON p.data_source_category_id = ik.dscid
  LEFT JOIN visits v ON p.advertiser_id = v.advertiser_id AND ik.ip = v.ip
  GROUP BY 1, 2, 3, 4, 5
)

SELECT
  CASE rank_tier
    WHEN 1 THEN 'top_third'
    WHEN 2 THEN 'middle_third'
    WHEN 3 THEN 'bottom_third'
  END AS keyword_tier,
  COUNT(*) AS n_advertiser_keyword_pairs,
  AVG(keyword_visit_rate) AS avg_visit_rate,
  APPROX_QUANTILES(keyword_visit_rate, 4)[OFFSET(2)] AS median_visit_rate,
  MIN(keyword_visit_rate) AS min_visit_rate,
  MAX(keyword_visit_rate) AS max_visit_rate,
  SUM(ips_who_visited) AS total_visitors,
  SUM(ips_with_keyword) AS total_ips
FROM per_keyword
GROUP BY 1
ORDER BY 1
