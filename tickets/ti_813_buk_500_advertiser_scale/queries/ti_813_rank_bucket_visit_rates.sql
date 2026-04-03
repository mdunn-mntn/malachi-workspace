-- TI-813: Rank bucket visit rates — 500 advertisers
-- Scaled from TI-804 (50 advertisers)
-- Keywords: ipdsc DS19, 2026-03-01 to 2026-03-15
-- Outcomes: ui_visits, 2026-03-16 to 2026-03-26

WITH sample_advs AS (
  SELECT DISTINCT advertiser_id
  FROM `dw-main-silver.logdata.buk_predictions_20260316`
  ORDER BY FARM_FINGERPRINT(CAST(advertiser_id AS STRING))
  LIMIT 500
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

ip_best_rank AS (
  SELECT
    p.advertiser_id,
    ik.ip,
    MIN(p.rank) AS best_rank
  FROM preds p
  JOIN ip_keywords ik ON p.data_source_category_id = ik.dscid
  GROUP BY 1, 2
),

scored AS (
  SELECT
    ibr.advertiser_id,
    ibr.ip,
    ibr.best_rank,
    CASE
      WHEN ibr.best_rank <= 5 THEN 'rank_1-5'
      WHEN ibr.best_rank <= 10 THEN 'rank_6-10'
      WHEN ibr.best_rank <= 20 THEN 'rank_11-20'
      WHEN ibr.best_rank <= 30 THEN 'rank_21-30'
      WHEN ibr.best_rank <= 50 THEN 'rank_31-50'
      ELSE 'rank_51+'
    END AS rank_bucket,
    IF(v.ip IS NOT NULL, 1, 0) AS visited
  FROM ip_best_rank ibr
  LEFT JOIN visits v ON ibr.advertiser_id = v.advertiser_id AND ibr.ip = v.ip
)

SELECT
  rank_bucket,
  COUNT(*) AS n_ips,
  SUM(visited) AS n_visitors,
  SAFE_DIVIDE(SUM(visited), COUNT(*)) AS visit_rate,
  SAFE_DIVIDE(
    SAFE_DIVIDE(SUM(visited), COUNT(*)),
    (SELECT SAFE_DIVIDE(SUM(visited), COUNT(*)) FROM scored WHERE rank_bucket = 'rank_51+')
  ) AS lift_vs_worst
FROM scored
GROUP BY 1
ORDER BY 1
