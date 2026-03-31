-- TI-797: DCG IP-level scoring — BQ replication of Alex's ti_620_dcg_logic.py
-- Replicates the Databricks DCG scoring pipeline in BigQuery
-- Sample: 50 advertisers, 30-day ipdsc window, then validate with 10-day visit post-period
--
-- Data sources:
--   Predictions: gs://targeting-infra-vertex-pipelines-prod/bottom-up-keywords/batch-predictions/dt=2026-03-16/
--   IPDSC DS19:  dw-main-bronze.external.ipdsc__v1 (30-day window before 2026-03-16)
--   Visits:      dw-main-silver.summarydata.ui_visits (10-day post-period after 2026-03-16)
--
-- DCG formula: discount = 1 / log2(rank + 1)
-- Adjusted score: 1 - exp(-beta * sum(discounts))
-- Beta = 1.8634 (calibrated so p90 of DCG → 0.9 adjusted score)

-- Step 1: Sample 50 advertisers deterministically
WITH sample_advs AS (
  SELECT DISTINCT advertiser_id
  FROM `dw-main-silver.logdata.buk_predictions_20260316`
  ORDER BY FARM_FINGERPRINT(CAST(advertiser_id AS STRING))
  LIMIT 50
),

-- Step 2: BUK predictions with DCG discount
preds AS (
  SELECT
    p.advertiser_id,
    p.data_source_category_id,
    p.rank,
    1.0 / LOG(p.rank + 1, 2) AS discount
  FROM `dw-main-silver.logdata.buk_predictions_20260316` p
  JOIN sample_advs sa USING (advertiser_id)
),

-- Step 3: Predicted DSCIDs (for ipdsc filter)
pred_dscids AS (
  SELECT DISTINCT data_source_category_id FROM preds
),

-- Step 4: Distinct IP × DSCID from ipdsc DS19 (30-day window)
ip_keywords AS (
  SELECT DISTINCT
    ip,
    dscid.element AS dscid
  FROM `dw-main-bronze.external.ipdsc__v1`,
    UNNEST(data_source_category_ids.list) AS dscid
  WHERE data_source_id = 19
    AND dt BETWEEN '2026-02-15' AND '2026-03-15'
    AND dscid.element IN (SELECT data_source_category_id FROM pred_dscids)
),

-- Step 5: Join IPs with predictions → DCG per (advertiser, IP)
ip_adv_dcg AS (
  SELECT
    p.advertiser_id,
    ik.ip,
    SUM(p.discount) AS dcg,
    COUNT(*) AS n_hit_cats
  FROM ip_keywords ik
  JOIN preds p ON ik.dscid = p.data_source_category_id
  GROUP BY 1, 2
),

-- Step 6: Apply exponential normalization
ip_scores AS (
  SELECT
    advertiser_id,
    ip,
    dcg,
    n_hit_cats,
    1.0 - EXP(-1.8634 * dcg) AS adjusted_keyword_score
  FROM ip_adv_dcg
),

-- Step 7: Bin scores and compute visit rates
-- Get visits from ui_visits in 10-day post-period (2026-03-16 to 2026-03-26)
visits AS (
  SELECT
    c.advertiser_id,
    v.impression_ip AS ip,
    COUNT(*) AS visits
  FROM `dw-main-silver.summarydata.ui_visits` v
  JOIN `dw-main-bronze.integrationprod.campaigns` c
    ON v.advertiser_id = c.advertiser_id AND v.campaign_id = c.campaign_id
  WHERE DATE(v.time) BETWEEN '2026-03-16' AND '2026-03-26'
    AND c.advertiser_id IN (SELECT advertiser_id FROM sample_advs)
    AND c.deleted = FALSE
  GROUP BY 1, 2
),

-- Step 8: Join scores with visits
combined AS (
  SELECT
    s.advertiser_id,
    s.ip,
    s.adjusted_keyword_score AS score,
    COALESCE(v.visits, 0) AS visits,
    IF(COALESCE(v.visits, 0) > 0, 1, 0) AS visited_any
  FROM ip_scores s
  LEFT JOIN visits v ON s.advertiser_id = v.advertiser_id AND s.ip = v.ip
),

-- Step 9: Bin by score (0.05 width) and compute visit rate per bin
binned AS (
  SELECT
    LEAST(FLOOR(score / 0.05) * 0.05, 0.95) AS score_bin,
    LEAST(FLOOR(score / 0.05) * 0.05, 0.95) + 0.025 AS bin_center,
    COUNT(*) AS n_ips,
    SUM(visited_any) AS n_visitors,
    SAFE_DIVIDE(SUM(visited_any), COUNT(*)) AS visit_rate
  FROM combined
  GROUP BY 1, 2
)

SELECT
  score_bin,
  bin_center,
  n_ips,
  n_visitors,
  visit_rate,
  -- 95% CI
  visit_rate - 1.96 * SQRT(visit_rate * (1 - visit_rate) / n_ips) AS ci_lo,
  visit_rate + 1.96 * SQRT(visit_rate * (1 - visit_rate) / n_ips) AS ci_hi
FROM binned
ORDER BY score_bin
