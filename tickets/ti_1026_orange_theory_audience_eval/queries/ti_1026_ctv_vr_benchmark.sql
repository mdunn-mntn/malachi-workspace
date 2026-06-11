WITH camp AS (
  SELECT s.campaign_id,
    SUM(s.impressions) AS imps,
    SUM(s.video_impressions) AS vid,
    HLL_COUNT.MERGE(s.site_visitors) AS visitors
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` s
  JOIN (SELECT DISTINCT campaign_id FROM `dw-main-silver.dso.household_score_thresholds` WHERE threshold > 0) h
    ON s.campaign_id = h.campaign_id
  WHERE s.day >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  GROUP BY s.campaign_id
  HAVING imps >= 100000 AND SAFE_DIVIDE(vid, imps) >= 0.95
),
vr AS (SELECT campaign_id, SAFE_DIVIDE(visitors, imps) AS visit_rate FROM camp)
SELECT
  (SELECT COUNT(*) FROM vr) AS n_peer_campaigns,
  ROUND(APPROX_QUANTILES(visit_rate, 100)[OFFSET(10)] * 100, 4) AS p10,
  ROUND(APPROX_QUANTILES(visit_rate, 100)[OFFSET(25)] * 100, 4) AS p25,
  ROUND(APPROX_QUANTILES(visit_rate, 100)[OFFSET(50)] * 100, 4) AS median,
  ROUND(APPROX_QUANTILES(visit_rate, 100)[OFFSET(75)] * 100, 4) AS p75,
  ROUND(APPROX_QUANTILES(visit_rate, 100)[OFFSET(90)] * 100, 4) AS p90,
  ROUND((SELECT visit_rate FROM vr WHERE campaign_id=319137) * 100, 4) AS otf_319137_vr_pct,
  (SELECT COUNTIF(visit_rate <= (SELECT visit_rate FROM vr WHERE campaign_id=319137)) FROM vr) AS n_peers_at_or_below_otf
FROM vr
