WITH flips AS (
  SELECT
    advertiser_id,
    DATE(MIN(TIMESTAMP_MILLIS(datastream_metadata.source_timestamp)), "America/Los_Angeles") AS flip_date_pt
  FROM `dw-main-bronze.integrationprod.audience_advertiser_configurations`
  WHERE vertical_data_source = 46
  GROUP BY advertiser_id
),
recent_flips AS (
  SELECT *
  FROM flips
  WHERE flip_date_pt BETWEEN '2026-04-01' AND CURRENT_DATE()
),
cil AS (
  SELECT
    DATE(time) AS dt,
    CAST(advertiser_id AS INT64) AS advertiser_id,
    advertiser_household_score AS hhst,
    impression_id
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE DATE(time) BETWEEN '2026-04-01' AND CURRENT_DATE()
    AND CAST(advertiser_id AS INT64) IN (SELECT advertiser_id FROM recent_flips)
),
joined AS (
  SELECT
    c.advertiser_id,
    c.dt,
    f.flip_date_pt,
    DATE_DIFF(c.dt, f.flip_date_pt, DAY) AS days_from_flip,
    c.hhst,
    CASE
      WHEN c.hhst = -1 THEN 'unscored'
      WHEN c.hhst = 10000 THEN 'hhst_10000'
      WHEN c.hhst BETWEEN 1 AND 9999 THEN 'partial'
      ELSE 'other'
    END AS band
  FROM cil c
  JOIN recent_flips f USING (advertiser_id)
),
adv_pre AS (
  SELECT
    advertiser_id,
    flip_date_pt,
    COUNT(*) AS imps_pre,
    SUM(CASE WHEN band='hhst_10000' THEN 1 ELSE 0 END) AS hhst10k_pre,
    SUM(CASE WHEN band='unscored'   THEN 1 ELSE 0 END) AS unscored_pre
  FROM joined
  WHERE days_from_flip BETWEEN -7 AND -1
  GROUP BY advertiser_id, flip_date_pt
),
adv_post AS (
  SELECT
    advertiser_id,
    COUNT(*) AS imps_post,
    SUM(CASE WHEN band='hhst_10000' THEN 1 ELSE 0 END) AS hhst10k_post,
    SUM(CASE WHEN band='unscored'   THEN 1 ELSE 0 END) AS unscored_post
  FROM joined
  WHERE days_from_flip BETWEEN 1 AND 14
  GROUP BY advertiser_id
)
SELECT
  p.advertiser_id,
  p.flip_date_pt,
  p.imps_pre,
  COALESCE(po.imps_post, 0) AS imps_post,
  ROUND(p.hhst10k_pre   * 100.0 / NULLIF(p.imps_pre, 0),  1) AS pct_hhst10k_pre,
  ROUND(COALESCE(po.hhst10k_post, 0) * 100.0 / NULLIF(po.imps_post, 0), 1) AS pct_hhst10k_post,
  ROUND(p.unscored_pre  * 100.0 / NULLIF(p.imps_pre, 0),  1) AS pct_unscored_pre,
  ROUND(COALESCE(po.unscored_post, 0) * 100.0 / NULLIF(po.imps_post, 0), 1) AS pct_unscored_post,
  ROUND((COALESCE(po.hhst10k_post, 0)  * 100.0 / NULLIF(po.imps_post, 0))
      - (p.hhst10k_pre   * 100.0 / NULLIF(p.imps_pre, 0)), 1) AS hhst10k_delta_pp,
  ROUND((COALESCE(po.unscored_post, 0) * 100.0 / NULLIF(po.imps_post, 0))
      - (p.unscored_pre  * 100.0 / NULLIF(p.imps_pre, 0)), 1) AS unscored_delta_pp
FROM adv_pre p
LEFT JOIN adv_post po ON p.advertiser_id = po.advertiser_id
WHERE p.imps_pre > 1000 AND COALESCE(po.imps_post, 0) > 1000
ORDER BY hhst10k_delta_pp ASC
