WITH cil AS (
  SELECT
    DATE(time) AS dt,
    impression_id,
    advertiser_household_score AS hhst
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE DATE(time) BETWEEN '2026-05-01' AND CURRENT_DATE()
    AND campaign_id = 570106
),
banded AS (
  SELECT
    dt,
    CASE
      WHEN hhst = -1 THEN 'a_unscored_neg1'
      WHEN hhst = 0  THEN 'b_hhst_0'
      WHEN hhst BETWEEN 1     AND 3332 THEN 'c_mr_pp_band_1_3332'
      WHEN hhst BETWEEN 3333  AND 6665 THEN 'd_mi_band_3333_6665'
      WHEN hhst BETWEEN 6666  AND 9999 THEN 'e_hi_band_6666_9999'
      WHEN hhst = 10000 THEN 'f_hhst_10000'
      ELSE 'z_other'
    END AS hhst_band,
    impression_id
  FROM cil
),
imps AS (
  SELECT dt, hhst_band, COUNT(*) AS impressions
  FROM banded GROUP BY dt, hhst_band
),
daily_totals AS (
  SELECT dt, SUM(impressions) AS total_imps FROM imps GROUP BY dt
)
SELECT
  i.dt,
  i.hhst_band,
  i.impressions,
  ROUND(i.impressions * 100.0 / NULLIF(d.total_imps,0), 2) AS pct_imps,
  d.total_imps
FROM imps i
JOIN daily_totals d USING (dt)
ORDER BY dt, hhst_band
