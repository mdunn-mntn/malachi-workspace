-- ti_1313_window_sensitivity.sql: pooled prospecting lift under three candidate windows.
-- Entry-cohort anchored per data_catalog.md; the first window day is dropped as a left-censored stock.
-- Shows that the trailing 30 days sits at the depleted end of the table, not that it is more current.

WITH e AS (
  SELECT dt, campaign_group_id, ip, arm, visited,
         ROW_NUMBER() OVER (PARTITION BY advertiser_id, campaign_group_id, ip ORDER BY dt) AS rn
  FROM `dw-main-silver.enriched.lift__ghost_bid_visits`
  WHERE dt BETWEEN '2026-06-22' AND '2026-09-01'
    AND partner_id = 8
),
entry AS (
  SELECT * FROM e WHERE rn = 1 AND dt > '2026-06-22'
),
w AS (
  SELECT 'Documented clean band, 23 Jun to 7 Jul' AS window_label, 1 AS ord, * FROM entry
   WHERE dt BETWEEN '2026-06-23' AND '2026-07-07'
  UNION ALL
  SELECT 'Full span, 23 Jun to 1 Sep', 2, * FROM entry
  UNION ALL
  SELECT 'Trailing 30 days, 27 Jul to 25 Aug', 3, * FROM entry
   WHERE dt BETWEEN '2026-07-27' AND '2026-08-25'
),
per_cg AS (
  SELECT window_label, ord, campaign_group_id,
         COUNTIF(arm = 'submitted') AS n_t,
         COUNTIF(arm = 'ghost') AS n_h,
         COUNTIF(arm = 'submitted' AND visited) AS v_t,
         COUNTIF(arm = 'ghost' AND visited) AS v_h
  FROM w GROUP BY 1,2,3
)
SELECT
  window_label, ord,
  COUNT(*) AS campaign_groups,
  COUNTIF(v_h >= 100) AS powered_campaign_groups,
  COUNTIF(v_h >= 100 AND SAFE_DIVIDE(n_h, n_t + n_h) BETWEEN 0.09 AND 0.11) AS powered_and_in_band,
  SAFE_DIVIDE(SUM(n_h), SUM(n_t) + SUM(n_h)) AS ghost_frac_all,
  SAFE_DIVIDE(SAFE_DIVIDE(SUM(v_t), SUM(n_t)), NULLIF(SAFE_DIVIDE(SUM(v_h), SUM(n_h)), 0)) - 1 AS lift_all,
  SAFE_DIVIDE(SUM(IF(v_h >= 100, n_h, 0)), SUM(IF(v_h >= 100, n_t + n_h, 0))) AS ghost_frac_powered,
  SAFE_DIVIDE(
    SAFE_DIVIDE(SUM(IF(v_h >= 100, v_t, 0)), SUM(IF(v_h >= 100, n_t, 0))),
    NULLIF(SAFE_DIVIDE(SUM(IF(v_h >= 100, v_h, 0)), SUM(IF(v_h >= 100, n_h, 0))), 0)) - 1 AS lift_powered,
  SAFE_DIVIDE(
    SAFE_DIVIDE(SUM(IF(in_band, v_t, 0)), SUM(IF(in_band, n_t, 0))),
    NULLIF(SAFE_DIVIDE(SUM(IF(in_band, v_h, 0)), SUM(IF(in_band, n_h, 0))), 0)) - 1 AS lift_powered_in_band
FROM (SELECT *, (v_h >= 100 AND SAFE_DIVIDE(n_h, n_t + n_h) BETWEEN 0.09 AND 0.11) AS in_band FROM per_cg)
GROUP BY 1,2
ORDER BY ord
