-- TI-999: per-dscid quality proxy for DS17/18/35.
-- Stand-in for Alex's TI-956 scoring framework until it ships.
-- Uses: (a) days-since-update from tpa.categories, (b) IP volume from one day of ipdsc__v1.
-- Output: per (data_source_id, dscid), an activity rank within DS, plus freshness signal.

WITH
ipdsc_unnested AS (
  SELECT
    data_source_id,
    c.element AS dscid,
    ip
  FROM `dw-main-bronze.external.ipdsc__v1` t,
       UNNEST(t.data_source_category_ids.list) AS c
  WHERE dt = '2026-05-26'
    AND data_source_id IN (17, 18, 35)
),
dscid_ipcount AS (
  SELECT
    data_source_id,
    dscid,
    COUNT(DISTINCT ip) AS n_ips_1d
  FROM ipdsc_unnested
  GROUP BY data_source_id, dscid
),
cats AS (
  SELECT
    data_source_id,
    data_source_category_id AS dscid,
    DATE_DIFF(CURRENT_DATE(), COALESCE(updated_date, created_date), DAY) AS days_since_update,
    deprecated
  FROM `dw-main-bronze.tpa.categories`
  WHERE data_source_id IN (17, 18, 35)
    AND deprecated = FALSE
)
SELECT
  c.data_source_id,
  c.dscid,
  COALESCE(d.n_ips_1d, 0)                                                                          AS n_ips_1d,
  c.days_since_update,
  -- Rank within DS by activity (n_ips, descending).
  RANK() OVER (PARTITION BY c.data_source_id ORDER BY COALESCE(d.n_ips_1d, 0) DESC)                AS rank_by_activity,
  -- Rank within DS by freshness (lower days_since_update is better).
  RANK() OVER (PARTITION BY c.data_source_id ORDER BY c.days_since_update ASC NULLS LAST)          AS rank_by_freshness,
  -- Activity percentile (0-100 within DS, higher = better)
  ROUND(100.0 * (1.0 - PERCENT_RANK() OVER (PARTITION BY c.data_source_id ORDER BY COALESCE(d.n_ips_1d, 0) DESC)), 1) AS activity_pctile,
  -- DS-level total (for context)
  COUNT(*) OVER (PARTITION BY c.data_source_id) AS n_active_dscids_in_ds
FROM cats c
LEFT JOIN dscid_ipcount d
  ON c.data_source_id = d.data_source_id AND c.dscid = d.dscid;
