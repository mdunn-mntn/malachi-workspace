-- TI-999: empirical classification of every data_source_id by IPDSC volume + category count.
-- Purpose: produce the operational "interest segment" DS set, data-driven.
-- Definition of interest segment (operational): bought third-party with material IPDSC volume.
-- One day of ipdsc__v1 (2026-05-26) gives a clean read on which DSes actively carry IPs.

SELECT
  ds.data_source_id,
  ds.name,
  COALESCE(ipdsc.n_ipdsc_rows_1d, 0)        AS n_ipdsc_rows_1d_2026_05_26,
  COALESCE(cats.n_active_categories, 0)     AS n_active_categories,
  COALESCE(cats.n_active_stale_gt_365, 0)   AS n_active_stale_gt_365
FROM (
  SELECT data_source_id, name
  FROM `dw-main-bronze.integrationprod.data_sources`
  WHERE data_source_id BETWEEN -1 AND 100
) ds
LEFT JOIN (
  SELECT data_source_id, COUNT(*) AS n_ipdsc_rows_1d
  FROM `dw-main-bronze.external.ipdsc__v1`
  WHERE dt = '2026-05-26'
  GROUP BY data_source_id
) ipdsc USING (data_source_id)
LEFT JOIN (
  SELECT
    data_source_id,
    SUM(IF(deprecated, 0, 1))                                                     AS n_active_categories,
    SUM(IF(NOT deprecated AND DATE_DIFF(CURRENT_DATE(), updated_date, DAY) > 365,
           1, 0))                                                                 AS n_active_stale_gt_365
  FROM `dw-main-bronze.tpa.categories`
  WHERE data_source_id BETWEEN -1 AND 100
  GROUP BY data_source_id
) cats USING (data_source_id)
ORDER BY ds.data_source_id;
