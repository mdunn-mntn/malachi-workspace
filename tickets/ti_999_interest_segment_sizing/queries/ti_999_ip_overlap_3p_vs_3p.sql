-- TI-999: HONEST 3P-vs-3P IP overlap (LiveRamp vs ShareThis vs Dstillery).
-- Replaces the misleading CRM-vs-3P slide. These three are the actual catalog
-- segments offered to all advertisers; their pairwise overlap measures whether
-- buying multiple 3P providers brings incremental reach.

WITH per_ip AS (
  SELECT
    ip,
    LOGICAL_OR(data_source_id = 35) AS in_liveramp,
    LOGICAL_OR(data_source_id = 17) AS in_sharethis,
    LOGICAL_OR(data_source_id = 18) AS in_dstillery
  FROM `dw-main-bronze.external.ipdsc__v1`
  WHERE dt = '2026-05-26'
    AND data_source_id IN (17, 18, 35)
  GROUP BY ip
)
SELECT
  COUNT(*)                                                                       AS total_ips_3p_universe,
  COUNTIF(in_liveramp)                                                           AS n_liveramp,
  COUNTIF(in_sharethis)                                                          AS n_sharethis,
  COUNTIF(in_dstillery)                                                          AS n_dstillery,
  -- pairwise overlaps
  COUNTIF(in_liveramp AND in_sharethis)                                          AS n_lr_and_st,
  COUNTIF(in_liveramp AND in_dstillery)                                          AS n_lr_and_ds,
  COUNTIF(in_sharethis AND in_dstillery)                                         AS n_st_and_ds,
  -- exclusive
  COUNTIF(in_liveramp AND NOT in_sharethis AND NOT in_dstillery)                 AS n_liveramp_only,
  COUNTIF(in_sharethis AND NOT in_liveramp AND NOT in_dstillery)                 AS n_sharethis_only,
  COUNTIF(in_dstillery AND NOT in_liveramp AND NOT in_sharethis)                 AS n_dstillery_only,
  -- triple
  COUNTIF(in_liveramp AND in_sharethis AND in_dstillery)                         AS n_all_three,
  -- overlap rates
  ROUND(100.0 * COUNTIF(in_liveramp AND in_sharethis) /
                NULLIF(COUNTIF(in_sharethis), 0), 1)                             AS pct_sharethis_in_liveramp,
  ROUND(100.0 * COUNTIF(in_liveramp AND in_dstillery) /
                NULLIF(COUNTIF(in_dstillery), 0), 1)                             AS pct_dstillery_in_liveramp,
  ROUND(100.0 * COUNTIF(in_sharethis AND in_dstillery) /
                NULLIF(COUNTIF(in_dstillery), 0), 1)                             AS pct_dstillery_in_sharethis
FROM per_ip;
