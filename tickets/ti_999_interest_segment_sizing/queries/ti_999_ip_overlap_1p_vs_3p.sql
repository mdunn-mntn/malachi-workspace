-- TI-999: global IP overlap between 1P-CRM and 3P-interest universes.
-- One day's ipdsc snapshot to bound the universe.
-- This measures: of the IPs in DS4 (CRM upload), what fraction also appear in DS17/18/35 (3P interest)?

WITH per_ip AS (
  SELECT
    ip,
    LOGICAL_OR(data_source_id = 4)             AS in_1p_crm,
    LOGICAL_OR(data_source_id = 17)            AS in_sharethis,
    LOGICAL_OR(data_source_id = 18)            AS in_dstillery,
    LOGICAL_OR(data_source_id = 35)            AS in_liveramp,
    LOGICAL_OR(data_source_id IN (17, 18, 35)) AS in_3p
  FROM `dw-main-bronze.external.ipdsc__v1`
  WHERE dt = '2026-05-26'
    AND data_source_id IN (4, 17, 18, 35)
  GROUP BY ip
)
SELECT
  COUNT(*)                                                 AS total_ips_in_universe,
  COUNTIF(in_1p_crm)                                       AS n_ips_1p_crm,
  COUNTIF(in_3p)                                           AS n_ips_3p_any,
  COUNTIF(in_liveramp)                                     AS n_ips_liveramp,
  COUNTIF(in_sharethis)                                    AS n_ips_sharethis,
  COUNTIF(in_dstillery)                                    AS n_ips_dstillery,
  COUNTIF(in_1p_crm AND in_3p)                             AS n_ips_1p_and_3p,
  COUNTIF(in_1p_crm AND NOT in_3p)                         AS n_ips_1p_only,
  COUNTIF(NOT in_1p_crm AND in_3p)                         AS n_ips_3p_only,
  ROUND(100.0 * COUNTIF(in_1p_crm AND in_3p) /
                NULLIF(COUNTIF(in_1p_crm), 0), 1)          AS pct_1p_overlapping_3p,
  ROUND(100.0 * COUNTIF(in_1p_crm AND in_3p) /
                NULLIF(COUNTIF(in_3p), 0), 1)              AS pct_3p_overlapping_1p,
  ROUND(100.0 * COUNTIF(in_1p_crm AND in_3p) /
                NULLIF(COUNTIF(in_1p_crm OR in_3p), 0), 1) AS jaccard_pct
FROM per_ip;
