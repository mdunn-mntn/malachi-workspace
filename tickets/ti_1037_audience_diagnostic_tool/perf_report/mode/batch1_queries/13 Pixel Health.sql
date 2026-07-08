-- Module 13 -- PIXEL HEALTH (monthly, ADVERTISER-level): detect advertiser-side
-- tag/pixel changes from the receiving side. No MNTN table tracks their tag manager
-- (confirmed by Kevin Cipriani 2026-07-08) -- but every fire lands in conversion_log
-- with its raw payload, so changes are reconstructed from shape:
--   px_new_types    -> conversion types first seen that month (registry create_time
--                      = first-ever fire; '-100'/'-101' untyped sentinels excluded)
--   px_n_amt/rows   -> order_amt coverage (collapsing = amount param broke/removed)
--   px_sum_amt      -> vs px_n_amt: equal = $1-placeholder amounts (the WGU pattern)
--   px_rows         -> raw fire volume; steps = tag firing scope changed, independent
--                      of MNTN delivery/spend
-- Pixels are advertiser-level (not per-campaign). Silver conversion_log retains
-- ~2024-01+. Playbook: data_knowledge.md "Conversion pixel payload anatomy".
-- Dynamic param defaults (Mode date params are static-only, so sentinels map in SQL):
--   Period_Start = 1900-01-01 (the default) -> Jan 1 of the CURRENT year; any other date honored.
--   Period_End is CLAMPED to the first day of the current month (exclusive end ->
--   data through the last FULL month); the far-future default (2099-01-01) relies on this.
WITH monthly AS (
  SELECT
    FORMAT_DATE('%Y-%m', DATE(time)) AS mo,
    COUNT(*) AS rows_n,
    COUNT(DISTINCT ip) AS ips,
    COUNTIF(order_amt IS NOT NULL) AS n_amt,
    ROUND(SUM(order_amt), 0) AS sum_amt,
    COUNT(DISTINCT IFNULL(conversion_type, '<NULL>')) AS n_types
  FROM `dw-main-silver.logdata.conversion_log`
  WHERE advertiser_id = {{ Advertiser_ID }}
    AND time >= TIMESTAMP(DATE_SUB(IF(DATE(LEFT('{{ Period_Start }}', 10)) = DATE '1900-01-01', DATE_TRUNC(CURRENT_DATE(), YEAR), DATE(LEFT('{{ Period_Start }}', 10))), INTERVAL 1 YEAR))
    AND time <  TIMESTAMP(LEAST(DATE(LEFT('{{ Period_End }}', 10)), DATE_TRUNC(CURRENT_DATE(), MONTH)))
  GROUP BY 1
),
new_types AS (
  SELECT
    FORMAT_DATE('%Y-%m', DATE(create_time)) AS mo,
    COUNT(*) AS new_types,
    STRING_AGG(SUBSTR(conversion_type, 1, 24), ', ' ORDER BY create_time LIMIT 3) AS types_added
  FROM `dw-main-bronze.integrationprod.core_advertiser_conversion_types`
  WHERE advertiser_id = {{ Advertiser_ID }}
    AND conversion_type NOT IN ('-100', '-101')
  GROUP BY 1
)
SELECT
  m.mo AS px_mo,
  m.rows_n AS px_rows,
  m.ips AS px_ips,
  m.n_amt AS px_n_amt,
  m.sum_amt AS px_sum_amt,
  m.n_types AS px_n_types,
  IFNULL(t.new_types, 0) AS px_new_types,
  t.types_added AS px_types_added,
  DATE_SUB(IF(DATE(LEFT('{{ Period_Start }}', 10)) = DATE '1900-01-01', DATE_TRUNC(CURRENT_DATE(), YEAR), DATE(LEFT('{{ Period_Start }}', 10))), INTERVAL 1 YEAR) AS p1_start,
  DATE_SUB(LEAST(DATE(LEFT('{{ Period_End }}', 10)), DATE_TRUNC(CURRENT_DATE(), MONTH)), INTERVAL 1 YEAR) AS p1_end,
  IF(DATE(LEFT('{{ Period_Start }}', 10)) = DATE '1900-01-01', DATE_TRUNC(CURRENT_DATE(), YEAR), DATE(LEFT('{{ Period_Start }}', 10))) AS p2_start,
  LEAST(DATE(LEFT('{{ Period_End }}', 10)), DATE_TRUNC(CURRENT_DATE(), MONTH)) AS p2_end
FROM monthly m
LEFT JOIN new_types t ON t.mo = m.mo
ORDER BY px_mo
