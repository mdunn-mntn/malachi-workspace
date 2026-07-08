-- Module 13 -- PIXEL HEALTH (monthly, ADVERTISER-level): detect advertiser-side
-- tag/pixel changes from the receiving side. No MNTN table tracks their tag manager
-- (confirmed by Kevin Cipriani 2026-07-08) -- but every fire lands in conversion_log
-- with its raw payload, so changes are reconstructed from shape:
--   px_new_types    -> conversion types first seen that month (registry create_time
--                      = first-ever fire; ALL bare-negative platform sentinels excluded
--                      via regex -- the registry has six, not two: -100/-101/-102/-105/-106/-107)
--   px_n_amt/rows   -> order_amt coverage (collapsing = amount param broke/removed)
--   px_sum_amt      -> vs px_n_amt: equal = $1-placeholder amounts (the WGU pattern);
--                      a lone huge sum = amount injection (Feb'26 WGU pentest: $222.9M)
--   px_rows         -> raw fire volume; steps = tag firing scope changed, independent
--                      of MNTN delivery/spend. Month spine emits ZERO rows for
--                      zero-fire months so a complete pixel death fails CLOSED.
-- SCOPE CAVEATS: source is SILVER conversion_log (corrupt amounts >= ~$100M arrive
-- NULLed), advertiser-scoped (fires tagged to a dead/legacy AID -- e.g. WGU's 10942
-- lead pixel -- are invisible by design), floored ~2024-01. Scan bounds are
-- month-truncated so partial user-picked months can't fabricate volume steps.
-- Pixels are advertiser-level (not per-campaign). Playbook: data_knowledge.md
-- "Conversion pixel payload anatomy" / "Detecting an advertiser pixel/tag change".
-- Param defaults are real dates set in Mode (start = Jan 1 of current year, end =
-- next Jan 1); the SQL still maps the 1900-01-01 sentinel and clamps Period_End to
-- the first of the current month (exclusive end -> through the last FULL month).
WITH months AS (
  SELECT FORMAT_DATE('%Y-%m', m) AS mo
  FROM UNNEST(GENERATE_DATE_ARRAY(
    DATE_TRUNC(DATE_SUB(IF(DATE(LEFT('{{ Period_Start }}', 10)) = DATE '1900-01-01', DATE_TRUNC(CURRENT_DATE(), YEAR), DATE(LEFT('{{ Period_Start }}', 10))), INTERVAL 1 YEAR), MONTH),
    DATE_SUB(DATE_TRUNC(LEAST(DATE(LEFT('{{ Period_End }}', 10)), DATE_TRUNC(CURRENT_DATE(), MONTH)), MONTH), INTERVAL 1 MONTH),
    INTERVAL 1 MONTH)) AS m
),
monthly AS (
  SELECT
    FORMAT_DATE('%Y-%m', DATE(time)) AS mo,
    COUNT(*) AS rows_n,
    COUNT(DISTINCT ip) AS ips,
    COUNTIF(order_amt IS NOT NULL) AS n_amt,
    ROUND(SUM(order_amt), 0) AS sum_amt,
    COUNT(DISTINCT IFNULL(conversion_type, '<NULL>')) AS n_types
  FROM `dw-main-silver.logdata.conversion_log`
  WHERE advertiser_id = {{ Advertiser_ID }}
    AND time >= TIMESTAMP(DATE_TRUNC(DATE_SUB(IF(DATE(LEFT('{{ Period_Start }}', 10)) = DATE '1900-01-01', DATE_TRUNC(CURRENT_DATE(), YEAR), DATE(LEFT('{{ Period_Start }}', 10))), INTERVAL 1 YEAR), MONTH))
    AND time <  TIMESTAMP(DATE_TRUNC(LEAST(DATE(LEFT('{{ Period_End }}', 10)), DATE_TRUNC(CURRENT_DATE(), MONTH)), MONTH))
  GROUP BY 1
),
new_types AS (
  SELECT
    FORMAT_DATE('%Y-%m', DATE(create_time)) AS mo,
    COUNT(*) AS new_types,
    STRING_AGG(SUBSTR(conversion_type, 1, 24), ', ' ORDER BY create_time LIMIT 3) AS types_added
  FROM `dw-main-bronze.integrationprod.core_advertiser_conversion_types`
  WHERE advertiser_id = {{ Advertiser_ID }}
    -- bare-negative integers = platform pseudo-types (-100/-101/-102/-105/-106/-107),
    -- auto-created per source migration, never a client tag change. Regex, not a list:
    -- verified registry-wide it matches exactly those six and no client/pentest string.
    AND NOT REGEXP_CONTAINS(conversion_type, r'^-[0-9]+$')
  GROUP BY 1
)
SELECT
  s.mo AS px_mo,
  IFNULL(m.rows_n, 0) AS px_rows,
  IFNULL(m.ips, 0) AS px_ips,
  IFNULL(m.n_amt, 0) AS px_n_amt,
  m.sum_amt AS px_sum_amt,
  IFNULL(m.n_types, 0) AS px_n_types,
  IFNULL(t.new_types, 0) AS px_new_types,
  t.types_added AS px_types_added,
  DATE_SUB(IF(DATE(LEFT('{{ Period_Start }}', 10)) = DATE '1900-01-01', DATE_TRUNC(CURRENT_DATE(), YEAR), DATE(LEFT('{{ Period_Start }}', 10))), INTERVAL 1 YEAR) AS p1_start,
  DATE_SUB(LEAST(DATE(LEFT('{{ Period_End }}', 10)), DATE_TRUNC(CURRENT_DATE(), MONTH)), INTERVAL 1 YEAR) AS p1_end,
  IF(DATE(LEFT('{{ Period_Start }}', 10)) = DATE '1900-01-01', DATE_TRUNC(CURRENT_DATE(), YEAR), DATE(LEFT('{{ Period_Start }}', 10))) AS p2_start,
  LEAST(DATE(LEFT('{{ Period_End }}', 10)), DATE_TRUNC(CURRENT_DATE(), MONTH)) AS p2_end
FROM months s
LEFT JOIN monthly m ON m.mo = s.mo
LEFT JOIN new_types t ON t.mo = s.mo
ORDER BY px_mo
