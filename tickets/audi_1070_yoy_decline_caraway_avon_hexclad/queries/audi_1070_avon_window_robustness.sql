-- AUDI-1070 — Avon (31921) YoY is window-INDEPENDENT. Compare across full calendar year,
-- trailing-12-month (TTM, no seasonality / no month-picking), H2 (Jul-Dec, the "after July" window),
-- and Jan-May. ROAS rises in EVERY window. periods JOIN base lets a day fall into multiple periods.
WITH base AS (
  SELECT day, media_spend+platform_spend+data_spend AS spend, impressions,
    views+clicks AS visits, view_conversions+click_conversions AS conv,
    view_order_value+click_order_value AS rev, uniques
  FROM `dw-main-silver.summarydata.sum_by_advertiser_by_day`
  WHERE advertiser_id=31921 AND day >= '2024-01-01'
),
periods AS (
  SELECT 'CY2024' p, DATE'2024-01-01' s, DATE'2024-12-31' e UNION ALL
  SELECT 'CY2025','2025-01-01','2025-12-31' UNION ALL
  SELECT 'TTM_end_2025_05','2024-06-01','2025-05-31' UNION ALL
  SELECT 'TTM_end_2026_05','2025-06-01','2026-05-31' UNION ALL
  SELECT 'H2_2024_JulDec','2024-07-01','2024-12-31' UNION ALL
  SELECT 'H2_2025_JulDec','2025-07-01','2025-12-31' UNION ALL
  SELECT 'JanMay_2025','2025-01-01','2025-05-31' UNION ALL
  SELECT 'JanMay_2026','2026-01-01','2026-05-31'
)
SELECT p.p AS period, ROUND(SUM(b.spend)) spend, SUM(b.impressions) impr,
  HLL_COUNT.MERGE(b.uniques) reach, SUM(b.visits) visits, SUM(b.conv) conv, ROUND(SUM(b.rev)) rev,
  ROUND(SAFE_DIVIDE(SUM(b.rev),SUM(b.spend)),2) roas,
  ROUND(100*SAFE_DIVIDE(SUM(b.visits),SUM(b.impressions)),3) vr_pct,
  ROUND(100*SAFE_DIVIDE(SUM(b.conv),SUM(b.visits)),3) cvr_pct
FROM periods p JOIN base b ON b.day BETWEEN p.s AND p.e
GROUP BY period ORDER BY period;
