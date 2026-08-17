-- Scale anchor: what deepsync bills today through the legacy DS4 path.
-- Result 2026-08-17: Jan-Jul 2026 = $22,379.79 (~$38K/yr run rate).
-- 2026-07 $2,102.61 | 06 $930.04 | 05 $2,644.31 | 04 $8,886.14 | 03 $4,432.91 | 02 $1,862.30 | 01 $1,521.48
-- No DS63 rows yet: DS63 crediting has never been billed.
SELECT dt, data_source_id, ROUND(SUM(impressions)) AS impressions, ROUND(SUM(usage),2) AS usd
FROM `dw-main-bronze.coredw.usage_reporting_data`
WHERE data_source_id IN (29,4,63) AND dt >= DATE("2026-01-01")
GROUP BY 1,2
ORDER BY dt DESC, data_source_id
LIMIT 40;
