-- Kill-check: does PR bae-sql-utility#24 reference columns that exist?
-- PR #24 uses ats.translation_date / gts.translation_date in 5 positions.
-- Result 2026-08-17: the column is translation_timestamp on both signals. PR #24 cannot compile.
SELECT table_name, STRING_AGG(column_name, ", " ORDER BY ordinal_position) AS cols
FROM `dw-main-silver.identity.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name LIKE "%translation%" OR table_name LIKE "crm_audience%"
GROUP BY table_name
ORDER BY table_name
LIMIT 50;
