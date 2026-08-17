-- Do the umbrella signal views union anything beyond CRM?
-- Result 2026-08-17: no. Both are SELECT * over a single sqlmesh CRM table, so the
-- log_translation output from airflow-ti #1200/#1201 has no reader.
SELECT table_name, table_type, ddl
FROM `dw-main-silver.identity.INFORMATION_SCHEMA.TABLES`
WHERE table_name IN ("auction_translation_signal","graph_translation_signal",
                     "auction_translation_crm","graph_translation_crm")
LIMIT 10;
