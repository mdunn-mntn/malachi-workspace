SELECT
  FARM_FINGERPRINT(CONCAT(
    (SELECT CAST(MAX(last_modified_time) AS STRING)
     FROM `dw-main-silver.sqlmesh__summarydata.INFORMATION_SCHEMA.PARTITIONS`
     WHERE REGEXP_CONTAINS(table_name, r'^summarydata__all_facts__[0-9]+$')),
    '|',
    (SELECT CAST(FARM_FINGERPRINT(STRING_AGG(CONCAT(CAST(campaign_group_id AS STRING), ':', IFNULL(CAST(flight_start_time_local AS STRING), 'null')), ',' ORDER BY campaign_group_id, flight_start_time_local)) AS STRING)
     FROM `dw-main-gold.dso.campaign_group_flight`),
    '|',
    CAST(CURRENT_DATE() AS STRING)
  )) AS fingerprint,
  (SELECT MAX(last_modified_time) FROM `dw-main-silver.sqlmesh__summarydata.INFORMATION_SCHEMA.PARTITIONS` WHERE REGEXP_CONTAINS(table_name, r'^summarydata__all_facts__[0-9]+$')) AS all_facts_last_modified,
  (SELECT COUNT(*) FROM `dw-main-gold.dso.campaign_group_flight`) AS flight_rows
LIMIT 1
