-- PS-8572 check 1b: freshness sanity on the archives table (all advertisers)
-- Confirms CDC replication is current, i.e. no unreplicated v8 for 58797.
SELECT
  MAX(create_time) AS max_archive_create_time,
  COUNT(*) AS rows_since_aug4
FROM `dw-main-bronze.integrationprod.archives_advertiser_configuration_archives`
WHERE create_time >= TIMESTAMP('2026-08-04')
