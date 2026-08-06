/* PS-8572 check 1c — step 1: archive version metadata for campaign 614193, NO expression column
   (table is 42 GB, ~40 GB of it the expression column; unpartitioned, clustered on
   audience_segment_archive_id — so metadata first, then fetch expressions by clustered key). */
SELECT
  audience_segment_archive_id,
  audience_segment_id,
  audience_id,
  campaign_id,
  segment_id,
  expression_type_id,
  is_targeted,
  version,
  create_time,
  update_time
FROM `dw-main-silver.archives.audience_segment_archives`
WHERE campaign_id = 614193
ORDER BY update_time
LIMIT 200
