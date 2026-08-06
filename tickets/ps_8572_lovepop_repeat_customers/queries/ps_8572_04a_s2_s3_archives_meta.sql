/* PS-8572 check 4a — step 1: archive version metadata for S2/S3 (614191, 614192), NO expression column
   (42 GB table, unpartitioned, clustered on audience_segment_archive_id — metadata first, then
   fetch expressions by clustered key IN-list). */
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
WHERE campaign_id IN (614191, 614192)
ORDER BY campaign_id, update_time
LIMIT 400
