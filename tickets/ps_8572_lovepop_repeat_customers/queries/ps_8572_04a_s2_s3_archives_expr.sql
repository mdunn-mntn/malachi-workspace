/* PS-8572 check 4a — step 2: expressions for the 10 S2/S3 archive versions, by clustered key IN-list. */
SELECT
  audience_segment_archive_id,
  campaign_id,
  version,
  update_time,
  LENGTH(expression) AS expr_len,
  expression
FROM `dw-main-silver.archives.audience_segment_archives`
WHERE audience_segment_archive_id IN (
  4172131, 4188014, 4188020, 4210458, 4229296,
  4172135, 4188013, 4188019, 4210457, 4229295)
ORDER BY campaign_id, CAST(version AS INT64)
LIMIT 20
