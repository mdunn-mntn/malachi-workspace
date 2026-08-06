/* PS-8572 check 1c — full version history of audience_segments for campaign 614193.
   Archives table located empirically: silver view archives.audience_segment_archives
   (over bronze.integrationprod.archives_audience_segment_archives).
   Full history pulled (not just Jun 1..Aug 4) so the FIRST appearance of the CRM clause is provable.
   NOT RUN — dry-run 42 GB (unpartitioned, expression col dominates). Split into the two-step
   _meta.sql (0.3 GB) + _expr.sql (cluster-pruned 0.18 GB billed) instead. */
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
  update_time,
  LENGTH(expression) AS expr_len,
  expression
FROM `dw-main-silver.archives.audience_segment_archives`
WHERE campaign_id = 614193
ORDER BY update_time
LIMIT 200
