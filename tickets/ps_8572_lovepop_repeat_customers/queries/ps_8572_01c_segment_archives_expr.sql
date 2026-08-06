/* PS-8572 check 1c — step 2: expressions for the 12 archived versions of campaign 614193's segments,
   fetched by the clustered key (audience_segment_archive_id) so block pruning cuts the 42 GB scan.
   Dry-run shows full-column upper bound; actual billed bytes are cluster-pruned (expected). */
SELECT
  audience_segment_archive_id,
  audience_segment_id,
  audience_id,
  segment_id,
  expression_type_id,
  is_targeted,
  version,
  create_time,
  update_time,
  LENGTH(expression) AS expr_len,
  expression
FROM `dw-main-silver.archives.audience_segment_archives`
WHERE audience_segment_archive_id IN (4172130, 4172994, 4188012, 4188018, 4198445, 4199635,
                                      4209955, 4210455, 4219678, 4229290, 4229292, 4229293)
  AND campaign_id = 614193
ORDER BY update_time
LIMIT 200
