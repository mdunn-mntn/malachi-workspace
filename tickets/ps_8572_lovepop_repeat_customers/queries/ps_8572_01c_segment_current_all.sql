/* PS-8572 check 1c — ALL current audience_segments rows for campaign 614193 (any type / targeted flag),
   to see live-table history alongside the archives. */
SELECT
  audience_segment_id,
  audience_id,
  segment_id,
  expression_type_id,
  is_targeted,
  create_time,
  update_time,
  LENGTH(expression) AS expr_len,
  expression
FROM `dw-main-silver.audience.audience_segments`
WHERE campaign_id = 614193
ORDER BY update_time
LIMIT 200
