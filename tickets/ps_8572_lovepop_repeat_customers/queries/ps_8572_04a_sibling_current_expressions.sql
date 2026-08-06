/* PS-8572 check 4a — current bidder expression (expression_type_id=2, is_targeted) per sibling campaign:
   614191 (S2), 614192 (S3), RT cg 129046 (637329-637332), old S1 587084. Latest row per campaign. */
SELECT
  campaign_id,
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
WHERE campaign_id IN (614191, 614192, 637329, 637330, 637331, 637332, 587084)
  AND expression_type_id = 2
  AND is_targeted = TRUE
QUALIFY ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY update_time DESC) = 1
ORDER BY campaign_id
LIMIT 20
