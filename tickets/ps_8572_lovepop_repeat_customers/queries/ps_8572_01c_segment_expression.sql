/* PS-8572 check 1c — latest bidder-operative segment expression for campaign 614193.
   Bidder evaluates audience.audience_segments (v2 op-tree): expression_type_id=2, is_targeted=TRUE,
   latest by update_time. Shape mirrors ti_1037 perf_report/queries/02_prospecting_audience_expressions.sql. */
SELECT
  campaign_id,
  audience_id,
  segment_id,
  audience_segment_id,
  expression_type_id,
  is_targeted,
  update_time,
  create_time,
  LENGTH(expression) AS expr_len,
  expression
FROM `dw-main-silver.audience.audience_segments`
WHERE campaign_id = 614193
  AND expression_type_id = 2
  AND is_targeted = TRUE
QUALIFY ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY update_time DESC) = 1
