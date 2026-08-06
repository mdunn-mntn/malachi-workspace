/* PS-8572 check 1c — user-facing audience template (v1 expression) for audience_id 95073. */
SELECT
  audience_id,
  advertiser_id,
  name,
  update_time,
  create_time,
  LENGTH(expression) AS expr_len,
  expression
FROM `dw-main-silver.audience.audiences`
WHERE audience_id = 95073
