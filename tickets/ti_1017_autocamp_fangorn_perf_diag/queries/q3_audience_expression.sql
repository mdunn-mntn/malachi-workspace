-- Audience expression for Autocamp campaign 570106
-- Shows MM segments, 3P interest segments, and any DS-segment composition
-- Per knowledge/data_catalog.md: audience.audience_segments is the silver view that includes campaign_id + is_targeted
SELECT
  audience_segment_id,
  audience_id,
  campaign_id,
  segment_id,
  expression_type_id,
  expression,
  is_targeted,
  update_time,
  create_time
FROM `dw-main-silver.audience.audience_segments`
WHERE campaign_id = 570106
ORDER BY update_time DESC, is_targeted DESC
LIMIT 200
