-- Campaign group detail: channels, campaigns, creative types
-- Usage: replace campaign_group_id value in WHERE clause
SELECT
  cg.campaign_group_id,
  cg.name AS campaign_group_name,
  cg.objective_id AS cg_objective_id,
  cg.product_id,
  cg.platform_id,
  c.campaign_id,
  c.name AS campaign_name,
  c.channel_id,
  ch.name AS channel_name,
  c.funnel_level,
  c.objective_id AS campaign_objective_id,
  c.partner_id,
  cs.creative_size_id,
  cs.description AS creative_size_desc,
  cs.width AS creative_width,
  cs.height AS creative_height,
  cs.ctv AS creative_is_ctv,
  cs.video AS creative_is_video,
  cs.web AS creative_is_web,
  cs.mobile AS creative_is_mobile,
  cr.media_type_id,
  COUNT(DISTINCT cr.creative_id) AS creative_count
FROM `dw-main-bronze.integrationprod.campaign_groups` cg
JOIN `dw-main-bronze.integrationprod.campaigns` c
  ON c.campaign_group_id = cg.campaign_group_id
  AND c.deleted = FALSE
LEFT JOIN `dw-main-bronze.integrationprod.channels` ch
  ON ch.channel_id = c.channel_id
LEFT JOIN `dw-main-silver.core.creative_groups` crg
  ON crg.campaign_id = c.campaign_id
LEFT JOIN `dw-main-silver.core.creative_groups_x_creatives` cgxc
  ON cgxc.group_id = crg.group_id
LEFT JOIN `dw-main-silver.core.creatives` cr
  ON cr.creative_id = cgxc.creative_id
LEFT JOIN `dw-main-silver.core.creative_sizes` cs
  ON cs.creative_size_id = cr.creative_size_id
WHERE cg.campaign_group_id = 93957  -- << change this
  AND cg.deleted = FALSE
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
ORDER BY c.channel_id, c.campaign_id, cs.creative_size_id
