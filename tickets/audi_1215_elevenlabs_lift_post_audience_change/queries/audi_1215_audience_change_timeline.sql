-- audi_1215_audience_change_timeline.sql
-- AUDI-1215: what changed in CG 122748's audience and when (AID 51660 ElevenLabs)
-- 1. Audience<->CG mapping history (archive of audience_audience_x_campaign_groups)
SELECT audience_x_campaign_group_archive_id, campaign_group_id, audience_id, version, user_id, create_time, update_time
FROM `dw-main-silver.archives.audience_x_campaign_group_archives`
WHERE campaign_group_id = 122748
ORDER BY create_time;

-- Live mapping row (current state)
SELECT axcg.audience_id, axcg.create_time, axcg.update_time, a.name
FROM `dw-main-bronze.integrationprod.audience_audience_x_campaign_groups` axcg
JOIN `dw-main-bronze.integrationprod.audience_audiences` a USING (audience_id)
WHERE axcg.campaign_group_id = 122748;

-- 2. Audience-level expression versions (archive rows: update_time = version effective, create_time = archived)
SELECT audience_id, version, user_id, create_time, update_time,
       ARRAY_TO_STRING(ARRAY(SELECT DISTINCT x FROM UNNEST(REGEXP_EXTRACT_ALL(expression, r'"data_source_id":\s*([0-9]+)')) x ORDER BY CAST(x AS INT64)), ',') AS ds_ids,
       LENGTH(expression) AS expr_len
FROM `dw-main-silver.archives.audiences_archives`
WHERE audience_id IN (66146, 77883, 88532)
ORDER BY audience_id, create_time;

-- 3. Per-campaign TPA expression history (the expressions the bidder evaluates)
SELECT campaign_id, audience_segment_id, segment_id, version, create_time, update_time,
       ARRAY_TO_STRING(ARRAY(SELECT DISTINCT x FROM UNNEST(REGEXP_EXTRACT_ALL(expression, r'"data_source_id":\s*([0-9]+)')) x ORDER BY CAST(x AS INT64)), ',') AS ds_ids,
       LENGTH(expression) AS expr_len
FROM `dw-main-silver.archives.audience_segment_archives`
WHERE campaign_id IN (608810,608811,608812,608813,608814,608815)
  AND expression_type_id = 2 AND is_targeted = TRUE
ORDER BY campaign_id, update_time;

-- 4. Full expression text, prospecting campaign 608814, last pre-change and first post-change versions
SELECT 'pre' AS phase, expression
FROM `dw-main-silver.archives.audience_segment_archives`
WHERE campaign_id = 608814 AND segment_id = 657261 AND version = 7
UNION ALL
SELECT 'post', expression
FROM `dw-main-silver.archives.audience_segment_archives`
WHERE campaign_id = 608814 AND segment_id = 687759 AND version = 1;

-- 5. CG-level config changes around the window
SELECT version, update_time, campaign_group_status_id, budget, start_time, end_time, name
FROM `dw-main-bronze.integrationprod.archives_campaign_group_archives`
WHERE campaign_group_id = 122748
  AND update_time BETWEEN '2026-06-20' AND '2026-07-20'
ORDER BY update_time;