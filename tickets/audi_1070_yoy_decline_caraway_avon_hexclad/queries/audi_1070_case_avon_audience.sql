-- AUDI-1070 case — Avon (31921) audience construction: latest targeting expression + data_source_ids per campaign.
-- data_source_id labels via bronze.integrationprod.audience_data_sources.
WITH ac AS (
  SELECT DISTINCT campaign_id FROM `dw-main-bronze.integrationprod.archives_campaign_archives` WHERE advertiser_id=31921 ),
seg AS (
  SELECT s.campaign_id, s.expression, s.is_targeted, s.expression_type_id,
    ROW_NUMBER() OVER (PARTITION BY s.campaign_id ORDER BY s.version DESC) rn
  FROM `dw-main-bronze.integrationprod.archives_audience_segment_archives` s JOIN ac USING (campaign_id) )
SELECT campaign_id, is_targeted, expression_type_id expr_type,
  ARRAY_TO_STRING(ARRAY(SELECT DISTINCT x FROM UNNEST(REGEXP_EXTRACT_ALL(expression, r"data_source_id\D{0,4}(\d+)")) x ORDER BY CAST(x AS INT64)), ",") AS data_source_ids,
  LENGTH(expression) expr_len, SUBSTR(REGEXP_REPLACE(expression, r"\s+", " "), 1, 220) expr_sample
FROM seg WHERE rn=1 ORDER BY campaign_id;
