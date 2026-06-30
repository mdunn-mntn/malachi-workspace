WITH camp AS (
  SELECT campaign_id, advertiser_id FROM `dw-main-bronze.integrationprod.campaigns`
  WHERE advertiser_id IN (40341,31921,34611) AND funnel_level=1 AND deleted=FALSE AND is_test=FALSE
),
ex AS (
  SELECT c.advertiser_id, DATE_TRUNC(DATE(a.create_time),MONTH) mo, a.campaign_id, a.version,
    REGEXP_EXTRACT_ALL(a.expression, r'"data_source_id"\s*:\s*"?(\d+)') AS ds
  FROM `dw-main-bronze.integrationprod.archives_audience_segment_archives` a
  JOIN camp c ON a.campaign_id=c.campaign_id
  WHERE a.is_targeted=TRUE
)
SELECT advertiser_id, mo,
  COUNT(DISTINCT campaign_id) ncamp,
  STRING_AGG(DISTINCT ds_id ORDER BY ds_id) data_source_ids
FROM ex, UNNEST(ds) ds_id
GROUP BY 1,2 ORDER BY 1,2
