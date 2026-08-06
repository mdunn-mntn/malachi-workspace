-- PS-8572 check 1d: match_rate history via ui_audience_uploads CDC rows (if append-only)
SELECT
  audience_upload_id,
  match_rate,
  entry_count,
  status,
  update_time,
  TIMESTAMP_MICROS(CAST(datastream_metadata.source_timestamp AS INT64)) AS cdc_ts
FROM `dw-main-bronze.integrationprod.ui_audience_uploads`
WHERE audience_upload_id IN (28594, 32697)
ORDER BY audience_upload_id, update_time
LIMIT 200
