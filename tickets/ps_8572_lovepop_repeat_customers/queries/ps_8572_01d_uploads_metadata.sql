-- PS-8572 check 1d: CRM upload metadata for uploads 28594 + 32697 (Lovepop adv 58797)
-- Confirms data_source_category_id mapping (identity vs legacy divergence) for ipdsc filters.
SELECT
  audience_upload_id,
  data_source_category_id,
  (data_source_category_id = audience_upload_id) AS dsc_equals_pk,
  advertiser_id,
  name,
  data_source_id,
  audience_upload_type_id,
  entry_count,
  match_rate,
  ROUND(match_rate * entry_count) AS est_matched_ips,
  status,
  deprecated,
  create_time,
  update_time,
  upload_start_date,
  user_id,
  md5_hash
FROM `dw-main-bronze.integrationprod.audience_uploads`
WHERE audience_upload_id IN (28594, 32697)
