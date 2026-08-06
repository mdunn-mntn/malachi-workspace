-- PS-8572 check 1d OPTIONAL: HEM counts per upload, TI-644 pattern (pre_hash_case='UPPERCASE' dedup)
-- SKIPPED: table is 433 GB, unpartitioned + unclustered; dry-run scan 395,905,012,165 bytes (~368.7 GiB) >> 2 GB cap.
SELECT
  audience_upload_id,
  COUNT(*) AS rows_uppercase,
  COUNT(DISTINCT hashed_email) AS distinct_hems
FROM `dw-main-bronze.tpa.audience_upload_hashed_emails`
WHERE audience_upload_id IN (28594, 32697)
  AND pre_hash_case = 'UPPERCASE'
GROUP BY audience_upload_id
