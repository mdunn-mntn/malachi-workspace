-- PS-8572 Task B: membership of 3 adjudication IPs in DS4 snapshot dt='2026-06-30' (exact-match layer).
-- DS47 probes at 7/02 and 7/17 returned zero rows (blocks in 04b_03 / 04b_04); this checks the DS4 basis.
SELECT t.ip, dscid.element AS list_id
FROM `dw-main-bronze.external.ipdsc__v1` t, UNNEST(t.data_source_category_ids.list) AS dscid
WHERE t.data_source_id = 4 AND t.dt = '2026-06-30' AND dscid.element IN (28594, 32697)
  AND t.ip IN ('172.58.116.210', '172.59.172.138', '99.24.48.111')
ORDER BY t.ip, list_id
