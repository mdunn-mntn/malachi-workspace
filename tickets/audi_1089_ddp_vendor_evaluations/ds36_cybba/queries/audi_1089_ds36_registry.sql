-- AUDI-1089 / DS36 Cybba — registry row (CDC dupes; dedupe by valid_from)
-- Result 2026-07-10: 2 identical rows -> 1 distinct:
--   billing_type=fixed_cpm, fixed_cpm=0.5, enabled=true, used_in_mntn_match=true,
--   used_in_interests=false, type=mntn_matched, valid_from=2025-01-01, valid_to=null, notes=NULL
SELECT
  data_source_id, data_partner_name, billing_type, fixed_cpm, enabled,
  used_in_mntn_match, used_in_interests, type, valid_from, valid_to, notes
FROM `dw-main-silver.tpa.direct_data_partners`
WHERE data_source_id = "36"
ORDER BY valid_from DESC
LIMIT 10;
