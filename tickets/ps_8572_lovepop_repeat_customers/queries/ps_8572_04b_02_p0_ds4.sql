-- PS-8572 Task B, P0 (2026-06-01 .. 2026-06-30 02:08:18 UTC): CONTEXT ONLY, no CRM exclusion existed.
-- Impressions to would-be members of each list, vs DS4 snapshot dt='2026-06-30'. Split by element x stage.
WITH mem AS (
  SELECT DISTINCT t.ip, dscid.element AS list_id
  FROM `dw-main-bronze.external.ipdsc__v1` t, UNNEST(t.data_source_category_ids.list) AS dscid
  WHERE t.data_source_id = 4 AND t.dt = '2026-06-30' AND dscid.element IN (28594, 32697)
),
imps AS (
  SELECT ip, time, campaign_id,
    CASE campaign_id
      WHEN 614193 THEN 'S1' WHEN 614191 THEN 'S2' WHEN 614192 THEN 'S3'
      WHEN 637329 THEN 'RT' WHEN 637330 THEN 'RT' WHEN 637331 THEN 'RT' WHEN 637332 THEN 'RT'
      ELSE 'other' END AS stage
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE advertiser_id = 58797
    AND DATE(time) BETWEEN '2026-06-01' AND '2026-06-30'
    AND time < TIMESTAMP '2026-06-30 02:08:18+00'
),
hits AS (
  SELECT i.ip, i.time, i.stage, m.list_id
  FROM imps i JOIN mem m USING (ip)
)
SELECT 'p0_denominator_by_stage' AS block, stage, CAST(NULL AS INT64) AS list_id,
       COUNT(*) AS imps, COUNT(DISTINCT ip) AS distinct_ips
FROM imps GROUP BY stage
UNION ALL
SELECT 'p0_member_hits_by_stage_element', stage, list_id, COUNT(*), COUNT(DISTINCT ip)
FROM hits GROUP BY stage, list_id
ORDER BY block, stage, list_id
