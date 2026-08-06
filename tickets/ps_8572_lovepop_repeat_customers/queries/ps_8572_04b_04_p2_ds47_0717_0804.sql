-- PS-8572 Task B, P2 (2026-07-16 18:17:02 .. 2026-08-04 23:59:59 UTC) vs DS47 dt='2026-07-17' (primary)
-- and DS47 dt='2026-08-04' (sensitivity). Grace ends 2026-07-19 18:17:02.
-- Blocks: denominators (full + post-grace); post-grace hits by stage x element x snapshot; grace-window hits (0717);
--         later-joined = (ip, element) in 0804 snapshot but NOT in 0717; probe membership at 0717;
--         up to 100 S1 post-grace sample rows vs 0717 snapshot (TRUE violation candidates).
WITH mem17 AS (
  SELECT DISTINCT t.ip, dscid.element AS list_id
  FROM `dw-main-bronze.external.ipdsc__v1` t, UNNEST(t.data_source_category_ids.list) AS dscid
  WHERE t.data_source_id = 47 AND t.dt = '2026-07-17' AND dscid.element IN (28594, 32697)
),
mem04 AS (
  SELECT DISTINCT t.ip, dscid.element AS list_id
  FROM `dw-main-bronze.external.ipdsc__v1` t, UNNEST(t.data_source_category_ids.list) AS dscid
  WHERE t.data_source_id = 47 AND t.dt = '2026-08-04' AND dscid.element IN (28594, 32697)
),
imps AS (
  SELECT ip, time, campaign_id,
    CASE campaign_id
      WHEN 614193 THEN 'S1' WHEN 614191 THEN 'S2' WHEN 614192 THEN 'S3'
      WHEN 637329 THEN 'RT' WHEN 637330 THEN 'RT' WHEN 637331 THEN 'RT' WHEN 637332 THEN 'RT'
      ELSE 'other' END AS stage
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE advertiser_id = 58797
    AND DATE(time) BETWEEN '2026-07-16' AND '2026-08-04'
    AND time >= TIMESTAMP '2026-07-16 18:17:02+00'
),
pg AS (
  SELECT * FROM imps WHERE time > TIMESTAMP '2026-07-19 18:17:02+00'
),
hits17 AS (
  SELECT p.ip, p.time, p.campaign_id, p.stage, m.list_id
  FROM pg p JOIN mem17 m USING (ip)
),
hits04 AS (
  SELECT p.ip, p.time, p.campaign_id, p.stage, m.list_id
  FROM pg p JOIN mem04 m USING (ip)
),
later_joined AS (
  SELECT h.*
  FROM hits04 h
  LEFT JOIN mem17 m ON h.ip = m.ip AND h.list_id = m.list_id
  WHERE m.ip IS NULL
)
SELECT 'p2_denominator_full_by_stage' AS block, stage, CAST(NULL AS INT64) AS list_id,
       COUNT(*) AS imps, COUNT(DISTINCT ip) AS distinct_ips,
       CAST(NULL AS STRING) AS ip, CAST(NULL AS TIMESTAMP) AS ts, CAST(NULL AS INT64) AS campaign_id
FROM imps GROUP BY stage
UNION ALL
SELECT 'p2_denominator_postgrace_by_stage', stage, NULL, COUNT(*), COUNT(DISTINCT ip), NULL, NULL, NULL
FROM pg GROUP BY stage
UNION ALL
SELECT 'p2_hits_postgrace_snap0717', stage, list_id, COUNT(*), COUNT(DISTINCT ip), NULL, NULL, NULL
FROM hits17 GROUP BY stage, list_id
UNION ALL
SELECT 'p2_hits_postgrace_snap0804', stage, list_id, COUNT(*), COUNT(DISTINCT ip), NULL, NULL, NULL
FROM hits04 GROUP BY stage, list_id
UNION ALL
SELECT 'p2_hits_gracewindow_snap0717', i.stage, m.list_id, COUNT(*), COUNT(DISTINCT i.ip), NULL, NULL, NULL
FROM imps i JOIN mem17 m USING (ip)
WHERE i.time <= TIMESTAMP '2026-07-19 18:17:02+00'
GROUP BY i.stage, m.list_id
UNION ALL
SELECT 'p2_later_joined_postgrace', stage, list_id, COUNT(*), COUNT(DISTINCT ip), NULL, NULL, NULL
FROM later_joined GROUP BY stage, list_id
UNION ALL
SELECT 'p2_probe_membership_ds47_0717', NULL, m.list_id, NULL, NULL, m.ip, NULL, NULL
FROM mem17 m WHERE m.ip IN ('172.58.116.210', '172.59.172.138', '99.24.48.111')
UNION ALL
SELECT * FROM (
  SELECT 'p2_s1_postgrace_sample_snap0717', h.stage, h.list_id, CAST(NULL AS INT64), CAST(NULL AS INT64),
         h.ip, h.time, h.campaign_id
  FROM hits17 h
  WHERE h.stage = 'S1'
  ORDER BY h.time
  LIMIT 100
)
ORDER BY block, stage, list_id, ts
