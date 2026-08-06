-- PS-8572 Task B, P1 (2026-06-30 02:08:18 .. 2026-07-16 18:17:02 UTC) vs DS47 snapshot dt='2026-07-02'.
-- Blocks: denominators; 28594 GAP cohort (stage x day + stage totals); 32697 post-grace (grace ends 2026-07-03 02:08:18);
--         probe membership of 3 adjudication IPs; up to 50 S1 sample rows for 32697 post-grace.
WITH mem AS (
  SELECT DISTINCT t.ip, dscid.element AS list_id
  FROM `dw-main-bronze.external.ipdsc__v1` t, UNNEST(t.data_source_category_ids.list) AS dscid
  WHERE t.data_source_id = 47 AND t.dt = '2026-07-02' AND dscid.element IN (28594, 32697)
),
imps AS (
  SELECT ip, time, campaign_id,
    CASE campaign_id
      WHEN 614193 THEN 'S1' WHEN 614191 THEN 'S2' WHEN 614192 THEN 'S3'
      WHEN 637329 THEN 'RT' WHEN 637330 THEN 'RT' WHEN 637331 THEN 'RT' WHEN 637332 THEN 'RT'
      ELSE 'other' END AS stage
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE advertiser_id = 58797
    AND DATE(time) BETWEEN '2026-06-30' AND '2026-07-16'
    AND time >= TIMESTAMP '2026-06-30 02:08:18+00'
    AND time < TIMESTAMP '2026-07-16 18:17:02+00'
),
hits AS (
  SELECT i.ip, i.time, i.campaign_id, i.stage, m.list_id
  FROM imps i JOIN mem m USING (ip)
)
SELECT 'p1_denominator_by_stage' AS block, stage, CAST(NULL AS INT64) AS list_id, CAST(NULL AS DATE) AS day,
       COUNT(*) AS imps, COUNT(DISTINCT ip) AS distinct_ips,
       CAST(NULL AS STRING) AS ip, CAST(NULL AS TIMESTAMP) AS ts, CAST(NULL AS INT64) AS campaign_id
FROM imps GROUP BY stage
UNION ALL
SELECT 'p1_gap_28594_total_by_stage', stage, 28594, NULL, COUNT(*), COUNT(DISTINCT ip), NULL, NULL, NULL
FROM hits WHERE list_id = 28594 GROUP BY stage
UNION ALL
SELECT 'p1_gap_28594_by_stage_day', stage, 28594, DATE(time) AS day, COUNT(*), COUNT(DISTINCT ip), NULL, NULL, NULL
FROM hits WHERE list_id = 28594 GROUP BY stage, day
UNION ALL
SELECT 'p1_32697_pregrace_by_stage', stage, 32697, NULL, COUNT(*), COUNT(DISTINCT ip), NULL, NULL, NULL
FROM hits WHERE list_id = 32697 AND time <= TIMESTAMP '2026-07-03 02:08:18+00' GROUP BY stage
UNION ALL
SELECT 'p1_32697_postgrace_by_stage', stage, 32697, NULL, COUNT(*), COUNT(DISTINCT ip), NULL, NULL, NULL
FROM hits WHERE list_id = 32697 AND time > TIMESTAMP '2026-07-03 02:08:18+00' GROUP BY stage
UNION ALL
SELECT 'p1_probe_membership_ds47_0702', NULL, m.list_id, NULL, NULL, NULL, m.ip, NULL, NULL
FROM mem m WHERE m.ip IN ('172.58.116.210', '172.59.172.138', '99.24.48.111')
UNION ALL
SELECT * FROM (
  SELECT 'p1_s1_32697_postgrace_sample', h.stage, h.list_id, CAST(NULL AS DATE), CAST(NULL AS INT64), CAST(NULL AS INT64),
         h.ip, h.time, h.campaign_id
  FROM hits h
  WHERE h.list_id = 32697 AND h.stage = 'S1' AND h.time > TIMESTAMP '2026-07-03 02:08:18+00'
  ORDER BY h.time
  LIMIT 50
)
ORDER BY block, stage, day, ts
