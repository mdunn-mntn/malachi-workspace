-- PS-8572 Task B: all impressions 2026-06-01..2026-08-04 for the 3 adjudication IPs, with period/grace bucketing.
SELECT
  ip, time, campaign_id,
  CASE campaign_id
    WHEN 614193 THEN 'S1' WHEN 614191 THEN 'S2' WHEN 614192 THEN 'S3'
    WHEN 637329 THEN 'RT' WHEN 637330 THEN 'RT' WHEN 637331 THEN 'RT' WHEN 637332 THEN 'RT'
    ELSE 'other' END AS stage,
  CASE
    WHEN time < TIMESTAMP '2026-06-30 02:08:18+00' THEN 'P0'
    WHEN time < TIMESTAMP '2026-07-16 18:17:02+00' THEN 'P1'
    ELSE 'P2' END AS period,
  time > TIMESTAMP '2026-07-03 02:08:18+00' AS after_p1_grace,
  time > TIMESTAMP '2026-07-19 18:17:02+00' AS after_p2_grace
FROM `dw-main-silver.logdata.cost_impression_log`
WHERE advertiser_id = 58797
  AND DATE(time) BETWEEN '2026-06-01' AND '2026-08-04'
  AND ip IN ('172.58.116.210', '172.59.172.138', '99.24.48.111')
ORDER BY ip, time
