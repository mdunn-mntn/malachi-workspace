-- TI-835: Aggregate Incrementality Analysis
-- Compare visit rates between 10% holdout (bucket 0-99) and 90% targeted (bucket 100-999)
-- Using guid_log for ALL visits (including holdout IPs that never received impressions)
-- Holdout hash: MD5('{AID}:{IP}') unsigned mod 1000
--
-- Under null (no ad effect): targeted:holdout visit ratio = 9:1
-- Deviation above 9:1 = incremental lift from ads
-- Lift = (observed_ratio / 9.0) - 1

CREATE TEMP FUNCTION holdout_bucket(hex_str STRING)
RETURNS INT64
LANGUAGE js AS r"""
  var hex16 = hex_str.substring(0, 16);
  var val = BigInt("0x" + hex16);
  return Number(val % BigInt(1000));
""";

WITH selected_advertisers AS (
  -- Diverse set: large, mid, small volume; different verticals
  SELECT advertiser_id, company_name FROM UNNEST([
    STRUCT(31455 AS advertiser_id, 'Ancient Nutrition' AS company_name),
    STRUCT(31276, 'Ferguson Home'),
    STRUCT(37775, 'Zazzle'),
    STRUCT(32766, 'Angi'),
    STRUCT(39036, 'Function Health'),
    STRUCT(34838, 'Clayton Homes'),
    STRUCT(40563, 'Northern Tool'),
    STRUCT(53308, 'REVOLVE'),
    STRUCT(34143, 'First Watch'),
    STRUCT(34611, 'HexClad')
  ])
),
visits AS (
  SELECT
    g.advertiser_id,
    g.ip,
    COUNT(*) AS visit_count
  FROM `dw-main-silver.logdata.guid_log` g
  WHERE DATE(g.time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    AND g.advertiser_id IN (31455, 31276, 37775, 32766, 39036, 34838, 40563, 53308, 34143, 34611)
  GROUP BY 1, 2
),
bucketed AS (
  SELECT
    v.advertiser_id,
    sa.company_name,
    v.ip,
    v.visit_count,
    holdout_bucket(TO_HEX(MD5(CONCAT(CAST(v.advertiser_id AS STRING), ':', v.ip)))) AS bucket
  FROM visits v
  JOIN selected_advertisers sa ON v.advertiser_id = sa.advertiser_id
)
SELECT
  advertiser_id,
  company_name,
  CASE WHEN bucket BETWEEN 0 AND 99 THEN 'holdout' ELSE 'targeted' END AS group_name,
  COUNT(DISTINCT ip) AS unique_visitors,
  SUM(visit_count) AS total_visits
FROM bucketed
GROUP BY 1, 2, 3
ORDER BY advertiser_id, group_name;
