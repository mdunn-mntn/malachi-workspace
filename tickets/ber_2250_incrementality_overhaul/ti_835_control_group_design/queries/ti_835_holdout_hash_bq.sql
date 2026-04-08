-- TI-835: Holdout Bucket Hash Function (BigQuery)
-- Replicates Zach's Greenplum function / Rust audience service
-- GP: (('x' || substr(md5('{AID}:{IP}'), 1, 16))::bit(64)::bigint % 1000)
-- Bucket 0-99 = holdout (10%), 100-999 = targeted (90%)
-- Hash input: '{advertiser_id}:{ip}' — per-advertiser per-IP assignment
--
-- NOTE: Uses UNSIGNED mod (Rust u64 behavior), not signed Postgres behavior.
-- The GP query produces signed results (can be negative), but the Rust service
-- uses unsigned. We use unsigned here to match production.
--
-- VALIDATED: On WGU (31357), 7-day clickpass_log visits show 4.59% holdout / 95.41% targeted.
-- Holdout share < 10% is expected — holdout IPs visit less because they never receive ads.
-- This IS the incremental lift signal.

CREATE TEMP FUNCTION holdout_bucket(hex_str STRING)
RETURNS INT64
LANGUAGE js AS r"""
  // Take first 16 hex chars of MD5, parse as unsigned BigInt, mod 1000
  var hex16 = hex_str.substring(0, 16);
  var val = BigInt("0x" + hex16);
  return Number(val % BigInt(1000));
""";

-- Usage: compute holdout bucket for any IP + advertiser pair
-- SELECT
--   ip,
--   holdout_bucket(TO_HEX(MD5(CONCAT(CAST(advertiser_id AS STRING), ':', ip)))) AS bucket,
--   CASE
--     WHEN holdout_bucket(TO_HEX(MD5(CONCAT(CAST(advertiser_id AS STRING), ':', ip)))) BETWEEN 0 AND 99 THEN 'holdout'
--     ELSE 'targeted'
--   END AS group_assignment
-- FROM <table>
