-- TI-837 — Augmentor_log holdout bucket verification
--
-- Question: do holdout IPs (bucket 0-99 of the production per-advertiser MD5 hash)
-- appear in augmentor_log? Alex Knorr (Apr 17) said yes; Ryan Kleck (Apr 20) said no.
-- Resolving this determines the ghost-bidding pipeline design:
--   - holdouts in augmentor_log  → post-process existing data (no ETL change)
--   - holdouts not in augmentor  → ETL change with Zach/Jordan + bidder-side change with Kevaughn
--
-- Methodology: augmentor_log has no advertiser_id column (pre-bid enrichment stream, IP-level).
-- For a fixed advertiser (31357 = WGU), compute bucket for each IP seen in augmentor_log.
-- If augmentor is advertiser-agnostic and IP-complete (Alex's read): ~10% fall in buckets 0-99.
-- If holdouts are filtered upstream (Ryan's read): ~0%.
--
-- Hash formula: production GP uses ((x || substr(md5(aid||':'||ip), 1, 16))::bit(64)::bigint % 1000).
-- BQ port via Chinese-remainder split (INT64 is signed, so treat as two 32-bit unsigned halves):
--   bucket = ((top32 mod 1000) * (2^32 mod 1000) + bot32 mod 1000) mod 1000
--          = ((top32 mod 1000) * 296           + bot32 mod 1000) mod 1000
-- This matches the unsigned-normalized production bucket in [0, 999].
-- NOTE: If production applies sign-aware modulo on the signed bigint (range -999..+999)
-- without normalization, only positive buckets 0-99 would be holdouts (~5% expected).
-- Follow-up with Zach to confirm the actual normalization.
--
-- Cost: ~22 GB (one hourly partition of bronze.raw.augmentor_log). Justified: one-time verification
-- that unblocks the full BER-2250 methodology.

WITH hashed AS (
  SELECT
    TO_HEX(MD5(CONCAT('31357', ':', ip))) AS md5hex,
    ip
  FROM `dw-main-bronze.raw.augmentor_log`
  WHERE time >= TIMESTAMP('2026-04-19 00:00:00 UTC')
    AND time <  TIMESTAMP('2026-04-19 01:00:00 UTC')
    AND ip IS NOT NULL
    AND ip != '0.0.0.0'
),
bucketed AS (
  SELECT
    MOD(
      MOD(CAST(CONCAT('0x', SUBSTR(md5hex, 1, 8)) AS INT64), 1000) * 296
      + MOD(CAST(CONCAT('0x', SUBSTR(md5hex, 9, 8)) AS INT64), 1000),
      1000
    ) AS bucket,
    ip
  FROM hashed
)
SELECT
  bucket < 100 AS in_holdout_bucket,
  COUNT(*)          AS n_rows,
  COUNT(DISTINCT ip) AS unique_ips
FROM bucketed
GROUP BY in_holdout_bucket
ORDER BY in_holdout_bucket;
