/* ========================================================================
   TI-921 — Discover newly-flipped AIDs not yet in wave_config.csv

   Run this after each Fangorn rollout wave (e.g., the morning after Matt
   posts a Slack update like "rolled out to next 50 advertisers"). It
   identifies any AID with vertical_data_source = 46 that's missing from
   the wave_config.csv inline UNION ALL block, and returns:
     - advertiser_id
     - company_name
     - vertical_name
     - first-flip timestamp (UTC) and PT flip_date

   Workflow:
     1. Run this query.
     2. For each row returned, append a line to wave_config.csv with the
        PT flip_date (which is the day Fangorn first targeted them — see
        note below).
     3. Re-run the Databricks notebook.

   Why PT, not UTC, for flip_date:
     Household scoring runs nightly between midnight-1am PT. Matt's update
     adds AIDs to `tpa.fangorn_advertiser_inclusion` (TPA Postgres source).
     The next scheduled scoring run propagates to BQ via vertical_data_source
     = 46 — and that's the day Fangorn-eligible bidding actually starts.
     The PT date of source_timestamp is the closest BQ-observable proxy.

   Notes on the 4 currently-known AIDs (as of 2026-05-05 PT):
     - 32320, 38659, 32233 flipped 2026-05-01 (Tier1-Wave1, original launch)
     - 46538 flipped 2026-05-05 (Tier1-Wave2 vanguard)
     - Matt's 50 from 2026-05-05 PM update will appear after tonight's
       scoring run, with flip_date = 2026-05-06 PT.
   ======================================================================== */

WITH wave_config AS (
  -- KEEP IN SYNC WITH artifacts/wave_config.csv
  SELECT 32320 AS advertiser_id UNION ALL
  SELECT 38659                  UNION ALL
  SELECT 32233                  UNION ALL
  SELECT 46538
),

current_treated AS (
  SELECT
    c.advertiser_id,
    TIMESTAMP_MILLIS(c.datastream_metadata.source_timestamp) AS flip_ts_utc,
    DATE(TIMESTAMP_MILLIS(c.datastream_metadata.source_timestamp), "America/Los_Angeles") AS flip_date_pt
  FROM `dw-main-bronze.integrationprod.audience_advertiser_configurations` c
  WHERE c.vertical_data_source = 46
)

SELECT
  ct.advertiser_id,
  a.company_name,
  av.vertical_id,
  av.vertical_name,
  ct.flip_ts_utc,
  ct.flip_date_pt,
  -- Suggested CSV row (paste into wave_config.csv after picking the cohort label):
  CONCAT(
    CAST(ct.advertiser_id AS STRING), ',',
    a.company_name, ',',
    CAST(ct.flip_date_pt AS STRING), ',',
    'TierX-WaveY,',                      -- update cohort label manually
    COALESCE(av.vertical_name, 'unknown'), ',',
    'true,true,',                          -- pixel/dollar — verify or override
    'review pixel + dollar status'
  ) AS suggested_csv_row
FROM current_treated ct
LEFT JOIN `dw-main-bronze.integrationprod.advertisers` a USING(advertiser_id)
LEFT JOIN `dw-main-silver.fpa.advertiser_verticals` av
  ON ct.advertiser_id = av.advertiser_id AND av.type = 1
WHERE ct.advertiser_id NOT IN (SELECT advertiser_id FROM wave_config)
ORDER BY ct.flip_ts_utc;
