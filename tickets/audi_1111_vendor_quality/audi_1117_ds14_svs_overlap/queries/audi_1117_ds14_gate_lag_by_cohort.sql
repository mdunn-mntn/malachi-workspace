-- ============================================================================
-- AUDI-1117 Q2: DS14 gate lag split by (channel x funnel) cohort
--
-- Claim: Q1 (audi_1117_ds14_gate_lag.sql) showed NO hard gate edge over ALL
-- impressions: aug(1d) OR guid(4d) covers only 85.5% of served IPs; 5.1% saw
-- NEITHER free log in 11 days; lag distributions decay smoothly. Hypothesis:
-- DS14 gates audience EXPRESSIONS (prospecting-style targeting), while
-- retargeting (own visitor lists) and display (cookie-based) bypass it. This
-- query repeats the lag histogram per cohort: channel_id (8=CTV / 1=display /
-- other) x funnel_level (1=prospecting / 4=retargeting / other, from
-- integrationprod.campaigns — authoritative per house convention; objective_id
-- is NOT used for stage). If the gate is real, the CTV-prospecting cohort
-- should show a hard edge; the bypass cohorts explain the Q1 residual.
--
-- Grain/hygiene: IPv4; served = CIL won imps on DAY; free-log lags = most
-- recent prior-or-same-day appearance over the 11d svs lookback, capped at 10,
-- NULL = not in window. +-1 day partition fuzz expected (gate binds at bid
-- time). campaigns filtered deleted=FALSE AND is_test=FALSE; imps with no
-- campaign match fall into the 'other' funnel bucket.
--
-- ARCHITECTURE NOTE (cost): svs and CIL each read EXACTLY ONCE (linear chain,
-- direct small-histogram output; campaigns is a tiny dim).
--
-- MODERATE (svs 11d + CIL 1 day + campaigns dim) — dry-run, background.
--
-- Run: paste this whole block into a terminal, in the folder holding this
-- file (prereqs: gcloud auth login; bq CLI; python3; GCS read on
-- mntn-data-archive-prod; BQ read on dw-main-silver + dw-main-bronze):
--   URIS=""; for d in $(python3 -c "import datetime as t; s=t.date(2026,6,21); print(' '.join(str(s+t.timedelta(i)) for i in range(11)))"); do \
--     URIS="${URIS}gs://mntn-data-archive-prod/signals/site_visit_signal/dt=${d}/*.parquet,"; done; URIS="${URIS%,}"
--   bq query \
--     --external_table_definition="svs::PARQUET=${URIS}" \
--     --use_legacy_sql=false --location=us-central1 --format=csv --max_rows=2000 --project_id=dw-main-silver \
--     "$(grep -v '^[[:space:]]*--' audi_1117_ds14_gate_lag_by_cohort.sql)" \
--     > audi_1117_ds14_gate_lag_by_cohort.csv
--
-- Parameters: DAY = 2026-07-01, LOOKBACK = 2026-06-21..2026-07-01 (11d)
-- ============================================================================

WITH served AS (
  SELECT
    c.ip,
    CASE WHEN k.channel_id = 8 THEN 'ctv' WHEN k.channel_id = 1 THEN 'display'
         WHEN k.channel_id IS NULL THEN 'unmatched'
         ELSE 'other_channel' END AS channel,
    CASE WHEN k.funnel_level = 1 THEN 'prospecting'
         WHEN k.funnel_level = 4 THEN 'retargeting'
         WHEN k.funnel_level IS NULL THEN 'unmatched'
         ELSE CONCAT('funnel_', CAST(k.funnel_level AS STRING)) END AS funnel,
    COUNT(*) AS imps
  FROM `dw-main-silver.logdata.cost_impression_log` c
  LEFT JOIN (
    SELECT campaign_id, funnel_level, channel_id
    FROM `dw-main-bronze.integrationprod.public_campaigns`
    WHERE deleted = FALSE AND is_test = FALSE
  ) k USING (campaign_id)
  WHERE DATE(c.time) = '2026-07-01'
    AND c.ip IS NOT NULL AND c.ip NOT LIKE '%:%'
  GROUP BY 1, 2, 3
),

freelog AS (
  SELECT ip,
         MAX(IF(CAST(data_source_id AS INT64) = 30, CAST(dt AS DATE), NULL)) AS last_aug,
         MAX(IF(CAST(data_source_id AS INT64) = 23, CAST(dt AS DATE), NULL)) AS last_guid
  FROM svs
  WHERE CAST(data_source_id AS INT64) IN (23, 30)
    AND ip IS NOT NULL AND ip NOT LIKE '%:%'
  GROUP BY ip
)

SELECT
  s.channel,
  s.funnel,
  LEAST(DATE_DIFF(DATE '2026-07-01', f.last_aug, DAY), 10) AS aug_lag_days,
  LEAST(DATE_DIFF(DATE '2026-07-01', f.last_guid, DAY), 10) AS guid_lag_days,
  COUNT(DISTINCT s.ip) AS served_ips,
  SUM(s.imps) AS imps
FROM served s
LEFT JOIN freelog f USING (ip)
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4
