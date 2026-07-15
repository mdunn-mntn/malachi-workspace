-- Claim: score-tier mix (HI 10000 / PP 8000 / high-grad / mid / max-reach / unscored)
-- per vendor for the TOUCHED and SOLE served cohorts — CIL valuation-week per-IP
-- MAX(household_score) on the 37d svs membership. Output q5_score_tiers.csv.
-- Run (from workspace root; svs 37d union external table + internal CIL):
--   URIS=""; for d in $(python3 -c "import datetime as t; s=t.date(2026,6,2); print(' '.join(str(s+t.timedelta(i)) for i in range(37)))"); do \
--     URIS="${URIS}gs://mntn-data-archive-prod/signals/site_visit_signal/dt=${d}/*.parquet,"; done; URIS="${URIS%,}"
--   bash .claude/scripts/bq_run.sh --ticket AUDI-1089 --label "<label>" \
--     --external_table_definition="svs::PARQUET=${URIS}" \
--     --use_legacy_sql=false --format=csv --max_rows=100 --project_id=dw-main-silver \
--     "$(grep -v '^[[:space:]]*--' <this file>)" > outputs/run_<YYYY_MM_DD>/<output>.csv
--
-- CANONICAL runbook copy of queries/audi_1089_q3_score_tiers.sql (AUDI-1089 eval, windows per runbook params).
-- Output: outputs/run_<date>/q5_score_tiers.csv. Full run pattern in the original header below.
--
-- AUDI-1089 Q3: delivered score-tier mix per vendor — all-touched vs sole IPs
-- Klickly (DS39) focal; all DS computed for reuse (24,25,26,28,33,36,39,40 external; 23,30 internal).
-- Substrate: gs://mntn-data-archive-prod/signals/site_visit_signal/dt=/hh=/data_source_id=N/*.parquet
--   via BQ temp external table (read-only; zzz_temp.site_visit_signal is manual/stale).
-- Windows:
--   svs union window  = dt 2026-06-02 .. 2026-07-08 (37 days) → IP membership (per-ds presence + n_ds)
--   CIL valuation week = DATE(time) BETWEEN 2026-07-02 AND 2026-07-08 → per-ip MAX(household_score), delivered flag
-- Tier buckets copied EXACTLY from ti_1027_analysis_queries.sql Phase 3b (lines 115-132):
--   hi=10000, pp=8000, high_grad 6666-9999 (<>8000), mid 3333-6665, maxreach 1-3332, unscored <=0.
-- Cohorts: touched = all vendor IPs; sole = IPs with n_ds=1 (seen by that vendor only, across the full roster).
-- IP hygiene: ip IS NOT NULL AND ip NOT LIKE '%:%' (IPv6 excluded; quantified in Q1).
-- Run pattern:
--   URIS=""; for each day d in window: URIS="${URIS}gs://mntn-data-archive-prod/signals/site_visit_signal/dt=<date>/*.parquet,"; URIS="${URIS%,}"
--   bq query --external_table_definition="svs::PARQUET=${URIS}" --use_legacy_sql=false \
--            --project_id=dw-main-silver --format=csv --max_rows=100 "$(cat this_file.sql)"

WITH vip AS (
  SELECT DISTINCT data_source_id, ip
  FROM svs
  WHERE ip IS NOT NULL AND ip NOT LIKE '%:%'
),
ipm AS (
  SELECT ip, COUNT(DISTINCT data_source_id) AS n_ds
  FROM vip
  GROUP BY ip
),
scored AS (
  SELECT ip, MAX(household_score) AS sc
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE DATE(time) BETWEEN '2026-07-02' AND '2026-07-08'
  GROUP BY ip
),
j AS (
  SELECT v.data_source_id, v.ip, m.n_ds, s.sc
  FROM vip v
  JOIN ipm m USING (ip)
  LEFT JOIN scored s USING (ip)
),
expanded AS (
  SELECT data_source_id, 'touched' AS cohort, ip, sc FROM j
  UNION ALL
  SELECT data_source_id, 'sole' AS cohort, ip, sc FROM j WHERE n_ds = 1
)
SELECT
  data_source_id,
  cohort,
  COUNT(*) AS vendor_ips,
  COUNTIF(sc IS NOT NULL) AS delivered_ips,
  ROUND(100*COUNTIF(sc IS NOT NULL)/COUNT(*),1) AS pct_delivered,
  -- tier counts (among delivered)
  COUNTIF(sc = 10000) AS hi_10000,
  COUNTIF(sc = 8000) AS pp_8000,
  COUNTIF(sc BETWEEN 6666 AND 9999 AND sc <> 8000) AS high_grad,
  COUNTIF(sc BETWEEN 3333 AND 6665) AS mid,
  COUNTIF(sc BETWEEN 1 AND 3332) AS maxreach,
  COUNTIF(sc <= 0) AS unscored_delivered,
  -- tier pcts (share of delivered)
  ROUND(100*COUNTIF(sc = 10000)/NULLIF(COUNTIF(sc IS NOT NULL),0),1) AS pct_hi,
  ROUND(100*COUNTIF(sc = 8000)/NULLIF(COUNTIF(sc IS NOT NULL),0),1) AS pct_pp,
  ROUND(100*COUNTIF(sc BETWEEN 6666 AND 9999 AND sc <> 8000)/NULLIF(COUNTIF(sc IS NOT NULL),0),1) AS pct_high_grad,
  ROUND(100*COUNTIF(sc BETWEEN 3333 AND 6665)/NULLIF(COUNTIF(sc IS NOT NULL),0),1) AS pct_mid,
  ROUND(100*COUNTIF(sc BETWEEN 1 AND 3332)/NULLIF(COUNTIF(sc IS NOT NULL),0),1) AS pct_maxreach,
  ROUND(100*COUNTIF(sc <= 0)/NULLIF(COUNTIF(sc IS NOT NULL),0),1) AS pct_unscored_delivered,
  ROUND(100*COUNTIF(sc >= 6666)/NULLIF(COUNTIF(sc IS NOT NULL),0),1) AS pct_of_delivered_high
FROM expanded
GROUP BY data_source_id, cohort
ORDER BY data_source_id, cohort DESC;
