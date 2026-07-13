-- ============================================================================
-- DDP quality-score runbook, STEP 7b: performance + avg score per vendor x cohort
-- Claim: fills the mega-pivot performance rows — per source, for TOUCHED (all its IPs
-- that served) and SOLE (its unique IPs) cohorts: won imps, visits, VR, average
-- household score (scored imps only), % scored, media. Visit join = the AUDI-1070
-- validated pattern (CIL LEFT JOIN clickpass per ad_served_id; trail truncated
-- uniformly at 2026-07-10 for comparability with the q7 pull).
--
-- Grain: data_source_id x cohort {touched, sole}. Membership = 37d svs union;
-- CIL = valuation week. avg_hs over household_score > 0 only (RT rows carry HS=-1).
--
-- Run (from workspace root; svs 37d + CIL week + clickpass — background class):
--   URIS=""; for d in $(python3 -c "import datetime as t; s=t.date(2026,6,2); print(' '.join(str(s+t.timedelta(i)) for i in range(37)))"); do \
--     URIS="${URIS}gs://mntn-data-archive-prod/signals/site_visit_signal/dt=${d}/*.parquet,"; done; URIS="${URIS%,}"
--   bash .claude/scripts/bq_run.sh --ticket AUDI-1089 --label "canonical q7b perf by cohort" \
--     --external_table_definition="svs::PARQUET=${URIS}" \
--     --use_legacy_sql=false --format=csv --max_rows=100 --project_id=dw-main-silver \
--     "$(grep -v '^[[:space:]]*--' tickets/audi_1089_ddp_vendor_evaluations/runbook/queries/q7b_perf_by_cohort.sql)" \
--     > tickets/audi_1089_ddp_vendor_evaluations/outputs/run_<YYYY_MM_DD>/q7b_perf_by_cohort.csv
-- ============================================================================

WITH svs_ip AS (
  SELECT ip,
         ARRAY_AGG(DISTINCT CAST(data_source_id AS INT64)) AS ds_list,
         COUNT(DISTINCT data_source_id) AS n_ds
  FROM svs
  WHERE ip IS NOT NULL AND ip NOT LIKE '%:%'
  GROUP BY ip
),

imps AS (
  SELECT ad_served_id, ip, household_score, media_spend
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE DATE(time) BETWEEN '2026-07-02' AND '2026-07-08'  -- PARAM VALUE week
    AND ip IS NOT NULL AND ip NOT LIKE '%:%'
),

vis AS (
  SELECT ad_served_id, COUNT(*) AS visits
  FROM `dw-main-silver.logdata.clickpass_log`
  WHERE time >= TIMESTAMP('2026-07-02') AND time < TIMESTAMP('2026-07-10')
    AND ad_served_id IS NOT NULL
  GROUP BY ad_served_id
)

SELECT
  ds,
  cohort,
  COUNT(*) AS imps,
  APPROX_COUNT_DISTINCT(i.ip) AS ips_served,
  SUM(COALESCE(v.visits, 0)) AS visits,
  ROUND(100 * SUM(COALESCE(v.visits, 0)) / COUNT(*), 4) AS vr_pct,
  ROUND(AVG(IF(i.household_score > 0, i.household_score, NULL)), 0) AS avg_hs_scored,
  ROUND(100 * COUNTIF(i.household_score > 0) / COUNT(*), 1) AS pct_scored,
  ROUND(SUM(i.media_spend), 2) AS media
FROM imps i
JOIN svs_ip s ON i.ip = s.ip
LEFT JOIN vis v USING (ad_served_id),
UNNEST(s.ds_list) AS ds,
UNNEST(IF(s.n_ds = 1, ['touched', 'sole'], ['touched'])) AS cohort
GROUP BY ds, cohort
ORDER BY ds, cohort;
