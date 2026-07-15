-- NOTE: file holds TWO statements; the shipped q7_sole_vr.csv holds Query B's
-- per-ds rows (run statements separately).
-- Claim: sole-cohort visit performance — visits, IVR (vs the 0.0223% no-svs baseline)
-- and Poisson CI inputs on sole-IP serves. Output q7_sole_vr.csv.
-- Run (from workspace root; svs 37d union external table + internal CIL):
--   URIS=""; for d in $(python3 -c "import datetime as t; s=t.date(2026,6,2); print(' '.join(str(s+t.timedelta(i)) for i in range(37)))"); do \
--     URIS="${URIS}gs://mntn-data-archive-prod/signals/site_visit_signal/dt=${d}/*.parquet,"; done; URIS="${URIS%,}"
--   bash .claude/scripts/bq_run.sh --ticket AUDI-1089 --label "<label>" \
--     --external_table_definition="svs::PARQUET=${URIS}" \
--     --use_legacy_sql=false --format=csv --max_rows=100 --project_id=dw-main-silver \
--     "$(grep -v '^[[:space:]]*--' <this file>)" > outputs/run_<YYYY_MM_DD>/<output>.csv
--
-- CANONICAL runbook copy of queries/audi_1089_q5_vr_membership.sql (AUDI-1089 eval, windows per runbook params).
-- Output: outputs/run_<date>/q7_sole_vr.csv. Full run pattern in the original header below.
--
-- AUDI-1089 legacy step Q5 (UNRELATED to runbook q5_score_tiers.sql): PERFORMANCE — do impressions to Klickly-sole (DS39) IPs produce visits at parity?
-- Visit join pattern reused EXACTLY from AUDI-1070 inv2 (validated): cost_impression_log LEFT JOIN
-- clickpass_log aggregated per ad_served_id; visit window trails the impression window; RTC rows
-- excluded via model_params NOT LIKE '%realtime_conquest_score=10000%' so band 10000 = Fangorn pins.
-- Here the visit trail is truncated at the data edge (run date 2026-07-09) — truncation is uniform
-- across membership classes, so VR comparisons remain apples-to-apples.
--
-- svs = TEMP EXTERNAL TABLE over GCS parquet (site_visit_signal), 37-day union window
-- dt=2026-06-02..2026-07-08. Run pattern (per TI-1027):
--   URIS=""; for d in <days>; do URIS="${URIS}gs://mntn-data-archive-prod/signals/site_visit_signal/dt=<date>/*.parquet,"; done; URIS="${URIS%,}"
--   bq query --external_table_definition="svs::PARQUET=${URIS}" --use_legacy_sql=false --project_id=dw-main-silver ... 'SQL'
--
-- CIL valuation week: DATE(time) BETWEEN '2026-07-02' AND '2026-07-08'.
-- IP hygiene (both sides): ip IS NOT NULL AND ip NOT LIKE '%:%' (IPv6 excluded; quantified in Q1).
-- DS roster in svs: external DDPs 24 Justuno, 25 5x5, 26 Predactiv, 28 33Across, 33 Sovrn,
-- 36 Cybba, 39 Klickly (FOCAL), 40 33Across API; internal 23 guid_log, 30 augmentor.

-- ============================================================
-- Query A: membership class x score band -> imps, distinct IPs, visits, VR
--   membership: 1_klickly_sole (DS39 only) / 2_klickly_shared (DS39 + others)
--               / 3_other_svs (svs, no DS39) / 4_no_svs (delivered IP absent from svs 37d)
--   bands: 1_10000 / 2_8000_9999 / 3_6666_7999 / 4_1_6665 / 5_unscored (HS NULL or <=0; RT rows HS=-1)
-- Output: outputs/run_<date>/q7_sole_vr.csv
-- ============================================================
WITH svs_ip AS (
  SELECT ip,
         LOGICAL_OR(data_source_id = 39) AS has_39,
         COUNT(DISTINCT data_source_id) AS n_ds
  FROM svs
  WHERE ip IS NOT NULL AND ip NOT LIKE '%:%'
  GROUP BY ip
),
imps AS (
  SELECT
    c.ad_served_id, c.ip,
    CASE
      WHEN c.household_score IS NULL OR c.household_score <= 0 THEN '5_unscored'
      WHEN c.household_score BETWEEN 1 AND 6665 THEN '4_1_6665'
      WHEN c.household_score BETWEEN 6666 AND 7999 THEN '3_6666_7999'
      WHEN c.household_score BETWEEN 8000 AND 9999 THEN '2_8000_9999'
      WHEN c.household_score = 10000 THEN '1_10000'
    END AS band
  FROM `dw-main-silver.logdata.cost_impression_log` c
  WHERE DATE(c.time) BETWEEN '2026-07-02' AND '2026-07-08'
    AND c.model_params NOT LIKE '%realtime_conquest_score=10000%'
    AND c.ip IS NOT NULL AND c.ip NOT LIKE '%:%'
),
vis AS (
  SELECT ad_served_id, COUNT(*) AS visits
  FROM `dw-main-silver.logdata.clickpass_log`
  WHERE time >= TIMESTAMP('2026-07-02') AND time < TIMESTAMP('2026-07-10')
    AND ad_served_id IS NOT NULL
  GROUP BY ad_served_id
)
SELECT
  CASE
    WHEN s.ip IS NULL THEN '4_no_svs'
    WHEN s.has_39 AND s.n_ds = 1 THEN '1_klickly_sole'
    WHEN s.has_39 THEN '2_klickly_shared'
    ELSE '3_other_svs'
  END AS membership,
  i.band,
  COUNT(*) AS imps,
  COUNT(DISTINCT i.ip) AS distinct_ips,
  SUM(IFNULL(v.visits, 0)) AS visits,
  ROUND(100 * SUM(IFNULL(v.visits, 0)) / COUNT(*), 4) AS vr_pct
FROM imps i
LEFT JOIN svs_ip s USING (ip)
LEFT JOIN vis v USING (ad_served_id)
GROUP BY membership, band
ORDER BY membership, band;

-- ============================================================
-- Query B: per-ds SOLE-IP VR for all 10 ds (seeds the other six vendor evals).
-- sole_ds = the single ds that saw the IP in the 37d window (MIN over n_ds=1, per TI-1027 pattern).
-- One row per ds: sole delivered IPs, sole imps, visits, VR overall + at band 10000.
-- Output: outputs/run_<date>/q7_sole_vr.csv
-- ============================================================
WITH svs_ip AS (
  SELECT ip,
         COUNT(DISTINCT data_source_id) AS n_ds,
         MIN(data_source_id) AS sole_ds
  FROM svs
  WHERE ip IS NOT NULL AND ip NOT LIKE '%:%'
  GROUP BY ip
),
imps AS (
  SELECT
    c.ad_served_id, c.ip,
    CASE
      WHEN c.household_score IS NULL OR c.household_score <= 0 THEN '5_unscored'
      WHEN c.household_score BETWEEN 1 AND 6665 THEN '4_1_6665'
      WHEN c.household_score BETWEEN 6666 AND 7999 THEN '3_6666_7999'
      WHEN c.household_score BETWEEN 8000 AND 9999 THEN '2_8000_9999'
      WHEN c.household_score = 10000 THEN '1_10000'
    END AS band
  FROM `dw-main-silver.logdata.cost_impression_log` c
  WHERE DATE(c.time) BETWEEN '2026-07-02' AND '2026-07-08'
    AND c.model_params NOT LIKE '%realtime_conquest_score=10000%'
    AND c.ip IS NOT NULL AND c.ip NOT LIKE '%:%'
),
vis AS (
  SELECT ad_served_id, COUNT(*) AS visits
  FROM `dw-main-silver.logdata.clickpass_log`
  WHERE time >= TIMESTAMP('2026-07-02') AND time < TIMESTAMP('2026-07-10')
    AND ad_served_id IS NOT NULL
  GROUP BY ad_served_id
)
SELECT
  s.sole_ds AS data_source_id,
  COUNT(DISTINCT i.ip) AS sole_ips_delivered,
  COUNT(*) AS sole_imps,
  SUM(IFNULL(v.visits, 0)) AS sole_visits,
  ROUND(100 * SUM(IFNULL(v.visits, 0)) / COUNT(*), 4) AS vr_overall_pct,
  COUNTIF(i.band = '1_10000') AS imps_10000,
  SUM(IF(i.band = '1_10000', IFNULL(v.visits, 0), 0)) AS visits_10000,
  ROUND(100 * SUM(IF(i.band = '1_10000', IFNULL(v.visits, 0), 0))
        / NULLIF(COUNTIF(i.band = '1_10000'), 0), 4) AS vr_10000_pct
FROM imps i
JOIN svs_ip s USING (ip)
LEFT JOIN vis v USING (ad_served_id)
WHERE s.n_ds = 1
GROUP BY data_source_id
ORDER BY data_source_id;
