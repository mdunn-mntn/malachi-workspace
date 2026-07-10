-- CANONICAL runbook copy of queries/audi_1089_q4_value_tiers.sql (AUDI-1089 eval, windows per runbook params).
-- Output: outputs/run_<date>/q6_value_tiers.csv. Full run pattern in the original header below.
--
-- AUDI-1089 Q4: value tiers (media/data-cost lens ONLY — no platform_spend; take rates sensitive)
-- Klickly (DS39) focal; ALL data_source_ids computed for reuse by later vendor evals.
-- Substrate: gs://mntn-data-archive-prod/signals/site_visit_signal/dt=/hh=/data_source_id=N/*.parquet
-- Run pattern (temp external table over GCS parquet; zzz_temp.site_visit_signal is manual/stale):
--   URIS=""; for d in <2026-06-02..2026-07-08>; do URIS="${URIS}gs://mntn-data-archive-prod/signals/site_visit_signal/dt=<date>/*.parquet,"; done; URIS="${URIS%,}"
--   bq query --external_table_definition="svs::PARQUET=${URIS}" --use_legacy_sql=false --project_id=dw-main-silver ...
-- Windows:
--   svs membership = UNION window dt 2026-06-02..2026-07-08 (37 days) — IP membership joined to delivery
--   CIL valuation week = DATE(time) BETWEEN '2026-07-02' AND '2026-07-08' (dw-main-silver.logdata.cost_impression_log)
-- IP hygiene: ip IS NOT NULL AND ip NOT LIKE '%:%' (IPv6 excluded; quantified separately in Q1) — applied to BOTH sides.
-- Scored non-RTC = household_score >= 6666 AND NOT model_params token realtime_conquest_score=10000
--   (the realtime_conquest_score KEY appears on all rows; VALUE 10000 = RTC fired).
-- Tiers per ds: T1 = sole + scored + non-RTC (imps/media/data), T2 = sole, T3 = touched.
-- Reference: ti_1027_analysis_queries.sql PHASE 2 WTP anchor (lines 156-166), generalized per-ds.

-- ============================================================
-- Q4a: per-ds value tiers → outputs/audi_1089_value_tiers.csv
-- "touched" = delivered (CIL week) IPs seen by the ds in the 37d svs window (INNER JOIN, matches TI-1027 anchor).
-- "sole"    = touched AND the IP appears under exactly one data_source_id in the window (n_ds = 1).
-- ============================================================
WITH mem AS (
  SELECT ip,
         ARRAY_AGG(DISTINCT data_source_id) AS ds_list,
         COUNT(DISTINCT data_source_id) AS n_ds
  FROM (SELECT DISTINCT data_source_id, ip
        FROM svs
        WHERE ip IS NOT NULL AND ip NOT LIKE '%:%')
  GROUP BY ip
),
cil AS (
  SELECT ip,
         COUNT(*) AS imps,
         SUM(media_spend) AS media,
         SUM(data_spend) AS data,
         COUNTIF(household_score >= 6666
                 AND NOT REGEXP_CONTAINS(COALESCE(model_params, ''), r'realtime_conquest_score=10000')) AS imps_scored_nonrtc,
         SUM(IF(household_score >= 6666
                AND NOT REGEXP_CONTAINS(COALESCE(model_params, ''), r'realtime_conquest_score=10000'), media_spend, 0)) AS media_scored_nonrtc,
         SUM(IF(household_score >= 6666
                AND NOT REGEXP_CONTAINS(COALESCE(model_params, ''), r'realtime_conquest_score=10000'), data_spend, 0)) AS data_scored_nonrtc
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE DATE(time) BETWEEN '2026-07-02' AND '2026-07-08'
    AND ip IS NOT NULL AND ip NOT LIKE '%:%'
  GROUP BY ip
)
SELECT ds AS data_source_id,
       COUNT(*) AS ips_touched,
       COUNTIF(m.n_ds = 1) AS ips_sole,
       SUM(c.imps) AS imps_touched,
       SUM(IF(m.n_ds = 1, c.imps, 0)) AS imps_sole,
       ROUND(SUM(c.media), 2) AS media_touched,
       ROUND(SUM(IF(m.n_ds = 1, c.media, 0)), 2) AS media_sole,
       ROUND(SUM(c.data), 2) AS data_touched,
       ROUND(SUM(IF(m.n_ds = 1, c.data, 0)), 2) AS data_sole,
       SUM(IF(m.n_ds = 1, c.imps_scored_nonrtc, 0)) AS imps_sole_scored_nonrtc,
       ROUND(SUM(IF(m.n_ds = 1, c.media_scored_nonrtc, 0)), 2) AS media_sole_scored,
       ROUND(SUM(IF(m.n_ds = 1, c.data_scored_nonrtc, 0)), 2) AS data_sole_scored
FROM mem m
JOIN cil c USING (ip),
UNNEST(m.ds_list) AS ds
GROUP BY ds
ORDER BY ds;

-- ============================================================
-- Q4b (check A): of DELIVERED IPs in the CIL week with household_score >= 6666,
-- what fraction r has NO svs membership at all (any ds, 37d union window)?
-- → outputs/audi_1089_check_scored_no_svs.csv
-- ============================================================
WITH mem AS (
  SELECT DISTINCT ip FROM svs WHERE ip IS NOT NULL AND ip NOT LIKE '%:%'
),
scored AS (
  SELECT DISTINCT ip
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE DATE(time) BETWEEN '2026-07-02' AND '2026-07-08'
    AND ip IS NOT NULL AND ip NOT LIKE '%:%'
    AND household_score >= 6666
)
SELECT COUNT(*) AS scored_delivered_ips,
       COUNTIF(m.ip IS NULL) AS ips_with_no_svs,
       ROUND(COUNTIF(m.ip IS NULL) / COUNT(*), 4) AS r
FROM scored s
LEFT JOIN mem m USING (ip);
