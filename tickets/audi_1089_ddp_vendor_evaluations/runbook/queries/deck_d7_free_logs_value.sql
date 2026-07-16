-- ============================================================================
-- AUDI-1089 DECK QUERY D7 of 7: the FREE LOGS' value beyond ALL paid vendors
-- FILLS: deck sheet FREE LOGS table (below block 3): media $/yr and profit band
--        on the slice of delivery only the free logs cover.
--
-- Claim: this measures a DIFFERENT cohort family than the vendor blocks. Vendor
-- profit (deck_d2/blocks 2-3 via q8b) = the vendor's media on IPs OUTSIDE the
-- free logs. This query flips the direction — the free side's media on IPs
-- OUTSIDE the paid roster, three cohorts in one pass:
--   guid_log_strictly_sole  = won-imp media on IPs ONLY guid_log delivered
--                             (no paid vendor, no augmentor)
--   augmentor_strictly_sole = ... ONLY augmentor (no paid vendor, no guid_log)
--   free_union_no_paid      = ... NO paid vendor delivered (either free log did)
-- The union row EXCEEDS the two strictly-sole rows summed: IPs BOTH free logs
-- hold with no paid co-holder belong to the union cohort but to neither
-- strictly-sole cohort. That gap is real signal, not double counting.
--
-- Grain: RAW 37d svs membership x CIL valuation week, IPv4 both sides (the
-- serving-cohort convention; identical to q6/q8b/q15).
--
-- ALREADY MEASURED — running this is OPTIONAL, for independent validation only:
-- the three cohorts reproduce (to the cent) q6's media_sole for ds 23/30 and
-- q15's sole media for the union: weekly media 5,336.59 (guid) / 3,220.95
-- (augmentor) / 11,594.17 (union) -> annualized $277,503 / $167,489 / $602,897.
--
-- ARCHITECTURE NOTE (cost): mem and cil are each referenced exactly once (CTE
-- re-references re-read temp external tables); the three cohorts are
-- conditional sums in ONE aggregation, unpivoted to rows at the end.
--
-- BIG SCAN (svs 37d ip pass + CIL week; ~30-45min) — background.
--
-- Run: paste this whole block into a terminal, in the folder holding this
-- file (prereqs: gcloud auth login; bq CLI; GCS read on mntn-data-archive-prod):
--   URIS=""; for d in $(python3 -c "import datetime as t; s=t.date(2026,6,2); print(' '.join(str(s+t.timedelta(i)) for i in range(37)))"); do \
--     URIS="${URIS}gs://mntn-data-archive-prod/signals/site_visit_signal/dt=${d}/*.parquet,"; done; URIS="${URIS%,}"
--   bq query --external_table_definition="svs::PARQUET=${URIS}" \
--     --use_legacy_sql=false --format=csv --max_rows=10 --project_id=dw-main-silver \
--     "$(grep -v '^[[:space:]]*--' deck_d7_free_logs_value.sql)" \
--     > deck_d7_free_logs_value.csv
--
-- Parameters: SIGNAL_START = 2026-06-02, SIGNAL_DAYS = 37; VALUE week 2026-07-02..08
-- Bit order: ds 23,24,25,26,28,30,33,36,39,40 = bits 0..9; guid-only mask = 1,
-- augmentor-only mask = 32, free mask = 33, paid mask = 990.
-- ============================================================================

WITH mem AS (
  SELECT ip,
         SUM(1 << (CASE ds WHEN 23 THEN 0 WHEN 24 THEN 1 WHEN 25 THEN 2 WHEN 26 THEN 3
                           WHEN 28 THEN 4 WHEN 30 THEN 5 WHEN 33 THEN 6 WHEN 36 THEN 7
                           WHEN 39 THEN 8 WHEN 40 THEN 9 END)) AS m
  FROM (SELECT DISTINCT CAST(data_source_id AS INT64) AS ds, ip
        FROM svs
        WHERE ip IS NOT NULL AND ip NOT LIKE '%:%')
  GROUP BY ip
),

cil AS (
  SELECT ip, COUNT(*) AS imps, SUM(media_spend) AS media
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE DATE(time) BETWEEN '2026-07-02' AND '2026-07-08'  -- PARAM VALUE week
    AND ip IS NOT NULL AND ip NOT LIKE '%:%'
  GROUP BY ip
),

agg AS (
  SELECT
    SUM(IF(m.m = 1, c.imps, 0)) AS g_imps,
    SUM(IF(m.m = 1, c.media, 0)) AS g_media,
    SUM(IF(m.m = 32, c.imps, 0)) AS a_imps,
    SUM(IF(m.m = 32, c.media, 0)) AS a_media,
    SUM(IF((m.m & 33) != 0 AND (m.m & 990) = 0, c.imps, 0)) AS u_imps,
    SUM(IF((m.m & 33) != 0 AND (m.m & 990) = 0, c.media, 0)) AS u_media
  FROM cil c
  JOIN mem m USING (ip)
)

SELECT o.cohort, o.imps_week, ROUND(o.media_week, 2) AS media_week,
       ROUND(o.media_week * 52, 2) AS media_annualized
FROM agg,
UNNEST([
  STRUCT('guid_log_strictly_sole' AS cohort, g_imps AS imps_week, g_media AS media_week),
  STRUCT('augmentor_strictly_sole', a_imps, a_media),
  STRUCT('free_union_no_paid', u_imps, u_media)
]) o
ORDER BY o.cohort;
