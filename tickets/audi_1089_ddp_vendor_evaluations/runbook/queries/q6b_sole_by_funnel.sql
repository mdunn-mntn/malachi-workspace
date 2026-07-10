-- ============================================================================
-- DDP quality-score runbook, STEP 6b: sole-IP impressions split by campaign funnel
-- Claim: resolves the T1-vs-T2 attribution dispute. Serves to a vendor's sole IPs
-- via MM-audience-gated PROSPECTING campaigns required the vendor's data (the IP is
-- only in the MM membership because the vendor loaded it — including max-reach tier);
-- serves via RETARGETING campaigns did not (audience = the advertiser's own pixel).
-- True dependency = T1 (scored) + the prospecting/max-reach slice, NOT all of T2.
--
-- Grain: per data_source_id (SOLE IPs only, union-window soleness vs all 10 sources)
--   x campaign funnel_level x objective bucket: imps, media, scored-nonRTC imps/media.
-- funnel_level (public_campaigns) is authoritative for stage; objective_id shown as
-- a secondary bucket (1/5/6 = prospecting family, 4 = retargeting) for cross-checking.
-- Campaigns joined with deleted = FALSE AND is_test = FALSE; unmatched -> funnel NULL.
--
-- Windows: svs membership = 37d union (SIGNAL_START..VALUE_END); CIL = valuation week.
--
-- Run (from workspace root; ~10 TB scan class — launch in background):
--   URIS=""; for d in $(python3 -c "import datetime as t; s=t.date(2026,6,2); print(' '.join(str(s+t.timedelta(i)) for i in range(37)))"); do \
--     URIS="${URIS}gs://mntn-data-archive-prod/signals/site_visit_signal/dt=${d}/*.parquet,"; done; URIS="${URIS%,}"
--   bash .claude/scripts/bq_run.sh --ticket AUDI-1089 --label "canonical q6b sole by funnel" \
--     --external_table_definition="svs::PARQUET=${URIS}" \
--     --use_legacy_sql=false --format=csv --max_rows=500 --project_id=dw-main-silver \
--     "$(grep -v '^[[:space:]]*--' tickets/audi_1089_ddp_vendor_evaluations/runbook/queries/q6b_sole_by_funnel.sql)" \
--     > tickets/audi_1089_ddp_vendor_evaluations/outputs/run_<YYYY_MM_DD>/q6b_sole_by_funnel.csv
--
-- Parameters: UNION window 2026-06-02..2026-07-08 (37d) in URIS; VALUE week inlined below.
-- ============================================================================

WITH mem AS (
  SELECT ip,
         ARRAY_AGG(DISTINCT data_source_id)[OFFSET(0)] AS only_ds,
         COUNT(DISTINCT data_source_id) AS n_ds
  FROM (SELECT DISTINCT CAST(data_source_id AS INT64) AS data_source_id, ip
        FROM svs
        WHERE ip IS NOT NULL AND ip NOT LIKE '%:%')
  GROUP BY ip
  HAVING n_ds = 1
),

cil AS (
  SELECT
    ip,
    campaign_id,
    COUNT(*) AS imps,
    SUM(media_spend) AS media,
    COUNTIF(household_score >= 6666
            AND NOT REGEXP_CONTAINS(COALESCE(model_params, ''), r'realtime_conquest_score=10000')) AS imps_scored_nonrtc,
    SUM(IF(household_score >= 6666
           AND NOT REGEXP_CONTAINS(COALESCE(model_params, ''), r'realtime_conquest_score=10000'), media_spend, 0)) AS media_scored_nonrtc
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE DATE(time) BETWEEN '2026-07-02' AND '2026-07-08'  -- PARAM VALUE week
    AND ip IS NOT NULL AND ip NOT LIKE '%:%'
  GROUP BY ip, campaign_id
),

camp AS (
  SELECT campaign_id, funnel_level, objective_id
  FROM `dw-main-bronze.integrationprod.public_campaigns`
  WHERE deleted = FALSE AND is_test = FALSE
)

SELECT
  m.only_ds AS ds,
  c.funnel_level,
  CASE WHEN c.objective_id IN (1, 5, 6) THEN 'prospecting_family'
       WHEN c.objective_id = 4 THEN 'retargeting'
       WHEN c.objective_id IS NULL THEN 'unmatched'
       ELSE 'other' END AS obj_bucket,
  SUM(i.imps) AS imps,
  ROUND(SUM(i.media), 2) AS media,
  SUM(i.imps_scored_nonrtc) AS imps_scored_nonrtc,
  ROUND(SUM(i.media_scored_nonrtc), 2) AS media_scored_nonrtc,
  APPROX_COUNT_DISTINCT(i.ip) AS sole_ips_served
FROM cil i
JOIN mem m USING (ip)
LEFT JOIN camp c USING (campaign_id)
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;
