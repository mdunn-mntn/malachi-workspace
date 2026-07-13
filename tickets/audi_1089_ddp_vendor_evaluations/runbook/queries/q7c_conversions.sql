-- ============================================================================
-- DDP quality-score runbook, STEP 7c: conversions + revenue per vendor x cohort
-- Claim: fills the mega-pivot conversion rows — per source, for TOUCHED (all its
-- IPs that served) and SOLE (its unique IPs) cohorts: attributed conversions and
-- order revenue. Join = ui_conversions.ad_served_id -> valuation-week CIL imps
-- (same pattern as q7b's visit join; conversion trail truncated uniformly at
-- 2026-07-10 for comparability).
--
-- Attribution dedup: ui_conversions carries one row PER attribution model per
-- conversion event (models 1-16 observed 2026-07-06). Dedup to one row per
-- (advertiser_id, guid, event_epoch), preferring last-touch
-- (attribution_model_type_id 0 treated as 1 per data_knowledge), lowest model id
-- as tiebreak. Assists (conversion_assist) and disputed rows excluded.
-- Revenue = order_amt (order_amt_usd is NULL in ui_conversions — data_catalog).
--
-- Grain: data_source_id x cohort {touched, sole}. Membership = 37d svs union;
-- CIL = valuation week. imps column = anchor vs q7b/q6 (must match q7b).
--
-- Run (from workspace root; svs 37d + CIL week + ui_conversions — background class):
--   URIS=""; for d in $(python3 -c "import datetime as t; s=t.date(2026,6,2); print(' '.join(str(s+t.timedelta(i)) for i in range(37)))"); do \
--     URIS="${URIS}gs://mntn-data-archive-prod/signals/site_visit_signal/dt=${d}/*.parquet,"; done; URIS="${URIS%,}"
--   bash .claude/scripts/bq_run.sh --ticket AUDI-1089 --label "canonical q7c conversions by cohort" \
--     --external_table_definition="svs::PARQUET=${URIS}" \
--     --use_legacy_sql=false --format=csv --max_rows=100 --project_id=dw-main-silver \
--     "$(grep -v '^[[:space:]]*--' tickets/audi_1089_ddp_vendor_evaluations/runbook/queries/q7c_conversions.sql)" \
--     > tickets/audi_1089_ddp_vendor_evaluations/outputs/run_<YYYY_MM_DD>/q7c_conversions.csv
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
  SELECT ad_served_id, ip
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE DATE(time) BETWEEN '2026-07-02' AND '2026-07-08'  -- PARAM VALUE week
    AND ip IS NOT NULL AND ip NOT LIKE '%:%'
),

conv AS (
  SELECT ad_served_id, order_amt
  FROM (
    SELECT ad_served_id, order_amt,
           ROW_NUMBER() OVER (
             PARTITION BY advertiser_id, guid, event_epoch
             ORDER BY IF(attribution_model_type_id = 0, 1, attribution_model_type_id),
                      attribution_model_id
           ) AS rn
    FROM `dw-main-silver.summarydata.ui_conversions`
    WHERE time >= TIMESTAMP('2026-07-02') AND time < TIMESTAMP('2026-07-10')
      AND ad_served_id IS NOT NULL
      AND COALESCE(disputed, FALSE) = FALSE
      AND COALESCE(conversion_assist, FALSE) = FALSE
  )
  WHERE rn = 1
),

conv_by_asid AS (
  SELECT ad_served_id, COUNT(*) AS convs, SUM(COALESCE(order_amt, 0)) AS revenue
  FROM conv
  GROUP BY ad_served_id
)

SELECT
  ds,
  cohort,
  COUNT(*) AS imps,
  SUM(COALESCE(c.convs, 0)) AS conversions,
  ROUND(SUM(COALESCE(c.revenue, 0)), 2) AS revenue
FROM imps i
JOIN svs_ip s ON i.ip = s.ip
LEFT JOIN conv_by_asid c USING (ad_served_id),
UNNEST(s.ds_list) AS ds,
UNNEST(IF(s.n_ds = 1, ['touched', 'sole'], ['touched'])) AS cohort
GROUP BY ds, cohort
ORDER BY ds, cohort;
