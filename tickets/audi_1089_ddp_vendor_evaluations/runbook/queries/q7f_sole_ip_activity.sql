-- ============================================================================
-- DDP quality-score runbook, STEP 7f: are sole IPs dark, or just unattributed?
-- Claim: adjudicates the q7 "abysmal" sole visit rates (0.01-0.03% vs platform
-- 0.7-2.9% by campaign bucket, q7e). Counts ANY clickpass event (any advertiser,
-- NO impression join) landing on each vendor's sole IPs in the valuation week,
-- split by whether the IP was served a won impression that week.
--   - If served sole IPs show ~zero unconditional clickpass activity -> the
--     households are genuinely dark; q7's 116 visits is true behavior.
--   - If they show substantial raw activity -> the ad_served_id visit join
--     undercounts this cohort (IP instability breaking attribution) and the
--     sole VR rows must be caveated/corrected.
--
-- Grain: sole-vendor ds x served flag. Membership = 37d svs union (sole = n_ds=1).
--
-- Run (from workspace root; svs 37d + clickpass week + CIL week — background class):
--   URIS=""; for d in $(python3 -c "import datetime as t; s=t.date(2026,6,2); print(' '.join(str(s+t.timedelta(i)) for i in range(37)))"); do \
--     URIS="${URIS}gs://mntn-data-archive-prod/signals/site_visit_signal/dt=${d}/*.parquet,"; done; URIS="${URIS%,}"
--   bash .claude/scripts/bq_run.sh --ticket AUDI-1089 --label "canonical q7f sole ip activity" \
--     --external_table_definition="svs::PARQUET=${URIS}" \
--     --use_legacy_sql=false --format=csv --max_rows=100 --project_id=dw-main-silver \
--     "$(grep -v '^[[:space:]]*--' tickets/audi_1089_ddp_vendor_evaluations/runbook/queries/q7f_sole_ip_activity.sql)" \
--     > tickets/audi_1089_ddp_vendor_evaluations/outputs/run_<YYYY_MM_DD>/q7f_sole_ip_activity.csv
-- ============================================================================

WITH sole AS (
  SELECT ip, MIN(CAST(data_source_id AS INT64)) AS ds
  FROM svs
  WHERE ip IS NOT NULL AND ip NOT LIKE '%:%'
  GROUP BY ip
  HAVING COUNT(DISTINCT data_source_id) = 1
),

served AS (
  SELECT DISTINCT ip
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE DATE(time) BETWEEN '2026-07-02' AND '2026-07-08'  -- PARAM VALUE week
    AND ip IS NOT NULL AND ip NOT LIKE '%:%'
),

cp AS (
  SELECT ip, COUNT(*) AS visits
  FROM `dw-main-silver.logdata.clickpass_log`
  WHERE time >= TIMESTAMP('2026-07-02') AND time < TIMESTAMP('2026-07-10')
    AND ip IS NOT NULL AND ip NOT LIKE '%:%'
  GROUP BY ip
)

SELECT
  s.ds,
  IF(sv.ip IS NOT NULL, 'served', 'not_served') AS served_flag,
  COUNT(*) AS sole_ips,
  COUNTIF(cp.ip IS NOT NULL) AS ips_with_any_clickpass,
  SUM(COALESCE(cp.visits, 0)) AS clickpass_events_any_advertiser,
  ROUND(100 * COUNTIF(cp.ip IS NOT NULL) / COUNT(*), 4) AS pct_ips_active
FROM sole s
LEFT JOIN served sv ON s.ip = sv.ip
LEFT JOIN cp ON s.ip = cp.ip
GROUP BY 1, 2
ORDER BY 1, 2;
