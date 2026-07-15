-- ============================================================================
-- DDP quality-score runbook, STEP 15: free_logs COMBINED pseudo-vendor (guid+augmentor
-- as ONE source) — measured touched/sole cohorts for the numbers+solo sheet column
-- Claim: the workbook treats guid_log (DS23) and augmentor (DS30) as separate columns;
-- treating them as ONE vendor ("free_logs") requires UNION cohorts (unique counts are
-- NOT additive — the two logs overlap heavily). This scan measures the union cohorts:
--   touched = IP delivered by DS23 OR DS30 in the 37d union
--   sole    = touched AND no PAID vendor delivered the IP (mask & ~free == 0)
-- and outputs the q5/q6/q7b/q7c/q7-equivalent fields so fill_template can inject the
-- combined column (pseudo-ds 99) into every existing row formula.
--
-- Grain: cohort {touched, sole}. Membership = raw 37d svs union, IPv4 (q5/q6 world).
-- CIL = valuation week 2026-07-02..08; visits = clickpass per ad_served_id (trail
-- 07-10); conversions = ui_conversions with q7c's dedup verbatim. avg_hs over hs>0
-- (RT rows carry -1). Scored non-RTC gate as q6. Single mem reference (q8b lesson).
--
-- Output: ONE CSV (rec, k1, k2, v); k1 = cohort:
--   rec='mem'   k2='member_ips'
--   rec='serve' k2 in {ips_served, imps, media, data, imps_scored_nonrtc, media_scored,
--                      data_scored, avg_hs, imps_hs_pos}
--   rec='perf'  k2 in {visits, conversions, revenue}
--   rec='tier'  k2 in {hi, pp, hg, mid, maxreach, unscored}  (per-IP MAX(hs), served)
--
-- Validation anchors: touched member_ips <= q5 vendor_ips(23)+q5 vendor_ips(30) and
-- >= max of the two (union bounds); sole tier counts vs q3d masks (bits subset of 0|5);
-- serve imps consistency checked in fill_template.
--
-- BIG SCAN (svs 37d 2-col + CIL week + clickpass + ui_conversions; ~45-60min) —
-- background, never preempt.
--
-- Run (from workspace root):
--   URIS=""; for d in $(python3 -c "import datetime as t; s=t.date(2026,6,2); print(' '.join(str(s+t.timedelta(i)) for i in range(37)))"); do \
--     URIS="${URIS}gs://mntn-data-archive-prod/signals/site_visit_signal/dt=${d}/*.parquet,"; done; URIS="${URIS%,}"
--   bash .claude/scripts/bq_run.sh --ticket AUDI-1089 --label "canonical q15 free union perf" \
--     --external_table_definition="svs::PARQUET=${URIS}" \
--     --use_legacy_sql=false --format=csv --max_rows=100 --project_id=dw-main-silver \
--     "$(grep -v '^[[:space:]]*--' tickets/audi_1089_ddp_vendor_evaluations/runbook/queries/q15_free_union_perf.sql)" \
--     > tickets/audi_1089_ddp_vendor_evaluations/outputs/run_<YYYY_MM_DD>/q15_free_union_perf.csv
--
-- Parameters: SIGNAL_START = 2026-06-02, SIGNAL_DAYS = 37; VALUE week 2026-07-02..08
-- ============================================================================

WITH mem37 AS (
  SELECT ip,
         SUM(1 << (CASE ds WHEN 23 THEN 0 WHEN 24 THEN 1 WHEN 25 THEN 2 WHEN 26 THEN 3
                           WHEN 28 THEN 4 WHEN 30 THEN 5 WHEN 33 THEN 6 WHEN 36 THEN 7
                           WHEN 39 THEN 8 WHEN 40 THEN 9 END)) AS m
  FROM (SELECT DISTINCT CAST(data_source_id AS INT64) AS ds, ip
        FROM svs
        WHERE ip IS NOT NULL AND ip NOT LIKE '%:%')
  GROUP BY ip
),

cohorts AS (
  SELECT ip, cohort
  FROM mem37,
  UNNEST(IF((m & 33) != 0,
            IF((m & ~33 & 1023) = 0, ['touched', 'sole'], ['touched']),
            [])) AS cohort
),

imps AS (
  SELECT ad_served_id, ip, household_score, media_spend, data_spend,
         (household_score >= 6666
          AND NOT REGEXP_CONTAINS(COALESCE(model_params, ''), r'realtime_conquest_score=10000')) AS scored_nonrtc
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
),

ip_stats AS (
  SELECT i.ip,
         COUNT(*) AS imps,
         SUM(i.media_spend) AS media,
         SUM(i.data_spend) AS data,
         COUNTIF(i.scored_nonrtc) AS imps_scored_nonrtc,
         SUM(IF(i.scored_nonrtc, i.media_spend, 0)) AS media_scored,
         SUM(IF(i.scored_nonrtc, i.data_spend, 0)) AS data_scored,
         SUM(IF(i.household_score > 0, i.household_score, 0)) AS hs_sum,
         COUNTIF(i.household_score > 0) AS imps_hs_pos,
         SUM(COALESCE(v.visits, 0)) AS visits,
         SUM(COALESCE(c.convs, 0)) AS convs,
         SUM(COALESCE(c.revenue, 0)) AS revenue,
         MAX(i.household_score) AS msc
  FROM imps i
  LEFT JOIN vis v USING (ad_served_id)
  LEFT JOIN conv_by_asid c USING (ad_served_id)
  GROUP BY i.ip
),

agg AS (
  SELECT co.cohort,
         COUNT(*) AS member_ips,
         COUNTIF(s.ip IS NOT NULL) AS ips_served,
         SUM(COALESCE(s.imps, 0)) AS imps,
         ROUND(SUM(COALESCE(s.media, 0)), 2) AS media,
         ROUND(SUM(COALESCE(s.data, 0)), 2) AS data,
         SUM(COALESCE(s.imps_scored_nonrtc, 0)) AS imps_scored_nonrtc,
         ROUND(SUM(COALESCE(s.media_scored, 0)), 2) AS media_scored,
         ROUND(SUM(COALESCE(s.data_scored, 0)), 2) AS data_scored,
         ROUND(SAFE_DIVIDE(SUM(COALESCE(s.hs_sum, 0)), SUM(COALESCE(s.imps_hs_pos, 0))), 1) AS avg_hs,
         SUM(COALESCE(s.imps_hs_pos, 0)) AS imps_hs_pos,
         SUM(COALESCE(s.visits, 0)) AS visits,
         SUM(COALESCE(s.convs, 0)) AS conversions,
         ROUND(SUM(COALESCE(s.revenue, 0)), 2) AS revenue,
         COUNTIF(s.msc = 10000) AS hi,
         COUNTIF(s.msc = 8000) AS pp,
         COUNTIF(s.msc BETWEEN 6666 AND 9999 AND s.msc != 8000) AS hg,
         COUNTIF(s.msc BETWEEN 3333 AND 6665) AS mid,
         COUNTIF(s.msc BETWEEN 1 AND 3332) AS maxreach,
         COUNTIF(s.msc <= 0) AS unscored
  FROM cohorts co
  LEFT JOIN ip_stats s USING (ip)
  GROUP BY co.cohort
)

SELECT rec, k1, k2, v
FROM agg, UNNEST([
  STRUCT('mem' AS rec, cohort AS k1, 'member_ips' AS k2, CAST(member_ips AS FLOAT64) AS v),
  STRUCT('serve', cohort, 'ips_served', CAST(ips_served AS FLOAT64)),
  STRUCT('serve', cohort, 'imps', CAST(imps AS FLOAT64)),
  STRUCT('serve', cohort, 'media', media),
  STRUCT('serve', cohort, 'data', data),
  STRUCT('serve', cohort, 'imps_scored_nonrtc', CAST(imps_scored_nonrtc AS FLOAT64)),
  STRUCT('serve', cohort, 'media_scored', media_scored),
  STRUCT('serve', cohort, 'data_scored', data_scored),
  STRUCT('serve', cohort, 'avg_hs', avg_hs),
  STRUCT('serve', cohort, 'imps_hs_pos', CAST(imps_hs_pos AS FLOAT64)),
  STRUCT('perf', cohort, 'visits', CAST(visits AS FLOAT64)),
  STRUCT('perf', cohort, 'conversions', CAST(conversions AS FLOAT64)),
  STRUCT('perf', cohort, 'revenue', revenue),
  STRUCT('tier', cohort, 'hi', CAST(hi AS FLOAT64)),
  STRUCT('tier', cohort, 'pp', CAST(pp AS FLOAT64)),
  STRUCT('tier', cohort, 'hg', CAST(hg AS FLOAT64)),
  STRUCT('tier', cohort, 'mid', CAST(mid AS FLOAT64)),
  STRUCT('tier', cohort, 'maxreach', CAST(maxreach AS FLOAT64)),
  STRUCT('tier', cohort, 'unscored', CAST(unscored AS FLOAT64))
])
ORDER BY rec, k1, k2;
