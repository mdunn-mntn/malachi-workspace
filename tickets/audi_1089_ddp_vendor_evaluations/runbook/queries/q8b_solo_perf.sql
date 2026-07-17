-- ============================================================================
-- DDP quality-score runbook, STEP 8b: SOLO counterfactual — serving & performance
-- Claim: the numbers sheet's SOLE cohort (IPs no other source delivers, incl. free
-- logs AND the 7 other paid vendors) understates each vendor's individual impact.
-- The SOLO cohort assumes the vendor is the ONLY paid source: an IP is solo to V iff
-- V delivered it in the 37d union and NEITHER free log did (for the free-log columns:
-- the OTHER free log didn't). Solo cohort ⊇ sole cohort by construction, so every
-- measured metric here must be >= its q6/q7b/q7c sole counterpart (anchor).
--
-- Grain: data_source_id (solo cohort only). Membership = raw 37d svs union, IPv4,
-- NO usable-domain gate (q5/q6/q7b/q7c world). CIL = valuation week. Visits =
-- clickpass_log per ad_served_id (canonical q7b join; trail to 2026-07-10).
-- Conversions = ui_conversions with q7c's dedup verbatim: one row per (advertiser_id,
-- guid, event_epoch), prefer last-touch (model 0 treated as 1), assists/disputed
-- excluded, revenue = order_amt. avg_hs over household_score > 0 only (RT rows carry
-- HS=-1). Scored non-RTC = hs >= 6666 AND NOT model_params realtime_conquest_score=10000.
-- ips_solo = exact COUNT(DISTINCT ip) (no APPROX — feeds anchors).
--
-- Output: ONE CSV (rec, ds, k, v):
--   rec='serve' k in {imps, ips_solo, media, data, imps_scored_nonrtc, media_scored,
--                     data_scored, avg_hs, imps_hs_pos}  -- pct_scored = imps_hs_pos/imps
--   rec='perf'  k in {visits, conversions, revenue}
--   rec='tier'  k in {hi, pp, hg}   -- per-IP MAX(household_score) over CIL week:
--                                       hi=10000, pp=8000, hg=6666-9999 excl 8000
--
-- Validation anchors: serve metrics >= q6 sole per ds (superset monotonicity);
-- tier hi/pp vs Σ q3d solo-mask rows is a DIAGNOSTIC comparison, NOT an equality —
-- raw (here) vs usable-gated (q3d) membership lenses differ by 3-10% for clean
-- vendors and +55-68% for Sovrn (junk-carried IPs); see MANIFEST anchors.
--
-- BIG SCAN (svs 37d ip-only pass + CIL week x2 + clickpass 8d + ui_conversions 8d;
-- ~45-60min) — background, never preempt.
-- KNOWN COST DEBT (2026-07-16 verify-pass): the `solo` CTE (transitively reads the
-- svs externals) is referenced TWICE (main + tiers) → externals re-read 2x. Results
-- unaffected (deterministic scan); fold tiers into main's join before the next rerun.
--
-- Run (from workspace root):
--   URIS=""; for d in $(python3 -c "import datetime as t; s=t.date(2026,6,2); print(' '.join(str(s+t.timedelta(i)) for i in range(37)))"); do \
--     URIS="${URIS}gs://mntn-data-archive-prod/signals/site_visit_signal/dt=${d}/*.parquet,"; done; URIS="${URIS%,}"
--   bash .claude/scripts/bq_run.sh --ticket AUDI-1089 --label "canonical q8b solo perf" \
--     --external_table_definition="svs::PARQUET=${URIS}" \
--     --use_legacy_sql=false --format=csv --max_rows=2000 --project_id=dw-main-silver \
--     "$(grep -v '^[[:space:]]*--' tickets/audi_1089_ddp_vendor_evaluations/runbook/queries/q8b_solo_perf.sql)" \
--     > tickets/audi_1089_ddp_vendor_evaluations/outputs/run_<YYYY_MM_DD>/q8b_solo_perf.csv
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

solo AS (
  SELECT ip, ds
  FROM mem37, UNNEST([23, 24, 25, 26, 28, 30, 33, 36, 39, 40]) AS ds
  WHERE (m >> (CASE ds WHEN 23 THEN 0 WHEN 24 THEN 1 WHEN 25 THEN 2 WHEN 26 THEN 3
                       WHEN 28 THEN 4 WHEN 30 THEN 5 WHEN 33 THEN 6 WHEN 36 THEN 7
                       WHEN 39 THEN 8 WHEN 40 THEN 9 END)) & 1 = 1
    AND (m & (CASE WHEN ds = 23 THEN 32 WHEN ds = 30 THEN 1 ELSE 33 END)) = 0
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

main AS (
  SELECT
    s.ds,
    COUNT(*) AS imps,
    COUNT(DISTINCT i.ip) AS ips_solo,
    ROUND(SUM(i.media_spend), 2) AS media,
    ROUND(SUM(i.data_spend), 2) AS data,
    COUNTIF(i.scored_nonrtc) AS imps_scored_nonrtc,
    ROUND(SUM(IF(i.scored_nonrtc, i.media_spend, 0)), 2) AS media_scored,
    ROUND(SUM(IF(i.scored_nonrtc, i.data_spend, 0)), 2) AS data_scored,
    ROUND(AVG(IF(i.household_score > 0, i.household_score, NULL)), 1) AS avg_hs,
    COUNTIF(i.household_score > 0) AS imps_hs_pos,
    SUM(COALESCE(v.visits, 0)) AS visits,
    SUM(COALESCE(c.convs, 0)) AS conversions,
    ROUND(SUM(COALESCE(c.revenue, 0)), 2) AS revenue
  FROM imps i
  JOIN solo s ON i.ip = s.ip
  LEFT JOIN vis v USING (ad_served_id)
  LEFT JOIN conv_by_asid c USING (ad_served_id)
  GROUP BY s.ds
),

ip_max AS (
  SELECT ip, MAX(household_score) AS msc
  FROM imps
  GROUP BY ip
),

tiers AS (
  SELECT s.ds,
         COUNTIF(msc = 10000) AS hi,
         COUNTIF(msc = 8000) AS pp,
         COUNTIF(msc BETWEEN 6666 AND 9999 AND msc != 8000) AS hg
  FROM ip_max
  JOIN solo s USING (ip)
  GROUP BY s.ds
)

SELECT 'serve' AS rec, ds, kv.k, kv.v
FROM main, UNNEST([
  STRUCT('imps' AS k, CAST(imps AS FLOAT64) AS v),
  STRUCT('ips_solo', CAST(ips_solo AS FLOAT64)),
  STRUCT('media', media),
  STRUCT('data', data),
  STRUCT('imps_scored_nonrtc', CAST(imps_scored_nonrtc AS FLOAT64)),
  STRUCT('media_scored', media_scored),
  STRUCT('data_scored', data_scored),
  STRUCT('avg_hs', avg_hs),
  STRUCT('imps_hs_pos', CAST(imps_hs_pos AS FLOAT64))
]) AS kv
UNION ALL
SELECT 'perf', ds, kv.k, kv.v
FROM main, UNNEST([
  STRUCT('visits' AS k, CAST(visits AS FLOAT64) AS v),
  STRUCT('conversions', CAST(conversions AS FLOAT64)),
  STRUCT('revenue', revenue)
]) AS kv
UNION ALL
SELECT 'tier', ds, kv.k, kv.v
FROM tiers, UNNEST([
  STRUCT('hi' AS k, CAST(hi AS FLOAT64) AS v),
  STRUCT('pp', CAST(pp AS FLOAT64)),
  STRUCT('hg', CAST(hg AS FLOAT64))
]) AS kv
ORDER BY rec, ds, k;
