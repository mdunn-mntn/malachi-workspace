-- ============================================================================
-- DDP quality-score runbook, STEP 13b: DS19 membership — free-covered vs vendor-only,
-- score-tier mix + measured performance of the slice free-logs-only would LOSE
-- Claim: vendor removal cannot touch Fangorn (DS46, guid-only) and barely touches HI/PP
-- (q3d: free-only keeps 99.94%/99.25%); the exposed layer is DS19 "MM Core"/Keyword-Only
-- (unlocks Max Reach). This scan splits the DS19-member IP universe into
--   free_covered  — a free log (guid DS23 / augmentor DS30) delivered >=1 DS19-matched
--                   row for the IP in the 37d union (membership SURVIVES free-only)
--   vendor_only   — membership exists only via paid vendors (LOST under free-only)
-- and measures, per cohort: size, serving, media, visits (performance), and the per-IP
-- MAX(household_score) tier mix (hi/pp/hg/mid/MAXREACH/unscored) — the direct test of
-- "vendor loss lands in the max-reach / keyword-only tier".
--
-- DS19 membership row gates mirror q2c/airflow-ti (composite_key match, no blocklist).
-- Cohort membership: raw 37d union, IPv4. CIL = valuation week 2026-07-02..08; visits =
-- clickpass per ad_served_id, trail to 07-10 (q7b pattern). Single-pass design: mem is
-- referenced ONCE (adversarial review 2026-07-15 - CTE re-references re-read the external
-- svs+pc subtree; the naive 3-reference layout tripled the scan to ~30TB).
--
-- Output: ONE CSV (rec, k1, k2, v):
--   rec='mem'   k1=cohort, k2='member_ips'      (37d DS19-member IPs)
--   rec='serve' k1=cohort, k2 in {ips_served, imps, media, data, visits}
--   rec='tier'  k1=cohort, k2 in {hi, pp, hg, mid, maxreach, unscored}
--               (per-IP MAX(household_score) over CIL week: 10000 / 8000 / 6666-9999
--                excl 8000 / 3333-6665 / 1-3332 / <=0)
--
-- Validation anchors: free_covered share of member IPs ~ q13a IP-grain coverage;
-- tier hi free-covered share >= 99% expected (q3d consistency); vendor_only VR vs
-- free_covered VR vs q7e platform baselines (2.89/1.11/0.72%) = the performance answer;
-- vendor_only media x52 = DS19-lens dependent revenue at risk.
--
-- BIG SCAN (svs 37d + pc + CIL week + clickpass 8d; ~1h) — background, never preempt.
--
-- Run (from workspace root):
--   URIS=""; for d in $(python3 -c "import datetime as t; s=t.date(2026,6,2); print(' '.join(str(s+t.timedelta(i)) for i in range(37)))"); do \
--     URIS="${URIS}gs://mntn-data-archive-prod/signals/site_visit_signal/dt=${d}/*.parquet,"; done; URIS="${URIS%,}"
--   bash .claude/scripts/bq_run.sh --ticket AUDI-1089 --label "canonical q13b ds19 perf" \
--     --external_table_definition="pc::PARQUET=gs://mntn-data-archive-prod/shopper_graph/product_categorization/*.parquet" \
--     --external_table_definition="svs::PARQUET=${URIS}" \
--     --use_legacy_sql=false --format=csv --max_rows=100 --project_id=dw-main-silver \
--     "$(grep -v '^[[:space:]]*--' tickets/audi_1089_ddp_vendor_evaluations/runbook/queries/q13b_ds19_perf.sql)" \
--     > tickets/audi_1089_ddp_vendor_evaluations/outputs/run_<YYYY_MM_DD>/q13b_ds19_perf.csv
--
-- Parameters: SIGNAL_START = 2026-06-02, SIGNAL_DAYS = 37; VALUE week 2026-07-02..08
-- ============================================================================

WITH pc_k AS (
  SELECT DISTINCT composite_key
  FROM pc
  WHERE (SELECT COUNT(*) FROM UNNEST(data_source_category_id.list) x
         WHERE SAFE_CAST(x.element AS INT64) >= 900000) > 0
),

mem AS (
  SELECT ip,
         IF(LOGICAL_OR(CAST(s.data_source_id AS INT64) IN (23, 30)),
            'free_covered', 'vendor_only') AS cohort
  FROM svs s
  JOIN pc_k k ON CONCAT(SPLIT(s.url, '?')[SAFE_OFFSET(0)], '_1') = k.composite_key
  WHERE s.ip IS NOT NULL AND s.ip NOT LIKE '%:%'
    AND NOT (s.url IS NULL OR s.url = '')
    AND NOT (s.url LIKE '%steelhouse.com%' OR s.url LIKE '%googlesyndication.com%'
             OR s.url LIKE '%gtm-msr.appspot.com/render%')
  GROUP BY ip
),

imps AS (
  SELECT ad_served_id, ip, household_score, media_spend, data_spend
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

ip_stats AS (
  -- per-IP CIL-week stats pre-aggregated so mem is joined (and thus executed) ONCE
  SELECT i.ip,
         COUNT(*) AS imps,
         SUM(i.media_spend) AS media,
         SUM(i.data_spend) AS data,
         SUM(COALESCE(v.visits, 0)) AS visits,
         MAX(i.household_score) AS msc
  FROM imps i
  LEFT JOIN vis v USING (ad_served_id)
  GROUP BY i.ip
),

agg AS (
  SELECT m.cohort,
         COUNT(*) AS member_ips,
         COUNTIF(s.ip IS NOT NULL) AS ips_served,
         SUM(COALESCE(s.imps, 0)) AS imps,
         ROUND(SUM(COALESCE(s.media, 0)), 2) AS media,
         ROUND(SUM(COALESCE(s.data, 0)), 2) AS data,
         SUM(COALESCE(s.visits, 0)) AS visits,
         COUNTIF(s.msc = 10000) AS hi,
         COUNTIF(s.msc = 8000) AS pp,
         COUNTIF(s.msc BETWEEN 6666 AND 9999 AND s.msc != 8000) AS hg,
         COUNTIF(s.msc BETWEEN 3333 AND 6665) AS mid,
         COUNTIF(s.msc BETWEEN 1 AND 3332) AS maxreach,
         COUNTIF(s.msc <= 0) AS unscored
  FROM mem m
  LEFT JOIN ip_stats s USING (ip)
  GROUP BY m.cohort
)

SELECT rec, k1, k2, v
FROM agg, UNNEST([
  STRUCT('mem' AS rec, cohort AS k1, 'member_ips' AS k2, CAST(member_ips AS FLOAT64) AS v),
  STRUCT('serve', cohort, 'ips_served', CAST(ips_served AS FLOAT64)),
  STRUCT('serve', cohort, 'imps', CAST(imps AS FLOAT64)),
  STRUCT('serve', cohort, 'media', media),
  STRUCT('serve', cohort, 'data', data),
  STRUCT('serve', cohort, 'visits', CAST(visits AS FLOAT64)),
  STRUCT('tier', cohort, 'hi', CAST(hi AS FLOAT64)),
  STRUCT('tier', cohort, 'pp', CAST(pp AS FLOAT64)),
  STRUCT('tier', cohort, 'hg', CAST(hg AS FLOAT64)),
  STRUCT('tier', cohort, 'mid', CAST(mid AS FLOAT64)),
  STRUCT('tier', cohort, 'maxreach', CAST(maxreach AS FLOAT64)),
  STRUCT('tier', cohort, 'unscored', CAST(unscored AS FLOAT64))
])

ORDER BY rec, k1, k2;
