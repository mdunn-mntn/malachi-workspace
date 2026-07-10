-- ============================================================================
-- DDP quality-score runbook, STEP 1c: content quality — what's actually IN the columns
-- Claim: population % says a field is filled, not that it's worth anything. Per source,
-- measure junk/no-value markers on a sampled hour slice:
--   ip   — top-1 IP share (proxy/stamping), % distinct, % private/reserved ranges,
--          % in 66.249.x = Google crawler (crude bot marker)
--   uid  — duplicate share (0 => unique per row; high => repeated/stamped)
--   time — top-1 timestamp share (high => batch-stamped, not real event time)
--   url  — % that fail NET.REG_DOMAIN parse, % malformed (double protocol, the Sovrn bug),
--          top domain + share, top-5 domain share (concentration => narrow feed),
--          distinct domains in the hour
--   ua   — % of populated user_agents matching bot/crawl/spider/slurp/headless
-- All *_share/*_pct use APPROX_* where noted (±1-2% at this scale) — junk detection, not accounting.
--
-- Sample: same hour slice as q1b (SAMPLE_DT x SAMPLE_HH in the external-table URI).
--
-- Run (from workspace root):
--   bash .claude/scripts/bq_run.sh --ticket AUDI-1089 --label "canonical q1c content quality" \
--     --external_table_definition="svs::PARQUET=gs://mntn-data-archive-prod/signals/site_visit_signal/dt=2026-07-01/hh=12/*.parquet" \
--     --use_legacy_sql=false --format=csv --max_rows=50 --project_id=dw-main-silver \
--     "$(grep -v '^[[:space:]]*--' tickets/audi_1089_ddp_vendor_evaluations/runbook/queries/q1c_content_quality.sql)" \
--     > tickets/audi_1089_ddp_vendor_evaluations/outputs/run_<YYYY_MM_DD>/q1c_content_quality.csv
--
-- Parameters (in the external-table URI above, not in the SQL):
--   SAMPLE_DT = 2026-07-01, SAMPLE_HH = 12  (match q1b so the two are comparable)
-- ============================================================================

WITH agg AS (
  SELECT
    CAST(data_source_id AS INT64) AS ds,
    COUNT(*) AS n,
    APPROX_TOP_COUNT(ip, 1)[SAFE_OFFSET(0)] AS ip_top,
    APPROX_COUNT_DISTINCT(ip) AS ip_distinct,
    COUNTIF(STARTS_WITH(ip, '10.') OR STARTS_WITH(ip, '192.168.') OR STARTS_WITH(ip, '127.')
            OR ip = '0.0.0.0'
            OR REGEXP_CONTAINS(ip, r'^172\.(1[6-9]|2[0-9]|3[01])\.')) AS ip_private,
    COUNTIF(STARTS_WITH(ip, '66.249.')) AS ip_googlebot,
    APPROX_COUNT_DISTINCT(uid) AS uid_distinct,
    APPROX_TOP_COUNT(CAST(time AS STRING), 1)[SAFE_OFFSET(0)].count AS time_top1_n,
    COUNTIF(url IS NOT NULL AND url != '') AS url_n,
    COUNTIF(url IS NOT NULL AND url != '' AND NET.REG_DOMAIN(url) IS NULL) AS url_parse_fail,
    COUNTIF(url IS NOT NULL AND REGEXP_CONTAINS(url, r'https?://.+https?://')) AS url_malformed,
    APPROX_TOP_COUNT(NET.REG_DOMAIN(url), 5) AS dom_top5,
    APPROX_COUNT_DISTINCT(NET.REG_DOMAIN(url)) AS dom_distinct,
    COUNTIF(user_agent IS NOT NULL AND user_agent != '') AS ua_n,
    COUNTIF(REGEXP_CONTAINS(LOWER(user_agent), r'bot|crawl|spider|slurp|headless')) AS ua_bot
  FROM svs
  GROUP BY 1
)
SELECT
  ds,
  n,
  ip_top.value AS top_ip,
  ROUND(100 * ip_top.count / n, 2) AS top_ip_share,
  ROUND(100 * ip_distinct / n, 1) AS ip_distinct_pct,
  ROUND(100 * ip_private / n, 3) AS pct_private_ip,
  ROUND(100 * ip_googlebot / n, 2) AS pct_googlebot_ip,
  ROUND(100 * (1 - uid_distinct / n), 2) AS uid_dup_pct,
  ROUND(100 * time_top1_n / n, 2) AS time_top1_share,
  ROUND(100 * url_parse_fail / NULLIF(url_n, 0), 2) AS url_parse_fail_pct,
  ROUND(100 * url_malformed / NULLIF(url_n, 0), 2) AS url_malformed_pct,
  dom_top5[SAFE_OFFSET(0)].value AS top_domain,
  ROUND(100 * dom_top5[SAFE_OFFSET(0)].count / NULLIF(url_n, 0), 1) AS top_domain_share,
  ROUND(100 * (SELECT SUM(x.count) FROM UNNEST(dom_top5) x) / NULLIF(url_n, 0), 1) AS top5_domain_share,
  dom_distinct,
  ROUND(100 * ua_bot / NULLIF(ua_n, 0), 2) AS ua_bot_pct
FROM agg
ORDER BY n DESC;
