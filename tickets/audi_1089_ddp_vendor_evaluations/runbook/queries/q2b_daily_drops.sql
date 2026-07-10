-- ============================================================================
-- DDP quality-score runbook, STEP 2b: rows/IPs dropped per day, by reason
-- Claim: of what each source delivers in a day, this much never survives the
-- consumer filters — counted directly against the filters verified in code.
--
-- Filter provenance (airflow-ti audit 2026-07-10):
--   HARD drops (row can never be used):
--     empty_url    — url NULL/''
--     unparseable  — NET.REG_DOMAIN(url) IS NULL on a non-empty url (garbage never
--                    classifies; NOTE billing-side garbage hosts can still slip in via urlsplit)
--     infra_url    — steelhouse.com / googlesyndication.com / gtm-msr.appspot.com
--                    (svs feature model composite_key filters)
--   SOFT drops (blocked on one path / latent):
--     blocked_ds13 — reg domain in (yahoo.com, aol.com, easybrain.com): BLOCKED_DOMAIN_NAMES
--                    on the vertical path; may still be used by DS19 (billing shows yahoo credits)
--     bot_ua       — user_agent matches bot/crawl/spider/slurp/headless (only meaningful
--                    where UA is populated: 28/33/40 + internal)
--   dropped IPs = distinct IPs that appear ONLY on hard-dropped rows.
--
-- Sample: ONE full day (SAMPLE_DT, all hours) — drop rates are structural; multiply
-- rates by q1's median rows/day for other days.
--
-- Run (from workspace root; ~285 GB scan):
--   bash .claude/scripts/bq_run.sh --ticket AUDI-1089 --label "canonical q2b daily drops" \
--     --external_table_definition="svs::PARQUET=gs://mntn-data-archive-prod/signals/site_visit_signal/dt=2026-07-01/*.parquet" \
--     --use_legacy_sql=false --format=csv --max_rows=50 --project_id=dw-main-silver \
--     "$(grep -v '^[[:space:]]*--' tickets/audi_1089_ddp_vendor_evaluations/runbook/queries/q2b_daily_drops.sql)" \
--     > tickets/audi_1089_ddp_vendor_evaluations/outputs/run_<YYYY_MM_DD>/q2b_daily_drops.csv
--
-- Parameters (in the external-table URI above, not in the SQL):
--   SAMPLE_DT = 2026-07-01   any complete day inside the signal window
-- ============================================================================

WITH flagged AS (
  SELECT
    CAST(data_source_id AS INT64) AS ds,
    ip,
    user_agent,
    (url IS NULL OR url = '') AS empty_url,
    (url IS NOT NULL AND url != '' AND NET.REG_DOMAIN(url) IS NULL) AS unparseable,
    (url LIKE '%steelhouse.com%' OR url LIKE '%googlesyndication.com%'
     OR url LIKE '%gtm-msr.appspot.com/render%') AS infra_url,
    NET.REG_DOMAIN(url) IN ('yahoo.com', 'aol.com', 'easybrain.com') AS blocked_ds13,
    (user_agent IS NOT NULL AND user_agent != ''
     AND REGEXP_CONTAINS(LOWER(user_agent), r'bot|crawl|spider|slurp|headless')) AS bot_ua
  FROM svs
)

SELECT
  ds,
  COUNT(*) AS rows_day,
  COUNTIF(empty_url OR unparseable OR infra_url) AS rows_hard_dropped,
  ROUND(100 * COUNTIF(empty_url OR unparseable OR infra_url) / COUNT(*), 2) AS pct_hard_dropped,
  COUNTIF(unparseable) AS rows_unparseable,
  COUNTIF(empty_url) AS rows_empty_url,
  COUNTIF(infra_url) AS rows_infra_url,
  COUNTIF(blocked_ds13) AS rows_blocked_ds13,
  ROUND(100 * COUNTIF(blocked_ds13) / COUNT(*), 2) AS pct_blocked_ds13,
  COUNTIF(bot_ua) AS rows_bot_ua,
  APPROX_COUNT_DISTINCT(ip) AS ips_day,
  APPROX_COUNT_DISTINCT(IF(NOT (empty_url OR unparseable OR infra_url), ip, NULL)) AS ips_on_kept_rows,
  APPROX_COUNT_DISTINCT(ip)
    - APPROX_COUNT_DISTINCT(IF(NOT (empty_url OR unparseable OR infra_url), ip, NULL)) AS ips_hard_dropped
FROM flagged
GROUP BY ds
ORDER BY rows_day DESC;
