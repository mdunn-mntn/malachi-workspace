-- ============================================================================
-- DDP quality-score runbook, STEP 15c: free_logs COMBINED — hour-slice content quality
-- Claim: the combined free_logs column's concentration metrics (Top-1/Top-5 domain
-- share) cannot be derived from the per-log q1c rows (a union's top domain is not an
-- average of two top domains) — measure them directly on the guid+augmentor UNION,
-- same hour slice and same definitions as q1c. Also the union's bot-UA share.
--
-- Slice: dt=2026-07-01 hh=12 (the q1c sample). Bot regex identical to q1c.
-- '(empty url)' = the modal domain bucket is rows with no/unparseable URL (guid ~25%).
--
-- Output: ONE row (n = union rows in slice; top_domain STRING; top_domain_share,
-- top5_domain_share = % of rows; ua_bot_pct = % of populated user_agents matching
-- bot regex) -> injected as q1c[99] fields by fill_template.
--
-- CHEAP (one svs hour slice, ~12 GB) — console-friendly with the external table.
--
-- Run (from workspace root):
--   bash .claude/scripts/bq_run.sh --ticket AUDI-1089 --label "canonical q15c free union hour quality" \
--     --external_table_definition="svs::PARQUET=gs://mntn-data-archive-prod/signals/site_visit_signal/dt=2026-07-01/hh=12/*.parquet" \
--     --use_legacy_sql=false --format=csv --max_rows=10 --project_id=dw-main-silver \
--     "$(grep -v '^[[:space:]]*--' tickets/audi_1089_ddp_vendor_evaluations/runbook/queries/q15c_free_union_hour_quality.sql)" \
--     > tickets/audi_1089_ddp_vendor_evaluations/outputs/run_<YYYY_MM_DD>/q15c_free_union_hour_quality.csv
--
-- Parameters: SLICE = dt=2026-07-01/hh=12 (must match the q1c run's slice)
-- ============================================================================

WITH r AS (
  SELECT NET.REG_DOMAIN(url) AS dom, user_agent
  FROM svs
  WHERE CAST(data_source_id AS INT64) IN (23, 30)
),

d AS (
  SELECT dom, COUNT(*) AS c
  FROM r
  GROUP BY dom
),

top AS (
  SELECT dom, c, ROW_NUMBER() OVER (ORDER BY c DESC) AS rn
  FROM d
),

tot AS (
  SELECT COUNT(*) AS n,
         COUNTIF(user_agent IS NOT NULL AND user_agent != '') AS n_ua,
         COUNTIF(REGEXP_CONTAINS(LOWER(user_agent), r'bot|crawl|spider|slurp|headless')) AS ua_bot
  FROM r
)

SELECT
  t.n,
  (SELECT IFNULL(dom, '(empty url)') FROM top WHERE rn = 1) AS top_domain,
  ROUND(100 * (SELECT c FROM top WHERE rn = 1) / t.n, 1) AS top_domain_share,
  ROUND(100 * (SELECT SUM(c) FROM top WHERE rn <= 5) / t.n, 1) AS top5_domain_share,
  ROUND(100 * t.ua_bot / NULLIF(t.n_ua, 0), 2) AS ua_bot_pct
FROM tot t;
