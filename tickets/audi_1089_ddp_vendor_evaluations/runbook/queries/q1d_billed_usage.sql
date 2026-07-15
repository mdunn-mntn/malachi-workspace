-- ============================================================================
-- DDP quality-score runbook, STEP 1d: what we ACTUALLY paid for (billed usage)
-- Claim: billing follows USE, not delivery (Ryan Kleck 2026-07-10) — vendors are
-- credited only when their data lands on MM-targeted serves. So junk discounts
-- VALUE not COST, and the real consumption funnel is: raw feed -> billed usage.
--
-- Meter: dw-main-bronze.coredw.usage_reporting_data. Grain: month-end snapshot
-- (dt = last day of reporting_month — mid-month dt queries return NOTHING),
-- one row per billed unit with a domains.list RECORD naming the domain(s) that
-- drove the credit (populated ONLY for MM site-visit CPM vendors 24/28/33/36/40;
-- empty for interests/CRM). Large rows with empty domains.list = unattributed
-- aggregate credits. impressions carried decimals (1/N split) only through Apr 2026; May 2026+
-- (including this BILL_MONTH) is integer single-vendor credit — see q0b.
--
-- Output per source: billed imps, billed $, % of imps domain-attributed,
-- distinct billed domains, top-5 billed domains w/ share of attributed imps.
-- The delivered-side comparison (raw rows, window domains) joins in the chart
-- script from q1_scale_by_day.csv + q2_window_reach.csv.
--
-- Run (from workspace root):
--   bash .claude/scripts/bq_run.sh --ticket AUDI-1089 --label "canonical q1d billed usage" \
--     --use_legacy_sql=false --format=csv --max_rows=50 --project_id=dw-main-silver \
--     "$(grep -v '^[[:space:]]*--' tickets/audi_1089_ddp_vendor_evaluations/runbook/queries/q1d_billed_usage.sql)" \
--     > tickets/audi_1089_ddp_vendor_evaluations/outputs/run_<YYYY_MM_DD>/q1d_billed_usage.csv
--
-- Parameters:
--   BILL_MONTH = 2026-06-01   the reporting month overlapping the signal window
-- ============================================================================

WITH base AS (
  SELECT data_source_id AS ds, impressions, usage, domains
  FROM `dw-main-bronze.coredw.usage_reporting_data`
  WHERE reporting_month = '2026-06-01'  -- PARAM BILL_MONTH
),

dom AS (
  SELECT
    ds,
    d.element AS domain,
    SUM(impressions) AS imps,
    SUM(SUM(impressions)) OVER (PARTITION BY ds) AS ds_attr_imps
  FROM base, UNNEST(domains.list) AS d
  GROUP BY 1, 2
),

dom_stats AS (
  SELECT
    ds,
    COUNT(DISTINCT domain) AS billed_domains,
    STRING_AGG(CONCAT(domain, ' ', CAST(ROUND(100 * imps / ds_attr_imps, 1) AS STRING), '%'),
               ', ' ORDER BY imps DESC LIMIT 5) AS top5_billed_domains
  FROM dom
  GROUP BY 1
)

SELECT
  t.ds,
  ROUND(SUM(t.impressions), 0) AS billed_imps,
  ROUND(SUM(t.usage), 2) AS billed_usd,
  ROUND(100 * SUM(IF(ARRAY_LENGTH(t.domains.list) > 0, t.impressions, 0)) / NULLIF(SUM(t.impressions), 0), 1)
    AS pct_imps_domain_attributed,
  d.billed_domains,
  d.top5_billed_domains
FROM base t
LEFT JOIN dom_stats d USING (ds)
GROUP BY t.ds, d.billed_domains, d.top5_billed_domains
ORDER BY billed_usd DESC;
