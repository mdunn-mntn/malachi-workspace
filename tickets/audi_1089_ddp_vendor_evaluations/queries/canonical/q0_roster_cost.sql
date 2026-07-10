-- ============================================================================
-- DDP quality-score runbook, STEP 0: roster + actual cost
-- Claim: these are the vendors, what they bill, and what we actually paid each month.
-- Runbook: documentation/docs/ddp_quality_score_runbook.md
--
-- Roster is read from the registry every run, never hardcoded. Scope keeps:
--   (a) every used_in_mntn_match = TRUE vendor (the eval roster, including disabled
--       ones such as DS27 LaunchLabs), and
--   (b) any other data source with metered usage in the window, as context rows:
--       interests/CRM DDPs share the same meter (DS35 LiveRamp IP ~$280K/mo,
--       DS17 ShareThis at $0.95 CPM, DS29 Deepsync).
--
-- Registry gotchas: CDC duplicate rows (dedupe by valid_from DESC; DS26 SCD is broken
--   so is_current is not trusted); registry data_source_id is STRING (cast to INT64).
-- Meter: dw-main-bronze.coredw.usage_reporting_data. usage = impressions x (cpm/1000)
--   credited on MM-targeted serves with 1/N split on shared IPs (30d lookback).
--   implied_cpm is printed per month and checked against the registry fixed_cpm
--   (meter_check_ok; NULL where billing is flat fee / variable or nothing was metered).
--
-- Output: q0_roster_cost.csv, one row per data_source x reporting_month; vendors with
--   no metered usage keep a single row with NULL reporting_month (flat fee = amount
--   unknown to our data, must come from the renewal schedule; CPM + NULL = $0 bill).
--
-- Run (from workspace root; redirect stdout, perf summary goes to stderr).
-- The grep strips full-line comments: bq treats any argument starting with "--" as a flag.
-- Single statement on purpose: DECLARE turns the query into a script and bq then echoes
-- the statement text to stdout, contaminating the CSV.
--   bash .claude/scripts/bq_run.sh --ticket AUDI-1089 --label "canonical q0 roster+cost" \
--     --use_legacy_sql=false --format=csv --max_rows=200 --project_id=dw-main-silver \
--     "$(grep -v '^[[:space:]]*--' tickets/audi_1089_ddp_vendor_evaluations/queries/canonical/q0_roster_cost.sql)" \
--     > tickets/audi_1089_ddp_vendor_evaluations/outputs/run_<YYYY_MM_DD>/q0_roster_cost.csv
--
-- Parameters (inlined literals, marked with PARAM comments for the runner to substitute):
--   MONTHS_BACK = 6   complete calendar months of billing history
-- ============================================================================

WITH roster AS (
  SELECT
    CAST(data_source_id AS INT64) AS data_source_id,
    data_partner_name,
    billing_type,
    fixed_cpm,
    enabled,
    used_in_mntn_match,
    used_in_interests,
    type,
    DATE(valid_from) AS valid_from,
    go_live_date,
    notes
  FROM `dw-main-silver.tpa.direct_data_partners`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY data_source_id ORDER BY valid_from DESC) = 1
),

monthly AS (
  SELECT
    CAST(data_source_id AS INT64) AS data_source_id,
    reporting_month,
    SUM(impressions) AS impressions,
    ROUND(SUM(usage), 2) AS usage_dollars
  FROM `dw-main-bronze.coredw.usage_reporting_data`
  WHERE reporting_month >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 6 MONTH)  -- PARAM MONTHS_BACK
  GROUP BY 1, 2
)

SELECT
  r.data_source_id,
  r.data_partner_name,
  r.billing_type,
  r.fixed_cpm,
  r.enabled,
  r.used_in_mntn_match,
  r.used_in_interests,
  r.type,
  r.valid_from,
  r.go_live_date,
  r.notes,
  m.reporting_month,
  m.impressions,
  m.usage_dollars,
  ROUND(SAFE_DIVIDE(m.usage_dollars, m.impressions) * 1000, 4) AS implied_cpm,
  CASE
    WHEN r.fixed_cpm IS NULL OR m.impressions IS NULL THEN NULL
    ELSE ABS(m.usage_dollars - m.impressions * r.fixed_cpm / 1000) < 1.0
  END AS meter_check_ok
FROM roster r
LEFT JOIN monthly m USING (data_source_id)
WHERE r.used_in_mntn_match OR m.reporting_month IS NOT NULL
ORDER BY r.used_in_mntn_match DESC, r.data_source_id, m.reporting_month;
