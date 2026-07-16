-- ============================================================================
-- AUDI-1089 DECK QUERY D3 of 7: what each vendor bills — CPM and annualized cost
-- FILLS: deck sheet BLOCK 2 (rows 10-16): contract CPM and "Bill / yr cost"
--        (block 2's media CPMs come from q6/q15 media+imps, not this query);
--        BLOCK 3 (rows 18-24) col B "Bill/yr cost if we
--        removed free_logs from billing" is a SHEET FORMULA, not a query:
--        bill_annualized x (1 - free_cohold_pct/100), free_cohold_pct from D1.
--        The two Profit columns are also sheet formulas (CPM/value x the internal
--        margin range — margin parameters are internal-only and intentionally NOT
--        embedded in any shareable query).
--
-- Claim: the vendor roster comes from the registry (never hardcoded) and the bill
-- from the meter: usage = billed impressions x contract CPM / 1000, credited on
-- MM-targeted serves. June 2026 is the meter month (latest complete, single-credit
-- regime — the meter's credit rule changed May 2026 from fractional 1/N to integer
-- single-vendor credit; never mix months across that boundary). Annual = June x 12.
-- meter_check_ok proves imps x CPM == usage within $1 for every metered vendor
-- (q0's proven tolerance; residuals are rounding cents).
--
-- Flat-fee vendors (5x5, Predactiv, Klickly) have NO meter rows — their amounts
-- come from the renewal schedule (finance), not from our data: bill columns NULL.
-- Free internal logs (guid_log ds23, augmentor ds30) are not in the vendor
-- registry at all: $0 — no row here; enter 0 in the sheet.
-- DS27 LaunchLabs (disabled, never metered) ALSO prints — used_in_mntn_match
-- keeps it on the registry roster. It is a context row (enabled = false, NULL
-- bills): do NOT paste it into the sheet.
--
-- Expected reconciliation (June 2026): 33Across $35,168.66/mo -> $422,024/yr;
-- 33Across API $175,879/yr; Sovrn $115,880/yr; Justuno $77,111/yr; Cybba
-- $21,504/yr; metered total $812,397/yr.
--
-- CHEAP, console-pasteable (internal tables only, no external definitions).
--
-- Run: paste into the BigQuery console (project dw-main-silver), or from this
-- folder:
--   bq query --use_legacy_sql=false --format=csv --max_rows=50 --project_id=dw-main-silver \
--     "$(grep -v '^[[:space:]]*--' deck_d3_bills_cpm.sql)" \
--     > ../../outputs/run_<YYYY_MM_DD>/deck_d3_bills_cpm.csv
--
-- Parameters: METER_MONTH = 2026-06-01
-- ============================================================================

WITH roster AS (
  -- dedupe to the LATEST registry version FIRST, filter the flag AFTER (q0's
  -- proven order) — filtering first would resurrect a dropped vendor via an
  -- older still-TRUE version with a stale CPM
  SELECT
    CAST(data_source_id AS INT64) AS data_source_id,
    data_partner_name,
    billing_type,
    fixed_cpm AS contract_cpm,
    enabled,
    used_in_mntn_match
  FROM `dw-main-silver.tpa.direct_data_partners`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY data_source_id ORDER BY valid_from DESC) = 1
),

june AS (
  SELECT
    CAST(data_source_id AS INT64) AS data_source_id,
    SUM(impressions) AS billed_imps_month,
    ROUND(SUM(usage), 2) AS bill_month
  FROM `dw-main-bronze.coredw.usage_reporting_data`
  WHERE reporting_month = DATE '2026-06-01'  -- PARAM METER_MONTH
  GROUP BY 1
)

SELECT
  r.data_source_id,
  r.data_partner_name,
  r.billing_type,
  r.enabled,
  r.contract_cpm,
  j.billed_imps_month,
  j.bill_month,
  ROUND(j.bill_month * 12, 2) AS bill_annualized,
  ROUND(SAFE_DIVIDE(j.bill_month, j.billed_imps_month) * 1000, 4) AS implied_cpm,
  CASE
    WHEN r.contract_cpm IS NULL OR j.billed_imps_month IS NULL THEN NULL
    ELSE ABS(j.bill_month - j.billed_imps_month * r.contract_cpm / 1000) < 1.0
  END AS meter_check_ok
FROM roster r
LEFT JOIN june j USING (data_source_id)
WHERE r.used_in_mntn_match
ORDER BY j.bill_month DESC NULLS LAST, r.data_source_id;
