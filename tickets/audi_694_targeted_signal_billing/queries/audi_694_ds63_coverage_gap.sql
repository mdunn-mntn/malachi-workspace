-- How many billable DS63 impressions never get a crediting row?
-- The DS4 leg inserts unmatched CRM impressions at cpm 0 (main script L299-322) so they still
-- compete in the OR groups and do not hand their share to TPA/MM. The graph leg has no equivalent.
-- Result 2026-08-17 for 2026-08-06..08-12:
--   DS63 total 376,727 | in billing scope 352,830 (93.7%) | in ddp_crm_graph_cpm 214,251
--   IN SCOPE BUT UNCREDITED = 138,579 (39.3%)
-- Caveat: the 214,251 baseline is the 2026-08-13 build and may cover a narrower window.
WITH ei AS (
  SELECT e.dt, e.ad_served_id, e.channel_id, e.objective_id, c.funnel_level
  FROM `mntn-analytics-prod-01.analytics_curated.enriched_impressions` e
  JOIN `dw-main-gold.public.campaigns` c ON c.campaign_id = e.campaign_id
  WHERE e.dt BETWEEN DATE("2026-08-06") AND DATE("2026-08-12") AND e.data_source_id = 63
),
scoped AS (
  SELECT DISTINCT ad_served_id FROM ei
  WHERE channel_id = 8 AND funnel_level = 1 AND objective_id = 1
),
credited AS (
  SELECT DISTINCT ad_served_id FROM `dw-main-gold.reporting.ddp_crm_graph_cpm`
)
SELECT
  (SELECT COUNT(DISTINCT ad_served_id) FROM ei) AS ds63_all_imps,
  (SELECT COUNT(*) FROM scoped) AS ds63_in_billing_scope,
  (SELECT COUNT(*) FROM credited) AS rows_in_gold_credit_table,
  (SELECT COUNT(*) FROM scoped s
    WHERE NOT EXISTS (SELECT 1 FROM credited c WHERE c.ad_served_id = s.ad_served_id)) AS in_scope_but_uncredited
LIMIT 5;
