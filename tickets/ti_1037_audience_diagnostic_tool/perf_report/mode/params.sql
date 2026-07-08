-- Params — advertiser + period for the Client Performance Diagnostic.
-- The SQL feeds the Advertiser_ID dropdown: every non-test advertiser with delivery
-- spend in the last 18 months (the diagnostic is meaningless without delivery),
-- sorted A-Z by company name (unnamed last). If the dropdown ever loses its tail
-- (Mode documents a 1,000-option / 1MB cap on dynamic params), check for a "Z"
-- advertiser — the fallback is re-ranking by spend DESC so big accounts survive.
-- Dropdown label = "id · name" — the filter box matches EITHER, so users can type
-- an advertiser_id or a company name; the bare advertiser_id is what substitutes
-- into consumer queries (labels vs values).
-- Period_Start / Period_End are free date pickers (users pick any two dates).
-- End is EXCLUSIVE and every query clamps it to the first of the current month, so
-- the 2027-01-01 default = "through the last full month" all year with no edits;
-- only the start default needs a bump each January. (The 1900-01-01/2099-01-01
-- sentinel mappings remain in the SQL — harmless, nobody types them.)
-- NOTE: this query now hits BigQuery — its DB connection must be BigQuery
-- (dw-main-silver), not the default core dw.
-- IMPORTANT: keep Liquid tags OUT of comments. Mode parses parameter/form tags
-- even inside SQL comments, so a stray tag here breaks the parser.
SELECT
  a.advertiser_id,
  CONCAT(CAST(a.advertiser_id AS STRING), ' · ',
         COALESCE(NULLIF(TRIM(a.company_name), ''), '(unnamed)')) AS advertiser_label
FROM `dw-main-bronze.integrationprod.advertisers` a
JOIN (
  SELECT advertiser_id, SUM(media_spend + data_spend + platform_spend) AS spend
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`
  WHERE day >= DATE_SUB(CURRENT_DATE(), INTERVAL 18 MONTH)
  GROUP BY 1
  HAVING spend > 0
) s USING (advertiser_id)
WHERE a.deleted = FALSE AND a.is_test = FALSE
ORDER BY (NULLIF(TRIM(a.company_name), '') IS NULL), LOWER(TRIM(a.company_name)), a.advertiser_id

{% form %}
Advertiser_ID:
  type: select
  default: 32147
  label: "Advertiser"
  options:
    labels: advertiser_label
    values: advertiser_id
Period_Start:
  type: date
  default: 2026-01-01
  label: "Period start"
  description: "Compared against the same dates one year earlier (YoY)"
Period_End:
  type: date
  default: 2027-01-01
  label: "Period end (exclusive)"
  description: "First day AFTER the period. Future dates are capped to the last completed month - so the default means: through the last full month"
{% endform %}
