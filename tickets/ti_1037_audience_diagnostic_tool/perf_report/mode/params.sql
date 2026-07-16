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
  label: "Period 2 start"
  description: "Period 2 = the recent period under diagnosis"
Period_End:
  type: date
  default: 2027-01-01
  label: "Period 2 end (exclusive)"
  description: "First day AFTER the period; future dates cap to the last completed month, so the default = through the last full month"
P1_Start:
  type: date
  default: 1900-01-01
  label: "Period 1 start (optional)"
  description: "Leave at 1900-01-01 = auto: Period 2 minus one year (YoY). Periods must NOT overlap."
P1_End:
  type: date
  default: 1900-01-01
  label: "Period 1 end (optional, exclusive)"
  description: "Leave at 1900-01-01 = auto: Period 2 end minus one year"
Stages:
  type: multiselect
  default: ['ALL']
  label: "Funnel stage"
  description: "1 = Prospecting, 2/3 = Mid-funnel. Specific picks OVERRIDE ALL (ALL only means everything when it is the only box checked). Applies to every campaign-scoped module"
  options: ['ALL', '1', '2', '3']
Min_Spend_Pct:
  type: text
  default: 0
  label: "Min campaign spend % (0-100)"
  description: "Hide campaigns below this share of total window spend (whole-group basis); any number 0-100, e.g. 1 or 2.5"
{% endform %}
