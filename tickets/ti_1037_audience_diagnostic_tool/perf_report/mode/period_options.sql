-- Period options — feeds the Period_Start / Period_End dropdowns (query-backed
-- selects, one row per month boundary for the last 36 completed months).
-- The FIRST row is the "Auto" option: its label is human-readable but its VALUE is
-- the sentinel every consumer query already maps in SQL:
--   1900-01-01 -> Jan 1 of the current year
--   2099-01-01 -> clamped to the first day of the current month (= through the
--                 last FULL month; Period_End is exclusive everywhere)
-- So defaults never go stale AND the picker never shows a weird year.
-- Values are formatted STRINGS (not DATE) so Mode substitutes clean 'YYYY-MM-DD'
-- text into consumer queries.
-- NOTE: DB connection must be BigQuery. Keep Liquid tags out of comments.
WITH months AS (
  SELECT m
  FROM UNNEST(GENERATE_DATE_ARRAY(
    DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 36 MONTH),
    DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 MONTH),
    INTERVAL 1 MONTH)) AS m
)
SELECT * FROM (
  SELECT
    0 AS ord,
    '1900-01-01' AS start_value,
    'Auto - Jan 1 of current year' AS start_label,
    '2099-01-01' AS end_value,
    'Auto - through last full month' AS end_label
  UNION ALL
  SELECT
    ROW_NUMBER() OVER (ORDER BY m DESC) AS ord,
    FORMAT_DATE('%Y-%m-%d', m) AS start_value,
    FORMAT_DATE('from %b 1, %Y', m) AS start_label,
    FORMAT_DATE('%Y-%m-%d', DATE_ADD(m, INTERVAL 1 MONTH)) AS end_value,
    FORMAT_DATE('through %b %Y', m) AS end_label
  FROM months
)
ORDER BY ord

{% form %}
Period_Start:
  type: select
  default: '1900-01-01'
  label: "Period start"
  description: "Compared against the same dates one year earlier (YoY)"
  options:
    labels: start_label
    values: start_value
Period_End:
  type: select
  default: '2099-01-01'
  label: "Period end"
  description: "Full months only; Auto = through the most recent completed month"
  options:
    labels: end_label
    values: end_value
{% endform %}
