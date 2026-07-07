-- Params - advertiser + period for the Client Performance Diagnostic.
-- Defines the 3 Mode parameters the dashboard runs on: Advertiser_ID,
-- Period_Start, Period_End. Consumer queries reference them with the
-- usual double-brace parameter syntax. The user picks ONE advertiser + ONE
-- period (the recent period = P2); P1 (the YoY comparison) and the trend
-- window are derived per-query as "same dates, minus one year".
--
-- IMPORTANT: keep Liquid tags OUT of comments. Mode parses parameter/form
-- tags even inside SQL comments, so a stray tag here breaks the parser.
-- This query only needs to run; the form block below renders the inputs.
SELECT 1 AS ok

{% form %}
Advertiser_ID:
  type: text
  default: 32147
Period_Start:
  type: date
  default: 2026-01-01
Period_End:
  type: date
  default: 2026-06-01
{% endform %}
