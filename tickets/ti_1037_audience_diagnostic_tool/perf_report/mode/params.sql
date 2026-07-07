-- =====================================================================
-- Params — advertiser + period for the Client Performance Diagnostic.
-- Defines the 3 Mode parameters the whole dashboard runs on. Consumer
-- queries reference {{ Advertiser_ID }} / {{ Period_Start }} / {{ Period_End }}.
--
-- Model (from the Nick/Mode walkthrough): the user picks ONE advertiser +
-- ONE period (the recent period = P2). P1 (the YoY comparison) and the
-- continuous trend window are DERIVED in each query as "same dates, minus
-- one year" — so there are only the two parameters Malachi described.
--
-- This query just needs to run (SELECT 1); the {% form %} block is what
-- Mode reads to render the parameter inputs.
-- =====================================================================
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
