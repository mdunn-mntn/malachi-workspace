WITH base AS (
  SELECT DISTINCT ip,
    CASE WHEN c.element IN (1000090104,1000090114) THEN 'equifax'
         WHEN c.element = 1004602219 THEN 'experian'
         WHEN c.element = 1005852081 THEN 'transunion' END AS provider
  FROM `dw-main-bronze.external.ipdsc__v1`, UNNEST(data_source_category_ids.list) c
  WHERE dt BETWEEN '2026-06-04' AND '2026-06-17' AND data_source_id=35
    AND c.element IN (1000090104,1000090114,1004602219,1005852081)
),
piv AS (
  SELECT ip, LOGICAL_OR(provider='equifax') eq, LOGICAL_OR(provider='experian') ex,
         LOGICAL_OR(provider='transunion') tu
  FROM base GROUP BY ip
)
SELECT
  COUNTIF(eq) AS equifax_lowinc_ips,
  COUNTIF(ex) AS experian_lowinc_ips,
  COUNTIF(tu) AS transunion_lowinc_ips,
  COUNTIF(eq AND ex) AS eq_and_ex,
  COUNTIF(eq AND tu) AS eq_and_tu,
  COUNTIF(ex AND tu) AS ex_and_tu,
  COUNTIF(eq AND ex AND tu) AS all_three_agree,
  COUNT(*) AS flagged_by_any
FROM piv
