-- TI-999 Finding 15 Pass 20 — RTC as its own axis (independent of MM)
--
-- Pass 18 had RTC lumped into "nothing".
-- Pass 19 folded RTC into MM-touching (based on Sean's first reading).
-- Pass 20 corrects: per Sean's revised reading (2026-05-29), RTC is a
-- pipeline running INDEPENDENTLY of MM — MM is batch via IPDSC filling
-- household_score; RTC is a real-time match-and-tag pipeline filling
-- realtime_conquest_score. They produce similar scores via separate paths.
--
-- Five independent binary audience axes:
--   MM   = {DS13, DS19, DS38, DS46} (batch MM via buyer-picked DS clauses)
--   RTC  = score_type=rtc in expression (real-time MM-style pipeline, auto-attached)
--   Select = {DS9, DS42}
--   3P   = {DS17, DS18, DS35} (Oracle DS1 carved out — dead-weight)
--   CRM  = {DS4, DS8, DS47} (any polarity for now)
--
-- "Any signal" = positive OR negative DS reference. RTC is a top-level
-- expression flag, not polarity-aware.

CREATE TEMP FUNCTION parse_expression(expr STRING)
RETURNS ARRAY<STRUCT<data_source_id INT64, category_id INT64, polarity STRING>>
LANGUAGE js AS r"""
  if (!expr) return [];
  let parsed;
  try { parsed = JSON.parse(expr); } catch (e) { return []; }
  const out = [];
  function walk(node, negDepth) {
    if (!node || typeof node !== 'object') return;
    if (Array.isArray(node)) { for (const n of node) walk(n, negDepth); return; }
    const op = node.op;
    if (op === 'not') { walk(node.value, negDepth + 1); return; }
    if (op === 'any') {
      if (node.value && node.value.data_source_id != null && Array.isArray(node.value.category_ids)) {
        const ds = node.value.data_source_id;
        const polarity = (negDepth % 2 === 1) ? 'negative' : 'positive';
        for (const cid of node.value.category_ids) out.push({data_source_id: ds, category_id: cid, polarity: polarity});
      }
      return;
    }
    if (node.value !== undefined) walk(node.value, negDepth);
  }
  if (parsed && parsed.categories && parsed.categories.where) walk(parsed.categories.where, 0);
  return out;
""";

WITH active_prospecting AS (
  SELECT s.campaign_id, s.advertiser_id,
         SUM(s.impressions) AS impressions_30d,
         SUM(s.media_spend + s.data_spend + s.platform_spend) AS spend_30d,
         SUM(s.click_conversions + s.view_conversions) AS conversions_30d
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` s
  JOIN `dw-main-bronze.integrationprod.campaigns` c USING (campaign_id)
  WHERE s.day BETWEEN DATE('2026-04-29') AND DATE('2026-05-28')
    AND c.objective_id IN (1, 5, 6)
  GROUP BY 1, 2 HAVING SUM(s.impressions) > 0
),
parsed AS (
  SELECT campaign_id, expression, parse_expression(expression) AS cats
  FROM (
    SELECT campaign_id, expression,
           ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY update_time DESC) AS rn
    FROM `dw-main-silver.audience.audience_segments`
    WHERE expression_type_id = 2 AND is_targeted = TRUE
      AND campaign_id IN (SELECT campaign_id FROM active_prospecting)
  ) WHERE rn = 1
),
flags AS (
  SELECT p.campaign_id,
    LOGICAL_OR(c.data_source_id IN (13, 19, 38, 46)) AS has_mm,
    ANY_VALUE(REGEXP_CONTAINS(p.expression, r'"score_type"\s*:\s*"rtc"')) AS has_rtc,
    LOGICAL_OR(c.data_source_id IN (9, 42)) AS has_select,
    LOGICAL_OR(c.data_source_id IN (17, 18, 35)) AS has_3p,
    LOGICAL_OR(c.data_source_id IN (4, 8, 47)) AS has_crm
  FROM parsed p LEFT JOIN UNNEST(p.cats) c
  GROUP BY 1
),
bucketed AS (
  SELECT ap.*,
    COALESCE(f.has_mm,FALSE) AS has_mm,
    COALESCE(f.has_rtc,FALSE) AS has_rtc,
    COALESCE(f.has_select,FALSE) AS has_select,
    COALESCE(f.has_3p,FALSE) AS has_3p,
    COALESCE(f.has_crm,FALSE) AS has_crm,
    CASE
      WHEN NOT COALESCE(f.has_mm,FALSE) AND NOT COALESCE(f.has_rtc,FALSE)
       AND NOT COALESCE(f.has_select,FALSE) AND NOT COALESCE(f.has_3p,FALSE)
       AND NOT COALESCE(f.has_crm,FALSE) THEN 'A01_nothing_truly_bare_geo_only'
      WHEN NOT COALESCE(f.has_mm,FALSE) AND COALESCE(f.has_rtc,FALSE)
       AND NOT COALESCE(f.has_select,FALSE) AND NOT COALESCE(f.has_3p,FALSE)
       AND NOT COALESCE(f.has_crm,FALSE) THEN 'A02_RTC_only_no_buyer_picks'
      WHEN COALESCE(f.has_mm,FALSE) AND COALESCE(f.has_rtc,FALSE)
       AND NOT COALESCE(f.has_select,FALSE) AND NOT COALESCE(f.has_3p,FALSE)
       AND NOT COALESCE(f.has_crm,FALSE) THEN 'A03_MM_plus_RTC'
      WHEN COALESCE(f.has_mm,FALSE) AND NOT COALESCE(f.has_rtc,FALSE)
       AND NOT COALESCE(f.has_select,FALSE) AND NOT COALESCE(f.has_3p,FALSE)
       AND NOT COALESCE(f.has_crm,FALSE) THEN 'A04_MM_only_no_RTC'
      WHEN COALESCE(f.has_mm,FALSE) AND COALESCE(f.has_rtc,FALSE)
       AND NOT COALESCE(f.has_select,FALSE) AND COALESCE(f.has_3p,FALSE)
       AND NOT COALESCE(f.has_crm,FALSE) THEN 'A05_MM_RTC_3P'
      WHEN COALESCE(f.has_mm,FALSE) AND COALESCE(f.has_rtc,FALSE)
       AND NOT COALESCE(f.has_select,FALSE) AND NOT COALESCE(f.has_3p,FALSE)
       AND COALESCE(f.has_crm,FALSE) THEN 'A06_MM_RTC_CRM'
      WHEN COALESCE(f.has_mm,FALSE) AND COALESCE(f.has_rtc,FALSE)
       AND NOT COALESCE(f.has_select,FALSE) AND COALESCE(f.has_3p,FALSE)
       AND COALESCE(f.has_crm,FALSE) THEN 'A07_MM_RTC_3P_CRM'
      WHEN NOT COALESCE(f.has_mm,FALSE) AND COALESCE(f.has_rtc,FALSE)
       AND NOT COALESCE(f.has_select,FALSE) AND COALESCE(f.has_3p,FALSE)
       AND NOT COALESCE(f.has_crm,FALSE) THEN 'A08_RTC_3P_no_MM'
      WHEN NOT COALESCE(f.has_mm,FALSE) AND COALESCE(f.has_rtc,FALSE)
       AND NOT COALESCE(f.has_select,FALSE) AND NOT COALESCE(f.has_3p,FALSE)
       AND COALESCE(f.has_crm,FALSE) THEN 'A09_RTC_CRM_no_MM'
      WHEN NOT COALESCE(f.has_mm,FALSE) AND COALESCE(f.has_rtc,FALSE)
       AND NOT COALESCE(f.has_select,FALSE) AND COALESCE(f.has_3p,FALSE)
       AND COALESCE(f.has_crm,FALSE) THEN 'A10_RTC_3P_CRM_no_MM'
      WHEN COALESCE(f.has_select,FALSE) AND NOT COALESCE(f.has_mm,FALSE)
       AND NOT COALESCE(f.has_rtc,FALSE) AND NOT COALESCE(f.has_3p,FALSE)
       AND NOT COALESCE(f.has_crm,FALSE) THEN 'A11_Select_only'
      WHEN COALESCE(f.has_select,FALSE) THEN 'A12_Select_combo'
      ELSE 'A99_other_combo'
    END AS bucket
  FROM active_prospecting ap LEFT JOIN flags f USING (campaign_id)
)
SELECT
  bucket,
  COUNT(*) AS n_campaigns,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_campaigns,
  COUNT(DISTINCT advertiser_id) AS n_advertisers,
  ROUND(SUM(spend_30d) / 1e6, 3) AS spend_30d_M,
  ROUND(100.0 * SUM(spend_30d) / SUM(SUM(spend_30d)) OVER (), 1) AS pct_spend,
  ROUND(SUM(spend_30d) * 12 / 1e6, 1) AS spend_annualized_M
FROM bucketed
GROUP BY bucket
ORDER BY bucket;
