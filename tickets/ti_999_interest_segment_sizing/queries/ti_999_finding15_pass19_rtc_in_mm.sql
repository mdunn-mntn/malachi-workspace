-- TI-999 Finding 15 Pass 19 — fold RTC into MM-touching
--
-- Change from Pass 18: an audience expression counts as MM if EITHER
--   (a) it references any MM DS in op:any  (DS13, DS19, DS38, DS46), OR
--   (b) it carries the score_type=rtc flag at the expression top level.
--
-- Rationale: Sean Yang (TI team, 2026-05-29) confirmed RTC = MM real-time
-- variant — same scoring system, just the hot-path that fires within an
-- hour for recent-site visitors. Bucket math should treat the two as one.
--
-- Audience axes (post-Pass 19):
--   MM   = {DS13, DS19, DS38, DS46} ∪ {score_type=rtc}   -- includes RTC pathway
--   MNTN Select = {DS9, DS42}
--   3P   = {DS17, DS18, DS35}   -- DS1 Oracle carved out (Sean: legacy, not in IPDSC)
--   CRM  = {DS4, DS8, DS47}      -- any polarity (include + exclude) for now
--
-- "Any signal" = positive OR negative DS reference, OR (for MM) the RTC flag.

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
    -- MM = any MM DS OR score_type=rtc flag
    LOGICAL_OR(c.data_source_id IN (13, 19, 38, 46)) AS has_mm_ds,
    ANY_VALUE(REGEXP_CONTAINS(p.expression, r'"score_type"\s*:\s*"rtc"')) AS has_rtc_flag,
    LOGICAL_OR(c.data_source_id IN (9, 42)) AS has_select,
    LOGICAL_OR(c.data_source_id IN (17, 18, 35)) AS has_3p,
    LOGICAL_OR(c.data_source_id IN (4, 8, 47)) AS has_crm
  FROM parsed p LEFT JOIN UNNEST(p.cats) c
  GROUP BY 1
),
bucketed AS (
  SELECT ap.*,
    -- has_mm = MM-DS OR RTC flag (per Sean Yang: same scoring system)
    (COALESCE(f.has_mm_ds, FALSE) OR COALESCE(f.has_rtc_flag, FALSE)) AS has_mm,
    f.has_select, f.has_3p, f.has_crm,
    -- Sub-flags so we can split MM-touching into batch vs RTC-only
    f.has_mm_ds, f.has_rtc_flag,
    CASE
      WHEN (COALESCE(f.has_mm_ds,FALSE) OR COALESCE(f.has_rtc_flag,FALSE))
        AND COALESCE(f.has_select,FALSE) AND COALESCE(f.has_3p,FALSE) AND COALESCE(f.has_crm,FALSE)
        THEN '16_MM_Select_3P_CRM'
      WHEN (COALESCE(f.has_mm_ds,FALSE) OR COALESCE(f.has_rtc_flag,FALSE))
        AND COALESCE(f.has_select,FALSE) AND COALESCE(f.has_3p,FALSE) THEN '15_MM_Select_3P'
      WHEN (COALESCE(f.has_mm_ds,FALSE) OR COALESCE(f.has_rtc_flag,FALSE))
        AND COALESCE(f.has_select,FALSE) AND COALESCE(f.has_crm,FALSE) THEN '14_MM_Select_CRM'
      WHEN (COALESCE(f.has_mm_ds,FALSE) OR COALESCE(f.has_rtc_flag,FALSE))
        AND COALESCE(f.has_3p,FALSE) AND COALESCE(f.has_crm,FALSE) THEN '13_MM_3P_CRM'
      WHEN COALESCE(f.has_select,FALSE) AND COALESCE(f.has_3p,FALSE) AND COALESCE(f.has_crm,FALSE)
        THEN '12_Select_3P_CRM'
      WHEN (COALESCE(f.has_mm_ds,FALSE) OR COALESCE(f.has_rtc_flag,FALSE))
        AND COALESCE(f.has_select,FALSE) THEN '11_MM_Select'
      WHEN (COALESCE(f.has_mm_ds,FALSE) OR COALESCE(f.has_rtc_flag,FALSE))
        AND COALESCE(f.has_3p,FALSE) THEN '10_MM_3P'
      WHEN (COALESCE(f.has_mm_ds,FALSE) OR COALESCE(f.has_rtc_flag,FALSE))
        AND COALESCE(f.has_crm,FALSE) THEN '09_MM_CRM'
      WHEN COALESCE(f.has_select,FALSE) AND COALESCE(f.has_3p,FALSE) THEN '08_Select_3P'
      WHEN COALESCE(f.has_select,FALSE) AND COALESCE(f.has_crm,FALSE) THEN '07_Select_CRM'
      WHEN COALESCE(f.has_3p,FALSE) AND COALESCE(f.has_crm,FALSE) THEN '06_3P_CRM'
      WHEN (COALESCE(f.has_mm_ds,FALSE) OR COALESCE(f.has_rtc_flag,FALSE)) THEN '05_MM_only'
      WHEN COALESCE(f.has_select,FALSE) THEN '04_Select_only'
      WHEN COALESCE(f.has_3p,FALSE) THEN '03_3P_only'
      WHEN COALESCE(f.has_crm,FALSE) THEN '02_CRM_only'
      ELSE '01_nothing'
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
  ROUND(SUM(spend_30d) * 12 / 1e6, 1) AS spend_annualized_M,
  -- Sub-breakdown: of MM-touching campaigns, how many are RTC-only (no batch MM DS) vs batch MM
  COUNTIF(has_mm_ds AND NOT has_rtc_flag) AS n_mm_batch_only,
  COUNTIF(NOT has_mm_ds AND has_rtc_flag) AS n_mm_rtc_only,
  COUNTIF(has_mm_ds AND has_rtc_flag) AS n_mm_batch_and_rtc
FROM bucketed
GROUP BY bucket
ORDER BY bucket;
