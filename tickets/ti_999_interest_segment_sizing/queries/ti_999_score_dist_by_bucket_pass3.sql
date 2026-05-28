-- TI-999 Finding 15 — Pass 3: delivered score distribution per Pass 2 sub-bucket
--
-- Empirical test of the AND-intersection vs OR-additive hypothesis:
--
--   - If MM + 3P incl_only delivers the SAME household_score distribution as
--     MM only, the 3P inclusion clause is dead weight (additive but never
--     reached because unscored IPs sit at the bottom of the bidder ranking).
--   - If MM + 3P incl_only shows a MEANINGFULLY higher unscored share, the
--     3P inclusion IS pulling unscored IPs into delivery at non-trivial rates.
--   - For MM + 1P excl_only: should look like MM only in score-band shape
--     (AND-NOT just narrows, doesn't change scoring).
--
-- Window: single day 2026-05-26 (matches TI-999 Finding 14d for direct
-- comparability; 61M impressions on that day; single-day scan keeps cost
-- manageable).
--
-- model_params is a flat string of `key=value, key=value, ...` per impression.
-- household_score, advertiser_household_score, realtime_conquest_score, and
-- campaign_id all appear as regex-extractable substrings.

CREATE TEMP FUNCTION parse_expression(expr STRING)
RETURNS ARRAY<STRUCT<data_source_id INT64, category_id INT64, polarity STRING>>
LANGUAGE js AS r"""
  if (!expr) return [];
  let parsed;
  try { parsed = JSON.parse(expr); } catch (e) { return []; }
  const out = [];
  function walk(node, negDepth) {
    if (!node || typeof node !== 'object') return;
    if (Array.isArray(node)) {
      for (const n of node) walk(n, negDepth);
      return;
    }
    const op = node.op;
    if (op === 'not') { walk(node.value, negDepth + 1); return; }
    if (op === 'any') {
      if (node.value && node.value.data_source_id != null && Array.isArray(node.value.category_ids)) {
        const ds = node.value.data_source_id;
        const polarity = (negDepth % 2 === 1) ? 'negative' : 'positive';
        for (const cid of node.value.category_ids) {
          out.push({data_source_id: ds, category_id: cid, polarity: polarity});
        }
      }
      return;
    }
    if (node.value !== undefined) walk(node.value, negDepth);
  }
  if (parsed && parsed.categories && parsed.categories.where) {
    walk(parsed.categories.where, 0);
  }
  return out;
""";

WITH active_campaigns AS (
  SELECT campaign_id, advertiser_id
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`
  WHERE day BETWEEN DATE('2026-04-29') AND DATE('2026-05-28')
  GROUP BY 1, 2
  HAVING SUM(impressions) > 0
),
parsed_expressions AS (
  SELECT campaign_id, parse_expression(expression) AS cats
  FROM (
    SELECT
      campaign_id, expression,
      ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY update_time DESC) AS rn
    FROM `dw-main-silver.audience.audience_segments`
    WHERE expression_type_id = 2 AND is_targeted = TRUE
      AND campaign_id IN (SELECT campaign_id FROM active_campaigns)
  )
  WHERE rn = 1
),
campaign_flags AS (
  SELECT
    pe.campaign_id,
    LOGICAL_OR(cat.data_source_id IN (13, 38, 46)) AS has_mm,
    LOGICAL_OR(cat.data_source_id IN (4, 8, 47)) AS has_1p,
    LOGICAL_OR(cat.data_source_id IN (17, 18, 35)) AS has_3p,
    LOGICAL_OR(cat.data_source_id IN (4, 8, 47)   AND cat.polarity = 'positive') AS has_1p_pos,
    LOGICAL_OR(cat.data_source_id IN (4, 8, 47)   AND cat.polarity = 'negative') AS has_1p_neg,
    LOGICAL_OR(cat.data_source_id IN (17, 18, 35) AND cat.polarity = 'positive') AS has_3p_pos,
    LOGICAL_OR(cat.data_source_id IN (17, 18, 35) AND cat.polarity = 'negative') AS has_3p_neg
  FROM parsed_expressions pe
  LEFT JOIN UNNEST(pe.cats) AS cat
  GROUP BY 1
),
bucket_panel AS (
  SELECT
    ac.campaign_id,
    -- Pass 1 bucket
    CASE
      WHEN     COALESCE(cf.has_mm,FALSE) AND     COALESCE(cf.has_1p,FALSE) AND     COALESCE(cf.has_3p,FALSE) THEN '8_MM_plus_1P_plus_3P'
      WHEN     COALESCE(cf.has_mm,FALSE) AND     COALESCE(cf.has_1p,FALSE) AND NOT COALESCE(cf.has_3p,FALSE) THEN '6_MM_plus_1P'
      WHEN     COALESCE(cf.has_mm,FALSE) AND NOT COALESCE(cf.has_1p,FALSE) AND     COALESCE(cf.has_3p,FALSE) THEN '5_MM_plus_3P'
      WHEN     COALESCE(cf.has_mm,FALSE) AND NOT COALESCE(cf.has_1p,FALSE) AND NOT COALESCE(cf.has_3p,FALSE) THEN '2_MM_only'
      WHEN NOT COALESCE(cf.has_mm,FALSE) AND     COALESCE(cf.has_1p,FALSE) AND     COALESCE(cf.has_3p,FALSE) THEN '7_1P_plus_3P'
      WHEN NOT COALESCE(cf.has_mm,FALSE) AND     COALESCE(cf.has_1p,FALSE) AND NOT COALESCE(cf.has_3p,FALSE) THEN '3_1P_only'
      WHEN NOT COALESCE(cf.has_mm,FALSE) AND NOT COALESCE(cf.has_1p,FALSE) AND     COALESCE(cf.has_3p,FALSE) THEN '4_3P_only'
      ELSE '1_nothing'
    END AS pass1_bucket,
    -- Pass 2 sub-bucket
    CASE
      WHEN COALESCE(cf.has_mm,FALSE) AND NOT COALESCE(cf.has_1p,FALSE) AND COALESCE(cf.has_3p,FALSE) THEN
        CASE
          WHEN     COALESCE(cf.has_3p_pos,FALSE) AND NOT COALESCE(cf.has_3p_neg,FALSE) THEN '5a_MM_plus_3P_incl_only'
          WHEN NOT COALESCE(cf.has_3p_pos,FALSE) AND     COALESCE(cf.has_3p_neg,FALSE) THEN '5b_MM_plus_3P_excl_only'
          ELSE '5c_MM_plus_3P_mixed_polarity'
        END
      WHEN COALESCE(cf.has_mm,FALSE) AND COALESCE(cf.has_1p,FALSE) AND NOT COALESCE(cf.has_3p,FALSE) THEN
        CASE
          WHEN     COALESCE(cf.has_1p_pos,FALSE) AND NOT COALESCE(cf.has_1p_neg,FALSE) THEN '6a_MM_plus_1P_incl_only'
          WHEN NOT COALESCE(cf.has_1p_pos,FALSE) AND     COALESCE(cf.has_1p_neg,FALSE) THEN '6b_MM_plus_1P_excl_only'
          ELSE '6c_MM_plus_1P_mixed_polarity'
        END
      WHEN COALESCE(cf.has_mm,FALSE) AND COALESCE(cf.has_1p,FALSE) AND COALESCE(cf.has_3p,FALSE) THEN '8_MM_plus_1P_plus_3P'
      ELSE
        CASE
          WHEN     COALESCE(cf.has_mm,FALSE) AND NOT COALESCE(cf.has_1p,FALSE) AND NOT COALESCE(cf.has_3p,FALSE) THEN '2_MM_only'
          WHEN NOT COALESCE(cf.has_mm,FALSE) AND     COALESCE(cf.has_1p,FALSE) AND NOT COALESCE(cf.has_3p,FALSE) THEN '3_1P_only'
          WHEN NOT COALESCE(cf.has_mm,FALSE) AND NOT COALESCE(cf.has_1p,FALSE) AND     COALESCE(cf.has_3p,FALSE) THEN '4_3P_only'
          WHEN NOT COALESCE(cf.has_mm,FALSE) AND     COALESCE(cf.has_1p,FALSE) AND     COALESCE(cf.has_3p,FALSE) THEN '7_1P_plus_3P'
          ELSE '1_nothing'
        END
    END AS sub_bucket
  FROM active_campaigns ac
  LEFT JOIN campaign_flags cf USING (campaign_id)
),
imps AS (
  SELECT
    SAFE_CAST(REGEXP_EXTRACT(c.model_params, r'campaign_id=(\d+)')                       AS INT64) AS campaign_id,
    SAFE_CAST(REGEXP_EXTRACT(c.model_params, r'household_score=(-?\d+)')                 AS INT64) AS hh_score,
    SAFE_CAST(REGEXP_EXTRACT(c.model_params, r'advertiser_household_score=(-?\d+)')      AS INT64) AS adv_hh_score,
    SAFE_CAST(REGEXP_EXTRACT(c.model_params, r'realtime_conquest_score=(-?\d+)')         AS INT64) AS rtc_score
  FROM `dw-main-silver.logdata.cost_impression_log` c
  WHERE DATE(c.time) = '2026-05-26'
)
SELECT
  bp.sub_bucket,
  CASE
    WHEN i.hh_score = -1    THEN 'a_unscored_minus1'
    WHEN i.hh_score < 1000  THEN 'b_1_999'
    WHEN i.hh_score < 5000  THEN 'c_1k_5k'
    WHEN i.hh_score < 8000  THEN 'd_5k_8k'
    WHEN i.hh_score < 10000 THEN 'e_8k_10k'
    WHEN i.hh_score = 10000 THEN 'f_10000'
    ELSE 'z_other_or_null'
  END AS hh_band,
  COUNT(*) AS n_imps,
  COUNTIF(i.rtc_score = 10000) AS n_rtc_qualified
FROM imps i
JOIN bucket_panel bp USING (campaign_id)
GROUP BY 1, 2
ORDER BY 1, 2;
