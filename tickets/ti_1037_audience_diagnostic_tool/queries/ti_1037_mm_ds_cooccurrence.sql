/* ============================================================================
   TI-1037 — "MM" definition disambiguation: DS13/DS19/DS46 co-occurrence
   ----------------------------------------------------------------------------
   Question (Alyson): MNTN defines MM as "has DS19". But Fangorn-flipped
   flagships carry DS46, and legacy vertical-only audiences carry DS13. Which
   combinations of {DS13, DS19, DS46} actually occur on live prospecting?

   Unit  : prospecting campaigns (objective_id=1, funnel_level=1) that DELIVERED
           (impressions>0) in the last 45 days (absorbs the ~17-day rollup lag).
   Layer : SEGMENT level — dw-main-silver.audience.audience_segments
           (expression_type_id=2, is_targeted=TRUE) = the bidder-facing compiled
           expression. NB the TEMPLATE level (audience.audiences) still shows
           DS13/DS19 even after a Fangorn flip rewrites segments to DS46.
   DS ids: regex-extracted like dashboard module 07:
           REGEXP_EXTRACT_ALL(expression, r'"data_source_id":([0-9]+)')

   Result 2026-07-08: 6 of 8 cells occur; the two ZERO cells are exactly the
   ones with DS13 AND DS46 together — confirms the onFangorn flip SWAPS
   DS13 -> DS46 at segment level. DS19 survives the flip (DS19+DS46 = 18.9%
   of spend). DS46-only = 6.5% of spend (ex-vertical-only, MM-scored, but
   invisible to a "has DS19" definition); DS13-only = 1.1% (not yet flipped).
   ============================================================================ */
WITH deliv AS (
  SELECT campaign_id, ANY_VALUE(advertiser_id) AS advertiser_id,
         SUM(media_spend + data_spend + platform_spend) AS spend
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`
  WHERE day >= DATE_SUB(CURRENT_DATE(), INTERVAL 45 DAY) AND impressions > 0
  GROUP BY campaign_id
),
camp AS (
  SELECT c.campaign_id, d.advertiser_id, d.spend
  FROM `dw-main-bronze.integrationprod.campaigns` c
  JOIN deliv d USING (campaign_id)
  WHERE c.deleted = FALSE AND c.objective_id = 1 AND c.funnel_level = 1
),
seg AS (
  SELECT s.campaign_id,
    ARRAY_CONCAT_AGG(REGEXP_EXTRACT_ALL(s.expression, r'"data_source_id":([0-9]+)')) AS ids
  FROM `dw-main-silver.audience.audience_segments` s
  JOIN camp USING (campaign_id)
  WHERE s.expression_type_id = 2 AND s.is_targeted = TRUE
  GROUP BY s.campaign_id
),
flags AS (
  SELECT campaign_id,
    ('13' IN UNNEST(ids)) AS ds13,
    ('19' IN UNNEST(ids)) AS ds19,
    ('46' IN UNNEST(ids)) AS ds46,
    ('35' IN UNNEST(ids) OR '18' IN UNNEST(ids) OR '11' IN UNNEST(ids)) AS any3p
  FROM seg
)
SELECT f.ds13, f.ds19, f.ds46,
  COUNT(*) AS n_campaigns,
  COUNT(DISTINCT c.advertiser_id) AS n_advertisers,
  ROUND(SUM(c.spend)) AS spend_45d,
  ROUND(100 * SUM(c.spend) / SUM(SUM(c.spend)) OVER (), 1) AS pct_spend,
  COUNTIF(f.any3p) AS n_campaigns_with_3p
FROM flags f
JOIN camp c USING (campaign_id)
GROUP BY 1, 2, 3
ORDER BY spend_45d DESC;

/* ----------------------------------------------------------------------------
   Variant 2 (2026-07-08): bucket/vertical split INSIDE the DS13/DS46 leaves.
   Old MM 2.0 sheet: bucket = 3-digit DS13 segment id (industry), vertical =
   6-digit (subindustry). Result: ZERO live leaves contain a 3-digit bucket id —
   every DS13/DS46 leaf is {"data_source_id":13|46,"category_ids":[<6-digit
   vertical>]}, the SAME id as the campaign's score_type:rtc block. Bucket vs
   vertical is scoring-side only (bucket-not-vertical => MI tier), never a
   config axis. Config space = 2x3: DS19 y/n x anchor (none/DS13/DS46).

WITH deliv AS (... as above ...), camp AS (...),
seg AS (
  SELECT s.campaign_id,
    LOGICAL_OR(REGEXP_CONTAINS(s.expression, r"\"data_source_id\":13[^0-9]")) AS ds13,
    LOGICAL_OR(REGEXP_CONTAINS(s.expression, r"\"data_source_id\":19[^0-9]")) AS ds19,
    LOGICAL_OR(REGEXP_CONTAINS(s.expression, r"\"data_source_id\":46[^0-9]")) AS ds46,
    ARRAY_CONCAT_AGG(REGEXP_EXTRACT_ALL(s.expression, r"\"data_source_id\":13,\"category_ids\":\[([0-9,]+)\]")) AS l13,
    ARRAY_CONCAT_AGG(REGEXP_EXTRACT_ALL(s.expression, r"\"data_source_id\":46,\"category_ids\":\[([0-9,]+)\]")) AS l46
  FROM `dw-main-silver.audience.audience_segments` s
  JOIN camp USING (campaign_id)
  WHERE s.expression_type_id = 2 AND s.is_targeted = TRUE
  GROUP BY s.campaign_id
),
cls AS (
  SELECT campaign_id, ds13, ds19, ds46,
    COALESCE((SELECT LOGICAL_OR(LENGTH(TRIM(id)) = 3) FROM UNNEST(l13) l, UNNEST(SPLIT(l, ",")) id), FALSE) AS b13,
    COALESCE((SELECT LOGICAL_OR(LENGTH(TRIM(id)) = 6) FROM UNNEST(l13) l, UNNEST(SPLIT(l, ",")) id), FALSE) AS v13,
    COALESCE((SELECT LOGICAL_OR(LENGTH(TRIM(id)) = 3) FROM UNNEST(l46) l, UNNEST(SPLIT(l, ",")) id), FALSE) AS b46,
    COALESCE((SELECT LOGICAL_OR(LENGTH(TRIM(id)) = 6) FROM UNNEST(l46) l, UNNEST(SPLIT(l, ",")) id), FALSE) AS v46
  FROM seg
)
SELECT
  CASE WHEN NOT ds13 THEN "no DS13" WHEN b13 AND v13 THEN "DS13 bucket+vertical"
       WHEN v13 THEN "DS13 vertical only" WHEN b13 THEN "DS13 bucket only"
       ELSE "DS13 other-form" END AS ds13_kind,
  CASE WHEN NOT ds46 THEN "no DS46" WHEN b46 AND v46 THEN "DS46 bucket+vertical"
       WHEN v46 THEN "DS46 vertical only" WHEN b46 THEN "DS46 bucket only"
       ELSE "DS46 other-form" END AS ds46_kind,
  ds19, COUNT(*) n_campaigns, COUNT(DISTINCT c.advertiser_id) n_advertisers,
  ROUND(SUM(c.spend)) spend_45d
FROM cls JOIN camp c USING (campaign_id)
GROUP BY 1,2,3 ORDER BY spend_45d DESC;
---------------------------------------------------------------------------- */

/* ----------------------------------------------------------------------------
   Variant 3 (2026-07-08): v1 vs v2 DELIVERED SCORE DISTRIBUTION — proves the
   scoring-generation difference. 7d CIL, RTC excluded. Result:
   v1 (DS13) = fixed points ONLY (exactly 8000 / exactly 10000; ZERO imps at
   6666-7999 or 8001-9999). v2 (DS46) = two continuous bands with a pin at each
   top: PP pass 6666-7999 (1,206 distinct) + 8000 pin; HI pass 8001-9999
   (1,868 distinct) + 10000 pin. Split by DS19: 100% of the >8000 HI band sits
   on DS19-carrying campaigns — DS46-only ("vertical only") tops out at 8000.
   Methodology: Confluence 3414917161 (0.6/0.8 -> 3333/6666 transform).

WITH anchored AS (
  SELECT s.campaign_id,
    LOGICAL_OR(REGEXP_CONTAINS(s.expression, r"\"data_source_id\":13[^0-9]")) AS ds13,
    LOGICAL_OR(REGEXP_CONTAINS(s.expression, r"\"data_source_id\":19[^0-9]")) AS ds19,
    LOGICAL_OR(REGEXP_CONTAINS(s.expression, r"\"data_source_id\":46[^0-9]")) AS ds46
  FROM `dw-main-silver.audience.audience_segments` s
  JOIN `dw-main-bronze.integrationprod.campaigns` c USING (campaign_id)
  WHERE s.expression_type_id = 2 AND s.is_targeted = TRUE
    AND c.deleted = FALSE AND c.objective_id = 1 AND c.funnel_level = 1
  GROUP BY s.campaign_id
  HAVING ds13 OR ds46
)
SELECT
  IF(a.ds46, "v2 (DS46 Fangorn)", "v1 (DS13 legacy)") AS anchor,  -- add a.ds19 to split the HI band
  CASE WHEN l.household_score <= 0 THEN "a. unscored (<=0)"
       WHEN l.household_score BETWEEN 1 AND 3332 THEN "b. 1-3332 (MaxReach band)"
       WHEN l.household_score BETWEEN 3333 AND 6665 THEN "c. 3333-6665 (MI band)"
       WHEN l.household_score BETWEEN 6666 AND 7999 THEN "d. 6666-7999"
       WHEN l.household_score = 8000 THEN "e. exactly 8000"
       WHEN l.household_score BETWEEN 8001 AND 9999 THEN "f. 8001-9999"
       WHEN l.household_score = 10000 THEN "g. exactly 10000" END AS band,
  COUNT(*) AS imps,
  COUNT(DISTINCT l.household_score) AS distinct_scores
FROM `dw-main-silver.logdata.cost_impression_log` l
JOIN anchored a USING (campaign_id)
WHERE l.time >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY))
  AND (l.model_params IS NULL OR l.model_params NOT LIKE "%realtime_conquest_score=10000%")
GROUP BY 1, 2 ORDER BY 1, 2;
---------------------------------------------------------------------------- */

/* ----------------------------------------------------------------------------
   Companion: what does the "no DS13/19/46 at all" cell (26.8% of spend) run?
   Top DS sets by spend. Answer: DS14-only run-of-network ($2.5M / 42 adv),
   3P-only (DS35 LiveRamp IP / DS18 Dstillery / DS17 ShareThis), IP lists (DS8),
   1P (DS2), CRM excludes (DS4/47), own-funnel excludes (DS21/34), Oracle (DS1).
   Swap the final SELECT above for:

SELECT ds_set, COUNT(*) n_campaigns, COUNT(DISTINCT c.advertiser_id) n_advertisers,
       ROUND(SUM(c.spend)) spend_45d
FROM (SELECT campaign_id, ARRAY_TO_STRING(ARRAY(
        SELECT DISTINCT x FROM UNNEST(ids) x ORDER BY CAST(x AS INT64)), ',') AS ds_set
      FROM seg) s
JOIN camp c USING (campaign_id)
WHERE NOT REGEXP_CONTAINS(',' || ds_set || ',', r',(13|19|46),')
GROUP BY ds_set ORDER BY spend_45d DESC LIMIT 15;
---------------------------------------------------------------------------- */
