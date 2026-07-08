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
