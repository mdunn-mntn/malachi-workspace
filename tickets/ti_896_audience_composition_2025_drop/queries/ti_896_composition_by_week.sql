-- TI-896: Audience-type composition time series (v2 — canonical classifier)
-- Cohort: every advertiser with any 2025 campaign spend
-- Lookback: 2024-11-01 → CURRENT_DATE (18 months per Richard's directive)
-- Method: regex-extract data_source_ids from expression JSON, join to canonical
-- data_sources dim, bucket to war-room categories, explode to weeks, rollup.

WITH
-- 1) Cohort: any advertiser with impressions >= 1 on any day in 2025
cohort AS (
  SELECT DISTINCT advertiser_id
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`
  WHERE day BETWEEN DATE('2025-01-01') AND CURRENT_DATE()
    AND impressions > 0
),

-- 1b) Last delivery day per campaign — used to cap LEAD windows for paused campaigns
-- Fix M10: a campaign that was paused and stopped getting CDC updates would
-- otherwise extend its expression effective window to CURRENT_TIMESTAMP forever.
camp_last_active AS (
  SELECT campaign_id, MAX(day) AS last_active_day
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`
  WHERE day BETWEEN DATE('2024-11-01') AND CURRENT_DATE()
    AND impressions > 0
  GROUP BY campaign_id
),

-- 2) DS classifier — aligned to Bryce Wagg's war-room scope post (2026-04-22 13:25).
-- Bryce's canonical 5 buckets:
--   DS19 = Keywords
--   DS13 = Peak Performance
--   DS35 = 3rd Party
--   DS4  = CRM
--   Mountain Matched = separate (interpret as DS2 + per-advertiser First Party audiences)
-- Everything else → Other (reported for completeness but not headline)
ds_class AS (
  SELECT
    data_source_id, name,
    CASE
      WHEN data_source_id = 19                        THEN 'Keywords'
      WHEN data_source_id = 13                        THEN 'Peak Performance'
      WHEN data_source_id = 35                        THEN '3P'
      WHEN data_source_id = 4                         THEN 'CRM'
      WHEN data_source_id = 2                         THEN 'MM'
      WHEN name LIKE '% - First Party Audience'       THEN 'MM'
      ELSE 'Other'
    END AS category
  FROM `dw-main-bronze.integrationprod.data_sources`
),

-- 3) All archive rows for cohort advertisers, with LEAD for effective window.
-- Flag DS13+DS19 presence per-expression here so the strict PP detector works at version grain.
archive_rows AS (
  SELECT
    asa.campaign_id,
    c.advertiser_id,
    c.funnel_level,
    c.objective_id,
    asa.audience_id,
    asa.version,
    asa.update_time,
    -- Cap effective window at min(LEAD, last_active_day + 1) to prevent paused-but-not-edited
    -- campaigns from inflating current-week PP adoption (Fix M10).
    LEAST(
      COALESCE(
        LEAD(asa.update_time) OVER (PARTITION BY asa.campaign_id ORDER BY asa.update_time, asa.version),
        CURRENT_TIMESTAMP()
      ),
      COALESCE(TIMESTAMP_ADD(TIMESTAMP(la.last_active_day), INTERVAL 1 DAY),
               CURRENT_TIMESTAMP())
    ) AS next_update_time,
    asa.expression,
    -- Peak Performance detector (schema-specific to segment_archives).
    -- Named PP audiences (sampled in V1 from audiences_archives) pair DS13 intent layer with
    -- DS19 keywords. Translated to the segment-level schema they ALWAYS carry an RTC score
    -- directive: "score":{"types":[{"score_type":"rtc","id":<N>}]}. Pre-Oct-2025 matches that
    -- had DS13+DS19 but NO "score_type":"rtc" were legacy hybrid Interest+Keywords audiences,
    -- not PP. Require all three signals to count as PP.
    REGEXP_CONTAINS(asa.expression, r'"score_type"\s*:\s*"rtc"')
      AND REGEXP_CONTAINS(asa.expression, r'"data_source_id"\s*:\s*13\b')
      AND REGEXP_CONTAINS(asa.expression, r'"data_source_id"\s*:\s*19\b') AS is_pp_expr
  FROM `dw-main-bronze.integrationprod.archives_audience_segment_archives` asa
  JOIN `dw-main-bronze.integrationprod.campaigns` c USING (campaign_id)
  JOIN cohort USING (advertiser_id)
  LEFT JOIN camp_last_active la USING (campaign_id)
  WHERE asa.expression_type_id = 2
    AND asa.is_targeted = TRUE
    AND c.deleted = FALSE
    AND c.is_test = FALSE
    AND asa.update_time >= TIMESTAMP('2024-11-01')
),

-- 4) Extract every data_source_id mentioned in each expression
archive_ds AS (
  SELECT
    ar.campaign_id,
    ar.advertiser_id,
    ar.funnel_level,
    ar.objective_id,
    ar.audience_id,
    ar.version,
    ar.update_time,
    ar.next_update_time,
    ar.is_pp_expr,
    CAST(m_ds_id AS INT64) AS data_source_id
  FROM archive_rows ar,
  UNNEST(REGEXP_EXTRACT_ALL(ar.expression, r'"data_source_id"\s*:\s*(\d+)[,}\s]')) AS m_ds_id
),

-- 5) Collapse DS ids → category per (campaign, version) — one row per DS-category used
-- Keep the 5 canonical buckets + MM; drop 'Other' for headline cuts.
archive_cat AS (
  SELECT DISTINCT
    ar.campaign_id,
    ar.advertiser_id,
    ar.funnel_level,
    ar.objective_id,
    ar.version,
    ar.update_time,
    ar.next_update_time,
    ar.is_pp_expr,
    dsc.category
  FROM archive_ds ar
  JOIN ds_class dsc USING (data_source_id)
  WHERE dsc.category <> 'Other'
),

-- 6) Explode each version's effective window to weeks
weekly AS (
  SELECT
    ac.campaign_id,
    ac.advertiser_id,
    ac.funnel_level,
    ac.objective_id,
    ac.category,
    ac.is_pp_expr,
    week_start
  FROM archive_cat ac,
  UNNEST(GENERATE_DATE_ARRAY(
    GREATEST(DATE_TRUNC(DATE(update_time), WEEK(MONDAY)), DATE('2024-11-04')),
    LEAST(DATE_TRUNC(DATE(next_update_time), WEEK(MONDAY)), DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY))),
    INTERVAL 1 WEEK
  )) AS week_start
),

-- 7) Per-campaign-week category flags (dedup if version changed mid-week)
camp_week_cat AS (
  SELECT
    week_start, campaign_id, advertiser_id, category,
    ANY_VALUE(funnel_level) AS funnel_level,
    ANY_VALUE(objective_id) AS objective_id,
    LOGICAL_OR(is_pp_expr)  AS is_pp_expr
  FROM weekly
  GROUP BY week_start, campaign_id, advertiser_id, category
),

-- 8) Per-campaign-week set of categories used (LOGICAL_OR by unique campaign)
-- Peak Performance uses the STRICT detector (DS13 AND DS19 in same expression)
-- because post-V3 verification (2026-04-22):
--   - DS13-alone over-counts: pre-Oct baseline had ~13% DS13 usage without DS19 (plain
--     Vertical Categorization / Interest audiences, not Peak Performance).
--   - "Peak Performance"-named audiences ALWAYS pair DS13 intent layer with DS19 keywords
--     (OR clause). is_pp_expr on archive_rows carries that conjunction, LOGICAL_OR'd here.
camp_week AS (
  SELECT
    week_start, campaign_id, advertiser_id,
    ANY_VALUE(funnel_level) AS funnel_level,
    ANY_VALUE(objective_id) AS objective_id,
    LOGICAL_OR(category = 'MM')        AS has_mm,
    LOGICAL_OR(category = '3P')        AS has_3p,
    LOGICAL_OR(category = 'CRM')       AS has_crm,
    LOGICAL_OR(is_pp_expr)             AS has_pp,
    LOGICAL_OR(category = 'Keywords')  AS has_keywords
  FROM camp_week_cat
  GROUP BY week_start, campaign_id, advertiser_id
)

-- 9) Cohort rollup
SELECT
  week_start,
  COUNT(DISTINCT advertiser_id) AS n_advertisers,
  COUNT(DISTINCT campaign_id)   AS n_campaigns,

  -- Share of ADVERTISERS with any campaign in each bucket
  COUNT(DISTINCT IF(has_mm,       advertiser_id, NULL)) / NULLIF(COUNT(DISTINCT advertiser_id), 0) AS pct_adv_mm,
  COUNT(DISTINCT IF(has_3p,       advertiser_id, NULL)) / NULLIF(COUNT(DISTINCT advertiser_id), 0) AS pct_adv_3p,
  COUNT(DISTINCT IF(has_crm,      advertiser_id, NULL)) / NULLIF(COUNT(DISTINCT advertiser_id), 0) AS pct_adv_crm,
  COUNT(DISTINCT IF(has_pp,       advertiser_id, NULL)) / NULLIF(COUNT(DISTINCT advertiser_id), 0) AS pct_adv_pp,
  COUNT(DISTINCT IF(has_keywords, advertiser_id, NULL)) / NULLIF(COUNT(DISTINCT advertiser_id), 0) AS pct_adv_keywords,

  -- Share of CAMPAIGNS touching each bucket
  COUNTIF(has_mm)       / NULLIF(COUNT(*), 0) AS pct_camp_mm,
  COUNTIF(has_3p)       / NULLIF(COUNT(*), 0) AS pct_camp_3p,
  COUNTIF(has_crm)      / NULLIF(COUNT(*), 0) AS pct_camp_crm,
  COUNTIF(has_pp)       / NULLIF(COUNT(*), 0) AS pct_camp_pp,
  COUNTIF(has_keywords) / NULLIF(COUNT(*), 0) AS pct_camp_keywords,

  -- Retargeting cut (Alex Knorr): objective_id=4 retargeting; 1,5,6 prospecting
  COUNTIF(objective_id = 4)           / NULLIF(COUNT(*), 0) AS pct_camp_retargeting,
  COUNTIF(objective_id IN (1, 5, 6))  / NULLIF(COUNT(*), 0) AS pct_camp_prospecting,
  -- Funnel-level cross-check (known gotcha: objective_id unreliable post-TV-migration)
  COUNTIF(funnel_level = 1)           / NULLIF(COUNT(*), 0) AS pct_camp_funnel_1,
  COUNTIF(funnel_level >= 2)          / NULLIF(COUNT(*), 0) AS pct_camp_funnel_ge2
FROM camp_week
GROUP BY week_start
ORDER BY week_start
