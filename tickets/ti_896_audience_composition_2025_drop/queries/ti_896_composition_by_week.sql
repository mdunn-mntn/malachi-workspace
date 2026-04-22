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

-- 2) DS classifier: map every data_source_id to a war-room category
ds_class AS (
  SELECT
    data_source_id, name,
    CASE
      -- MM (Mountain Match) — MNTN's targetable first-party + per-advertiser FP
      WHEN data_source_id = 19                                          THEN 'MM'
      WHEN data_source_id = 2                                           THEN 'MM'
      WHEN name LIKE '% - First Party Audience'                         THEN 'MM'
      -- 3P (Third-Party vendors + per-advertiser 3P)
      WHEN data_source_id IN (1, 3, 11, 17, 18, 20, 22, 24, 25, 26, 27, 28, 29, 32, 33, 35, 36, 39, 40, 44, 50, 51, 52, 55, 56)
                                                                        THEN '3P'
      WHEN name LIKE '% - Third Party Audience'                         THEN '3P'
      -- CRM (first-party email/IP uploads)
      WHEN data_source_id IN (4, 31, 47)                                THEN 'CRM'
      -- Interest / Intent / Peak Performance family
      WHEN data_source_id IN (13, 42, 46)                               THEN 'Interest'
      -- RTC Keywords / MNTN-managed keyword lists
      WHEN data_source_id IN (38)                                       THEN 'RTC Keywords'
      -- Extension / lookalike
      WHEN data_source_id IN (7)                                        THEN 'Extension'
      WHEN name LIKE '% - Extension Audience'                           THEN 'Extension'
      -- Control / retargeting exclusion (NOT ad-targeting audience types)
      WHEN data_source_id IN (6, 21, 23, 34)                            THEN 'Exclusion/Control'
      WHEN name LIKE '% - Control Group Audience'                       THEN 'Exclusion/Control'
      -- IP lists
      WHEN data_source_id IN (8, 10)                                    THEN 'IP/Geo List'
      -- Infrastructure (ignore in headline cuts)
      WHEN data_source_id IN (9, 12, 14, 15, 16, 30, 38)                THEN 'Infrastructure'
      ELSE 'Other'
    END AS category
  FROM `dw-main-bronze.integrationprod.data_sources`
),

-- 3) All archive rows for cohort advertisers, with LEAD for effective window
archive_rows AS (
  SELECT
    asa.campaign_id,
    c.advertiser_id,
    c.funnel_level,
    c.objective_id,
    asa.audience_id,
    asa.version,
    asa.update_time,
    COALESCE(
      LEAD(asa.update_time) OVER (PARTITION BY asa.campaign_id ORDER BY asa.update_time, asa.version),
      CURRENT_TIMESTAMP()
    ) AS next_update_time,
    asa.expression
  FROM `dw-main-bronze.integrationprod.archives_audience_segment_archives` asa
  JOIN `dw-main-bronze.integrationprod.campaigns` c USING (campaign_id)
  JOIN cohort USING (advertiser_id)
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
    CAST(m_ds_id AS INT64) AS data_source_id
  FROM archive_rows ar,
  UNNEST(REGEXP_EXTRACT_ALL(ar.expression, r'"data_source_id"\s*:\s*(\d+)[,}\s]')) AS m_ds_id
),

-- 5) Collapse DS ids → category per (campaign, version) — one row per DS-category used
archive_cat AS (
  SELECT DISTINCT
    ar.campaign_id,
    ar.advertiser_id,
    ar.funnel_level,
    ar.objective_id,
    ar.version,
    ar.update_time,
    ar.next_update_time,
    dsc.category
  FROM archive_ds ar
  JOIN ds_class dsc USING (data_source_id)
  WHERE dsc.category NOT IN ('Infrastructure', 'Exclusion/Control', 'Other', 'IP/Geo List')
),

-- 6) Explode each version's effective window to weeks
weekly AS (
  SELECT
    ac.campaign_id,
    ac.advertiser_id,
    ac.funnel_level,
    ac.objective_id,
    ac.category,
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
    ANY_VALUE(objective_id) AS objective_id
  FROM weekly
  GROUP BY week_start, campaign_id, advertiser_id, category
),

-- 8) Per-campaign-week set of categories used (LOGICAL_OR by unique campaign)
camp_week AS (
  SELECT
    week_start, campaign_id, advertiser_id,
    ANY_VALUE(funnel_level) AS funnel_level,
    ANY_VALUE(objective_id) AS objective_id,
    LOGICAL_OR(category = 'MM')           AS has_mm,
    LOGICAL_OR(category = '3P')           AS has_3p,
    LOGICAL_OR(category = 'CRM')          AS has_crm,
    LOGICAL_OR(category = 'Interest')     AS has_interest,
    LOGICAL_OR(category = 'RTC Keywords') AS has_rtc_kw,
    LOGICAL_OR(category = 'Extension')    AS has_extension
  FROM camp_week_cat
  GROUP BY week_start, campaign_id, advertiser_id
)

-- 9) Cohort rollup
SELECT
  week_start,
  COUNT(DISTINCT advertiser_id) AS n_advertisers,
  COUNT(DISTINCT campaign_id)   AS n_campaigns,

  -- Share of ADVERTISERS with any campaign in each bucket
  COUNT(DISTINCT IF(has_mm,        advertiser_id, NULL)) / NULLIF(COUNT(DISTINCT advertiser_id), 0) AS pct_adv_mm,
  COUNT(DISTINCT IF(has_3p,        advertiser_id, NULL)) / NULLIF(COUNT(DISTINCT advertiser_id), 0) AS pct_adv_3p,
  COUNT(DISTINCT IF(has_crm,       advertiser_id, NULL)) / NULLIF(COUNT(DISTINCT advertiser_id), 0) AS pct_adv_crm,
  COUNT(DISTINCT IF(has_interest,  advertiser_id, NULL)) / NULLIF(COUNT(DISTINCT advertiser_id), 0) AS pct_adv_interest,
  COUNT(DISTINCT IF(has_rtc_kw,    advertiser_id, NULL)) / NULLIF(COUNT(DISTINCT advertiser_id), 0) AS pct_adv_rtc_kw,
  COUNT(DISTINCT IF(has_extension, advertiser_id, NULL)) / NULLIF(COUNT(DISTINCT advertiser_id), 0) AS pct_adv_extension,

  -- Share of CAMPAIGNS touching each bucket
  COUNTIF(has_mm)        / NULLIF(COUNT(*), 0) AS pct_camp_mm,
  COUNTIF(has_3p)        / NULLIF(COUNT(*), 0) AS pct_camp_3p,
  COUNTIF(has_crm)       / NULLIF(COUNT(*), 0) AS pct_camp_crm,
  COUNTIF(has_interest)  / NULLIF(COUNT(*), 0) AS pct_camp_interest,
  COUNTIF(has_rtc_kw)    / NULLIF(COUNT(*), 0) AS pct_camp_rtc_kw,
  COUNTIF(has_extension) / NULLIF(COUNT(*), 0) AS pct_camp_extension,

  -- Retargeting cut (Alex Knorr): objective_id=4 retargeting; 1,5,6 prospecting
  COUNTIF(objective_id = 4)           / NULLIF(COUNT(*), 0) AS pct_camp_retargeting,
  COUNTIF(objective_id IN (1, 5, 6))  / NULLIF(COUNT(*), 0) AS pct_camp_prospecting,
  -- Funnel-level cross-check (known gotcha: objective_id unreliable post-TV-migration)
  COUNTIF(funnel_level = 1)           / NULLIF(COUNT(*), 0) AS pct_camp_funnel_1,
  COUNTIF(funnel_level >= 2)          / NULLIF(COUNT(*), 0) AS pct_camp_funnel_ge2
FROM camp_week
GROUP BY week_start
ORDER BY week_start
