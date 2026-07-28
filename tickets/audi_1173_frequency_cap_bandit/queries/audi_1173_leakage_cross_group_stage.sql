-- AUDI-1173 frequency-cap leakage: cross-GROUP + cross-STAGE, per household = (ip, advertiser_id).
-- fcap counters are keyed per campaign / campaign_group on IPv4 with NO advertiser rollup, so an
-- advertiser's frequency LEAKS across its campaign_groups (each group counts the IP independently);
-- and each funnel stage (S1 prospecting / S2-S3 retargeting) counts the IP independently too.
-- This quantifies both, plus an over-delivery estimate for the leaked households.
--
-- Source of campaign_group_id + funnel_level = the DIM join to public_campaigns (NEVER CIL.model_params).
-- Window 7d (2026-07-06..07-12) to match Phase-0 §4c; conservative floor (short window understates leakage).
-- Exclusions: WGU (advertiser_id=31357) and AID 90 (PSA). Spend = media_spend+data_spend+platform_spend.
-- ONE CIL scan; all rollups operate on the household-grain intermediate. Output = tagged UNION ALL sections.

WITH camp AS (
  SELECT campaign_id, campaign_group_id, funnel_level
  FROM `dw-main-bronze.integrationprod.public_campaigns`
  WHERE deleted = FALSE AND is_test = FALSE
),
-- finest grain: household x campaign_group x funnel_level (single CIL scan)
imp_grp AS (
  SELECT
    c.ip, c.advertiser_id,
    camp.campaign_group_id,
    camp.funnel_level,
    COUNT(*)                                                    AS imps,
    SUM(c.media_spend + c.data_spend + c.platform_spend)        AS spend
  FROM `dw-main-silver.logdata.cost_impression_log` c
  JOIN camp ON c.campaign_id = camp.campaign_id
  WHERE DATE(c.time) BETWEEN '2026-07-06' AND '2026-07-12'
    AND c.advertiser_id NOT IN (31357, 90)
    AND c.ip IS NOT NULL AND c.ip <> '0.0.0.0'
  GROUP BY 1, 2, 3, 4
),
-- household x campaign_group (roll stage up within group) -> heaviest single group counter
hhg AS (
  SELECT ip, advertiser_id, campaign_group_id, SUM(imps) AS gi, SUM(spend) AS gs
  FROM imp_grp GROUP BY 1, 2, 3
),
hhg_max AS (
  SELECT ip, advertiser_id, MAX(gi) AS max_gi FROM hhg GROUP BY 1, 2
),
-- household x funnel_level -> heaviest single stage counter
hhs AS (
  SELECT ip, advertiser_id, funnel_level, SUM(imps) AS si
  FROM imp_grp GROUP BY 1, 2, 3
),
hhs_max AS (
  SELECT ip, advertiser_id, MAX(si) AS max_si FROM hhs GROUP BY 1, 2
),
-- household grain
hh AS (
  SELECT
    ip, advertiser_id,
    SUM(imps)                                        AS total_imps,
    SUM(spend)                                       AS total_spend,
    COUNT(DISTINCT campaign_group_id)                AS n_groups,
    COUNT(DISTINCT funnel_level)                     AS n_stages,        -- NULL funnel excluded
    MAX(IF(funnel_level = 1, 1, 0))                  AS has_prosp,       -- S1 prospecting
    MAX(IF(funnel_level >= 2, 1, 0))                 AS has_retgt        -- S2/S3/S4 engaged/retargeting
  FROM imp_grp GROUP BY 1, 2
),
hh2 AS (
  SELECT h.*, g.max_gi, s.max_si,
    (h.total_spend / NULLIF(h.total_imps, 0))        AS cost_per_imp
  FROM hh h
  LEFT JOIN hhg_max g USING (ip, advertiser_id)
  LEFT JOIN hhs_max s USING (ip, advertiser_id)
),
-- ===== section outputs =====
-- 1) cross-GROUP distribution
sec_group AS (
  SELECT
    'group' AS section,
    CASE WHEN n_groups = 1 THEN '1_group' WHEN n_groups = 2 THEN '2_groups'
         WHEN n_groups = 3 THEN '3_groups' WHEN n_groups <= 5 THEN '4-5_groups'
         ELSE '6+_groups' END AS bucket,
    COUNT(*)                                         AS n_hh,
    ROUND(100*COUNT(*)/SUM(COUNT(*)) OVER (), 3)     AS hh_pct,
    SUM(total_imps)                                  AS imps,
    ROUND(100*SUM(total_imps)/SUM(SUM(total_imps)) OVER (), 3)  AS imp_pct,
    ROUND(SUM(total_spend), 2)                       AS spend,
    ROUND(100*SUM(total_spend)/SUM(SUM(total_spend)) OVER (), 3) AS spend_pct,
    ROUND(AVG(total_imps), 3)                        AS avg_imps,
    CAST(NULL AS INT64)                              AS excess_imps,
    CAST(NULL AS FLOAT64)                            AS excess_spend
  FROM hh2 GROUP BY 1, 2
),
-- 2) cross-STAGE distribution (distinct funnel_level count)
sec_stage AS (
  SELECT
    'stage' AS section,
    CASE WHEN n_stages = 0 THEN '0_unknown' WHEN n_stages = 1 THEN '1_stage'
         WHEN n_stages = 2 THEN '2_stages' WHEN n_stages = 3 THEN '3_stages'
         ELSE '4_stages' END AS bucket,
    COUNT(*), ROUND(100*COUNT(*)/SUM(COUNT(*)) OVER (), 3),
    SUM(total_imps), ROUND(100*SUM(total_imps)/SUM(SUM(total_imps)) OVER (), 3),
    ROUND(SUM(total_spend), 2), ROUND(100*SUM(total_spend)/SUM(SUM(total_spend)) OVER (), 3),
    ROUND(AVG(total_imps), 3), CAST(NULL AS INT64), CAST(NULL AS FLOAT64)
  FROM hh2 GROUP BY 1, 2
),
-- 3) prospecting AND retargeting on the SAME household (the headline cross-stage leak)
sec_pr AS (
  SELECT
    'prosp_x_retgt' AS section,
    CASE WHEN has_prosp = 1 AND has_retgt = 1 THEN 'both_S1_and_S2plus'
         WHEN has_prosp = 1 THEN 'prospecting_only'
         WHEN has_retgt = 1 THEN 'retargeting_only'
         ELSE 'unknown_stage_only' END AS bucket,
    COUNT(*), ROUND(100*COUNT(*)/SUM(COUNT(*)) OVER (), 3),
    SUM(total_imps), ROUND(100*SUM(total_imps)/SUM(SUM(total_imps)) OVER (), 3),
    ROUND(SUM(total_spend), 2), ROUND(100*SUM(total_spend)/SUM(SUM(total_spend)) OVER (), 3),
    ROUND(AVG(total_imps), 3), CAST(NULL AS INT64), CAST(NULL AS FLOAT64)
  FROM hh2 GROUP BY 1, 2
),
-- 4) over-delivery estimate. excess = impressions an advertiser-level rollup counter would have suppressed.
--    Method A (cap-agnostic): roll up to the household's HEAVIEST single counter (group / stage).
--      cross-group excess (leaked = n_groups>=2): total_imps - max_gi
--      cross-stage excess (leaked = n_stages>=2): total_imps - max_si
--    Method B (explicit advertiser cap of N/wk on leaked hh, leaked = 2+ groups OR 2+ stages): total_imps - N
sec_over AS (
  SELECT 'overdelivery' AS section, 'A_rollup_to_max_group' AS bucket,
    COUNTIF(n_groups >= 2) AS n_hh, CAST(NULL AS FLOAT64),
    CAST(NULL AS INT64), CAST(NULL AS FLOAT64),
    CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64),
    CAST(SUM(IF(n_groups >= 2, total_imps - max_gi, 0)) AS INT64) AS excess_imps,
    ROUND(SUM(IF(n_groups >= 2, (total_imps - max_gi) * cost_per_imp, 0)), 2) AS excess_spend
  FROM hh2
  UNION ALL
  SELECT 'overdelivery', 'A_rollup_to_max_stage',
    COUNTIF(n_stages >= 2), NULL, NULL, NULL, NULL, NULL, NULL,
    CAST(SUM(IF(n_stages >= 2, total_imps - max_si, 0)) AS INT64),
    ROUND(SUM(IF(n_stages >= 2, (total_imps - max_si) * cost_per_imp, 0)), 2)
  FROM hh2
  UNION ALL
  SELECT 'overdelivery', 'B_cap8_leaked',
    COUNTIF((n_groups >= 2 OR n_stages >= 2) AND total_imps > 8), NULL, NULL, NULL, NULL, NULL, NULL,
    CAST(SUM(IF((n_groups >= 2 OR n_stages >= 2) AND total_imps > 8, total_imps - 8, 0)) AS INT64),
    ROUND(SUM(IF((n_groups >= 2 OR n_stages >= 2) AND total_imps > 8, (total_imps - 8) * cost_per_imp, 0)), 2)
  FROM hh2
  UNION ALL
  SELECT 'overdelivery', 'B_cap12_leaked',
    COUNTIF((n_groups >= 2 OR n_stages >= 2) AND total_imps > 12), NULL, NULL, NULL, NULL, NULL, NULL,
    CAST(SUM(IF((n_groups >= 2 OR n_stages >= 2) AND total_imps > 12, total_imps - 12, 0)) AS INT64),
    ROUND(SUM(IF((n_groups >= 2 OR n_stages >= 2) AND total_imps > 12, (total_imps - 12) * cost_per_imp, 0)), 2)
  FROM hh2
),
-- grand totals for context
sec_tot AS (
  SELECT 'total' AS section, 'all_households' AS bucket,
    COUNT(*), CAST(100.0 AS FLOAT64),
    SUM(total_imps), CAST(100.0 AS FLOAT64),
    ROUND(SUM(total_spend), 2), CAST(100.0 AS FLOAT64),
    ROUND(AVG(total_imps), 3), CAST(NULL AS INT64), CAST(NULL AS FLOAT64)
  FROM hh2
)
SELECT * FROM sec_group
UNION ALL SELECT * FROM sec_stage
UNION ALL SELECT * FROM sec_pr
UNION ALL SELECT * FROM sec_over
UNION ALL SELECT * FROM sec_tot
ORDER BY section, bucket
