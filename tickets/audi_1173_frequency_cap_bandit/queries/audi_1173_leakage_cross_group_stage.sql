-- AUDI-1173 frequency-cap leakage (RE-RUN, fixes 2 adversarial blockers): cross-GROUP + cross-STAGE,
-- household = (ip, advertiser_id). fcap counters are keyed per campaign / campaign_group on IPv4 with NO
-- advertiser rollup, so an advertiser's frequency LEAKS across its campaign_groups and its funnel stages
-- (each counts the IP independently). This quantifies both, plus the over-delivery a hypothetical
-- advertiser-level total-frequency cap would suppress.
--
-- BLOCKER-1 FIX (shared-IP contamination): (ip, advertiser) is NOT one household -- CGNAT/NAT gateways put
--   many real households behind one IPv4, inflating apparent leakage. Reuse the SAME shared-IP purge as
--   audi_1173_delivered_freq_curve.sql: shared_ip = ndev>=51 OR nadv>=121 OR nimp>=501 (per-IP, this window).
--   Every distribution + over-delivery figure is emitted RAW and PURGED; PURGED is the honest number.
--   The ip_nadv_diag section reproduces the contamination gradient (leaked% rises with distinct-advertiser
--   count per IP) that the purge removes.
-- BLOCKER-2 FIX (over-delivery mis-definition): Method A (total_imps - heaviest_counter) is DROPPED -- it
--   counted excess on households with total_imps <= cap, which no cap would suppress. Over-delivery is now
--   ONLY what an advertiser total-frequency cap at C would actually suppress = SUM_hh max(0, total_imps - C),
--   C in {3,8,12} (mirrors overcap_spend_capN in the freq-curve). Split by population (all / leaked /
--   non-leaked) and by DEFAULT-cap-only impressions (the slice the proposed fix is capable of governing).
--
-- CAP-TYPE: frequency_cap_type_id from public_campaigns (2=default, 1=custom). Only the DEFAULT population is
--   addressable by the proposed advertiser-rollup fix. advertiser_frequency_caps is EMPTY (0 rows) -> the
--   advertiser-rollup counter does not exist today (a missing CAPABILITY, not a mis-set value).
--
-- Source of campaign_group_id + funnel_level + frequency_cap_type_id = the DIM join to public_campaigns
--   (NEVER cost_impression_log.model_params). Window 7d (2026-07-06..07-12) to match Phase-0 §4c;
--   conservative floor (short window understates true household frequency). Exclusions: WGU (31357), AID 90.
--   Spend = media_spend+data_spend+platform_spend. ONE CIL scan; all rollups on the household intermediate.
--
-- Output column map (single tagged UNION ALL, 13 cols):
--   section, purge, bucket, cap_c, n_hh, hh_pct, imps, imp_pct, spend, spend_pct, avg_imps, x1, x2
--   distributions (group/stage/prosp_x_retgt/total): x1,x2 = NULL
--   ip_nadv_diag:   x1 = leaked_hh (n_groups>=2 OR n_stages>=2), x2 = leaked_pct within nadv bucket
--   captype:        x1 = leaked_hh, x2 = leaked_pct within cap_class
--   overdelivery:   cap_c = C; n_hh = households over C in that population; x1 = excess_imps; x2 = excess_spend

WITH camp AS (
  SELECT campaign_id, campaign_group_id, funnel_level, frequency_cap_type_id AS cap_type
  FROM `dw-main-bronze.integrationprod.public_campaigns`
  WHERE deleted = FALSE AND is_test = FALSE
),
-- ONE CIL scan, aggregated to device grain so the per-IP purge flags (distinct guid / advertiser / imps)
-- are computable from the same intermediate.
base AS (
  SELECT
    c.ip, c.advertiser_id,
    camp.campaign_group_id, camp.funnel_level, camp.cap_type,
    c.guid,
    COUNT(*)                                              AS imps,
    SUM(c.media_spend + c.data_spend + c.platform_spend)  AS spend
  FROM `dw-main-silver.logdata.cost_impression_log` c
  JOIN camp ON c.campaign_id = camp.campaign_id
  WHERE DATE(c.time) BETWEEN '2026-07-06' AND '2026-07-12'
    AND c.advertiser_id NOT IN (31357, 90)
    AND c.ip IS NOT NULL AND c.ip <> '0.0.0.0'
  GROUP BY 1, 2, 3, 4, 5, 6
),
-- per-IP shared/NAT flag (SAME thresholds as the freq-curve) + distinct-advertiser count for the diagnostic
ip_flag AS (
  SELECT
    ip,
    (COUNT(DISTINCT guid) >= 51 OR COUNT(DISTINCT advertiser_id) >= 121 OR SUM(imps) >= 501) AS shared_ip,
    COUNT(DISTINCT advertiser_id) AS ip_nadv
  FROM base
  GROUP BY ip
),
-- household grain = (ip, advertiser_id)
hh AS (
  SELECT
    ip, advertiser_id,
    SUM(imps)                                    AS total_imps,
    SUM(spend)                                   AS total_spend,
    COUNT(DISTINCT campaign_group_id)            AS n_groups,
    COUNT(DISTINCT funnel_level)                 AS n_stages,        -- NULL funnel excluded by COUNT DISTINCT
    MAX(IF(funnel_level = 1, 1, 0))              AS has_prosp,       -- S1 prospecting
    MAX(IF(funnel_level >= 2, 1, 0))             AS has_retgt,       -- S2/S3/S4 engaged/retargeting
    MIN(cap_type)                                AS min_ct,
    MAX(cap_type)                                AS max_ct,
    SUM(IF(cap_type = 2, imps, 0))               AS default_imps,    -- impressions on DEFAULT-cap campaigns
    SUM(IF(cap_type = 2, spend, 0))              AS default_spend
  FROM base
  GROUP BY 1, 2
),
hh2 AS (
  SELECT
    h.*, f.shared_ip, f.ip_nadv,
    CASE WHEN min_ct = 2 AND max_ct = 2 THEN 'default_only'
         WHEN min_ct = 1 AND max_ct = 1 THEN 'custom_only'
         ELSE 'mixed' END AS cap_class
  FROM hh h
  JOIN ip_flag f USING (ip)
),
-- explode each household into RAW (always kept) + PURGED (kept only if not shared_ip)
hhp AS (
  SELECT p.purge, h.*
  FROM hh2 h,
    UNNEST([STRUCT('1_raw' AS purge, TRUE AS keep),
            STRUCT('2_purged' AS purge, NOT h.shared_ip AS keep)]) AS p
  WHERE p.keep
),
-- ===== distributions =====
sec_group AS (
  SELECT 'group' AS section, purge,
    CASE WHEN n_groups = 1 THEN '1_group' WHEN n_groups = 2 THEN '2_groups'
         WHEN n_groups = 3 THEN '3_groups' WHEN n_groups <= 5 THEN '4-5_groups'
         ELSE '6+_groups' END AS bucket,
    CAST(NULL AS INT64) AS cap_c,
    COUNT(*) AS n_hh,
    ROUND(100*COUNT(*)/SUM(COUNT(*)) OVER (PARTITION BY purge), 3) AS hh_pct,
    SUM(total_imps) AS imps,
    ROUND(100*SUM(total_imps)/SUM(SUM(total_imps)) OVER (PARTITION BY purge), 3) AS imp_pct,
    ROUND(SUM(total_spend), 2) AS spend,
    ROUND(100*SUM(total_spend)/SUM(SUM(total_spend)) OVER (PARTITION BY purge), 3) AS spend_pct,
    ROUND(AVG(total_imps), 3) AS avg_imps,
    CAST(NULL AS FLOAT64) AS x1, CAST(NULL AS FLOAT64) AS x2
  FROM hhp GROUP BY section, purge, bucket
),
sec_stage AS (
  SELECT 'stage' AS section, purge,
    CASE WHEN n_stages = 0 THEN '0_unknown' WHEN n_stages = 1 THEN '1_stage'
         WHEN n_stages = 2 THEN '2_stages' WHEN n_stages = 3 THEN '3_stages'
         ELSE '4_stages' END AS bucket,
    CAST(NULL AS INT64),
    COUNT(*),
    ROUND(100*COUNT(*)/SUM(COUNT(*)) OVER (PARTITION BY purge), 3),
    SUM(total_imps),
    ROUND(100*SUM(total_imps)/SUM(SUM(total_imps)) OVER (PARTITION BY purge), 3),
    ROUND(SUM(total_spend), 2),
    ROUND(100*SUM(total_spend)/SUM(SUM(total_spend)) OVER (PARTITION BY purge), 3),
    ROUND(AVG(total_imps), 3),
    CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64)
  FROM hhp GROUP BY section, purge, bucket
),
sec_pr AS (
  SELECT 'prosp_x_retgt' AS section, purge,
    CASE WHEN has_prosp = 1 AND has_retgt = 1 THEN 'both_S1_and_S2plus'
         WHEN has_prosp = 1 THEN 'prospecting_only'
         WHEN has_retgt = 1 THEN 'retargeting_only'
         ELSE 'unknown_stage_only' END AS bucket,
    CAST(NULL AS INT64),
    COUNT(*),
    ROUND(100*COUNT(*)/SUM(COUNT(*)) OVER (PARTITION BY purge), 3),
    SUM(total_imps),
    ROUND(100*SUM(total_imps)/SUM(SUM(total_imps)) OVER (PARTITION BY purge), 3),
    ROUND(SUM(total_spend), 2),
    ROUND(100*SUM(total_spend)/SUM(SUM(total_spend)) OVER (PARTITION BY purge), 3),
    ROUND(AVG(total_imps), 3),
    CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64)
  FROM hhp GROUP BY section, purge, bucket
),
-- ===== contamination diagnostic (RAW): leaked% rises with distinct-advertiser count per IP =====
sec_diag AS (
  SELECT 'ip_nadv_diag' AS section, '1_raw' AS purge,
    CASE WHEN ip_nadv = 1 THEN 'nadv_1' WHEN ip_nadv <= 4 THEN 'nadv_2-4'
         WHEN ip_nadv <= 9 THEN 'nadv_5-9' WHEN ip_nadv <= 49 THEN 'nadv_10-49'
         WHEN ip_nadv <= 120 THEN 'nadv_50-120' ELSE 'nadv_121+' END AS bucket,
    CAST(NULL AS INT64),
    COUNT(*),
    ROUND(100*COUNT(*)/SUM(COUNT(*)) OVER (), 3),
    SUM(total_imps),
    ROUND(100*SUM(total_imps)/SUM(SUM(total_imps)) OVER (), 3),
    ROUND(SUM(total_spend), 2),
    ROUND(100*SUM(total_spend)/SUM(SUM(total_spend)) OVER (), 3),
    ROUND(AVG(total_imps), 3),
    CAST(SUM(IF(n_groups >= 2 OR n_stages >= 2, 1, 0)) AS FLOAT64) AS x1,          -- leaked hh
    ROUND(100*SUM(IF(n_groups >= 2 OR n_stages >= 2, 1, 0))/COUNT(*), 3) AS x2     -- leaked pct in bucket
  FROM hh2 GROUP BY section, purge, bucket
),
-- ===== cap-type split (RAW + PURGED): default_only is the fix-addressable population =====
sec_captype AS (
  SELECT 'captype' AS section, purge, cap_class AS bucket,
    CAST(NULL AS INT64),
    COUNT(*),
    ROUND(100*COUNT(*)/SUM(COUNT(*)) OVER (PARTITION BY purge), 3),
    SUM(total_imps),
    ROUND(100*SUM(total_imps)/SUM(SUM(total_imps)) OVER (PARTITION BY purge), 3),
    ROUND(SUM(total_spend), 2),
    ROUND(100*SUM(total_spend)/SUM(SUM(total_spend)) OVER (PARTITION BY purge), 3),
    ROUND(AVG(total_imps), 3),
    CAST(SUM(IF(n_groups >= 2 OR n_stages >= 2, 1, 0)) AS FLOAT64) AS x1,          -- leaked hh
    ROUND(100*SUM(IF(n_groups >= 2 OR n_stages >= 2, 1, 0))/COUNT(*), 3) AS x2     -- leaked pct in class
  FROM hhp GROUP BY section, purge, bucket
),
-- ===== over-delivery: ONLY what an advertiser total-frequency cap at C would suppress =====
-- excess_imps = SUM_hh max(0, base_imps - C); excess_spend = per-hh spend on those over-cap impressions.
-- populations: total_all_hh | leaked_only (2+ group OR 2+ stage) | nonleaked_only | default_cap_imps (fix-addressable).
sec_over AS (
  SELECT 'overdelivery' AS section, hhp.purge, pop.name AS bucket,
    capv AS cap_c,
    COUNTIF(pop.include AND pop.base_imps > capv) AS n_hh,
    CAST(NULL AS FLOAT64) AS hh_pct,
    CAST(NULL AS INT64) AS imps, CAST(NULL AS FLOAT64) AS imp_pct,
    CAST(NULL AS FLOAT64) AS spend, CAST(NULL AS FLOAT64) AS spend_pct,
    CAST(NULL AS FLOAT64) AS avg_imps,
    CAST(SUM(IF(pop.include, GREATEST(pop.base_imps - capv, 0), 0)) AS FLOAT64) AS x1,
    ROUND(SUM(IF(pop.include,
                 pop.base_spend * GREATEST(pop.base_imps - capv, 0) / NULLIF(pop.base_imps, 0),
                 0)), 2) AS x2
  FROM hhp,
    UNNEST([3, 8, 12]) AS capv,
    UNNEST([
      STRUCT('total_all_hh'     AS name, TRUE AS include,                                   hhp.total_imps AS base_imps, hhp.total_spend AS base_spend),
      STRUCT('leaked_only'      AS name, (hhp.n_groups >= 2 OR hhp.n_stages >= 2) AS include, hhp.total_imps AS base_imps, hhp.total_spend AS base_spend),
      STRUCT('nonleaked_only'   AS name, (hhp.n_groups < 2 AND hhp.n_stages < 2)  AS include, hhp.total_imps AS base_imps, hhp.total_spend AS base_spend),
      STRUCT('default_cap_imps' AS name, TRUE AS include,                                   hhp.default_imps AS base_imps, hhp.default_spend AS base_spend)
    ]) AS pop
  GROUP BY section, purge, bucket, cap_c
),
sec_total AS (
  SELECT 'total' AS section, purge, 'all_households' AS bucket,
    CAST(NULL AS INT64),
    COUNT(*), CAST(100.0 AS FLOAT64),
    SUM(total_imps), CAST(100.0 AS FLOAT64),
    ROUND(SUM(total_spend), 2), CAST(100.0 AS FLOAT64),
    ROUND(AVG(total_imps), 3),
    CAST(SUM(IF(shared_ip, 1, 0)) AS FLOAT64) AS x1,     -- shared-IP households (RAW only meaningful)
    ROUND(100*SUM(IF(shared_ip, 1, 0))/COUNT(*), 3) AS x2
  FROM hhp GROUP BY section, purge, bucket
)
SELECT * FROM sec_group
UNION ALL SELECT * FROM sec_stage
UNION ALL SELECT * FROM sec_pr
UNION ALL SELECT * FROM sec_diag
UNION ALL SELECT * FROM sec_captype
UNION ALL SELECT * FROM sec_over
UNION ALL SELECT * FROM sec_total
ORDER BY section, purge, bucket, cap_c
