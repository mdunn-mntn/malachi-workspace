-- TI-650: Negative Case Analysis — Iterative S1 resolution gap investigation
-- Zach directive: "work with the negative case — find the ones where you can't go the full length"
-- Method: Pick ONE unresolved VV at a time, trace the failure, classify, fix or document.
--
-- Advertiser 37775, 7-day trace 2026-02-04 to 2026-02-10
-- 90-day lookback from 2025-11-06

--------------------------------------------------------------------------------
-- PHASE 1: Pick one unresolved S2 VV and trace the failure
-- Uses Q3 TEMP TABLEs (Steps 1-2f) then runs targeted diagnostics
--------------------------------------------------------------------------------

-- NOTE: Run Q3 Steps 1-2f first (impression_pool through s1_vv_guid)
-- Then run the queries below in the same session.

-- P1-Q1: Find one unresolved S2 VV with all available keys
-- (Run after Q3 TEMP TABLEs are created)
WITH campaigns_stage AS (
    SELECT campaign_id, funnel_level AS stage
    FROM `dw-main-bronze.integrationprod.campaigns`
    WHERE deleted = FALSE
),
cp_dedup AS (
    SELECT cp.ad_served_id, cp.advertiser_id, cp.campaign_id,
        cp.ip AS redirect_ip, cp.is_new, cp.is_cross_device,
        cp.first_touch_ad_served_id, cp.guid, cp.original_guid,
        cp.attribution_model_id, cp.time, c.stage AS vv_stage
    FROM `dw-main-silver.logdata.clickpass_log` cp
    LEFT JOIN campaigns_stage c ON c.campaign_id = cp.campaign_id
    WHERE cp.time >= TIMESTAMP('2026-02-04') AND cp.time < TIMESTAMP('2026-02-11')
      AND cp.advertiser_id = 37775
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),
v_dedup AS (
    SELECT CAST(ad_served_id AS STRING) AS ad_served_id, ip AS visit_ip, is_new AS visit_is_new, impression_ip
    FROM `dw-main-silver.summarydata.ui_visits`
    WHERE from_verified_impression = TRUE
      AND time >= TIMESTAMP('2026-01-28') AND time < TIMESTAMP('2026-02-18')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(ad_served_id AS STRING) ORDER BY time DESC) = 1
),
-- Get the S2 VV with its impression
s2_vvs AS (
    SELECT
        cp.ad_served_id,
        cp.campaign_id,
        cp.time AS vv_time,
        cp.redirect_ip,
        cp.guid AS vv_guid,
        cp.first_touch_ad_served_id,
        cp.is_cross_device,
        cp.attribution_model_id,
        lt.bid_ip AS this_bid_ip,
        lt.vast_start_ip AS this_vast_start_ip,
        lt.vast_impression_ip AS this_vast_impression_ip,
        lt.guid AS imp_guid,
        lt.time AS imp_time,
        v.visit_ip,
        v.impression_ip,
        -- Check each tier
        CASE WHEN lt.ad_served_id IS NULL THEN 'NO_IMPRESSION' ELSE 'has_imp' END AS imp_status,
        -- Tier 2: vv_chain_direct (S1 VV via vast match)
        pv_vast.prior_vv_ad_served_id AS pv_vast_asid,
        pv_vast.pv_stage AS pv_vast_stage,
        pv_vast.match_ip AS pv_vast_match_ip,
        -- Tier 2b: vv_chain_direct (S1 VV via redirect)
        pv_redir.prior_vv_ad_served_id AS pv_redir_asid,
        pv_redir.pv_stage AS pv_redir_stage,
        -- Tier 5: imp_direct (S1 impression at bid_ip)
        s1_imp_direct.ad_served_id AS s1_imp_direct_asid,
        s1_imp_direct.bid_ip AS s1_imp_direct_ip,
        -- guid_vv (tier 8)
        s1_guid_vv.s1_vv_ad_served_id AS guid_vv_asid,
        -- guid_imp (tier 9)
        s1_guid_imp.ad_served_id AS guid_imp_asid,
        -- s1_imp_redirect (tier 10)
        s1_imp_redir.ad_served_id AS s1_imp_redir_asid,
        -- ft_lt (tier 7)
        ft_lt.ad_served_id AS ft_lt_asid
    FROM cp_dedup cp
    LEFT JOIN impression_pool lt ON lt.ad_served_id = cp.ad_served_id AND lt.rn = 1
    LEFT JOIN v_dedup v ON v.ad_served_id = cp.ad_served_id

    -- Tier 2: prior VV via vast match
    LEFT JOIN pv_pool_vast pv_vast
        ON pv_vast.advertiser_id = cp.advertiser_id
        AND pv_vast.match_ip = lt.bid_ip
        AND pv_vast.prior_vv_time < cp.time
        AND pv_vast.prior_vv_ad_served_id != cp.ad_served_id
        AND pv_vast.pv_stage < cp.vv_stage

    -- Tier 2b: prior VV via redirect
    LEFT JOIN pv_pool_redir pv_redir
        ON pv_redir.advertiser_id = cp.advertiser_id
        AND pv_redir.pv_redirect_ip = cp.redirect_ip
        AND pv_redir.prior_vv_time < cp.time
        AND pv_redir.prior_vv_ad_served_id != cp.ad_served_id
        AND pv_redir.pv_stage < cp.vv_stage

    -- Tier 5: S1 impression at current bid_ip
    LEFT JOIN s1_imp_pool s1_imp_direct
        ON s1_imp_direct.bid_ip = lt.bid_ip
        AND s1_imp_direct.time < cp.time

    -- Tier 7: first_touch fallback
    LEFT JOIN impression_pool ft_lt
        ON ft_lt.ad_served_id = cp.first_touch_ad_served_id
        AND ft_lt.rn = 1

    -- Tier 8: guid_vv_match
    LEFT JOIN s1_vv_guid s1_guid_vv
        ON s1_guid_vv.guid = cp.guid
        AND s1_guid_vv.s1_vv_ad_served_id != cp.ad_served_id
        AND s1_guid_vv.s1_vv_time < cp.time

    -- Tier 9: guid_imp_match
    LEFT JOIN s1_imp_guid s1_guid_imp
        ON s1_guid_imp.guid = cp.guid
        AND s1_guid_imp.ad_served_id != cp.ad_served_id
        AND s1_guid_imp.time < cp.time

    -- Tier 10: s1_imp_redirect
    LEFT JOIN s1_imp_pool s1_imp_redir
        ON s1_imp_redir.bid_ip = cp.redirect_ip
        AND cp.redirect_ip != lt.bid_ip
        AND s1_imp_redir.time < cp.time

    WHERE cp.vv_stage = 2
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
)
SELECT *
FROM s2_vvs
WHERE pv_vast_asid IS NULL          -- no prior VV via vast
  AND pv_redir_asid IS NULL         -- no prior VV via redirect
  AND s1_imp_direct_asid IS NULL    -- no S1 impression at bid_ip
  AND ft_lt_asid IS NULL            -- no first_touch impression
  AND guid_vv_asid IS NULL          -- no S1 VV via guid
  AND guid_imp_asid IS NULL         -- no S1 impression via guid
  AND s1_imp_redir_asid IS NULL     -- no S1 impression at redirect_ip
ORDER BY vv_time
LIMIT 5;


--------------------------------------------------------------------------------
-- PHASE 2: Deep-dive queries for individual unresolved VVs
-- Replace IP and guid values for each VV being investigated
--------------------------------------------------------------------------------

-- P2-Q1: All impressions at target IP for adv 37775 (any stage, event_log)
-- Replace '208.97.32.204' with the VV's bid_ip
-- SELECT c.funnel_level AS stage, c.advertiser_id, el.ad_served_id, el.campaign_id,
--        el.bid_ip, el.ip AS vast_ip, el.time, el.guid
-- FROM `dw-main-silver.logdata.event_log` el
-- JOIN `dw-main-bronze.integrationprod.campaigns` c ON c.campaign_id = el.campaign_id AND c.deleted = FALSE
-- WHERE el.bid_ip = '208.97.32.204'
--   AND el.event_type_raw IN ('vast_start', 'vast_impression')
--   AND el.time >= TIMESTAMP('2025-11-06') AND el.time < TIMESTAMP('2026-02-11')
-- ORDER BY el.time DESC LIMIT 20;

-- P2-Q2: TMUL segment entry (tpa_membership_update_log)
-- Partition filter: dt (STRING 'YYYY-MM-DD'). Column = id (not ip).
-- Narrow date range to reduce scan cost.
-- SELECT td.time, td.id AS ip, td.data_source_id, isl.segment_id
-- FROM `dw-main-bronze.raw.tpa_membership_update_log` td,
--      UNNEST(td.in_segments.segments) AS isl
-- WHERE td.id = '208.97.32.204'
--   AND td.dt >= '2026-02-01' AND td.dt < '2026-02-11'
-- ORDER BY td.time DESC LIMIT 10;

-- P2-Q3: IPDSC identity resolution (CRM HEM→IP)
-- Partition filter: dt (STRING 'YYYY-MM-DD'). Column = ip.
-- SELECT dt, ip, data_source_id
-- FROM `dw-main-bronze.external.ipdsc__v1`
-- WHERE ip = '208.97.32.204'
--   AND dt >= '2026-02-01' AND dt < '2026-02-11'
-- ORDER BY dt DESC LIMIT 10;


--------------------------------------------------------------------------------
-- PHASE 3: Batch classification of ALL unresolved S2 VVs
-- Result: 18,047 of 37,090 S2 VVs (without S1 VV at IP) have zero S1 footprint
-- at any key (bid_ip, guid, redirect_ip). Structurally unresolvable.
--------------------------------------------------------------------------------
-- [See batch query in session — creates impression_pool, s1_imp_ips, s1_imp_guids,
--  s1_vv_guids, prior_vv_vast_ips TEMP TABLEs then classifies failure patterns]


--------------------------------------------------------------------------------
-- PHASE 4: Identity graph trace — find the ACTUAL S1 impression via LiveRamp
-- Proven for VV #1 (208.97.32.204): S1 impression exists at linked IP 35.145.60.7
-- Method: TMUL shared-segment-same-timestamp → candidate IPs → event_log S1 check
--------------------------------------------------------------------------------

-- P4-Q1: Get all DS3 segments for unresolved VV's IP
-- (Use TMUL results to establish segment fingerprint)
-- SELECT td.id AS ip, td.data_source_id, td.time, isl.segment_id
-- FROM `dw-main-bronze.raw.tpa_membership_update_log` td,
--      UNNEST(td.in_segments.segments) AS isl
-- WHERE td.id = '208.97.32.204'
--   AND td.dt >= '2026-02-09' AND td.dt < '2026-02-11'
--   AND td.data_source_id = 3
-- ORDER BY td.time DESC LIMIT 50;

-- P4-Q2: Find identity-linked IPs (same DS3 segments, same timestamp ±1 min)
-- Replace segment_ids with actual values from P4-Q1
-- Replace timestamp window to match the VV IP's entry time
-- SELECT td.id AS linked_ip, td.data_source_id, td.time,
--        COUNT(DISTINCT isl.segment_id) AS shared_segments
-- FROM `dw-main-bronze.raw.tpa_membership_update_log` td,
--      UNNEST(td.in_segments.segments) AS isl
-- WHERE td.dt >= '2026-02-09' AND td.dt < '2026-02-11'
--   AND td.data_source_id = 3
--   AND isl.segment_id IN (338198,528861,493738,516900,385587)  -- sample 5 segments from P4-Q1
--   AND td.id != '208.97.32.204'
--   AND td.time BETWEEN TIMESTAMP('2026-02-10 08:03:00') AND TIMESTAMP('2026-02-10 08:04:00')
-- GROUP BY td.id, td.data_source_id, td.time
-- HAVING shared_segments >= 3
-- ORDER BY shared_segments DESC LIMIT 20;

-- P4-Q3: Check linked IPs for S1 impressions (adv 37775)
-- Replace IP list with candidates from P4-Q2
-- SELECT c.funnel_level AS stage, el.bid_ip, el.campaign_id, el.ad_served_id,
--        el.time, el.event_type_raw
-- FROM `dw-main-silver.logdata.event_log` el
-- JOIN `dw-main-bronze.integrationprod.campaigns` c
--   ON c.campaign_id = el.campaign_id AND c.deleted = FALSE
-- WHERE el.bid_ip IN ('35.145.60.7','35.151.179.5','73.124.39.182')  -- from P4-Q2
--   AND c.advertiser_id = 37775
--   AND c.funnel_level = 1
--   AND el.event_type_raw IN ('vast_start','vast_impression')
--   AND el.time >= TIMESTAMP('2025-11-06') AND el.time < TIMESTAMP('2026-02-11')
-- ORDER BY el.time DESC LIMIT 20;

-- P4-Q4: Quantify segment overlap to validate identity linkage strength
-- (VV #1 result: 96/140 segments shared = 68.6% overlap with 35.145.60.7)
-- WITH ip1_segs AS (
--   SELECT DISTINCT isl.segment_id
--   FROM `dw-main-bronze.raw.tpa_membership_update_log` td,
--        UNNEST(td.in_segments.segments) AS isl
--   WHERE td.id = '208.97.32.204' AND td.dt >= '2026-02-09' AND td.dt < '2026-02-11' AND td.data_source_id = 3
-- ),
-- ip2_segs AS (
--   SELECT DISTINCT isl.segment_id
--   FROM `dw-main-bronze.raw.tpa_membership_update_log` td,
--        UNNEST(td.in_segments.segments) AS isl
--   WHERE td.id = '35.145.60.7' AND td.dt >= '2026-02-09' AND td.dt < '2026-02-11' AND td.data_source_id = 3
-- )
-- SELECT
--   (SELECT COUNT(*) FROM ip1_segs) AS ip1_total,
--   (SELECT COUNT(*) FROM ip2_segs) AS ip2_total,
--   (SELECT COUNT(*) FROM ip1_segs a JOIN ip2_segs b ON a.segment_id = b.segment_id) AS shared,
--   ROUND((SELECT COUNT(*) FROM ip1_segs a JOIN ip2_segs b ON a.segment_id = b.segment_id) * 100.0 /
--         GREATEST((SELECT COUNT(*) FROM ip1_segs), 1), 1) AS pct_overlap;

-- P4 RESULTS (VV #1 — RETARGETING, excluded from analysis):
-- VV #1 campaign 443862 = "TV Retargeting - Television - 5+ PV" (objective_id=4)
-- Zach confirmed retargeting is NOT relevant to this audit. This VV correctly has no S1.
-- S1 impressions confirmed at 3 linked IPs (identity graph trace still valid as methodology):
--   35.145.60.7    — 4 S1 imps (Feb 2-9), campaign 311974
--   35.151.179.5   — 2 S1 imps (Feb 4, 9), campaign 311974
--   73.124.39.182  — 4 S1 imps (Feb 2-9), campaigns 311968/450323
-- Segment overlap: 96/140 (68.6%) with 35.145.60.7

--------------------------------------------------------------------------------
-- PHASE 5: Prospecting-only CTV S2 resolution (2026-03-10)
-- Zach: retargeting not relevant. Filter to prospecting (objective_id NOT IN (4,7))
-- CTV only (SET_TOP_BOX + CONNECTED_TV) per user directive
--------------------------------------------------------------------------------

-- P5-Q1: Full CTV prospecting S2 resolution cascade
-- Results: 16,112 total → 15,880 resolved (98.56%), 232 truly unresolved (1.44%)
-- Primary VV unresolved: 54 (0.34%)
WITH s1_campaigns AS (
  SELECT campaign_id FROM `dw-main-bronze.integrationprod.campaigns`
  WHERE advertiser_id = 37775 AND deleted = FALSE AND is_test = FALSE
    AND funnel_level = 1 AND objective_id NOT IN (4, 7)
),
s2_campaigns AS (
  SELECT campaign_id FROM `dw-main-bronze.integrationprod.campaigns`
  WHERE advertiser_id = 37775 AND deleted = FALSE AND is_test = FALSE
    AND funnel_level = 2 AND objective_id NOT IN (4, 7)
),
s2_vvs AS (
  SELECT cp.ad_served_id, cp.ip AS redirect_ip, cp.guid,
    cp.attribution_model_id,
    cil.ip AS bid_ip, cil.device_type
  FROM `dw-main-silver.logdata.clickpass_log` cp
  JOIN s2_campaigns s2c ON cp.campaign_id = s2c.campaign_id
  LEFT JOIN `dw-main-silver.logdata.cost_impression_log` cil
    ON cp.ad_served_id = cil.ad_served_id AND cil.advertiser_id = 37775
    AND DATE(cil.time) BETWEEN '2025-11-06' AND '2026-02-11'
  WHERE DATE(cp.time) BETWEEN '2026-02-04' AND '2026-02-11'
    AND cp.advertiser_id = 37775
    AND cil.device_type IN ('SET_TOP_BOX', 'CONNECTED_TV')
),
s1_ips AS (
  SELECT DISTINCT ip FROM `dw-main-silver.logdata.cost_impression_log` s1
  JOIN s1_campaigns s1c ON s1.campaign_id = s1c.campaign_id
  WHERE s1.advertiser_id = 37775 AND DATE(s1.time) BETWEEN '2025-11-06' AND '2026-02-11'
),
s1_vv_guids AS (
  SELECT DISTINCT guid FROM `dw-main-silver.logdata.clickpass_log` cp
  JOIN s1_campaigns s1c ON cp.campaign_id = s1c.campaign_id
  WHERE cp.advertiser_id = 37775 AND DATE(cp.time) BETWEEN '2025-11-06' AND '2026-02-11'
),
s1_imp_guids AS (
  SELECT DISTINCT guid FROM `dw-main-silver.logdata.cost_impression_log` cil
  JOIN s1_campaigns s1c ON cil.campaign_id = s1c.campaign_id
  WHERE cil.advertiser_id = 37775 AND DATE(cil.time) BETWEEN '2025-11-06' AND '2026-02-11'
),
hh_s1_ips AS (
  SELECT DISTINCT g1.ip AS bid_ip
  FROM s2_vvs u
  JOIN `dw-main-bronze.tpa.graph_ips_aa_100pct_ip` g1 ON u.bid_ip = g1.ip
  JOIN `dw-main-bronze.tpa.graph_ips_aa_100pct_ip` g2
    ON g1.householdid = g2.householdid AND g2.ip != g1.ip
  JOIN s1_ips s1 ON g2.ip = s1.ip
)
SELECT
  CASE
    WHEN ip1.ip IS NOT NULL THEN 's1_at_bid_ip'
    WHEN g1.guid IS NOT NULL THEN 'guid_vv_match'
    WHEN g2.guid IS NOT NULL THEN 'guid_imp_match'
    WHEN r.ip IS NOT NULL THEN 's1_at_redirect_ip'
    WHEN hh.bid_ip IS NOT NULL THEN 'household_graph'
    ELSE 'truly_unresolved'
  END AS resolution_tier,
  COUNT(*) AS cnt
FROM s2_vvs s2
LEFT JOIN s1_ips ip1 ON s2.bid_ip = ip1.ip
LEFT JOIN s1_vv_guids g1 ON s2.guid = g1.guid AND ip1.ip IS NULL
LEFT JOIN s1_imp_guids g2 ON s2.guid = g2.guid AND ip1.ip IS NULL AND g1.guid IS NULL
LEFT JOIN s1_ips r ON s2.redirect_ip = r.ip AND ip1.ip IS NULL AND g1.guid IS NULL AND g2.guid IS NULL
LEFT JOIN hh_s1_ips hh ON s2.bid_ip = hh.bid_ip AND ip1.ip IS NULL AND g1.guid IS NULL AND g2.guid IS NULL AND r.ip IS NULL
GROUP BY 1
ORDER BY cnt DESC;

-- P5 RESULTS:
-- s1_at_bid_ip:      15,465 (95.98%)
-- guid_vv_match:        353 (2.19%)
-- guid_imp_match:          5 (0.03%)
-- s1_at_redirect_ip:      11 (0.07%)
-- household_graph:        46 (0.29%)
-- truly_unresolved:      232 (1.44%)
-- Total:              16,112
--
-- Attribution model breakdown of 232 truly unresolved:
--   Model 10 (Competing-ip):          106
--   Model 9  (Competing-guid):         72
--   Model 2  (Last Touch-ip):          26
--   Model 1  (Last Touch-guid):        11
--   Model 11 (Competing-ga_client_id): 10
--   Model 3  (Last Touch-ga_client_id): 7
--
-- 178/232 (76.7%) are competing VVs (secondary attribution)
-- Primary VV unresolved: 54/16,112 = 0.34%
