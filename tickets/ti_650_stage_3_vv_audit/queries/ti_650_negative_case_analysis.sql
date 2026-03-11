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
