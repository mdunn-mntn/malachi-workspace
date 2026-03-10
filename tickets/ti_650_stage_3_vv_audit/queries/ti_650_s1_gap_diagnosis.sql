-- TI-650: S1 Resolution Gap Diagnosis
-- Zach meeting 4: "work with the negative case"
-- Advertiser 37775, 7-day trace 2026-02-04 to 2026-02-10
-- 90-day lookback from 2025-11-06

--------------------------------------------------------------------------------
-- D1: Profile unresolved VVs
-- Uses Q3 TEMP TABLEs then profiles the unresolved subset
--------------------------------------------------------------------------------

-- Step 1: Reuse Q3 TEMP TABLEs (impression_pool, prior_vv_raw, pools)
-- [impression_pool, prior_vv_raw, pv_pool_vast, pv_pool_redir, s1_imp_pool
--  are created by running Q3 Steps 1-2d first]

-- Step 2: Run the main query into a TEMP TABLE for profiling
CREATE TEMP TABLE vv_result AS
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
with_all_joins AS (
    SELECT
        cp.ad_served_id, cp.advertiser_id, cp.campaign_id,
        cp.vv_stage, cp.time AS vv_time,
        cp.guid AS vv_guid, cp.original_guid AS vv_original_guid,
        cp.attribution_model_id AS vv_attribution_model_id,
        v.visit_ip, v.impression_ip, cp.redirect_ip,
        lt.bid_ip AS this_bid_ip,
        lt.vast_start_ip AS this_vast_start_ip,
        cp.first_touch_ad_served_id AS cp_ft_ad_served_id,
        cp.is_new AS clickpass_is_new, v.visit_is_new, cp.is_cross_device,

        -- S1 resolution method (same logic as Q3)
        CASE
            WHEN cp.vv_stage = 1 THEN 'current_is_s1'
            WHEN COALESCE(pv_vast.pv_stage, pv_redir.pv_stage) = 1 THEN 'vv_chain_direct'
            WHEN COALESCE(s1_vast.prior_vv_ad_served_id, s1_redir.prior_vv_ad_served_id) IS NOT NULL
              THEN 'vv_chain_s2_s1'
            WHEN s1_imp_chain.bid_ip IS NOT NULL THEN 'imp_chain'
            WHEN s1_imp_direct.bid_ip IS NOT NULL THEN 'imp_direct'
            WHEN s1_imp_visit_ip.bid_ip IS NOT NULL THEN 'imp_visit_ip'
            WHEN ft_lt.bid_ip IS NOT NULL THEN 'cp_ft_fallback'
            ELSE NULL
        END AS s1_resolution_method,

        -- Diagnostic: what do we have?
        CASE WHEN lt.ad_served_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_impression,
        CASE WHEN pv_vast.prior_vv_ad_served_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_pv_vast,
        CASE WHEN pv_redir.prior_vv_ad_served_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_pv_redir,
        COALESCE(pv_vast.pv_stage, pv_redir.pv_stage) AS pv_stage_found,

        ROW_NUMBER() OVER (
            PARTITION BY cp.ad_served_id
            ORDER BY
                CASE WHEN pv_vast.prior_vv_ad_served_id IS NOT NULL THEN 0
                     WHEN pv_redir.prior_vv_ad_served_id IS NOT NULL THEN 1
                     ELSE 2 END,
                COALESCE(pv_vast.prior_vv_time, pv_redir.prior_vv_time) DESC NULLS LAST,
                CASE WHEN s1_vast.prior_vv_ad_served_id IS NOT NULL THEN 0
                     WHEN s1_redir.prior_vv_ad_served_id IS NOT NULL THEN 1
                     ELSE 2 END,
                COALESCE(s1_vast.prior_vv_time, s1_redir.prior_vv_time) DESC NULLS LAST
        ) AS _pv_rn

    FROM cp_dedup cp
    LEFT JOIN impression_pool lt ON lt.ad_served_id = cp.ad_served_id AND lt.rn = 1
    LEFT JOIN v_dedup v ON v.ad_served_id = cp.ad_served_id
    LEFT JOIN pv_pool_vast pv_vast
        ON pv_vast.advertiser_id = cp.advertiser_id
        AND pv_vast.match_ip = lt.bid_ip
        AND pv_vast.prior_vv_time < cp.time
        AND pv_vast.prior_vv_ad_served_id != cp.ad_served_id
        AND pv_vast.pv_stage < cp.vv_stage
    LEFT JOIN pv_pool_redir pv_redir
        ON pv_redir.advertiser_id = cp.advertiser_id
        AND pv_redir.pv_redirect_ip = cp.redirect_ip
        AND pv_redir.prior_vv_time < cp.time
        AND pv_redir.prior_vv_ad_served_id != cp.ad_served_id
        AND pv_redir.pv_stage < cp.vv_stage
    LEFT JOIN impression_pool pv_lt
        ON pv_lt.ad_served_id = COALESCE(pv_vast.prior_vv_ad_served_id, pv_redir.prior_vv_ad_served_id)
        AND pv_lt.rn = 1
    LEFT JOIN pv_pool_vast s1_vast
        ON s1_vast.advertiser_id = cp.advertiser_id
        AND s1_vast.match_ip = pv_lt.bid_ip
        AND s1_vast.pv_stage = 1
        AND s1_vast.pv_stage < COALESCE(pv_vast.pv_stage, pv_redir.pv_stage)
        AND s1_vast.prior_vv_time < COALESCE(pv_vast.prior_vv_time, pv_redir.prior_vv_time)
        AND s1_vast.prior_vv_ad_served_id != COALESCE(pv_vast.prior_vv_ad_served_id, pv_redir.prior_vv_ad_served_id)
    LEFT JOIN pv_pool_redir s1_redir
        ON s1_redir.advertiser_id = cp.advertiser_id
        AND s1_redir.pv_redirect_ip = COALESCE(pv_vast.pv_redirect_ip, pv_redir.pv_redirect_ip)
        AND s1_redir.pv_stage = 1
        AND s1_redir.pv_stage < COALESCE(pv_vast.pv_stage, pv_redir.pv_stage)
        AND s1_redir.prior_vv_time < COALESCE(pv_vast.prior_vv_time, pv_redir.prior_vv_time)
        AND s1_redir.prior_vv_ad_served_id != COALESCE(pv_vast.prior_vv_ad_served_id, pv_redir.prior_vv_ad_served_id)
    LEFT JOIN impression_pool s1_lt
        ON s1_lt.ad_served_id = COALESCE(s1_vast.prior_vv_ad_served_id, s1_redir.prior_vv_ad_served_id)
        AND s1_lt.rn = 1
    LEFT JOIN s1_imp_pool s1_imp_chain
        ON s1_imp_chain.bid_ip = pv_lt.bid_ip
        AND s1_imp_chain.time < COALESCE(pv_vast.prior_vv_time, pv_redir.prior_vv_time)
    LEFT JOIN s1_imp_pool s1_imp_direct
        ON s1_imp_direct.bid_ip = lt.bid_ip
        AND s1_imp_direct.time < cp.time
    LEFT JOIN s1_imp_pool s1_imp_visit_ip
        ON s1_imp_visit_ip.bid_ip = v.impression_ip
        AND v.impression_ip != lt.bid_ip
        AND s1_imp_visit_ip.time < cp.time
    LEFT JOIN impression_pool ft_lt
        ON ft_lt.ad_served_id = cp.first_touch_ad_served_id
        AND ft_lt.rn = 1
)
SELECT * FROM with_all_joins WHERE _pv_rn = 1;


-- D1a: Overall profile of unresolved VVs
SELECT
    vv_stage,
    COUNT(*) AS total,
    COUNTIF(s1_resolution_method IS NULL) AS unresolved,
    ROUND(100.0 * COUNTIF(s1_resolution_method IS NULL) / COUNT(*), 2) AS unresolved_pct,
    -- Of the unresolved:
    COUNTIF(s1_resolution_method IS NULL AND cp_ft_ad_served_id IS NOT NULL) AS unres_has_ft,
    COUNTIF(s1_resolution_method IS NULL AND NOT has_impression) AS unres_no_impression,
    COUNTIF(s1_resolution_method IS NULL AND is_cross_device) AS unres_cross_device,
    COUNTIF(s1_resolution_method IS NULL AND NOT has_pv_vast AND NOT has_pv_redir) AS unres_no_prior_vv,
    COUNTIF(s1_resolution_method IS NULL AND has_pv_vast) AS unres_has_pv_vast,
    COUNTIF(s1_resolution_method IS NULL AND has_pv_redir AND NOT has_pv_vast) AS unres_has_pv_redir_only
FROM vv_result
WHERE vv_stage > 1
GROUP BY vv_stage
ORDER BY vv_stage;


-- D1b: Attribution model distribution for unresolved
SELECT
    vv_stage,
    vv_attribution_model_id,
    COUNT(*) AS cnt
FROM vv_result
WHERE vv_stage > 1 AND s1_resolution_method IS NULL
GROUP BY vv_stage, vv_attribution_model_id
ORDER BY vv_stage, cnt DESC;


-- D1c: Prior VV stage distribution for unresolved
SELECT
    vv_stage,
    pv_stage_found,
    COUNT(*) AS cnt
FROM vv_result
WHERE vv_stage > 1 AND s1_resolution_method IS NULL
GROUP BY vv_stage, pv_stage_found
ORDER BY vv_stage, pv_stage_found;


--------------------------------------------------------------------------------
-- D2: Check viewability_log for unresolved VVs' S1 impressions
--------------------------------------------------------------------------------

-- D2a: Do unresolved VVs' cp_ft_ad_served_ids exist in viewability_log?
SELECT
    'viewability_log' AS source,
    COUNT(DISTINCT u.ad_served_id) AS unresolved_with_ft,
    COUNT(DISTINCT CASE WHEN vl.ad_served_id IS NOT NULL THEN u.ad_served_id END) AS found_in_vl,
    COUNT(DISTINCT CASE WHEN ip_el.ad_served_id IS NOT NULL THEN u.ad_served_id END) AS found_in_el,
    COUNT(DISTINCT CASE WHEN ip_cil.ad_served_id IS NOT NULL THEN u.ad_served_id END) AS found_in_cil,
    COUNT(DISTINCT CASE WHEN vl.ad_served_id IS NOT NULL
                        AND ip_el.ad_served_id IS NULL
                        AND ip_cil.ad_served_id IS NULL THEN u.ad_served_id END) AS vl_only
FROM vv_result u
LEFT JOIN `dw-main-silver.logdata.viewability_log` vl
    ON vl.ad_served_id = u.cp_ft_ad_served_id
    AND vl.time >= TIMESTAMP('2025-11-06') AND vl.time < TIMESTAMP('2026-02-11')
LEFT JOIN (
    SELECT DISTINCT ad_served_id
    FROM `dw-main-silver.logdata.event_log`
    WHERE event_type_raw IN ('vast_start', 'vast_impression')
      AND time >= TIMESTAMP('2025-11-06') AND time < TIMESTAMP('2026-02-11')
      AND campaign_id IN (
          SELECT campaign_id FROM `dw-main-bronze.integrationprod.campaigns`
          WHERE advertiser_id = 37775 AND deleted = FALSE
      )
) ip_el ON ip_el.ad_served_id = u.cp_ft_ad_served_id
LEFT JOIN (
    SELECT DISTINCT ad_served_id
    FROM `dw-main-silver.logdata.cost_impression_log`
    WHERE time >= TIMESTAMP('2025-11-06') AND time < TIMESTAMP('2026-02-11')
      AND advertiser_id = 37775
) ip_cil ON ip_cil.ad_served_id = u.cp_ft_ad_served_id
WHERE u.vv_stage > 1 AND u.s1_resolution_method IS NULL AND u.cp_ft_ad_served_id IS NOT NULL;


-- D2b: For unresolved VVs WITHOUT cp_ft, check if their own bid_ip appears in viewability_log
-- as a bid_ip on an S1 campaign impression
SELECT
    COUNT(DISTINCT u.ad_served_id) AS unresolved_no_ft,
    COUNT(DISTINCT CASE WHEN vl_s1.ad_served_id IS NOT NULL THEN u.ad_served_id END) AS found_s1_in_vl
FROM vv_result u
LEFT JOIN (
    SELECT DISTINCT vl.bid_ip, vl.ad_served_id
    FROM `dw-main-silver.logdata.viewability_log` vl
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = vl.campaign_id AND c.deleted = FALSE AND c.funnel_level = 1
    WHERE vl.time >= TIMESTAMP('2025-11-06') AND vl.time < TIMESTAMP('2026-02-11')
      AND vl.advertiser_id = 37775
) vl_s1 ON vl_s1.bid_ip = u.this_bid_ip
WHERE u.vv_stage > 1 AND u.s1_resolution_method IS NULL AND u.cp_ft_ad_served_id IS NULL;


--------------------------------------------------------------------------------
-- D4: guid as S1 linking key
--------------------------------------------------------------------------------

-- Does the unresolved VV's guid match an S1 impression's guid?
SELECT
    COUNT(DISTINCT u.ad_served_id) AS total_unresolved,
    -- Check impression_pool (already has event_log + CIL)
    COUNT(DISTINCT CASE WHEN ip_guid.ad_served_id IS NOT NULL THEN u.ad_served_id END) AS guid_match_imp_pool,
    -- Check viewability_log
    COUNT(DISTINCT CASE WHEN vl_guid.ad_served_id IS NOT NULL THEN u.ad_served_id END) AS guid_match_vl
FROM vv_result u
LEFT JOIN (
    SELECT DISTINCT ip.guid, ip.ad_served_id
    FROM impression_pool ip
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = ip.campaign_id AND c.deleted = FALSE AND c.funnel_level = 1
    WHERE ip.rn = 1 AND ip.guid IS NOT NULL
) ip_guid ON ip_guid.guid = u.vv_guid AND ip_guid.ad_served_id != u.ad_served_id
LEFT JOIN (
    SELECT DISTINCT vl.guid, vl.ad_served_id
    FROM `dw-main-silver.logdata.viewability_log` vl
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = vl.campaign_id AND c.deleted = FALSE AND c.funnel_level = 1
    WHERE vl.time >= TIMESTAMP('2025-11-06') AND vl.time < TIMESTAMP('2026-02-11')
      AND vl.advertiser_id = 37775 AND vl.guid IS NOT NULL
) vl_guid ON vl_guid.guid = u.vv_guid AND vl_guid.ad_served_id != u.ad_served_id
WHERE u.vv_stage > 1 AND u.s1_resolution_method IS NULL;


--------------------------------------------------------------------------------
-- D5: redirect_ip and vast_ip as alternative S1 match keys
--------------------------------------------------------------------------------

SELECT
    COUNT(DISTINCT u.ad_served_id) AS total_unresolved,
    -- redirect_ip matches S1 impression bid_ip
    COUNT(DISTINCT CASE WHEN s1r.bid_ip IS NOT NULL THEN u.ad_served_id END) AS redirect_ip_matches_s1,
    -- bid_ip matches S1 impression vast_start_ip (reverse direction)
    COUNT(DISTINCT CASE WHEN s1v.vast_start_ip IS NOT NULL THEN u.ad_served_id END) AS bid_ip_matches_s1_vast,
    -- bid_ip matches S1 impression vast_impression_ip
    COUNT(DISTINCT CASE WHEN s1vi.vast_impression_ip IS NOT NULL THEN u.ad_served_id END) AS bid_ip_matches_s1_vast_imp
FROM vv_result u
LEFT JOIN s1_imp_pool s1r ON s1r.bid_ip = u.redirect_ip AND s1r.time < u.vv_time
LEFT JOIN (
    SELECT ip.vast_start_ip, ip.ad_served_id, ip.bid_ip, ip.time, ip.guid
    FROM impression_pool ip
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = ip.campaign_id AND c.deleted = FALSE AND c.funnel_level = 1
    WHERE ip.rn = 1
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ip.vast_start_ip ORDER BY ip.time) = 1
) s1v ON s1v.vast_start_ip = u.this_bid_ip AND s1v.time < u.vv_time
LEFT JOIN (
    SELECT ip.vast_impression_ip, ip.ad_served_id, ip.bid_ip, ip.time, ip.guid
    FROM impression_pool ip
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = ip.campaign_id AND c.deleted = FALSE AND c.funnel_level = 1
    WHERE ip.rn = 1
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ip.vast_impression_ip ORDER BY ip.time) = 1
) s1vi ON s1vi.vast_impression_ip = u.this_bid_ip AND s1vi.time < u.vv_time
WHERE u.vv_stage > 1 AND u.s1_resolution_method IS NULL;
