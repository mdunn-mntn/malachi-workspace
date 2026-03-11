CREATE TEMP TABLE impression_pool AS
WITH el AS (
    SELECT ad_served_id,
        MAX(CASE WHEN event_type_raw = 'vast_start' THEN ip END) AS vast_start_ip,
        MAX(CASE WHEN event_type_raw = 'vast_impression' THEN ip END) AS vast_impression_ip,
        MAX(bid_ip) AS bid_ip, MAX(campaign_id) AS campaign_id, MIN(time) AS time, MAX(guid) AS guid
    FROM `dw-main-silver.logdata.event_log`
    WHERE event_type_raw IN ('vast_start', 'vast_impression')
      AND time >= TIMESTAMP('2025-11-06') AND time < TIMESTAMP('2026-02-11')
      AND campaign_id IN (
          SELECT campaign_id FROM `dw-main-bronze.integrationprod.campaigns`
          WHERE advertiser_id = 37775 AND deleted = FALSE AND objective_id IN (1, 5, 6)
      )
    GROUP BY ad_served_id
),
cil AS (
    SELECT ad_served_id, ip AS vast_start_ip, ip AS vast_impression_ip, ip AS bid_ip, campaign_id, time, guid
    FROM `dw-main-silver.logdata.cost_impression_log`
    WHERE time >= TIMESTAMP('2025-11-06') AND time < TIMESTAMP('2026-02-11')
      AND advertiser_id = 37775
      AND campaign_id IN (
          SELECT campaign_id FROM `dw-main-bronze.integrationprod.campaigns`
          WHERE advertiser_id = 37775 AND deleted = FALSE AND objective_id IN (1, 5, 6)
      )
)
SELECT ad_served_id, vast_start_ip, vast_impression_ip, bid_ip, campaign_id, time, guid,
    ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) AS rn
FROM (SELECT * FROM el UNION ALL SELECT * FROM cil);

CREATE TEMP TABLE prior_vv_raw AS
SELECT cp.advertiser_id, cp.ad_served_id AS prior_vv_ad_served_id, cp.campaign_id AS pv_campaign_id,
    cp.time AS prior_vv_time, c.funnel_level AS pv_stage, cp.ip AS pv_redirect_ip,
    cp.guid AS pv_guid, cp.attribution_model_id AS pv_attribution_model_id,
    imp.vast_start_ip AS pv_vast_start_ip, imp.vast_impression_ip AS pv_vast_impression_ip,
    imp.bid_ip AS pv_bid_ip, imp.time AS pv_imp_time, imp.guid AS pv_imp_guid
FROM `dw-main-silver.logdata.clickpass_log` cp
JOIN `dw-main-bronze.integrationprod.campaigns` c
    ON c.campaign_id = cp.campaign_id AND c.deleted = FALSE AND c.objective_id IN (1, 5, 6)
LEFT JOIN impression_pool imp ON imp.ad_served_id = cp.ad_served_id AND imp.rn = 1
WHERE cp.time >= TIMESTAMP('2025-11-06') AND cp.time < TIMESTAMP('2026-02-11') AND cp.advertiser_id = 37775
QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1;

CREATE TEMP TABLE pv_pool_vast AS
SELECT * EXCEPT(prio) FROM (
    SELECT pv_vast_start_ip AS match_ip, 1 AS prio, pvr.* FROM prior_vv_raw pvr WHERE pv_vast_start_ip IS NOT NULL
    UNION ALL
    SELECT pv_vast_impression_ip AS match_ip, 2 AS prio, pvr.* FROM prior_vv_raw pvr WHERE pv_vast_impression_ip IS NOT NULL
) QUALIFY ROW_NUMBER() OVER (PARTITION BY match_ip, pv_stage ORDER BY prio, prior_vv_time DESC) = 1;

CREATE TEMP TABLE pv_pool_redir AS
SELECT * FROM prior_vv_raw WHERE pv_redirect_ip IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY pv_redirect_ip, pv_stage ORDER BY prior_vv_time DESC) = 1;

CREATE TEMP TABLE s1_imp_pool AS
SELECT ip.vast_start_ip, ip.vast_impression_ip, ip.bid_ip, ip.ad_served_id, ip.campaign_id, ip.time, ip.guid
FROM impression_pool ip
JOIN `dw-main-bronze.integrationprod.campaigns` c ON c.campaign_id = ip.campaign_id AND c.deleted = FALSE
WHERE c.funnel_level = 1 AND ip.rn = 1
QUALIFY ROW_NUMBER() OVER (PARTITION BY ip.bid_ip ORDER BY ip.time) = 1;

CREATE TEMP TABLE s1_imp_guid AS
SELECT ip.guid, ip.vast_start_ip, ip.vast_impression_ip, ip.bid_ip, ip.ad_served_id, ip.campaign_id, ip.time
FROM impression_pool ip
JOIN `dw-main-bronze.integrationprod.campaigns` c ON c.campaign_id = ip.campaign_id AND c.deleted = FALSE
WHERE c.funnel_level = 1 AND ip.rn = 1 AND ip.guid IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY ip.guid ORDER BY ip.time) = 1;

CREATE TEMP TABLE s1_vv_guid AS
SELECT cp.guid, cp.ad_served_id AS s1_vv_ad_served_id, cp.time AS s1_vv_time,
       imp.vast_start_ip, imp.vast_impression_ip, imp.bid_ip, imp.ad_served_id AS s1_imp_ad_served_id,
       imp.time AS s1_imp_time, imp.guid AS s1_imp_guid
FROM `dw-main-silver.logdata.clickpass_log` cp
JOIN `dw-main-bronze.integrationprod.campaigns` c
    ON c.campaign_id = cp.campaign_id AND c.deleted = FALSE AND c.funnel_level = 1 AND c.objective_id IN (1)
LEFT JOIN impression_pool imp ON imp.ad_served_id = cp.ad_served_id AND imp.rn = 1
WHERE cp.time >= TIMESTAMP('2025-11-06') AND cp.time < TIMESTAMP('2026-02-11') AND cp.advertiser_id = 37775
QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.guid ORDER BY cp.time DESC) = 1;

WITH campaigns_stage AS (
    SELECT campaign_id, funnel_level AS stage FROM `dw-main-bronze.integrationprod.campaigns`
    WHERE deleted = FALSE AND objective_id IN (1, 5, 6)
),
cp_dedup AS (
    SELECT cp.ad_served_id, cp.advertiser_id, cp.campaign_id, cp.ip AS redirect_ip, cp.is_new, cp.is_cross_device,
        cp.first_touch_ad_served_id, cp.guid, cp.original_guid, cp.attribution_model_id, cp.time, c.stage AS vv_stage
    FROM `dw-main-silver.logdata.clickpass_log` cp
    JOIN campaigns_stage c ON c.campaign_id = cp.campaign_id
    WHERE cp.time >= TIMESTAMP('2026-02-04') AND cp.time < TIMESTAMP('2026-02-11') AND cp.advertiser_id = 37775
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),
v_dedup AS (
    SELECT CAST(ad_served_id AS STRING) AS ad_served_id, ip AS visit_ip, is_new AS visit_is_new, impression_ip
    FROM `dw-main-silver.summarydata.ui_visits`
    WHERE from_verified_impression = TRUE AND time >= TIMESTAMP('2026-01-28') AND time < TIMESTAMP('2026-02-18')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(ad_served_id AS STRING) ORDER BY time DESC) = 1
),
with_all_joins AS (
    SELECT cp.ad_served_id, cp.vv_stage, cp.attribution_model_id,
        CASE
            WHEN cp.vv_stage = 1 THEN 'current_is_s1'
            WHEN COALESCE(pv_vast.pv_stage, pv_redir.pv_stage) = 1 THEN 'vv_chain_direct'
            WHEN COALESCE(s1_vast.prior_vv_ad_served_id, s1_redir.prior_vv_ad_served_id) IS NOT NULL THEN 'vv_chain_s2_s1'
            WHEN s1_imp_chain.bid_ip IS NOT NULL THEN 'imp_chain'
            WHEN s1_imp_direct.bid_ip IS NOT NULL THEN 'imp_direct'
            WHEN s1_imp_visit_ip.bid_ip IS NOT NULL THEN 'imp_visit_ip'
            WHEN ft_lt.bid_ip IS NOT NULL THEN 'cp_ft_fallback'
            WHEN s1_guid_vv.bid_ip IS NOT NULL THEN 'guid_vv_match'
            WHEN s1_guid_imp.bid_ip IS NOT NULL THEN 'guid_imp_match'
            WHEN s1_imp_redir.bid_ip IS NOT NULL THEN 's1_imp_redirect'
            ELSE NULL
        END AS s1_resolution_method,
        ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY
            CASE WHEN pv_vast.prior_vv_ad_served_id IS NOT NULL THEN 0 WHEN pv_redir.prior_vv_ad_served_id IS NOT NULL THEN 1 ELSE 2 END,
            COALESCE(pv_vast.prior_vv_time, pv_redir.prior_vv_time) DESC NULLS LAST,
            CASE WHEN s1_vast.prior_vv_ad_served_id IS NOT NULL THEN 0 WHEN s1_redir.prior_vv_ad_served_id IS NOT NULL THEN 1 ELSE 2 END,
            COALESCE(s1_vast.prior_vv_time, s1_redir.prior_vv_time) DESC NULLS LAST
        ) AS _pv_rn
    FROM cp_dedup cp
    LEFT JOIN impression_pool lt ON lt.ad_served_id = cp.ad_served_id AND lt.rn = 1
    LEFT JOIN v_dedup v ON v.ad_served_id = cp.ad_served_id
    LEFT JOIN pv_pool_vast pv_vast ON pv_vast.advertiser_id = cp.advertiser_id AND pv_vast.match_ip = lt.bid_ip AND pv_vast.prior_vv_time < cp.time AND pv_vast.prior_vv_ad_served_id != cp.ad_served_id AND pv_vast.pv_stage < cp.vv_stage
    LEFT JOIN pv_pool_redir pv_redir ON pv_redir.advertiser_id = cp.advertiser_id AND pv_redir.pv_redirect_ip = cp.redirect_ip AND pv_redir.prior_vv_time < cp.time AND pv_redir.prior_vv_ad_served_id != cp.ad_served_id AND pv_redir.pv_stage < cp.vv_stage
    LEFT JOIN impression_pool pv_lt ON pv_lt.ad_served_id = COALESCE(pv_vast.prior_vv_ad_served_id, pv_redir.prior_vv_ad_served_id) AND pv_lt.rn = 1
    LEFT JOIN pv_pool_vast s1_vast ON s1_vast.advertiser_id = cp.advertiser_id AND s1_vast.match_ip = pv_lt.bid_ip AND s1_vast.pv_stage = 1 AND s1_vast.pv_stage < COALESCE(pv_vast.pv_stage, pv_redir.pv_stage) AND s1_vast.prior_vv_time < COALESCE(pv_vast.prior_vv_time, pv_redir.prior_vv_time) AND s1_vast.prior_vv_ad_served_id != COALESCE(pv_vast.prior_vv_ad_served_id, pv_redir.prior_vv_ad_served_id)
    LEFT JOIN pv_pool_redir s1_redir ON s1_redir.advertiser_id = cp.advertiser_id AND s1_redir.pv_redirect_ip = COALESCE(pv_vast.pv_redirect_ip, pv_redir.pv_redirect_ip) AND s1_redir.pv_stage = 1 AND s1_redir.pv_stage < COALESCE(pv_vast.pv_stage, pv_redir.pv_stage) AND s1_redir.prior_vv_time < COALESCE(pv_vast.prior_vv_time, pv_redir.prior_vv_time) AND s1_redir.prior_vv_ad_served_id != COALESCE(pv_vast.prior_vv_ad_served_id, pv_redir.prior_vv_ad_served_id)
    LEFT JOIN impression_pool s1_lt ON s1_lt.ad_served_id = COALESCE(s1_vast.prior_vv_ad_served_id, s1_redir.prior_vv_ad_served_id) AND s1_lt.rn = 1
    LEFT JOIN s1_imp_pool s1_imp_chain ON s1_imp_chain.bid_ip = pv_lt.bid_ip AND s1_imp_chain.time < COALESCE(pv_vast.prior_vv_time, pv_redir.prior_vv_time)
    LEFT JOIN s1_imp_pool s1_imp_direct ON s1_imp_direct.bid_ip = lt.bid_ip AND s1_imp_direct.time < cp.time
    LEFT JOIN s1_imp_pool s1_imp_visit_ip ON s1_imp_visit_ip.bid_ip = v.impression_ip AND v.impression_ip != lt.bid_ip AND s1_imp_visit_ip.time < cp.time
    LEFT JOIN impression_pool ft_lt ON ft_lt.ad_served_id = cp.first_touch_ad_served_id AND ft_lt.rn = 1
    LEFT JOIN s1_vv_guid s1_guid_vv ON s1_guid_vv.guid = cp.guid AND s1_guid_vv.s1_vv_ad_served_id != cp.ad_served_id AND s1_guid_vv.s1_vv_time < cp.time
    LEFT JOIN s1_imp_guid s1_guid_imp ON s1_guid_imp.guid = cp.guid AND s1_guid_imp.ad_served_id != cp.ad_served_id AND s1_guid_imp.time < cp.time
    LEFT JOIN s1_imp_pool s1_imp_redir ON s1_imp_redir.bid_ip = cp.redirect_ip AND cp.redirect_ip != lt.bid_ip AND s1_imp_redir.time < cp.time
)
SELECT
  vv_stage,
  IFNULL(s1_resolution_method, 'UNRESOLVED') AS s1_resolution_method,
  COUNT(*) AS cnt,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(PARTITION BY vv_stage), 2) AS pct_of_stage
FROM with_all_joins
WHERE _pv_rn = 1
GROUP BY vv_stage, s1_resolution_method
ORDER BY vv_stage, cnt DESC;
