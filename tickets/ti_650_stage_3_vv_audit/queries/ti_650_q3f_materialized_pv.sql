-- Q3f: Split OR + s1_pool + materialized pv_matched TEMP TABLE
-- Adds: pv match resolved upfront as TEMP TABLE, main query does simple ad_served_id join

-- Step 1: event_log (same)
CREATE TEMP TABLE el_all AS
SELECT ad_served_id, ip AS vast_ip, bid_ip, campaign_id, time,
    ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time) AS rn
FROM `dw-main-silver.logdata.event_log`
WHERE event_type_raw = 'vast_impression'
  AND time >= TIMESTAMP('2026-01-05') AND time < TIMESTAMP('2026-02-05')
  AND campaign_id IN (
      SELECT campaign_id FROM `dw-main-bronze.integrationprod.campaigns`
      WHERE advertiser_id = 37775 AND deleted = FALSE
  );

-- Step 2: cost_impression_log (same)
CREATE TEMP TABLE cil_all AS
SELECT ad_served_id, ip AS vast_ip, ip AS bid_ip, campaign_id, time,
    ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time) AS rn
FROM `dw-main-silver.logdata.cost_impression_log`
WHERE time >= TIMESTAMP('2026-01-05') AND time < TIMESTAMP('2026-02-05')
  AND advertiser_id = 37775;

-- Step 3: prior_vv_pool (same)
CREATE TEMP TABLE prior_vv_pool AS
SELECT ip, advertiser_id, prior_vv_ad_served_id, pv_campaign_id, prior_vv_time, pv_stage
FROM (
    SELECT cp.ip, cp.advertiser_id, cp.ad_served_id AS prior_vv_ad_served_id,
        cp.campaign_id AS pv_campaign_id, cp.time AS prior_vv_time,
        c.funnel_level AS pv_stage
    FROM `dw-main-silver.logdata.clickpass_log` cp
    LEFT JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = cp.campaign_id AND c.deleted = FALSE
    WHERE cp.time >= TIMESTAMP('2026-01-05') AND cp.time < TIMESTAMP('2026-02-05')
      AND cp.advertiser_id = 37775
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
)
QUALIFY ROW_NUMBER() OVER (PARTITION BY ip, pv_stage ORDER BY prior_vv_time DESC) = 1;

-- Step 3b: s1_pool (stage 1 only)
CREATE TEMP TABLE s1_pool AS
SELECT ip, advertiser_id, prior_vv_ad_served_id, pv_campaign_id, prior_vv_time, pv_stage
FROM prior_vv_pool
WHERE pv_stage = 1;

-- Step 4: Materialize cp_anchor (needed for pv_matched step)
CREATE TEMP TABLE cp_anchor AS
SELECT cp.ad_served_id, cp.advertiser_id, cp.campaign_id, cp.ip, cp.is_new, cp.is_cross_device,
    cp.first_touch_ad_served_id, cp.time, c.funnel_level AS vv_stage
FROM `dw-main-silver.logdata.clickpass_log` cp
LEFT JOIN `dw-main-bronze.integrationprod.campaigns` c
    ON c.campaign_id = cp.campaign_id AND c.deleted = FALSE
WHERE cp.time >= TIMESTAMP('2026-02-04') AND cp.time < TIMESTAMP('2026-02-05')
  AND cp.advertiser_id = 37775
QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1;

-- Step 5: Materialize pv match (split OR → UNION ALL, deduped)
-- Resolves prior VV match upfront — main query does simple ad_served_id lookup
CREATE TEMP TABLE pv_matched AS
SELECT ad_served_id, prior_vv_ad_served_id, pv_campaign_id, prior_vv_time, pv_stage, pv_redirect_ip
FROM (
    -- Priority 1: match on bid_ip
    SELECT cp.ad_served_id, pvp.prior_vv_ad_served_id, pvp.pv_campaign_id,
        pvp.prior_vv_time, pvp.pv_stage, pvp.ip AS pv_redirect_ip, 0 AS match_priority
    FROM cp_anchor cp
    LEFT JOIN el_all lt ON lt.ad_served_id = cp.ad_served_id AND lt.rn = 1
    LEFT JOIN cil_all lt_d ON lt_d.ad_served_id = cp.ad_served_id AND lt_d.rn = 1
    INNER JOIN prior_vv_pool pvp
        ON pvp.advertiser_id = cp.advertiser_id
        AND pvp.ip = COALESCE(lt.bid_ip, lt_d.bid_ip)
        AND pvp.prior_vv_time < cp.time
        AND pvp.prior_vv_ad_served_id != cp.ad_served_id
        AND pvp.pv_stage < cp.vv_stage
    WHERE COALESCE(lt.bid_ip, lt_d.bid_ip) IS NOT NULL

    UNION ALL

    -- Priority 2: match on redirect_ip
    SELECT cp.ad_served_id, pvp.prior_vv_ad_served_id, pvp.pv_campaign_id,
        pvp.prior_vv_time, pvp.pv_stage, pvp.ip AS pv_redirect_ip, 1 AS match_priority
    FROM cp_anchor cp
    INNER JOIN prior_vv_pool pvp
        ON pvp.advertiser_id = cp.advertiser_id
        AND pvp.ip = cp.ip
        AND pvp.prior_vv_time < cp.time
        AND pvp.prior_vv_ad_served_id != cp.ad_served_id
        AND pvp.pv_stage < cp.vv_stage
)
QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY match_priority, prior_vv_time DESC) = 1;

-- Step 6: Materialize s1 match (split OR → UNION ALL, deduped)
CREATE TEMP TABLE s1_matched AS
SELECT pv.ad_served_id, s1.prior_vv_ad_served_id AS s1_ad_served_id
FROM pv_matched pv
LEFT JOIN el_all pv_lt ON pv_lt.ad_served_id = pv.prior_vv_ad_served_id AND pv_lt.rn = 1
LEFT JOIN cil_all pv_lt_d ON pv_lt_d.ad_served_id = pv.prior_vv_ad_served_id AND pv_lt_d.rn = 1
INNER JOIN s1_pool s1
    ON s1.advertiser_id = 37775
    AND s1.ip = COALESCE(pv_lt.bid_ip, pv_lt_d.bid_ip)
    AND s1.pv_stage < pv.pv_stage
    AND s1.prior_vv_time < pv.prior_vv_time
    AND s1.prior_vv_ad_served_id != pv.prior_vv_ad_served_id
WHERE pv.pv_stage > 1  -- only need s1 lookup when pv isn't already S1

UNION ALL

SELECT pv.ad_served_id, s1.prior_vv_ad_served_id AS s1_ad_served_id
FROM pv_matched pv
INNER JOIN s1_pool s1
    ON s1.advertiser_id = 37775
    AND s1.ip = pv.pv_redirect_ip
    AND s1.pv_stage < pv.pv_stage
    AND s1.prior_vv_time < pv.prior_vv_time
    AND s1.prior_vv_ad_served_id != pv.prior_vv_ad_served_id
WHERE pv.pv_stage > 1
QUALIFY ROW_NUMBER() OVER (PARTITION BY pv.ad_served_id ORDER BY s1.prior_vv_time DESC) = 1;

-- Step 7: Main query — simple hash joins by ad_served_id everywhere
WITH v_dedup AS (
    SELECT CAST(ad_served_id AS STRING) AS ad_served_id, ip, is_new, impression_ip
    FROM `dw-main-silver.summarydata.ui_visits`
    WHERE from_verified_impression = TRUE
      AND time >= TIMESTAMP('2026-01-28') AND time < TIMESTAMP('2026-02-12')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(ad_served_id AS STRING) ORDER BY time DESC) = 1
)
SELECT
    cp.ad_served_id, cp.advertiser_id, cp.campaign_id,
    cp.vv_stage, cp.time AS vv_time,
    COALESCE(lt.bid_ip, lt_d.bid_ip) AS lt_bid_ip,
    COALESCE(lt.vast_ip, lt_d.vast_ip) AS lt_vast_ip,
    cp.ip AS redirect_ip, v.ip AS visit_ip, v.impression_ip,
    cp.first_touch_ad_served_id AS cp_ft_ad_served_id,

    -- S1 resolution
    CASE
        WHEN cp.vv_stage = 1     THEN cp.ad_served_id
        WHEN pv.pv_stage = 1    THEN pv.prior_vv_ad_served_id
        ELSE                          s1.s1_ad_served_id
    END AS s1_ad_served_id,
    CASE
        WHEN cp.vv_stage = 1     THEN COALESCE(lt.bid_ip, lt_d.bid_ip)
        WHEN pv.pv_stage = 1    THEN COALESCE(pv_lt.bid_ip, pv_lt_d.bid_ip)
        ELSE                          COALESCE(s1_lt.bid_ip, s1_lt_d.bid_ip)
    END AS s1_bid_ip,
    CASE
        WHEN cp.vv_stage = 1     THEN COALESCE(lt.vast_ip, lt_d.vast_ip)
        WHEN pv.pv_stage = 1    THEN COALESCE(pv_lt.vast_ip, pv_lt_d.vast_ip)
        ELSE                          COALESCE(s1_lt.vast_ip, s1_lt_d.vast_ip)
    END AS s1_vast_ip,

    pv.prior_vv_ad_served_id, pv.prior_vv_time,
    pv.pv_campaign_id, pv.pv_stage,
    pv.pv_redirect_ip,
    COALESCE(pv_lt.bid_ip, pv_lt_d.bid_ip) AS pv_lt_bid_ip,
    COALESCE(pv_lt.vast_ip, pv_lt_d.vast_ip) AS pv_lt_vast_ip,
    COALESCE(pv_lt.time, pv_lt_d.time) AS pv_lt_time,

    cp.is_new AS clickpass_is_new, v.is_new AS visit_is_new, cp.is_cross_device,
    DATE(cp.time) AS trace_date,
    CURRENT_TIMESTAMP() AS trace_run_timestamp

FROM cp_anchor cp
LEFT JOIN el_all lt ON lt.ad_served_id = cp.ad_served_id AND lt.rn = 1
LEFT JOIN cil_all lt_d ON lt_d.ad_served_id = cp.ad_served_id AND lt_d.rn = 1
LEFT JOIN v_dedup v ON v.ad_served_id = cp.ad_served_id
LEFT JOIN pv_matched pv ON pv.ad_served_id = cp.ad_served_id
LEFT JOIN el_all pv_lt ON pv_lt.ad_served_id = pv.prior_vv_ad_served_id AND pv_lt.rn = 1
LEFT JOIN cil_all pv_lt_d ON pv_lt_d.ad_served_id = pv.prior_vv_ad_served_id AND pv_lt_d.rn = 1
LEFT JOIN s1_matched s1 ON s1.ad_served_id = cp.ad_served_id
LEFT JOIN el_all s1_lt ON s1_lt.ad_served_id = s1.s1_ad_served_id AND s1_lt.rn = 1
LEFT JOIN cil_all s1_lt_d ON s1_lt_d.ad_served_id = s1.s1_ad_served_id AND s1_lt_d.rn = 1
LIMIT 100;
