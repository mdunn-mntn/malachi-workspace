-- Q3d: Split OR joins into separate hash-joinable LEFT JOINs
-- Target: eliminate S27/S29 nested-loop bottleneck (8,593 slot-seconds in Q3c)

-- Step 1: event_log (same as Q3c)
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

-- Step 2: cost_impression_log (same as Q3c)
CREATE TEMP TABLE cil_all AS
SELECT ad_served_id, ip AS vast_ip, ip AS bid_ip, campaign_id, time,
    ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time) AS rn
FROM `dw-main-silver.logdata.cost_impression_log`
WHERE time >= TIMESTAMP('2026-01-05') AND time < TIMESTAMP('2026-02-05')
  AND advertiser_id = 37775;

-- Step 3: prior_vv_pool (same as Q3c)
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

-- Step 4: Main query — OR conditions split into separate LEFT JOINs for hash join
-- Original: pv ON (ip = bid_ip OR ip = redirect_ip) → nested loop (75M rows, 69M reads)
-- Split:    pv_bid ON ip = bid_ip + pv_redir ON ip = redirect_ip → two hash joins
-- Same pattern for s1_pv → s1_bid + s1_redir
WITH campaigns_stage AS (
    SELECT campaign_id, funnel_level AS stage
    FROM `dw-main-bronze.integrationprod.campaigns`
    WHERE deleted = FALSE
),
cp_dedup AS (
    SELECT cp.ad_served_id, cp.advertiser_id, cp.campaign_id, cp.ip, cp.is_new, cp.is_cross_device,
        cp.first_touch_ad_served_id, cp.time, c.stage AS vv_stage
    FROM `dw-main-silver.logdata.clickpass_log` cp
    LEFT JOIN campaigns_stage c ON c.campaign_id = cp.campaign_id
    WHERE cp.time >= TIMESTAMP('2026-02-04') AND cp.time < TIMESTAMP('2026-02-05')
      AND cp.advertiser_id = 37775
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),
v_dedup AS (
    SELECT CAST(ad_served_id AS STRING) AS ad_served_id, ip, is_new, impression_ip
    FROM `dw-main-silver.summarydata.ui_visits`
    WHERE from_verified_impression = TRUE
      AND time >= TIMESTAMP('2026-01-28') AND time < TIMESTAMP('2026-02-12')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(ad_served_id AS STRING) ORDER BY time DESC) = 1
),
with_all_joins AS (
    SELECT
        cp.ad_served_id, cp.advertiser_id, cp.campaign_id,
        cp.vv_stage, cp.time AS vv_time,
        COALESCE(lt.bid_ip, lt_d.bid_ip) AS lt_bid_ip,
        COALESCE(lt.vast_ip, lt_d.vast_ip) AS lt_vast_ip,
        cp.ip AS redirect_ip, v.ip AS visit_ip, v.impression_ip,
        cp.first_touch_ad_served_id AS cp_ft_ad_served_id,

        -- S1 resolution (3-branch CASE, strict pv_stage < vv_stage)
        CASE
            WHEN cp.vv_stage = 1
                THEN cp.ad_served_id
            WHEN COALESCE(pv_bid.pv_stage, pv_redir.pv_stage) = 1
                THEN COALESCE(pv_bid.prior_vv_ad_served_id, pv_redir.prior_vv_ad_served_id)
            ELSE COALESCE(s1_bid.prior_vv_ad_served_id, s1_redir.prior_vv_ad_served_id)
        END AS s1_ad_served_id,
        CASE
            WHEN cp.vv_stage = 1
                THEN COALESCE(lt.bid_ip, lt_d.bid_ip)
            WHEN COALESCE(pv_bid.pv_stage, pv_redir.pv_stage) = 1
                THEN COALESCE(pv_lt.bid_ip, pv_lt_d.bid_ip)
            ELSE COALESCE(s1_lt.bid_ip, s1_lt_d.bid_ip)
        END AS s1_bid_ip,
        CASE
            WHEN cp.vv_stage = 1
                THEN COALESCE(lt.vast_ip, lt_d.vast_ip)
            WHEN COALESCE(pv_bid.pv_stage, pv_redir.pv_stage) = 1
                THEN COALESCE(pv_lt.vast_ip, pv_lt_d.vast_ip)
            ELSE COALESCE(s1_lt.vast_ip, s1_lt_d.vast_ip)
        END AS s1_vast_ip,

        -- Prior VV (coalesced from bid-match and redirect-match)
        COALESCE(pv_bid.prior_vv_ad_served_id, pv_redir.prior_vv_ad_served_id) AS prior_vv_ad_served_id,
        COALESCE(pv_bid.prior_vv_time, pv_redir.prior_vv_time) AS prior_vv_time,
        COALESCE(pv_bid.pv_campaign_id, pv_redir.pv_campaign_id) AS pv_campaign_id,
        COALESCE(pv_bid.pv_stage, pv_redir.pv_stage) AS pv_stage,
        COALESCE(pv_bid.ip, pv_redir.ip) AS pv_redirect_ip,
        COALESCE(pv_lt.bid_ip, pv_lt_d.bid_ip) AS pv_lt_bid_ip,
        COALESCE(pv_lt.vast_ip, pv_lt_d.vast_ip) AS pv_lt_vast_ip,
        COALESCE(pv_lt.time, pv_lt_d.time) AS pv_lt_time,

        cp.is_new AS clickpass_is_new, v.is_new AS visit_is_new, cp.is_cross_device,
        DATE(cp.time) AS trace_date,
        CURRENT_TIMESTAMP() AS trace_run_timestamp,

        ROW_NUMBER() OVER (
            PARTITION BY cp.ad_served_id
            ORDER BY
                -- Prefer bid_ip match over redirect_ip match
                CASE WHEN pv_bid.prior_vv_ad_served_id IS NOT NULL THEN 0 ELSE 1 END,
                COALESCE(pv_bid.prior_vv_time, pv_redir.prior_vv_time) DESC NULLS LAST,
                CASE WHEN s1_bid.prior_vv_ad_served_id IS NOT NULL THEN 0 ELSE 1 END,
                COALESCE(s1_bid.prior_vv_time, s1_redir.prior_vv_time) DESC NULLS LAST
        ) AS _pv_rn

    FROM cp_dedup cp

    -- Last-touch impression IP lookup (CTV then display fallback)
    LEFT JOIN el_all lt ON lt.ad_served_id = cp.ad_served_id AND lt.rn = 1
    LEFT JOIN cil_all lt_d ON lt_d.ad_served_id = cp.ad_served_id AND lt_d.rn = 1

    -- Visit IP
    LEFT JOIN v_dedup v ON v.ad_served_id = cp.ad_served_id

    -- Prior VV: SPLIT — bid_ip match (preferred, hash-joinable)
    LEFT JOIN prior_vv_pool pv_bid
        ON pv_bid.advertiser_id = cp.advertiser_id
        AND pv_bid.ip = COALESCE(lt.bid_ip, lt_d.bid_ip)
        AND pv_bid.prior_vv_time < cp.time
        AND pv_bid.prior_vv_ad_served_id != cp.ad_served_id
        AND pv_bid.pv_stage < cp.vv_stage

    -- Prior VV: SPLIT — redirect_ip match (fallback, hash-joinable)
    LEFT JOIN prior_vv_pool pv_redir
        ON pv_redir.advertiser_id = cp.advertiser_id
        AND pv_redir.ip = cp.ip
        AND pv_redir.prior_vv_time < cp.time
        AND pv_redir.prior_vv_ad_served_id != cp.ad_served_id
        AND pv_redir.pv_stage < cp.vv_stage

    -- Prior VV impression IP lookup (uses coalesced pv ad_served_id)
    LEFT JOIN el_all pv_lt
        ON pv_lt.ad_served_id = COALESCE(pv_bid.prior_vv_ad_served_id, pv_redir.prior_vv_ad_served_id)
        AND pv_lt.rn = 1
    LEFT JOIN cil_all pv_lt_d
        ON pv_lt_d.ad_served_id = COALESCE(pv_bid.prior_vv_ad_served_id, pv_redir.prior_vv_ad_served_id)
        AND pv_lt_d.rn = 1

    -- S1 chain: SPLIT — bid_ip match on prior VV's impression IP (preferred)
    LEFT JOIN prior_vv_pool s1_bid
        ON s1_bid.advertiser_id = cp.advertiser_id
        AND s1_bid.ip = COALESCE(pv_lt.bid_ip, pv_lt_d.bid_ip)
        AND s1_bid.pv_stage < COALESCE(pv_bid.pv_stage, pv_redir.pv_stage)
        AND s1_bid.prior_vv_time < COALESCE(pv_bid.prior_vv_time, pv_redir.prior_vv_time)
        AND s1_bid.prior_vv_ad_served_id != COALESCE(pv_bid.prior_vv_ad_served_id, pv_redir.prior_vv_ad_served_id)

    -- S1 chain: SPLIT — redirect_ip match on prior VV's redirect IP (fallback)
    LEFT JOIN prior_vv_pool s1_redir
        ON s1_redir.advertiser_id = cp.advertiser_id
        AND s1_redir.ip = COALESCE(pv_bid.ip, pv_redir.ip)
        AND s1_redir.pv_stage < COALESCE(pv_bid.pv_stage, pv_redir.pv_stage)
        AND s1_redir.prior_vv_time < COALESCE(pv_bid.prior_vv_time, pv_redir.prior_vv_time)
        AND s1_redir.prior_vv_ad_served_id != COALESCE(pv_bid.prior_vv_ad_served_id, pv_redir.prior_vv_ad_served_id)

    -- S1 impression IP lookup (uses coalesced s1 ad_served_id)
    LEFT JOIN el_all s1_lt
        ON s1_lt.ad_served_id = COALESCE(s1_bid.prior_vv_ad_served_id, s1_redir.prior_vv_ad_served_id)
        AND s1_lt.rn = 1
    LEFT JOIN cil_all s1_lt_d
        ON s1_lt_d.ad_served_id = COALESCE(s1_bid.prior_vv_ad_served_id, s1_redir.prior_vv_ad_served_id)
        AND s1_lt_d.rn = 1
)
SELECT
    ad_served_id, advertiser_id, campaign_id, vv_stage, vv_time,
    lt_bid_ip, lt_vast_ip, redirect_ip, visit_ip, impression_ip,
    cp_ft_ad_served_id, s1_ad_served_id, s1_bid_ip, s1_vast_ip,
    prior_vv_ad_served_id, prior_vv_time, pv_campaign_id, pv_stage,
    pv_redirect_ip, pv_lt_bid_ip, pv_lt_vast_ip, pv_lt_time,
    clickpass_is_new, visit_is_new, is_cross_device,
    trace_date, trace_run_timestamp
FROM with_all_joins
WHERE _pv_rn = 1
LIMIT 100;
