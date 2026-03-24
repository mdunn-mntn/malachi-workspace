-- TI-650 Validation Run: Step 6 — Full Detail for Truly Unresolved VVs
-- Gets all pipeline IPs + timestamps + campaign metadata + S1 campaign creation date
-- for specific ad_served_ids. Output is paste-ready for Google Sheets.
--
-- ╔══════════════════════════════════════════════════════════════════╗
-- ║  PARAMETERS — 1 thing to change (marked with ── PARAM ──)      ║
-- ╠══════════════════════════════════════════════════════════════════╣
-- ║  1. TARGET_IDS — the ad_served_ids to investigate               ║
-- ╚══════════════════════════════════════════════════════════════════╝

-- ── TARGET_IDS: paste truly unresolved ad_served_ids from Step 5 ──
WITH target_ids AS (
    SELECT ad_served_id FROM UNNEST([
        '8ae132b0-8566-406b-aaf3-e3a0b73423e6',
        'e87853c7-6e1c-4313-982b-6507cc2c539b'
    ]) AS ad_served_id
),

clickpass AS (
    SELECT cp.ad_served_id,
        SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] AS clickpass_ip,
        cp.time AS vv_time, cp.campaign_id, cp.advertiser_id, cp.guid, cp.is_new,
        cp.attribution_model_id
    FROM `dw-main-silver.logdata.clickpass_log` cp
    WHERE cp.ad_served_id IN (SELECT ad_served_id FROM target_ids)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),

ip_vast_start AS (
    SELECT ad_served_id, SPLIT(ip, '/')[SAFE_OFFSET(0)] AS vast_start_ip, time AS vast_start_time
    FROM `dw-main-silver.logdata.event_log`
    WHERE event_type_raw = 'vast_start'
      AND ad_served_id IN (SELECT ad_served_id FROM target_ids) AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

ip_vast_impression AS (
    SELECT ad_served_id, SPLIT(ip, '/')[SAFE_OFFSET(0)] AS vast_impression_ip, time AS vast_impression_time
    FROM `dw-main-silver.logdata.event_log`
    WHERE event_type_raw = 'vast_impression'
      AND ad_served_id IN (SELECT ad_served_id FROM target_ids) AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

ip_viewability AS (
    SELECT ad_served_id, SPLIT(ip, '/')[SAFE_OFFSET(0)] AS viewability_ip, time AS viewability_time
    FROM `dw-main-silver.logdata.viewability_log`
    WHERE ad_served_id IN (SELECT ad_served_id FROM target_ids) AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

ip_impression AS (
    SELECT ad_served_id, SPLIT(ip, '/')[SAFE_OFFSET(0)] AS impression_ip,
           time AS impression_time, ttd_impression_id
    FROM `dw-main-silver.logdata.impression_log`
    WHERE ad_served_id IN (SELECT ad_served_id FROM target_ids) AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

ip_win AS (
    SELECT il.ad_served_id, SPLIT(w.ip, '/')[SAFE_OFFSET(0)] AS win_ip, w.time AS win_time
    FROM `dw-main-silver.logdata.win_logs` w
    JOIN ip_impression il ON w.auction_id = il.ttd_impression_id
    QUALIFY ROW_NUMBER() OVER (PARTITION BY il.ad_served_id ORDER BY w.time ASC) = 1
),

ip_bid_direct AS (
    SELECT il.ad_served_id,
           NULLIF(SPLIT(b.ip, '/')[SAFE_OFFSET(0)], '0.0.0.0') AS bid_ip_direct,
           b.time AS bid_time
    FROM `dw-main-silver.logdata.bid_logs` b
    JOIN ip_impression il ON b.auction_id = il.ttd_impression_id
    QUALIFY ROW_NUMBER() OVER (PARTITION BY il.ad_served_id ORDER BY b.time ASC) = 1
),

-- bid_ip COALESCE: bid_logs (primary) → impression_log.bid_ip → event_log.bid_ip → viewability_log.bid_ip
ip_bid AS (
    SELECT
        imp.ad_served_id,
        COALESCE(
            bd.bid_ip_direct,
            NULLIF(SPLIT(imp.bid_ip, '/')[SAFE_OFFSET(0)], '0.0.0.0'),
            NULLIF(SPLIT(vs.bid_ip, '/')[SAFE_OFFSET(0)], '0.0.0.0'),
            NULLIF(SPLIT(vi.bid_ip, '/')[SAFE_OFFSET(0)], '0.0.0.0'),
            NULLIF(SPLIT(vw.bid_ip, '/')[SAFE_OFFSET(0)], '0.0.0.0')
        ) AS bid_ip,
        bd.bid_time,
        CASE
            WHEN bd.bid_ip_direct IS NOT NULL THEN 'bid_logs'
            WHEN imp.bid_ip IS NOT NULL THEN 'impression_log.bid_ip'
            WHEN vs.bid_ip IS NOT NULL THEN 'event_log.bid_ip'
            WHEN vi.bid_ip IS NOT NULL THEN 'event_log.bid_ip'
            WHEN vw.bid_ip IS NOT NULL THEN 'viewability_log.bid_ip'
            ELSE NULL
        END AS bid_ip_source
    FROM `dw-main-silver.logdata.impression_log` imp
    LEFT JOIN ip_bid_direct bd ON bd.ad_served_id = imp.ad_served_id
    LEFT JOIN `dw-main-silver.logdata.event_log` vs
        ON vs.ad_served_id = imp.ad_served_id AND vs.event_type_raw = 'vast_start'
    LEFT JOIN `dw-main-silver.logdata.event_log` vi
        ON vi.ad_served_id = imp.ad_served_id AND vi.event_type_raw = 'vast_impression'
    LEFT JOIN `dw-main-silver.logdata.viewability_log` vw
        ON vw.ad_served_id = imp.ad_served_id
    WHERE imp.ad_served_id IN (SELECT ad_served_id FROM target_ids)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY imp.ad_served_id ORDER BY imp.time ASC) = 1
),

s1_creation AS (
    SELECT campaign_group_id, MIN(create_time) AS s1_created
    FROM `dw-main-bronze.integrationprod.campaigns`
    WHERE funnel_level = 1 AND deleted = FALSE AND is_test = FALSE
      AND objective_id IN (1, 5, 6)
    GROUP BY campaign_group_id
)

SELECT
    cp.ad_served_id,
    cp.advertiser_id,
    adv.company_name AS advertiser_name,
    c.campaign_group_id,
    cg.name AS campaign_group_name,
    cp.campaign_id,
    c.name AS campaign_name,
    c.funnel_level,
    c.objective_id,
    c.channel_id,
    CASE
        WHEN vs.vast_start_ip IS NOT NULL THEN 'CTV'
        WHEN vw.viewability_ip IS NOT NULL THEN 'Viewable Display'
        WHEN imp.impression_ip IS NOT NULL THEN 'Non-Viewable Display'
        ELSE 'No Impression'
    END AS impression_type,
    cp.clickpass_ip,
    cp.vv_time,
    cp.guid,
    cp.is_new,
    cp.attribution_model_id,
    am.name AS attribution_model_name,
    vs.vast_start_ip,           vs.vast_start_time,
    vi.vast_impression_ip,      vi.vast_impression_time,
    vw.viewability_ip,          vw.viewability_time,
    imp.impression_ip,          imp.impression_time,
    w.win_ip,                   w.win_time,
    b.bid_ip,                   b.bid_time,
    s1c.s1_created AS s1_campaign_created,
    DATE_DIFF(DATE(cp.vv_time), DATE(s1c.s1_created), DAY) AS max_lookback_days,
    CASE
        WHEN DATE_DIFF(DATE(cp.vv_time), DATE(s1c.s1_created), DAY) > 365
        THEN 'LOOKBACK_TOO_SHORT'
        ELSE 'GENUINELY_UNRESOLVED'
    END AS refined_classification,
    'No prior S1/S2 VV found in clickpass_log (all-time scan) matching bid_ip in same campaign_group' AS investigation_note
FROM clickpass cp
LEFT JOIN ip_vast_start vs ON vs.ad_served_id = cp.ad_served_id
LEFT JOIN ip_vast_impression vi ON vi.ad_served_id = cp.ad_served_id
LEFT JOIN ip_viewability vw ON vw.ad_served_id = cp.ad_served_id
LEFT JOIN ip_impression imp ON imp.ad_served_id = cp.ad_served_id
LEFT JOIN ip_win w ON w.ad_served_id = cp.ad_served_id
LEFT JOIN ip_bid b ON b.ad_served_id = cp.ad_served_id
JOIN `dw-main-bronze.integrationprod.campaigns` c
    ON c.campaign_id = cp.campaign_id AND c.deleted = FALSE
JOIN `dw-main-bronze.integrationprod.advertisers` adv
    ON cp.advertiser_id = adv.advertiser_id AND adv.deleted = FALSE
JOIN `dw-main-bronze.integrationprod.campaign_groups` cg
    ON c.campaign_group_id = cg.campaign_group_id AND cg.deleted = FALSE
LEFT JOIN s1_creation s1c
    ON s1c.campaign_group_id = c.campaign_group_id
LEFT JOIN `dw-main-bronze.integrationprod.attribution_models` am
    ON cp.attribution_model_id = am.attribution_model_id
ORDER BY cp.advertiser_id;
