-- TI-650: Characterize the 1,074 "no CIL" S3 VVs
-- These have ad_served_id in clickpass_log but no cost_impression_log record.
-- Investigate: event_log presence, attribution model, impression age, campaign type.
-- Advertiser: 37775 | Trace: Feb 4–11 | Lookback: 90 days

WITH campaigns_prosp AS (
    SELECT campaign_id, funnel_level, objective_id
    FROM `dw-main-bronze.integrationprod.campaigns`
    WHERE advertiser_id = 37775 AND deleted = FALSE AND is_test = FALSE
      AND funnel_level = 3 AND objective_id IN (1, 5, 6)
),

cp_s3 AS (
    SELECT cp.ad_served_id, cp.time AS vv_time, cp.ip AS redirect_ip,
           cp.campaign_id, cp.attribution_model_id, cp.is_cross_device,
           cp.guid, cp.first_touch_ad_served_id
    FROM `dw-main-silver.logdata.clickpass_log` cp
    JOIN campaigns_prosp c ON c.campaign_id = cp.campaign_id
    WHERE cp.time >= TIMESTAMP('2026-02-04') AND cp.time < TIMESTAMP('2026-02-11')
      AND cp.advertiser_id = 37775
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),

cil_check AS (
    SELECT DISTINCT cil.ad_served_id
    FROM `dw-main-silver.logdata.cost_impression_log` cil
    WHERE cil.advertiser_id = 37775
      AND cil.time >= TIMESTAMP('2025-11-06') AND cil.time < TIMESTAMP('2026-02-11')
      AND cil.ad_served_id IN (SELECT ad_served_id FROM cp_s3)
),

no_cil AS (
    SELECT cp.*
    FROM cp_s3 cp
    WHERE cp.ad_served_id NOT IN (SELECT ad_served_id FROM cil_check)
),

event_log_check AS (
    SELECT el.ad_served_id,
           MIN(el.time) AS earliest_event_time,
           MAX(el.time) AS latest_event_time,
           ARRAY_AGG(DISTINCT el.event_type_raw) AS event_types,
           MAX(el.bid_ip) AS bid_ip,
           MAX(el.campaign_id) AS el_campaign_id
    FROM `dw-main-silver.logdata.event_log` el
    WHERE el.time >= TIMESTAMP('2025-08-06') AND el.time < TIMESTAMP('2026-02-11')
      AND el.ad_served_id IN (SELECT ad_served_id FROM no_cil)
    GROUP BY el.ad_served_id
),

campaign_info AS (
    SELECT c.campaign_id, c.name AS campaign_name, c.funnel_level, c.objective_id
    FROM `dw-main-bronze.integrationprod.campaigns` c
    WHERE c.advertiser_id = 37775 AND c.deleted = FALSE
)

SELECT
    COUNT(*) AS total_no_cil,

    -- Event log presence
    COUNTIF(el.ad_served_id IS NOT NULL) AS has_event_log,
    COUNTIF(el.ad_served_id IS NULL) AS no_event_log,

    -- Attribution model
    COUNTIF(nc.attribution_model_id = 1) AS model_1_guid,
    COUNTIF(nc.attribution_model_id = 2) AS model_2_ip,
    COUNTIF(nc.attribution_model_id = 3) AS model_3_ga,
    COUNTIF(nc.attribution_model_id = 9) AS model_9_comp_guid,
    COUNTIF(nc.attribution_model_id = 10) AS model_10_comp_ip,
    COUNTIF(nc.attribution_model_id = 11) AS model_11_comp_ga,
    COUNTIF(nc.attribution_model_id IN (1,2,3)) AS primary_attribution,
    COUNTIF(nc.attribution_model_id IN (9,10,11)) AS competing_attribution,

    -- Cross-device
    COUNTIF(nc.is_cross_device = TRUE) AS cross_device_true,
    COUNTIF(nc.is_cross_device = FALSE) AS cross_device_false,

    -- Impression age (days from event_log earliest to VV)
    ROUND(AVG(CASE WHEN el.earliest_event_time IS NOT NULL
        THEN TIMESTAMP_DIFF(nc.vv_time, el.earliest_event_time, HOUR) / 24.0 END), 1) AS avg_imp_age_days,
    ROUND(MAX(CASE WHEN el.earliest_event_time IS NOT NULL
        THEN TIMESTAMP_DIFF(nc.vv_time, el.earliest_event_time, HOUR) / 24.0 END), 1) AS max_imp_age_days,
    ROUND(MIN(CASE WHEN el.earliest_event_time IS NOT NULL
        THEN TIMESTAMP_DIFF(nc.vv_time, el.earliest_event_time, HOUR) / 24.0 END), 1) AS min_imp_age_days,

    -- How old is the impression itself? (days before trace start Feb 4)
    COUNTIF(el.earliest_event_time IS NOT NULL AND el.earliest_event_time < TIMESTAMP('2025-11-06')) AS imp_older_than_90d,
    COUNTIF(el.earliest_event_time IS NOT NULL AND el.earliest_event_time >= TIMESTAMP('2025-11-06')
        AND el.earliest_event_time < TIMESTAMP('2025-12-06')) AS imp_60_90d_old,
    COUNTIF(el.earliest_event_time IS NOT NULL AND el.earliest_event_time >= TIMESTAMP('2025-12-06')
        AND el.earliest_event_time < TIMESTAMP('2026-01-06')) AS imp_30_60d_old,
    COUNTIF(el.earliest_event_time IS NOT NULL AND el.earliest_event_time >= TIMESTAMP('2026-01-06')) AS imp_under_30d_old,

    -- Has first_touch_ad_served_id
    COUNTIF(nc.first_touch_ad_served_id IS NOT NULL) AS has_first_touch

FROM no_cil nc
LEFT JOIN event_log_check el ON el.ad_served_id = nc.ad_served_id;
