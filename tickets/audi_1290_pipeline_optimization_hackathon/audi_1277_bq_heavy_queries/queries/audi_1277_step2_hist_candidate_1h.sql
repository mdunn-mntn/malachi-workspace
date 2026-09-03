-- Candidate: score computed inside each source CTE, dedup keyed on FARM_FINGERPRINT(ip) (INT64) instead of the STRING ip. Same hour, same pin, same checksum.
WITH temp_campaigns AS (
    SELECT DISTINCT campaign_id FROM `dw-main-bronze.external.camperbid_prod__hhst_v4__campaign_bucket` FOR SYSTEM_TIME AS OF TIMESTAMP('2026-09-03 06:00:00+00')
    UNION DISTINCT
    SELECT DISTINCT campaign_id FROM `dw-main-bronze.external.camperbid_prod__hhst_v3__campaign_bucket` FOR SYSTEM_TIME AS OF TIMESTAMP('2026-09-03 06:00:00+00')
)
, temp_beeswax_bid_events AS (
    SELECT campaign_id, FARM_FINGERPRINT(ip) AS hh_key, COALESCE(CASE WHEN conquest_score > 1 AND TIMESTAMP_SECONDS(conquest_score_ttl) > time THEN GREATEST(conquest_score, 0) WHEN household_score > 1 THEN GREATEST(household_score, 0) ELSE GREATEST(advertiser_household_score, 0) END, 0) AS hh_score
    FROM `dw-main-bronze.raw.bid_price_log` FOR SYSTEM_TIME AS OF TIMESTAMP('2026-09-03 06:00:00+00')
    WHERE time >= TIMESTAMP('2026-09-02 12:00:00+00') AND time < TIMESTAMP('2026-09-02 13:00:00+00')
        AND campaign_id IN (SELECT campaign_id FROM temp_campaigns)
        AND ip IS NOT NULL
        AND (has_price OR threshold_failure_reasons IN ('invalidSegmentIntentScore', 'invalidCampaignIntentScore', 'invalidAdvertiserIntentScore', 'missingIntentScore', 'invalidHouseholdScore', 'invalidAdvertiserHouseholdScoreFailure'))
)
, temp_mntn_bid_events AS (
    SELECT
        campaign_id
        , FARM_FINGERPRINT(ip) AS hh_key
        , COALESCE(CASE WHEN conquest_score > 1 AND TIMESTAMP_SECONDS(conquest_score_ttl) > time THEN GREATEST(conquest_score, 0) WHEN household_score > 1 THEN GREATEST(household_score, 0) ELSE GREATEST(advertiser_household_score, 0) END, 0) AS hh_score
    FROM (
        SELECT
            campaign_id
            , COALESCE(CASE WHEN device_ip = '0.0.0.0' THEN NULL WHEN device_ip LIKE '10.%' THEN NULL WHEN device_ip LIKE '127.%' THEN NULL WHEN device_ip LIKE '169.254.%' THEN NULL WHEN device_ip LIKE '172.16.%' THEN NULL WHEN device_ip LIKE '172.31.%' THEN NULL WHEN device_ip LIKE '192.168.%' THEN NULL ELSE device_ip END, device_ipv6) AS ip
            , segment_intent_score AS conquest_score
            , segment_intent_score_ttl AS conquest_score_ttl
            , TIMESTAMP_MICROS(CAST(auction_timestamp / 1000 AS BIGINT)) AS time
            , campaign_intent_score AS household_score
            , advertiser_intent_score AS advertiser_household_score
        FROM `dw-main-bronze.raw.bidder_bid_events` FOR SYSTEM_TIME AS OF TIMESTAMP('2026-09-03 06:00:00+00')
        WHERE _PARTITIONTIME >= TIMESTAMP('2026-09-02 12:00:00+00') AND _PARTITIONTIME < TIMESTAMP('2026-09-02 13:00:00+00')
            AND campaign_id IN (SELECT campaign_id FROM temp_campaigns)
            AND (NOT bid_dropped OR bid_dropped_reason IN ('invalidSegmentIntentScore', 'invalidCampaignIntentScore', 'invalidAdvertiserIntentScore', 'missingIntentScore', 'invalidHouseholdScore', 'invalidAdvertiserHouseholdScoreFailure'))
    )
    WHERE ip IS NOT NULL
)
, temp_stacked AS (
    SELECT campaign_id, hh_key, hh_score FROM temp_beeswax_bid_events
    UNION ALL
    SELECT campaign_id, hh_key, hh_score FROM temp_mntn_bid_events
)
, temp_aggregate AS (
    SELECT campaign_id, 'ip' AS key_type, hh_key, MAX(hh_score) AS hh_score
    FROM temp_stacked
    GROUP BY 1, 2, 3
)
, temp_histogram AS (
    SELECT campaign_id, key_type, hh_score, COUNT(*) AS population
    FROM temp_aggregate
    GROUP BY 1, 2, 3
)
SELECT
    COUNT(*) AS histogram_rows
    , COUNT(DISTINCT campaign_id) AS campaigns
    , SUM(population) AS keys_total
    , BIT_XOR(FARM_FINGERPRINT(FORMAT('%d|%s|%d|%d', campaign_id, key_type, hh_score, population))) AS checksum
FROM temp_histogram
LIMIT 1
