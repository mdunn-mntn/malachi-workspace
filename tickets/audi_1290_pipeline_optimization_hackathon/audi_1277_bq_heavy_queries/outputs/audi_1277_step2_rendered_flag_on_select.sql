    WITH temp_campaigns AS (
        SELECT DISTINCT campaign_id
        FROM `dw-main-bronze.external.camperbid_prod__hhst_v4__campaign_bucket`
        UNION DISTINCT
        SELECT DISTINCT campaign_id
        FROM `dw-main-bronze.external.camperbid_prod__hhst_v3__campaign_bucket`
    )
    , temp_campaign_flag AS (
        SELECT DISTINCT campaign_id, uses_mntn_id
        FROM `dw-main-bronze.external.camperbid_prod__hhst_v4__campaign_bucket`
    )
    , temp_beeswax_bid_events AS (
        SELECT
            campaign_id
            , ip
            , CAST(NULL AS STRING) AS household_id_source
            , CAST(NULL AS STRING) AS household_id_value
            , COALESCE(
                CASE
                    WHEN conquest_score > 1 AND TIMESTAMP_SECONDS(conquest_score_ttl) > time THEN GREATEST(conquest_score, 0)
                    WHEN household_score > 1 THEN GREATEST(household_score, 0)
                    ELSE GREATEST(advertiser_household_score, 0)
                END,
                0
            ) AS hh_score
        FROM `dw-main-bronze.raw.bid_price_log`
        WHERE
            time >= TIMESTAMP_SUB(CAST('2026-09-03T00:00:00+00:00' AS TIMESTAMP), INTERVAL 24 HOUR)
            AND time < CAST('2026-09-03T00:00:00+00:00' AS TIMESTAMP)
            AND campaign_id IN (SELECT campaign_id FROM temp_campaigns)
            AND (
                has_price
                OR threshold_failure_reasons IN (
                    'invalidSegmentIntentScore', 'invalidCampaignIntentScore',
                    'invalidAdvertiserIntentScore', 'missingIntentScore',
                    'invalidHouseholdScore', 'invalidAdvertiserHouseholdScoreFailure'
                )
            )
    )
    , temp_mntn_bid_events AS (
        SELECT
            campaign_id
            , ip
            , household_id_source
            , household_id_value
            , COALESCE(
                CASE
                    WHEN conquest_score > 1 AND TIMESTAMP_SECONDS(conquest_score_ttl) > time THEN GREATEST(conquest_score, 0)
                    WHEN household_score > 1 THEN GREATEST(household_score, 0)
                    ELSE GREATEST(advertiser_household_score, 0)
                END,
                0
            ) AS hh_score
        FROM (
            SELECT
                campaign_id
                , COALESCE(
                    CASE
                        WHEN device_ip = "0.0.0.0" THEN NULL -- broadcast
                        WHEN device_ip LIKE "10.%" THEN NULL -- loopback
                        WHEN device_ip LIKE "127.%" THEN NULL -- loopback
                        WHEN device_ip LIKE "169.254.%" THEN NULL -- link-local
                        WHEN device_ip LIKE "172.16.%" THEN NULL -- private
                        WHEN device_ip LIKE "172.31.%" THEN NULL -- private
                        WHEN device_ip LIKE "192.168.%" THEN NULL -- private
                        ELSE device_ip
                    END,
                    device_ipv6
                ) AS ip
                , segment_intent_score AS conquest_score
                , segment_intent_score_ttl AS conquest_score_ttl
                , TIMESTAMP_MICROS(CAST(auction_timestamp / 1000 AS BIGINT)) AS time
                , campaign_intent_score AS household_score
                , advertiser_intent_score AS advertiser_household_score
                , household_id_source
                , household_id_value
            FROM `dw-main-bronze.raw.bidder_bid_events`
            WHERE
                _PARTITIONTIME >= TIMESTAMP_SUB(CAST('2026-09-03T00:00:00+00:00' AS TIMESTAMP), INTERVAL 24 HOUR)
                AND _PARTITIONTIME < CAST('2026-09-03T00:00:00+00:00' AS TIMESTAMP)
                AND campaign_id IN (SELECT campaign_id FROM temp_campaigns)
                AND (
                    NOT bid_dropped
                    OR bid_dropped_reason IN (
                        'invalidSegmentIntentScore', 'invalidCampaignIntentScore',
                        'invalidAdvertiserIntentScore', 'missingIntentScore',
                        'invalidHouseholdScore', 'invalidAdvertiserHouseholdScoreFailure'
                    )
                )
        )
    )
    , temp_stacked AS (
        SELECT campaign_id, ip, household_id_source, household_id_value, hh_score
        FROM temp_beeswax_bid_events
        UNION ALL
        SELECT campaign_id, ip, household_id_source, household_id_value, hh_score
        FROM temp_mntn_bid_events
    )
    -- hh_key is a dedup key only and is never published, so an INT64 hash of it keeps the shuffle narrow.
    , temp_keys AS (
        SELECT campaign_id, 'ip' AS key_type, FARM_FINGERPRINT(ip) AS hh_key, hh_score
        FROM temp_stacked
        WHERE ip IS NOT NULL
        UNION ALL
        SELECT s.campaign_id, 'mntn' AS key_type, FARM_FINGERPRINT(s.household_id_value) AS hh_key, s.hh_score
        FROM temp_stacked s
        INNER JOIN temp_campaign_flag cf
            ON s.campaign_id = cf.campaign_id
            AND cf.uses_mntn_id
        WHERE
            s.household_id_source = 'mntn_id'
            AND s.household_id_value IS NOT NULL
    )
    , temp_aggregate AS (
        SELECT campaign_id, key_type, hh_key, MAX(hh_score) AS hh_score
        FROM temp_keys
        GROUP BY 1, 2, 3
    )
    SELECT
        campaign_id
        , key_type
        , hh_score
        , COUNT(*) AS population
        , CAST('2026-09-03T00:00:00+00:00' AS TIMESTAMP) AS run_time
    FROM temp_aggregate
    GROUP BY 1, 2, 3
