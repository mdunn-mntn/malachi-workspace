-- Shared population scan for HHST v3 + v4 (PACE-6989).
--
-- One pass over 24h of both bid logs, deduped to MAX score per (campaign, key), rolled
-- up to a score histogram. Both DAGs' campaign_bucket_population steps then derive their
-- bucket populations from this table with a cheap range join instead of each re-scanning
-- the raw logs. Output-equivalence was verified against prod outputs on 2026-08-19
-- (zero row diffs across all shared campaigns, v3 and v4).
--
-- Runs as a BigQuery script so the household-id columns are only referenced (and their
-- ~15% extra scan billed, see #523) once an advertiser is actually flagged for MNTN ID
-- (PACE-6846). While uses_mntn_id is FALSE everywhere, the ELSE branch never touches them.
--
-- The failure-reason IN-lists (formerly THRESHOLD_FAILURE_REASONS in the pipelines) now
-- live only here, duplicated across the two branches below - if the bidder adds new
-- drop-reason strings, update both.
--
-- The scan window is data_interval_end minus 24h. Never template data_interval_start
-- here: Airflow 3 treats cron-string schedules as trigger-only, so data_interval_start
-- equals data_interval_end - that rendered a zero-width window and published an empty
-- histogram on 2026-08-21. end - 24h matches HHSTWindow in the pipelines, which always
-- derived the window that way.

DECLARE any_uses_mntn_id BOOL DEFAULT (
    SELECT COALESCE(LOGICAL_OR(uses_mntn_id), FALSE)
    FROM `dw-main-bronze.external.camperbid_{{ params.env }}__hhst_v4__campaign_bucket`
);

IF any_uses_mntn_id THEN

    -- Flag-on shape: two keyspaces. 'ip' rows serve v3 (which counts IPs for everyone)
    -- and v4's unflagged campaigns; 'mntn' rows serve v4's flagged campaigns and are
    -- source-gated so a premature flag shows up as zero population (caught by the
    -- nonblocking zero-population test) instead of silently counting raw device IPs.
    CREATE OR REPLACE TABLE `dw-main-bronze.external.camperbid_{{ params.env }}__hhst_shared__population_histogram`
    OPTIONS (description = 'Shared HHST population histogram: MAX intent score per (campaign, key), counted per campaign/key_type/score. Written nightly by intent_score_threshold_v4:population_histogram; read by v3+v4 campaign_bucket_population.')
    AS
    WITH temp_campaigns AS (
        SELECT DISTINCT campaign_id
        FROM `dw-main-bronze.external.camperbid_{{ params.env }}__hhst_v4__campaign_bucket`
        UNION DISTINCT
        SELECT DISTINCT campaign_id
        FROM `dw-main-bronze.external.camperbid_{{ params.env }}__hhst_v3__campaign_bucket`
    )
    , temp_campaign_flag AS (
        SELECT DISTINCT campaign_id, uses_mntn_id
        FROM `dw-main-bronze.external.camperbid_{{ params.env }}__hhst_v4__campaign_bucket`
    )
    , temp_beeswax_bid_events AS (
        SELECT
            campaign_id
            , ip
            , conquest_score
            , conquest_score_ttl
            , time
            , household_score
            , advertiser_household_score
            , CAST(NULL AS STRING) AS household_id_source
            , CAST(NULL AS STRING) AS household_id_value
        FROM `dw-main-bronze.raw.bid_price_log`
        WHERE
            time >= TIMESTAMP_SUB(CAST('{{ data_interval_end }}' AS TIMESTAMP), INTERVAL 24 HOUR)
            AND time < CAST('{{ data_interval_end }}' AS TIMESTAMP)
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
            _PARTITIONTIME >= TIMESTAMP_SUB(CAST('{{ data_interval_end }}' AS TIMESTAMP), INTERVAL 24 HOUR)
            AND _PARTITIONTIME < CAST('{{ data_interval_end }}' AS TIMESTAMP)
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
    , temp_stacked AS (
        SELECT campaign_id, ip, conquest_score, conquest_score_ttl, time,
            household_score, advertiser_household_score, household_id_source, household_id_value
        FROM temp_beeswax_bid_events
        UNION ALL
        SELECT campaign_id, ip, conquest_score, conquest_score_ttl, time,
            household_score, advertiser_household_score, household_id_source, household_id_value
        FROM temp_mntn_bid_events
    )
    , temp_scored AS (
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
        FROM temp_stacked
    )
    , temp_keys AS (
        SELECT campaign_id, 'ip' AS key_type, ip AS hh_key, hh_score
        FROM temp_scored
        WHERE ip IS NOT NULL
        UNION ALL
        SELECT s.campaign_id, 'mntn' AS key_type, s.household_id_value AS hh_key, s.hh_score
        FROM temp_scored s
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
        , CAST('{{ data_interval_end }}' AS TIMESTAMP) AS run_time
    FROM temp_aggregate
    GROUP BY 1, 2, 3;

ELSE

    -- Flag-off shape (today's reality): identical to the flag-on 'ip' keyspace, without
    -- ever referencing the household-id columns.
    CREATE OR REPLACE TABLE `dw-main-bronze.external.camperbid_{{ params.env }}__hhst_shared__population_histogram`
    OPTIONS (description = 'Shared HHST population histogram: MAX intent score per (campaign, key), counted per campaign/key_type/score. Written nightly by intent_score_threshold_v4:population_histogram; read by v3+v4 campaign_bucket_population.')
    AS
    WITH temp_campaigns AS (
        SELECT DISTINCT campaign_id
        FROM `dw-main-bronze.external.camperbid_{{ params.env }}__hhst_v4__campaign_bucket`
        UNION DISTINCT
        SELECT DISTINCT campaign_id
        FROM `dw-main-bronze.external.camperbid_{{ params.env }}__hhst_v3__campaign_bucket`
    )
    , temp_beeswax_bid_events AS (
        SELECT
            campaign_id
            , ip
            , conquest_score
            , conquest_score_ttl
            , time
            , household_score
            , advertiser_household_score
        FROM `dw-main-bronze.raw.bid_price_log`
        WHERE
            time >= TIMESTAMP_SUB(CAST('{{ data_interval_end }}' AS TIMESTAMP), INTERVAL 24 HOUR)
            AND time < CAST('{{ data_interval_end }}' AS TIMESTAMP)
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
        FROM `dw-main-bronze.raw.bidder_bid_events`
        WHERE
            _PARTITIONTIME >= TIMESTAMP_SUB(CAST('{{ data_interval_end }}' AS TIMESTAMP), INTERVAL 24 HOUR)
            AND _PARTITIONTIME < CAST('{{ data_interval_end }}' AS TIMESTAMP)
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
    , temp_stacked AS (
        SELECT campaign_id, ip, conquest_score, conquest_score_ttl, time,
            household_score, advertiser_household_score
        FROM temp_beeswax_bid_events
        UNION ALL
        SELECT campaign_id, ip, conquest_score, conquest_score_ttl, time,
            household_score, advertiser_household_score
        FROM temp_mntn_bid_events
    )
    , temp_scored AS (
        SELECT
            campaign_id
            , ip
            , COALESCE(
                CASE
                    WHEN conquest_score > 1 AND TIMESTAMP_SECONDS(conquest_score_ttl) > time THEN GREATEST(conquest_score, 0)
                    WHEN household_score > 1 THEN GREATEST(household_score, 0)
                    ELSE GREATEST(advertiser_household_score, 0)
                END,
                0
            ) AS hh_score
        FROM temp_stacked
    )
    , temp_aggregate AS (
        SELECT campaign_id, 'ip' AS key_type, ip AS hh_key, MAX(hh_score) AS hh_score
        FROM temp_scored
        WHERE ip IS NOT NULL
        GROUP BY 1, 2, 3
    )
    SELECT
        campaign_id
        , key_type
        , hh_score
        , COUNT(*) AS population
        , CAST('{{ data_interval_end }}' AS TIMESTAMP) AS run_time
    FROM temp_aggregate
    GROUP BY 1, 2, 3;

END IF;

-- Guard: an empty/truncated histogram must fail this task loudly (prod then keeps
-- yesterday's thresholds, the designed failure mode) instead of flowing all-zero
-- populations through the row-count-only blocking tests. Normal size is ~11M rows.
ASSERT (
    (SELECT COUNT(*) FROM `dw-main-bronze.external.camperbid_{{ params.env }}__hhst_shared__population_histogram`) > 100000
) AS 'population histogram is suspiciously small - scan-window or upstream bid-log problem';
