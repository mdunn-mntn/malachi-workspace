/* ########################################################################
   RTC PERFORMANCE ANALYSIS SUITE

   This file contains three comprehensive queries for analyzing the impact
   of Real-Time Conquesting (RTC) features on advertising performance.

   Queries included:
   1. Pre/Post Period Comparison Analysis (Vertical Level)
   2. Daily Performance Trend Analysis
   3. Campaign-Level Performance Comparison

   All queries use consistent filtering logic:
   - RTC-enabled campaigns only
   - Minimum 7 days of data in both pre and post periods
   - Attribution-aware metric calculations
   - Dynamic period calculations based on time since release
   ######################################################################## */


/* ========================================================================
   QUERY 1: PRE/POST PERIOD COMPARISON ANALYSIS

   Purpose: Compare advertising performance metrics before and after RTC
   feature release dates for different verticals

   Key Features:
   - Identifies RTC-enabled advertisers across specified verticals
   - Calculates performance metrics for equal periods before/after release
   - Filters to campaign groups with minimum 7 days in each period
   - Computes percentage changes for all key performance indicators
   ======================================================================== */

/* ---------- STEP 1: Identify RTC-enabled advertisers ---------- */
-- Build a list of advertisers using RTC features based on audience segment expressions

DROP TABLE IF EXISTS rtc_enabled_aids;
CREATE TEMP TABLE rtc_enabled_aids AS
SELECT av.advertiser_id,
       av.vertical_id,
       av.vertical_name
FROM audience.audience_segments       a
JOIN audience.audiences               aa  ON a.audience_id        = aa.audience_id
JOIN fpa.advertiser_verticals         av  ON aa.advertiser_id     = av.advertiser_id
                                           AND av.type            = 1
JOIN public.campaigns                 cc  ON a.campaign_id        = cc.campaign_id
JOIN dso.valid_campaign_groups        vcc ON cc.campaign_group_id = vcc.campaign_group_id
WHERE a.expression LIKE '%"data_source_id":19,%'
  AND a.expression_type_id = 2  -- TPA expression type (1=OPM, 2=TPA)
  AND av.vertical_id       IN (101001, 120002, 119001)  -- Target verticals
GROUP BY av.advertiser_id, av.vertical_id, av.vertical_name
HAVING MAX(CASE WHEN a.expression LIKE '%"rtc"%' THEN 1 ELSE 0 END) = 1;  -- Must have RTC in expression


/* ---------- STEP 2: Create advertiser list with release dates ---------- */
-- Assign feature release dates based on vertical ID

DROP TABLE IF EXISTS aids_list;
CREATE TEMP TABLE aids_list AS
SELECT
    advertiser_id,
    vertical_id,
    vertical_name,
    CASE
        WHEN vertical_id = 119001 THEN DATE '2025-07-15'      -- Vertical 119001 release
        WHEN vertical_id IN (101001, 120002) THEN DATE '2025-07-22'  -- Other verticals release
    END AS release_date
FROM rtc_enabled_aids;


/* ---------- STEP 3: Identify qualifying campaign groups ---------- */
-- Find campaigns with sufficient data (7+ days) before and after release

DROP TABLE IF EXISTS temp_cgids_serving;
CREATE TEMP TABLE temp_cgids_serving AS
SELECT
    a.advertiser_id,
    a.company_name,
    aids.vertical_id,
    aids.vertical_name,
    cgid.campaign_group_id,
    cgid.objective_id,
    aids.release_date,
    -- Calculate analysis period (rounded to 7-day increments)
    CASE
        WHEN ((current_date - 1) - aids.release_date) < 7 THEN 7
        ELSE FLOOR(((current_date - 1) - aids.release_date) / 7) * 7
    END as period_days,
    min(d.day) as min_day,
    max(d.day) as max_day,
    count(distinct case when d.day < aids.release_date then d.day end) as pre_days_count,
    count(distinct case when d.day > aids.release_date then d.day end) as post_days_count
FROM aids_list aids
JOIN summarydata.sum_by_campaign_group_by_day d
    ON aids.advertiser_id = d.advertiser_id
JOIN advertisers a
    ON a.advertiser_id = d.advertiser_id
JOIN campaign_groups_raw cgid
    ON d.campaign_group_id = cgid.campaign_group_id
    AND cgid.objective_id = 1  -- Funnel 1 campaigns only
WHERE
    -- Define analysis window based on calculated period
    d.day >= aids.release_date - CAST(
        CASE
            WHEN ((current_date - 1) - aids.release_date) < 7 THEN 7
            ELSE FLOOR(((current_date - 1) - aids.release_date) / 7) * 7
        END AS INTEGER
    ) * INTERVAL '1' DAY
    AND d.day <= aids.release_date + CAST(
        CASE
            WHEN ((current_date - 1) - aids.release_date) < 7 THEN 7
            ELSE FLOOR(((current_date - 1) - aids.release_date) / 7) * 7
        END AS INTEGER
    ) * INTERVAL '1' DAY
    AND d.day <> aids.release_date  -- Exclude release date from analysis
    AND d.impressions > 0  -- Active campaigns only
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
-- Ensure minimum 7 days of data in both periods
HAVING count(distinct case when d.day < aids.release_date then d.day end) >= 7
   AND count(distinct case when d.day > aids.release_date then d.day end) >= 7;


/* ---------- FINAL QUERY: Calculate performance metrics by vertical ---------- */
-- Aggregate metrics and calculate percentage changes between periods

SELECT
    vertical_id, vertical_name
    , period_days
    -- Percentage changes for all metrics
    , (spend_post - spend_pre) * 1.0 / nullif(spend_pre,0) * 1.0 as spend_pct_change
    , (order_value_post - order_value_pre) * 1.0 / nullif(order_value_pre,0) * 1.0 as order_value_pct_change
    , (conversions_post - conversions_pre) * 1.0 / nullif(conversions_pre,0) * 1.0 as conversions_pct_change
    , (impressions_post - impressions_pre) * 1.0 / nullif(impressions_pre,0) * 1.0 as impressions_pct_change
    , (vv_post - vv_pre) * 1.0 / nullif(vv_pre,0) * 1.0 as vv_pct_change
    , (aov_post - aov_pre) * 1.0 / nullif(aov_pre,0) * 1.0 as aov_pct_change
    , (roas_post - roas_pre) * 1.0 / nullif(roas_pre,0) * 1.0 as roas_pct_change
    , (cpv_post - cpv_pre) * 1.0 / nullif(cpv_pre,0) * 1.0 as cpv_pct_change
    , (cpa_post - cpa_pre) * 1.0 / nullif(cpa_pre,0) * 1.0 as cpa_pct_change
    , (cvr_post - cvr_pre) * 1.0 / nullif(cvr_pre,0) * 1.0 as cvr_pct_change
    , (vvr_post - vvr_pre) * 1.0 / nullif(vvr_pre,0) * 1.0 as vvr_pct_change
    , (ivr_post - ivr_pre) * 1.0 / nullif(ivr_pre,0) * 1.0 as ivr_pct_change
    -- Absolute values for reference
    , spend_pre
    , spend_post
    , order_value_pre
    , order_value_post
    , conversions_pre
    , conversions_post
    , impressions_pre
    , impressions_post
    , vv_pre
    , vv_post
    , aov_pre
    , aov_post
    , roas_pre
    , roas_post
    , cpv_pre
    , cpv_post
    , cpa_pre
    , cpa_post
    , cvr_pre
    , cvr_post
    , vvr_pre
    , vvr_post
    , ivr_pre
    , ivr_post
FROM (
    -- Pivot data to get pre/post metrics in columns
    SELECT
        vertical_id, vertical_name, period_days

        , max(spend) filter(where period = 'pre') as spend_pre
        , max(spend) filter(where period = 'post') as spend_post

        , max(order_value) filter(where period = 'pre') as order_value_pre
        , max(order_value) filter(where period = 'post') as order_value_post

        , max(conversions) filter(where period = 'pre') as conversions_pre
        , max(conversions) filter(where period = 'post') as conversions_post

        , max(impressions) filter(where period = 'pre') as impressions_pre
        , max(impressions) filter(where period = 'post') as impressions_post

        , max(vv) filter(where period = 'pre') as vv_pre
        , max(vv) filter(where period = 'post') as vv_post

        , max(aov) filter(where period = 'pre') as aov_pre
        , max(aov) filter(where period = 'post') as aov_post

        , max(roas) filter(where period = 'pre') as roas_pre
        , max(roas) filter(where period = 'post') as roas_post

        , max(cpv) filter(where period = 'pre') as cpv_pre
        , max(cpv) filter(where period = 'post') as cpv_post

        , max(cpa) filter(where period = 'pre') as cpa_pre
        , max(cpa) filter(where period = 'post') as cpa_post

        , max(cvr) filter(where period = 'pre') as cvr_pre
        , max(cvr) filter(where period = 'post') as cvr_post

        , max(vvr) filter(where period = 'pre') as vvr_pre
        , max(vvr) filter(where period = 'post') as vvr_post

        , max(ivr) filter(where period = 'pre') as ivr_pre
        , max(ivr) filter(where period = 'post') as ivr_post

    FROM (
        -- Calculate metrics for each period
        SELECT
              cgid.vertical_id
            , cgid.vertical_name
            , cgid.period_days
            , case
                when d.day < cgid.release_date then 'pre'
                when d.day > cgid.release_date then 'post'
            end as period
            , sum(d.impressions) as impressions
            , sum(d.media_spend + data_spend + platform_spend) as spend

            -- Conversion metrics (attribution-aware)
            , sum(
                case
                    when lt.advertiser_id is null then d.click_conversions + d.view_conversions + coalesce(d.competing_view_conversions,0)
                    else d.click_conversions + d.view_conversions
                end
            ) as conversions

            -- Revenue metrics (attribution-aware)
            , sum(
                case
                    when lt.advertiser_id IS NULL THEN d.click_order_value + d.view_order_value + coalesce(d.competing_view_order_value, 0)
                    else d.click_order_value + d.view_order_value
                end
            ) as order_value

            -- Cost Per Acquisition
            , sum(d.media_spend + d.data_spend + d.platform_spend) * 1.0 /
                nullif(
                    sum(
                        case
                            when lt.advertiser_id is null then d.click_conversions + d.view_conversions + coalesce(d.competing_view_conversions,0)
                            else d.click_conversions + d.view_conversions
                        end
                    )
                ,0) * 1.0
            as cpa

            -- Cost Per Visit
            , sum(d.media_spend + d.data_spend + d.platform_spend) * 1.0 /
                nullif(
                    sum(
                        case
                            when lt.advertiser_id is null then d.clicks + d.views + coalesce(d.competing_views,0)
                            else d.clicks + d.views
                        end
                    )
                ,0) * 1.0
            as cpv

            -- Return on Ad Spend
            , sum(
                case
                    when lt.advertiser_id IS NULL THEN d.click_order_value + d.view_order_value + coalesce(d.competing_view_order_value, 0)
                    else d.click_order_value + d.view_order_value
                end
            ) * 1.0 / nullif(sum(d.media_spend + d.data_spend + d.platform_spend),0) * 1.0
            as roas

            -- Impression to Visit Rate
            ,   sum(
                        case
                            when lt.advertiser_id is null then d.clicks + d.views + coalesce(d.competing_views,0)
                            else d.clicks + d.views
                        end
                    ) * 1.0 /

            nullif( sum(
              d.impressions
            )
            ,0) * 1.0

            as ivr

            -- Conversion Rate
            , sum(
                case
                    when lt.advertiser_id is null then d.click_conversions + d.view_conversions + coalesce(d.competing_view_conversions,0)
                    else d.click_conversions + d.view_conversions
                end
            ) * 1.0 /
                nullif(
                    sum(
                        case
                            when lt.advertiser_id is null then d.clicks + d.views + coalesce(d.competing_views,0)
                            else d.clicks + d.views
                        end
                    )
                ,0) * 1.0
            as cvr

            -- Verified Visit
            , sum(
                case
                    when lt.advertiser_id is null then d.clicks + d.views + coalesce(d.competing_views,0)
                    else d.clicks + d.views
                end
            ) as vv

            -- Average Order Value
            , sum(
                case
                    when lt.advertiser_id IS NULL THEN d.click_order_value + d.view_order_value + coalesce(d.competing_view_order_value, 0)
                    else d.click_order_value + d.view_order_value
                end
            ) * 1.0 / nullif(sum(
                case
                    when lt.advertiser_id IS NULL THEN d.click_conversions + d.view_conversions + coalesce(d.competing_view_conversions, 0)
                    else d.click_conversions + d.view_conversions
                end
            ),0) * 1.0
            as aov

            -- Verified Visit Rate (per unique user)
            , sum(
                case
                    when lt.advertiser_id is null then d.clicks + d.views + coalesce(d.competing_views,0)
                    else d.clicks + d.views
                end
            ) * 1.0 / sum(uniques)::int * 1.0
            as vvr

        FROM summarydata.sum_by_campaign_group_by_day d
        JOIN temp_cgids_serving cgid
            ON cgid.campaign_group_id = d.campaign_group_id
        LEFT JOIN (SELECT DISTINCT advertiser_id FROM r2.advertiser_settings WHERE reporting_style = 'last_touch' GROUP BY 1) lt
            ON lt.advertiser_id = cgid.advertiser_id
        WHERE 1 = 1
            AND d.day >= cgid.release_date - CAST(cgid.period_days AS INTEGER) * INTERVAL '1' DAY
            AND d.day <= cgid.release_date + CAST(cgid.period_days AS INTEGER) * INTERVAL '1' DAY
            AND d.day <> cgid.release_date -- Exclude release date
        GROUP BY 1,2,3,4
    ) a
    GROUP BY 1, 2, 3
) b
ORDER BY vertical_id;


/* ========================================================================
   QUERY 2: DAILY PERFORMANCE TREND ANALYSIS

   Purpose: Track day-by-day advertising performance for RTC-enabled
   campaigns to identify trends and patterns over time

   Key Features:
   - Daily granularity for trend analysis
   - Same RTC detection and filtering logic as Query 1
   - Attribution-aware metric calculations
   - Includes day-of-week for pattern recognition
   ======================================================================== */

-- Clean up any existing temp table
DROP TABLE IF EXISTS temp_daily_performance_analysis;

-- Build daily performance analysis table
CREATE TEMP TABLE temp_daily_performance_analysis AS
WITH rtc_enabled_campaigns AS (
    /* ---------- Identify RTC-enabled campaign groups ---------- */
    -- Extract campaign groups using RTC features with their release dates
    SELECT DISTINCT
        cc.campaign_group_id,
        av.advertiser_id,
        av.vertical_id,
        av.vertical_name,
        CASE
            WHEN av.vertical_id = 119001 THEN DATE '2025-07-15'
            WHEN av.vertical_id IN (101001, 120002) THEN DATE '2025-07-22'
        END AS release_date
    FROM audience.audience_segments a
    JOIN audience.audiences aa
        ON a.audience_id = aa.audience_id
    JOIN fpa.advertiser_verticals av
        ON aa.advertiser_id = av.advertiser_id
        AND av.type = 1  -- Primary vertical
    JOIN public.campaigns cc
        ON a.campaign_id = cc.campaign_id
    JOIN dso.valid_campaign_groups vcc
        ON cc.campaign_group_id = vcc.campaign_group_id
    WHERE a.expression LIKE '%"data_source_id":19,%'
        AND a.expression_type_id = 2  -- TPA expressions
        AND av.vertical_id IN (101001, 120002, 119001)
    GROUP BY cc.campaign_group_id, av.advertiser_id, av.vertical_id, av.vertical_name
    HAVING MAX(CASE WHEN a.expression LIKE '%"rtc"%' THEN 1 ELSE 0 END) = 1
),

campaigns_with_sufficient_data AS (
    /* ---------- Filter for campaigns with adequate historical data ---------- */
    -- Ensure at least 7 days of data before and after release
    SELECT
        rtc.campaign_group_id,
        rtc.advertiser_id,
        rtc.vertical_id,
        rtc.vertical_name,
        rtc.release_date,
        cgid.objective_id,
        -- Dynamic period calculation (7-day increments)
        CASE
            WHEN ((current_date - 1) - rtc.release_date) < 7 THEN ((current_date - 1) - rtc.release_date)
            ELSE FLOOR(((current_date - 1) - rtc.release_date) / 7) * 7
        END as period_days
    FROM rtc_enabled_campaigns rtc
    JOIN summarydata.sum_by_campaign_group_by_day d
        ON rtc.campaign_group_id = d.campaign_group_id
    JOIN campaign_groups_raw cgid
        ON d.campaign_group_id = cgid.campaign_group_id
        AND cgid.objective_id = 1
    WHERE
        -- Analysis window based on dynamic period
        d.day >= rtc.release_date - CAST(
            CASE
                WHEN ((current_date - 1) - rtc.release_date) < 7 THEN ((current_date - 1) - rtc.release_date)
                ELSE FLOOR(((current_date - 1) - rtc.release_date) / 7) * 7
            END AS INTEGER
        ) * INTERVAL '1' DAY
        AND d.day <= rtc.release_date + CAST(
            CASE
                WHEN ((current_date - 1) - rtc.release_date) < 7 THEN ((current_date - 1) - rtc.release_date)
                ELSE FLOOR(((current_date - 1) - rtc.release_date) / 7) * 7
            END AS INTEGER
        ) * INTERVAL '1' DAY
        AND d.day <> rtc.release_date  -- Exclude transition day
        AND d.impressions > 0
    GROUP BY 1, 2, 3, 4, 5, 6, 7
    -- Minimum 7 days requirement
    HAVING COUNT(DISTINCT CASE WHEN d.day < rtc.release_date THEN d.day END) >= 7
       AND COUNT(DISTINCT CASE WHEN d.day > rtc.release_date THEN d.day END) >= 7
),

last_touch_advertisers AS (
    /* ---------- Identify last-touch attribution advertisers ---------- */
    -- Used for proper metric calculation based on attribution model
    SELECT DISTINCT advertiser_id
    FROM r2.advertiser_settings
    WHERE reporting_style = 'last_touch'
),

daily_metrics AS (
    /* ---------- Aggregate daily performance metrics ---------- */
    -- Calculate all metrics at daily granularity by vertical
    SELECT
        csd.vertical_id,
        csd.vertical_name,
        d.day,
        CASE
            WHEN d.day < csd.release_date THEN 'Pre'
            WHEN d.day > csd.release_date THEN 'Post'
        END as period,

        -- Activity indicators
        COUNT(DISTINCT d.campaign_group_id) as active_campaigns,
        COUNT(DISTINCT csd.advertiser_id) as active_advertisers,

        -- Core volume metrics
        SUM(d.impressions) as impressions,
        SUM(d.media_spend + d.data_spend + d.platform_spend) as spend,

        -- Conversion metrics (attribution-aware)
        SUM(
            CASE
                WHEN lt.advertiser_id IS NULL THEN
                    d.click_conversions + d.view_conversions + COALESCE(d.competing_view_conversions, 0)
                ELSE
                    d.click_conversions + d.view_conversions
            END
        ) as conversions,

        -- Revenue metrics (attribution-aware)
        SUM(
            CASE
                WHEN lt.advertiser_id IS NULL THEN
                    d.click_order_value + d.view_order_value + COALESCE(d.competing_view_order_value, 0)
                ELSE
                    d.click_order_value + d.view_order_value
            END
        ) as order_value,

        -- Verified Visit metrics (attribution-aware)
        SUM(
            CASE
                WHEN lt.advertiser_id IS NULL THEN
                    d.clicks + d.views + COALESCE(d.competing_views, 0)
                ELSE
                    d.clicks + d.views
            END
        ) as vv,

        -- Unique users for rate calculations
        SUM(d.uniques) as uniques

    FROM summarydata.sum_by_campaign_group_by_day d
    JOIN campaigns_with_sufficient_data csd
        ON d.campaign_group_id = csd.campaign_group_id
    LEFT JOIN last_touch_advertisers lt
        ON csd.advertiser_id = lt.advertiser_id
    WHERE
        d.day >= csd.release_date - CAST(csd.period_days AS INTEGER) * INTERVAL '1' DAY
        AND d.day <= csd.release_date + CAST(csd.period_days AS INTEGER) * INTERVAL '1' DAY
        AND d.day <> csd.release_date
        AND d.impressions > 0  -- Active days only
    GROUP BY 1, 2, 3, 4
)

/* ---------- Final output with calculated efficiency metrics ---------- */
-- Present daily data with all performance indicators
SELECT
    vertical_id,
    vertical_name,
    day,
    period,
    TO_CHAR(day, 'Day') as day_of_week,  -- For weekly pattern analysis

    -- Campaign activity
    active_campaigns,
    active_advertisers,

    -- Raw daily metrics
    spend as daily_spend,
    impressions as daily_impressions,
    conversions as daily_conversions,
    order_value as daily_revenue,
    vv as daily_vv,
    uniques as daily_uniques,

    -- Efficiency metrics
    -- Cost Per Acquisition
    CASE
        WHEN conversions > 0 THEN (spend * 1.0 / conversions)::NUMERIC(12,2)
        ELSE 0::NUMERIC(12,2)
    END as cpa,

    -- Cost Per Visit
    CASE
        WHEN vv > 0 THEN (spend * 1.0 / vv)::NUMERIC(12,4)
        ELSE 0::NUMERIC(12,4)
    END as cpv,

    -- Return on Ad Spend
    CASE
        WHEN spend > 0 THEN (order_value * 1.0 / spend)::NUMERIC(12,3)
        ELSE 0::NUMERIC(12,3)
    END as roas,

    -- Average Order Value
    CASE
        WHEN conversions > 0 THEN (order_value * 1.0 / conversions)::NUMERIC(12,2)
        ELSE 0::NUMERIC(12,2)
    END as aov,

    -- Conversion Rate (conversions per visit)
    CASE
        WHEN vv > 0 THEN (conversions * 1.0 / vv)::NUMERIC(12,4)
        ELSE 0::NUMERIC(12,4)
    END as cvr,

    -- Verified Visit Rate
    CASE
        WHEN uniques > 0 THEN (vv * 1.0 / uniques)::NUMERIC(12,4)
        ELSE 0::NUMERIC(12,4)
    END as vvr,

    -- Impression to Visit Rate
    CASE
        WHEN impressions > 0 THEN (vv * 1.0 / impressions)::NUMERIC(12,4)
        ELSE 0::NUMERIC(12,4)
    END as ivr

FROM daily_metrics
ORDER BY vertical_name, day;

-- Output the complete daily analysis
SELECT * FROM temp_daily_performance_analysis
ORDER BY vertical_name, day;


/* ========================================================================
   QUERY 3: CAMPAIGN-LEVEL PERFORMANCE COMPARISON

   Purpose: Analyze individual campaign performance to identify specific
   winners and losers after RTC feature implementation

   Key Features:
   - Campaign-level granularity with advertiser context
   - Side-by-side pre/post period comparison
   - Percentage change calculations for all KPIs
   - Sorted by ROAS improvement to highlight best performers
   ======================================================================== */

-- Clean up any existing analysis table
DROP TABLE IF EXISTS temp_campaign_level_performance;

-- Build campaign-level performance comparison table
CREATE TEMP TABLE temp_campaign_level_performance AS
WITH rtc_enabled_campaigns AS (
    /* ---------- Extract RTC-enabled campaigns ---------- */
    SELECT DISTINCT
        cc.campaign_group_id,
        av.advertiser_id,
        av.vertical_id,
        av.vertical_name,
        CASE
            WHEN av.vertical_id = 119001 THEN DATE '2025-07-15'
            WHEN av.vertical_id IN (101001, 120002) THEN DATE '2025-07-22'
        END AS release_date
    FROM audience.audience_segments a
    JOIN audience.audiences aa
        ON a.audience_id = aa.audience_id
    JOIN fpa.advertiser_verticals av
        ON aa.advertiser_id = av.advertiser_id
        AND av.type = 1  -- Vertical
    JOIN public.campaigns cc
        ON a.campaign_id = cc.campaign_id
    JOIN dso.valid_campaign_groups vcc
        ON cc.campaign_group_id = vcc.campaign_group_id
    WHERE a.expression LIKE '%"data_source_id":19,%'
        AND a.expression_type_id = 2  -- TPA expression type
        AND av.vertical_id IN (101001, 120002, 119001)
    GROUP BY cc.campaign_group_id, av.advertiser_id, av.vertical_id, av.vertical_name
    HAVING MAX(CASE WHEN a.expression LIKE '%"rtc"%' THEN 1 ELSE 0 END) = 1
),

campaigns_with_sufficient_data AS (
    /* ---------- Apply data sufficiency filters ---------- */
    -- Ensure campaigns have adequate pre/post data for fair comparison
    SELECT
        rtc.campaign_group_id,
        rtc.advertiser_id,
        rtc.vertical_id,
        rtc.vertical_name,
        rtc.release_date,
        cgid.objective_id,
        -- Dynamic period calculation
        CASE
            WHEN ((current_date - 1) - rtc.release_date) < 7 THEN ((current_date - 1) - rtc.release_date)
            ELSE FLOOR(((current_date - 1) - rtc.release_date) / 7) * 7
        END as period_days,
        COUNT(DISTINCT CASE WHEN d.day < rtc.release_date THEN d.day END) as pre_days_count,
        COUNT(DISTINCT CASE WHEN d.day > rtc.release_date THEN d.day END) as post_days_count
    FROM rtc_enabled_campaigns rtc
    JOIN summarydata.sum_by_campaign_group_by_day d
        ON rtc.campaign_group_id = d.campaign_group_id
    JOIN campaign_groups_raw cgid
        ON d.campaign_group_id = cgid.campaign_group_id
        AND cgid.objective_id = 1  -- Conversion campaigns only
    WHERE
        -- Define analysis window
        d.day >= rtc.release_date - CAST(
            CASE
                WHEN ((current_date - 1) - rtc.release_date) < 7 THEN ((current_date - 1) - rtc.release_date)
                ELSE FLOOR(((current_date - 1) - rtc.release_date) / 7) * 7
            END AS INTEGER
        ) * INTERVAL '1' DAY
        AND d.day <= rtc.release_date + CAST(
            CASE
                WHEN ((current_date - 1) - rtc.release_date) < 7 THEN ((current_date - 1) - rtc.release_date)
                ELSE FLOOR(((current_date - 1) - rtc.release_date) / 7) * 7
            END AS INTEGER
        ) * INTERVAL '1' DAY
        AND d.day <> rtc.release_date
        AND d.impressions > 0
    GROUP BY 1, 2, 3, 4, 5, 6, 7
    -- Enforce minimum 7-day requirement
    HAVING COUNT(DISTINCT CASE WHEN d.day < rtc.release_date THEN d.day END) >= 7
       AND COUNT(DISTINCT CASE WHEN d.day > rtc.release_date THEN d.day END) >= 7
),

last_touch_advertisers AS (
    /* ---------- Get attribution model settings ---------- */
    SELECT DISTINCT advertiser_id
    FROM r2.advertiser_settings
    WHERE reporting_style = 'last_touch'
),

campaign_daily_metrics AS (
    /* ---------- Aggregate daily metrics by campaign ---------- */
    -- Collect daily performance data for each campaign
    SELECT
        csd.vertical_id,
        csd.vertical_name,
        csd.advertiser_id,
        d.campaign_group_id,
        cg.name as campaign_name,     -- Campaign identifier
        a.company_name,
        csd.release_date,
        csd.period_days,
        d.day,
        CASE
            WHEN d.day < csd.release_date THEN 'Pre'
            WHEN d.day > csd.release_date THEN 'Post'
        END as period,

        -- Core performance metrics
        SUM(d.impressions) as impressions,
        SUM(d.media_spend + d.data_spend + d.platform_spend) as spend,

        -- Attribution-aware conversion
        SUM(
            CASE
                WHEN lt.advertiser_id IS NULL THEN
                    d.click_conversions + d.view_conversions + COALESCE(d.competing_view_conversions, 0)
                ELSE
                    d.click_conversions + d.view_conversions
            END
        ) as conversions,

        -- Attribution-aware Revenue
        SUM(
            CASE
                WHEN lt.advertiser_id IS NULL THEN
                    d.click_order_value + d.view_order_value + COALESCE(d.competing_view_order_value, 0)
                ELSE
                    d.click_order_value + d.view_order_value
            END
        ) as order_value,

        -- Attribution-aware Verified Visit
        SUM(
            CASE
                WHEN lt.advertiser_id IS NULL THEN
                    d.clicks + d.views + COALESCE(d.competing_views, 0)
                ELSE
                    d.clicks + d.views
            END
        ) as vv,

        -- Unique users
        SUM(d.uniques) as uniques

    FROM summarydata.sum_by_campaign_group_by_day d
    JOIN campaigns_with_sufficient_data csd
        ON d.campaign_group_id = csd.campaign_group_id
    JOIN public.campaign_groups cg
        ON d.campaign_group_id = cg.campaign_group_id
    JOIN advertisers a
        ON csd.advertiser_id = a.advertiser_id
    LEFT JOIN last_touch_advertisers lt
        ON csd.advertiser_id = lt.advertiser_id
    WHERE
        d.day >= csd.release_date - CAST(csd.period_days AS INTEGER) * INTERVAL '1' DAY
        AND d.day <= csd.release_date + CAST(csd.period_days AS INTEGER) * INTERVAL '1' DAY
        AND d.day <> csd.release_date
        AND d.impressions > 0
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
),

campaign_period_aggregated AS (
    /* ---------- Calculate period-level metrics ---------- */
    -- Aggregate to pre/post periods with efficiency calculations
    SELECT
        vertical_id,
        vertical_name,
        advertiser_id,
        company_name,
        campaign_group_id,
        campaign_name,
        release_date,
        period_days,
        period,
        COUNT(DISTINCT day) as active_days,

        -- Aggregate volume metrics
        SUM(spend) as total_spend,
        SUM(impressions) as total_impressions,
        SUM(conversions) as total_conversions,
        SUM(order_value) as total_order_value,
        SUM(vv) as total_vv,
        SUM(uniques) as total_uniques,

        -- Calculate efficiency metrics
        -- Cost Per Acquisition
        CASE
            WHEN SUM(conversions) > 0 THEN SUM(spend) * 1.0 / SUM(conversions)
            ELSE 0
        END as cpa,

        -- Cost Per Visit
        CASE
            WHEN SUM(vv) > 0 THEN SUM(spend) * 1.0 / SUM(vv)
            ELSE 0
        END as cpv,

        -- Return on Ad Spend
        CASE
            WHEN SUM(spend) > 0 THEN SUM(order_value) * 1.0 / SUM(spend)
            ELSE 0
        END as roas,

        -- Average Order Value
        CASE
            WHEN SUM(conversions) > 0 THEN SUM(order_value) * 1.0 / SUM(conversions)
            ELSE 0
        END as aov,

        -- Conversion Rate
        CASE
            WHEN SUM(vv) > 0 THEN SUM(conversions) * 1.0 / SUM(vv)
            ELSE 0
        END as cvr,

        -- Verified Visit Rate
        CASE
            WHEN SUM(uniques) > 0 THEN SUM(vv) * 1.0 / SUM(uniques)
            ELSE 0
        END as vvr,

        -- Impression to Visit Rate
        CASE
            WHEN SUM(impressions) > 0 THEN SUM(vv) * 1.0 / SUM(impressions)
            ELSE 0
        END as ivr

    FROM campaign_daily_metrics
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
)

/* ---------- Final output: Side-by-side campaign comparison ---------- */
-- Join pre and post periods for comprehensive comparison
SELECT
    pre.vertical_name,
    pre.company_name,
    pre.campaign_group_id,
    pre.campaign_name,
    pre.release_date,
    pre.period_days,

    -- Pre-period performance snapshot
    pre.active_days as pre_active_days,
    pre.total_spend as pre_spend,
    pre.total_impressions as pre_impressions,
    pre.total_conversions as pre_conversions,
    pre.total_order_value as pre_order_value,
    pre.total_vv as pre_vv,
    pre.total_uniques as pre_uniques,
    pre.roas::NUMERIC(12,3) as pre_roas,
    pre.cpa::NUMERIC(12,2) as pre_cpa,
    pre.cpv::NUMERIC(12,4) as pre_cpv,
    pre.aov::NUMERIC(12,2) as pre_aov,
    pre.cvr::NUMERIC(12,4) as pre_cvr,
    pre.vvr::NUMERIC(12,4) as pre_vvr,
    pre.ivr::NUMERIC(12,4) as pre_ivr,

    -- Post-period performance snapshot
    post.active_days as post_active_days,
    post.total_spend as post_spend,
    post.total_impressions as post_impressions,
    post.total_conversions as post_conversions,
    post.total_order_value as post_order_value,
    post.total_vv as post_vv,
    post.total_uniques as post_uniques,
    post.roas::NUMERIC(12,3) as post_roas,
    post.cpa::NUMERIC(12,2) as post_cpa,
    post.cpv::NUMERIC(12,4) as post_cpv,
    post.aov::NUMERIC(12,2) as post_aov,
    post.cvr::NUMERIC(12,4) as post_cvr,
    post.vvr::NUMERIC(12,4) as post_vvr,
    post.ivr::NUMERIC(12,4) as post_ivr,

    -- Performance change calculations (as percentages)
    COALESCE(((post.total_spend - pre.total_spend) * 1.0 / NULLIF(pre.total_spend, 0) * 100), 0)::NUMERIC(12,1) as spend_change_pct,
    COALESCE(((post.total_impressions - pre.total_impressions) * 1.0 / NULLIF(pre.total_impressions, 0) * 100), 0)::NUMERIC(12,1) as impressions_change_pct,
    COALESCE(((post.total_conversions - pre.total_conversions) * 1.0 / NULLIF(pre.total_conversions, 0) * 100), 0)::NUMERIC(12,1) as conversions_change_pct,
    COALESCE(((post.total_order_value - pre.total_order_value) * 1.0 / NULLIF(pre.total_order_value, 0) * 100), 0)::NUMERIC(12,1) as order_value_change_pct,
    COALESCE(((post.total_vv - pre.total_vv) * 1.0 / NULLIF(pre.total_vv, 0) * 100), 0)::NUMERIC(12,1) as vv_change_pct,
    COALESCE(((post.roas - pre.roas) * 1.0 / NULLIF(pre.roas, 0) * 100), 0)::NUMERIC(12,1) as roas_change_pct,
    COALESCE(((post.cpa - pre.cpa) * 1.0 / NULLIF(pre.cpa, 0) * 100), 0)::NUMERIC(12,1) as cpa_change_pct,
    COALESCE(((post.cpv - pre.cpv) * 1.0 / NULLIF(pre.cpv, 0) * 100), 0)::NUMERIC(12,1) as cpv_change_pct,
    COALESCE(((post.aov - pre.aov) * 1.0 / NULLIF(pre.aov, 0) * 100), 0)::NUMERIC(12,1) as aov_change_pct,
    COALESCE(((post.cvr - pre.cvr) * 1.0 / NULLIF(pre.cvr, 0) * 100), 0)::NUMERIC(12,1) as cvr_change_pct,
    COALESCE(((post.vvr - pre.vvr) * 1.0 / NULLIF(pre.vvr, 0) * 100), 0)::NUMERIC(12,1) as vvr_change_pct,
    COALESCE(((post.ivr - pre.ivr) * 1.0 / NULLIF(pre.ivr, 0) * 100), 0)::NUMERIC(12,1) as ivr_change_pct

FROM campaign_period_aggregated pre
JOIN campaign_period_aggregated post
    ON pre.campaign_group_id = post.campaign_group_id
    AND pre.period = 'Pre'
    AND post.period = 'Post'
WHERE pre.total_spend > 0 OR post.total_spend > 0;  -- Include only active campaigns

-- Output results sorted by ROAS improvement
SELECT * FROM temp_campaign_level_performance
ORDER BY
    vertical_name,
    release_date,
    roas_change_pct DESC;  -- Best performers first