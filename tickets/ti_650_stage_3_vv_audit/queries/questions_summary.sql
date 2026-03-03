-- =============================================================
-- Q1: What is the relationship between ad_served_id and
--     first_touch_ad_served_id on clickpass_log?
-- =============================================================
-- How often are they the same vs different?

SELECT
  COUNT(*) AS total_vvs,
  COUNTIF(ad_served_id = first_touch_ad_served_id) AS same_id,
  COUNTIF(ad_served_id != first_touch_ad_served_id) AS different_id,
  COUNTIF(first_touch_ad_served_id IS NULL) AS ft_null,
  ROUND(COUNTIF(ad_served_id = first_touch_ad_served_id) * 100.0 / COUNT(*), 2) AS pct_same,
  ROUND(COUNTIF(ad_served_id != first_touch_ad_served_id) * 100.0 / COUNT(*), 2) AS pct_different
FROM `dw-main-silver.logdata.clickpass_log` cp
WHERE DATE(time) BETWEEN '2026-02-04' AND '2026-02-10'
  AND advertiser_id = 37775;

-- =============================================================
-- Q2: When they're different, which impression is more recent?
-- =============================================================
-- Join both ad_served_ids to event_log to get their VAST timestamps.
-- If ad_served_id always points to the most recent impression,
-- its VAST time should be closer to (but before) the clickpass time.

WITH el AS (
  SELECT ad_served_id, MIN(time) AS vast_time
  FROM `dw-main-silver.logdata.event_log`
  WHERE DATE(time) BETWEEN '2026-01-05' AND '2026-02-10'
    AND event_type_raw = 'vast_impression'
  GROUP BY ad_served_id
)
SELECT
  cp.ad_served_id,
  cp.first_touch_ad_served_id,
  cp.time AS vv_time,
  el_vv.vast_time AS vv_assoc_vast_time,
  el_ft.vast_time AS first_touch_vast_time,
  TIMESTAMP_DIFF(cp.time, el_vv.vast_time, HOUR) AS hours_vv_assoc_to_visit,
  TIMESTAMP_DIFF(cp.time, el_ft.vast_time, HOUR) AS hours_first_touch_to_visit,
  CASE
    WHEN el_vv.vast_time > el_ft.vast_time THEN 'ad_served_id is MORE RECENT'
    WHEN el_vv.vast_time < el_ft.vast_time THEN 'ad_served_id is OLDER (unexpected)'
    WHEN el_vv.vast_time = el_ft.vast_time THEN 'SAME TIME'
    ELSE 'NULL - could not resolve'
  END AS which_is_newer
FROM `dw-main-silver.logdata.clickpass_log` cp
LEFT JOIN el AS el_vv ON el_vv.ad_served_id = cp.ad_served_id
LEFT JOIN el AS el_ft ON el_ft.ad_served_id = cp.first_touch_ad_served_id
WHERE DATE(cp.time) BETWEEN '2026-02-04' AND '2026-02-10'
  AND cp.advertiser_id = 37775
  AND cp.ad_served_id != cp.first_touch_ad_served_id
LIMIT 100;

-- =============================================================
-- Q3: Aggregate view of Q2 — how consistent is the pattern?
-- =============================================================

WITH el AS (
  SELECT ad_served_id, MIN(time) AS vast_time
  FROM `dw-main-silver.logdata.event_log`
  WHERE DATE(time) BETWEEN '2026-01-05' AND '2026-02-10'
    AND event_type_raw = 'vast_impression'
  GROUP BY ad_served_id
)
SELECT
  CASE
    WHEN el_vv.vast_time > el_ft.vast_time THEN 'ad_served_id MORE RECENT'
    WHEN el_vv.vast_time < el_ft.vast_time THEN 'ad_served_id OLDER'
    WHEN el_vv.vast_time = el_ft.vast_time THEN 'SAME TIME'
    WHEN el_vv.vast_time IS NULL AND el_ft.vast_time IS NULL THEN 'BOTH NULL'
    WHEN el_vv.vast_time IS NULL THEN 'VV-assoc NULL only'
    WHEN el_ft.vast_time IS NULL THEN 'First-touch NULL only'
  END AS pattern,
  COUNT(*) AS cnt,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct
FROM `dw-main-silver.logdata.clickpass_log` cp
LEFT JOIN el AS el_vv ON el_vv.ad_served_id = cp.ad_served_id
LEFT JOIN el AS el_ft ON el_ft.ad_served_id = cp.first_touch_ad_served_id
WHERE DATE(cp.time) BETWEEN '2026-02-04' AND '2026-02-10'
  AND cp.advertiser_id = 37775
  AND cp.ad_served_id != cp.first_touch_ad_served_id
GROUP BY 1
ORDER BY cnt DESC;

-- =============================================================
-- Q4: When same, is it truly just 1 impression?
-- =============================================================
-- For VVs where ad_served_id = first_touch_ad_served_id,
-- how many total event_log rows exist for that IP + advertiser
-- in the 30 days before? This tells us if there were other
-- impressions that just weren't linked.

WITH same_id_vvs AS (
  SELECT cp.ad_served_id, cp.ip, cp.advertiser_id, cp.time AS vv_time
  FROM `dw-main-silver.logdata.clickpass_log` cp
  WHERE DATE(cp.time) BETWEEN '2026-02-04' AND '2026-02-10'
    AND cp.advertiser_id = 37775
    AND cp.ad_served_id = cp.first_touch_ad_served_id
  LIMIT 200
),
all_vast_for_ip AS (
  SELECT
    s.ad_served_id AS vv_ad_served_id,
    s.ip,
    el.ad_served_id AS el_ad_served_id,
    el.time AS vast_time,
    s.vv_time
  FROM same_id_vvs s
  JOIN `dw-main-silver.logdata.event_log` el
    ON el.ip = s.ip
    AND el.event_type_raw = 'vast_impression'
    AND el.time BETWEEN TIMESTAMP_SUB(s.vv_time, INTERVAL 30 DAY) AND s.vv_time
    AND el.advertiser_id = s.advertiser_id
)
SELECT
  vv_ad_served_id,
  ip,
  COUNT(DISTINCT el_ad_served_id) AS distinct_impressions_for_ip,
  MIN(vast_time) AS earliest_vast,
  MAX(vast_time) AS latest_vast,
  vv_time
FROM all_vast_for_ip
GROUP BY vv_ad_served_id, ip, vv_time
ORDER BY distinct_impressions_for_ip DESC
LIMIT 50;

-- =============================================================
-- Q5: When different, do the bid IPs differ between first touch
--     and VV-associated impression?
-- =============================================================
-- This tells us whether tracing first_touch adds new information.

WITH el AS (
  SELECT ad_served_id, bid_ip, MIN(time) AS vast_time
  FROM `dw-main-silver.logdata.event_log`
  WHERE DATE(time) BETWEEN '2026-01-05' AND '2026-02-10'
    AND event_type_raw = 'vast_impression'
  GROUP BY ad_served_id, bid_ip
)
SELECT
  COUNTIF(el_vv.bid_ip = el_ft.bid_ip) AS bid_ip_same,
  COUNTIF(el_vv.bid_ip != el_ft.bid_ip) AS bid_ip_different,
  COUNTIF(el_vv.bid_ip IS NULL OR el_ft.bid_ip IS NULL) AS one_or_both_null,
  COUNT(*) AS total,
  ROUND(COUNTIF(el_vv.bid_ip != el_ft.bid_ip) * 100.0 /
    NULLIF(COUNTIF(el_vv.bid_ip IS NOT NULL AND el_ft.bid_ip IS NOT NULL), 0), 2)
    AS pct_different_when_both_present
FROM `dw-main-silver.logdata.clickpass_log` cp
LEFT JOIN el AS el_vv ON el_vv.ad_served_id = cp.ad_served_id
LEFT JOIN el AS el_ft ON el_ft.ad_served_id = cp.first_touch_ad_served_id
WHERE DATE(cp.time) BETWEEN '2026-02-04' AND '2026-02-10'
  AND cp.advertiser_id = 37775
  AND cp.ad_served_id != cp.first_touch_ad_served_id;

-- Q6: Does NULL first_touch_ad_served_id correlate with non-CTV (low EL match)?
-- Hypothesis: NULL first_touch = display clicks, which have no VAST events
-- If true, the NULL group should have much lower EL match rates

WITH cp AS (
  SELECT
    ad_served_id,
    first_touch_ad_served_id,
    CASE
      WHEN first_touch_ad_served_id IS NULL THEN 'ft_null'
      WHEN ad_served_id = first_touch_ad_served_id THEN 'same_id'
      ELSE 'different_id'
    END AS ft_group
  FROM `dw-main-silver.logdata.clickpass_log`
  WHERE advertiser_id = 37775
    AND DATE(time) BETWEEN '2026-02-04' AND '2026-02-10'
),

el AS (
  SELECT DISTINCT ad_served_id
  FROM `dw-main-silver.logdata.event_log`
  WHERE event_type_raw = 'vast_impression'
    AND DATE(time) BETWEEN '2026-01-05' AND '2026-02-10'  -- 30-day lookback
)

SELECT
  cp.ft_group,
  COUNT(*) AS total,
  COUNTIF(el.ad_served_id IS NOT NULL) AS el_matched,
  ROUND(100.0 * COUNTIF(el.ad_served_id IS NOT NULL) / COUNT(*), 2) AS el_match_pct
FROM cp
LEFT JOIN el ON el.ad_served_id = cp.ad_served_id
GROUP BY cp.ft_group
ORDER BY cp.ft_group;

--------------------------------------------------------------------------------
-- Q7: Clickpass column schema — look for click-type indicators
-- If clickpass distinguishes clicks from visits, there'll be a column for it.
--------------------------------------------------------------------------------

SELECT column_name, data_type
FROM `dw-main-silver.logdata.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'clickpass_log'
ORDER BY ordinal_position;


--------------------------------------------------------------------------------
-- Q8: First-touch NULL rate by impression recency
-- HYPOTHESIS: If first_touch has a lookback window, VVs where the last-touch
-- impression was very recent (same day) should have HIGHER first_touch
-- population than VVs where it was weeks ago.
-- If the NULL rate is FLAT across all recencies, it's not a lookback issue.
--------------------------------------------------------------------------------

WITH cp AS (
    SELECT
        ad_served_id,
        first_touch_ad_served_id,
        time AS cp_time
    FROM `dw-main-silver.logdata.clickpass_log`
    WHERE advertiser_id = 37775
      AND DATE(time) BETWEEN '2026-02-04' AND '2026-02-10'
),
el AS (
    SELECT
        ad_served_id,
        time AS el_time,
        ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time) AS rn
    FROM `dw-main-silver.logdata.event_log`
    WHERE advertiser_id = 37775
      AND event_type_raw = 'vast_impression'
      AND DATE(time) BETWEEN '2026-01-05' AND '2026-02-10'
)
SELECT
    CASE
        WHEN TIMESTAMP_DIFF(cp.cp_time, el.el_time, HOUR) < 1 THEN '< 1 hour'
        WHEN TIMESTAMP_DIFF(cp.cp_time, el.el_time, HOUR) < 24 THEN '1-24 hours'
        WHEN TIMESTAMP_DIFF(cp.cp_time, el.el_time, HOUR) < 168 THEN '1-7 days'
        WHEN TIMESTAMP_DIFF(cp.cp_time, el.el_time, HOUR) < 336 THEN '7-14 days'
        WHEN TIMESTAMP_DIFF(cp.cp_time, el.el_time, HOUR) < 504 THEN '14-21 days'
        ELSE '21+ days'
    END AS impression_to_visit_gap,
    COUNT(*) AS total,
    COUNTIF(cp.first_touch_ad_served_id IS NOT NULL) AS has_first_touch,
    COUNTIF(cp.first_touch_ad_served_id IS NULL) AS ft_null,
    ROUND(100.0 * COUNTIF(cp.first_touch_ad_served_id IS NULL) / COUNT(*), 2) AS ft_null_pct
FROM cp
LEFT JOIN el ON el.ad_served_id = cp.ad_served_id AND el.rn = 1
WHERE el.ad_served_id IS NOT NULL  -- CTV only (has VAST event)
GROUP BY 1
ORDER BY MIN(TIMESTAMP_DIFF(cp.cp_time, el.el_time, HOUR));


--------------------------------------------------------------------------------
-- Q9: Example VVs at each stage/impression count
-- Finds concrete examples showing the full IP lineage for:
--   Type A: 1 impression total (stage 1 → stage 3, no stage 2 intermediary)
--   Type B: 2 impressions (1 stage 2 intermediary)
--   Type C: 3-5 impressions (a few stage 2 intermediaries)
--   Type D: 10+ impressions (extreme case)
--
-- For each, shows: every impression's ad_served_id, bid_ip, vast_ip, time,
-- plus the VV's redirect_ip and visit_ip.
--------------------------------------------------------------------------------

WITH cp AS (
    SELECT
        ad_served_id,
        first_touch_ad_served_id,
        ip AS redirect_ip,
        is_cross_device,
        is_new AS cp_is_new,
        time AS cp_time
    FROM `dw-main-silver.logdata.clickpass_log`
    WHERE advertiser_id = 37775
      AND DATE(time) BETWEEN '2026-02-04' AND '2026-02-10'
),
el AS (
    SELECT
        ad_served_id,
        bid_ip,
        ip AS vast_ip,
        time AS el_time,
        ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time) AS rn
    FROM `dw-main-silver.logdata.event_log`
    WHERE advertiser_id = 37775
      AND event_type_raw = 'vast_impression'
      AND DATE(time) BETWEEN '2026-01-05' AND '2026-02-10'
),
-- Get the bid_ip for this VV's ad_served_id
cp_with_el AS (
    SELECT
        cp.*,
        el.bid_ip,
        el.vast_ip,
        el.el_time
    FROM cp
    JOIN el ON el.ad_served_id = cp.ad_served_id AND el.rn = 1
),
-- Count all impressions for this bid_ip in the 30-day window
ip_impressions AS (
    SELECT
        ce.ad_served_id AS cp_ad_served_id,
        ce.redirect_ip,
        ce.bid_ip AS vv_bid_ip,
        ce.vast_ip AS vv_vast_ip,
        ce.cp_time,
        ce.el_time AS vv_el_time,
        ce.is_cross_device,
        ce.cp_is_new,
        ce.first_touch_ad_served_id,
        COUNT(DISTINCT el2.ad_served_id) AS total_impressions
    FROM cp_with_el ce
    -- Find all VAST impressions for this bid_ip in the 30-day window
    JOIN el el2
        ON el2.bid_ip = ce.bid_ip
        AND el2.rn = 1
        AND el2.el_time <= ce.cp_time  -- only before the visit
    GROUP BY ALL
),
-- Classify and pick one example per type
classified AS (
    SELECT
        *,
        CASE
            WHEN total_impressions = 1 THEN 'A: 1 impression (stage 1 → 3 direct)'
            WHEN total_impressions = 2 THEN 'B: 2 impressions (1 intermediary)'
            WHEN total_impressions BETWEEN 3 AND 5 THEN 'C: 3-5 impressions'
            WHEN total_impressions BETWEEN 6 AND 10 THEN 'D: 6-10 impressions'
            ELSE 'E: 10+ impressions (extreme)'
        END AS vv_type,
        ROW_NUMBER() OVER (
            PARTITION BY CASE
                WHEN total_impressions = 1 THEN 'A'
                WHEN total_impressions = 2 THEN 'B'
                WHEN total_impressions BETWEEN 3 AND 5 THEN 'C'
                WHEN total_impressions BETWEEN 6 AND 10 THEN 'D'
                ELSE 'E'
            END
            ORDER BY total_impressions DESC, cp_time
        ) AS example_rank
    FROM ip_impressions
)
SELECT
    vv_type,
    cp_ad_served_id,
    first_touch_ad_served_id,
    total_impressions,
    vv_bid_ip,
    vv_vast_ip,
    redirect_ip,
    is_cross_device,
    cp_is_new,
    vv_el_time AS last_impression_time,
    cp_time AS visit_time
FROM classified
WHERE example_rank = 1
ORDER BY total_impressions;


--------------------------------------------------------------------------------
-- Q10: Full impression timeline for one example per type
-- After running Q9, pick the cp_ad_served_id from each type and plug it in
-- below. This shows every impression in the chain with its IPs.
--
-- Replace @example_bid_ip with the vv_bid_ip from Q9 results.
-- Replace @example_cp_time with the visit_time from Q9 results.
--------------------------------------------------------------------------------

-- Run this once per example. Replace the values from Q9:
-- @example_bid_ip = '...'
-- @example_cp_time = '...'

WITH impressions AS (
    SELECT
        ad_served_id,
        bid_ip,
        ip AS vast_ip,
        time AS impression_time,
        ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time) AS rn
    FROM `dw-main-silver.logdata.event_log`
    WHERE advertiser_id = 37775
      AND event_type_raw = 'vast_impression'
      AND DATE(time) BETWEEN '2026-01-05' AND '2026-02-10'
)
SELECT
    ROW_NUMBER() OVER (ORDER BY impression_time) AS impression_num,
    ad_served_id,
    bid_ip,
    vast_ip,
    impression_time,
    (bid_ip = vast_ip) AS bid_eq_vast,
    LAG(bid_ip) OVER (ORDER BY impression_time) AS prev_bid_ip,
    (bid_ip = LAG(bid_ip) OVER (ORDER BY impression_time)) AS bid_ip_stable
FROM impressions
WHERE bid_ip = @example_bid_ip  -- replace with value from Q9
  AND rn = 1
  AND impression_time <= @example_cp_time  -- replace with value from Q9
ORDER BY impression_time;

--------------------------------------------------------------------------------
-- Q10a: Type A — 1 impression timeline (bid_ip = '173.184.150.62')
-- Expected: just 1 row. Stage 1 → Stage 3 direct.
--------------------------------------------------------------------------------

WITH impressions AS (
    SELECT
        ad_served_id,
        bid_ip,
        ip AS vast_ip,
        time AS impression_time,
        ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time) AS rn
    FROM `dw-main-silver.logdata.event_log`
    WHERE advertiser_id = 37775
      AND event_type_raw = 'vast_impression'
      AND DATE(time) BETWEEN '2026-01-05' AND '2026-02-10'
)
SELECT
    ROW_NUMBER() OVER (ORDER BY impression_time) AS impression_num,
    ad_served_id,
    bid_ip,
    vast_ip,
    impression_time,
    (bid_ip = vast_ip) AS bid_eq_vast,
    LAG(bid_ip) OVER (ORDER BY impression_time) AS prev_bid_ip,
    (bid_ip = LAG(bid_ip) OVER (ORDER BY impression_time)) AS bid_ip_stable
FROM impressions
WHERE bid_ip = '173.184.150.62'
  AND rn = 1
  AND impression_time <= TIMESTAMP('2026-02-04 00:00:11.000000 UTC')
ORDER BY impression_time;


--------------------------------------------------------------------------------
-- Q10b: Type B — 2 impressions timeline (bid_ip = '16.98.111.49')
-- Expected: 2 rows showing the impression chain.
--------------------------------------------------------------------------------

WITH impressions AS (
    SELECT
        ad_served_id,
        bid_ip,
        ip AS vast_ip,
        time AS impression_time,
        ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time) AS rn
    FROM `dw-main-silver.logdata.event_log`
    WHERE advertiser_id = 37775
      AND event_type_raw = 'vast_impression'
      AND DATE(time) BETWEEN '2026-01-05' AND '2026-02-10'
)
SELECT
    ROW_NUMBER() OVER (ORDER BY impression_time) AS impression_num,
    ad_served_id,
    bid_ip,
    vast_ip,
    impression_time,
    (bid_ip = vast_ip) AS bid_eq_vast,
    LAG(bid_ip) OVER (ORDER BY impression_time) AS prev_bid_ip,
    (bid_ip = LAG(bid_ip) OVER (ORDER BY impression_time)) AS bid_ip_stable
FROM impressions
WHERE bid_ip = '16.98.111.49'
  AND rn = 1
  AND impression_time <= TIMESTAMP('2026-02-04 00:00:19.000000 UTC')
ORDER BY impression_time;


--------------------------------------------------------------------------------
-- Q10c: Type C — 5 impressions timeline (bid_ip = '71.206.63.109')
--------------------------------------------------------------------------------

WITH impressions AS (
    SELECT
        ad_served_id,
        bid_ip,
        ip AS vast_ip,
        time AS impression_time,
        ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time) AS rn
    FROM `dw-main-silver.logdata.event_log`
    WHERE advertiser_id = 37775
      AND event_type_raw = 'vast_impression'
      AND DATE(time) BETWEEN '2026-01-05' AND '2026-02-10'
)
SELECT
    ROW_NUMBER() OVER (ORDER BY impression_time) AS impression_num,
    ad_served_id,
    bid_ip,
    vast_ip,
    impression_time,
    (bid_ip = vast_ip) AS bid_eq_vast,
    LAG(bid_ip) OVER (ORDER BY impression_time) AS prev_bid_ip,
    (bid_ip = LAG(bid_ip) OVER (ORDER BY impression_time)) AS bid_ip_stable
FROM impressions
WHERE bid_ip = '71.206.63.109'
  AND rn = 1
  AND impression_time <= TIMESTAMP('2026-02-04 00:00:43.917082 UTC')
ORDER BY impression_time;


--------------------------------------------------------------------------------
-- Q10e: Type E — 369 impressions (bid_ip = '104.171.65.16')
-- This is the extreme case. bid_ip != vast_ip on the VV impression.
-- Summarize instead of listing all 369 rows.
--------------------------------------------------------------------------------

WITH impressions AS (
    SELECT
        ad_served_id,
        bid_ip,
        ip AS vast_ip,
        time AS impression_time,
        ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time) AS rn
    FROM `dw-main-silver.logdata.event_log`
    WHERE advertiser_id = 37775
      AND event_type_raw = 'vast_impression'
      AND DATE(time) BETWEEN '2026-01-05' AND '2026-02-10'
),
timeline AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY impression_time) AS impression_num,
        ad_served_id,
        bid_ip,
        vast_ip,
        impression_time,
        (bid_ip = vast_ip) AS bid_eq_vast
    FROM impressions
    WHERE bid_ip = '104.171.65.16'
      AND rn = 1
      AND impression_time <= TIMESTAMP('2026-02-10 16:30:52.000000 UTC')
)
SELECT
    COUNT(*) AS total_impressions,
    MIN(impression_time) AS first_impression,
    MAX(impression_time) AS last_impression,
    COUNT(DISTINCT vast_ip) AS distinct_vast_ips,
    COUNTIF(bid_eq_vast) AS bid_eq_vast_count,
    COUNTIF(NOT bid_eq_vast) AS bid_ne_vast_count,
    -- Show first 3 and last 3 for context
    ARRAY_AGG(
        STRUCT(impression_num, ad_served_id, bid_ip, vast_ip, impression_time)
        ORDER BY impression_time LIMIT 3
    ) AS first_3,
    ARRAY_AGG(
        STRUCT(impression_num, ad_served_id, bid_ip, vast_ip, impression_time)
        ORDER BY impression_time DESC LIMIT 3
    ) AS last_3
FROM timeline;