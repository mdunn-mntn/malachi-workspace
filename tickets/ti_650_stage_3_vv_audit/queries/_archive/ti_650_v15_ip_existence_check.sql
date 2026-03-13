-- TI-650: v15 — Check if unresolved VV IPs exist ANYWHERE in the S1 impression pool
-- Tests multiple hypotheses:
--   1. IP exists in S1 pool but DIFFERENT campaign_group_id (cross-group)
--   2. IP exists in S1 pool but OUTSIDE 90-day window (lookback too short)
--   3. IP never appears in any S1 impression at all (identity graph only)
--
-- Uses a WIDE lookback (180 days) and ALL campaign_groups for adv 37775

WITH unresolved_ips AS (
    SELECT ip FROM UNNEST([
        '172.56.67.86', '172.56.92.210', '216.126.34.185', '172.58.244.220',
        '155.190.18.4', '107.122.105.113', '172.56.34.253', '172.56.226.190',
        '172.56.90.14', '172.56.35.97', '172.58.166.123', '172.56.67.25',
        '108.147.172.122', '66.138.93.17', '172.59.213.32', '172.56.33.162',
        '172.56.42.130', '172.58.242.252', '172.56.80.164', '172.59.202.4',
        '172.56.64.148', '172.56.163.41', '172.56.167.154', '216.126.35.46',
        '172.56.67.74', '107.115.159.66', '172.56.162.134', '172.56.220.146',
        '172.59.96.228', '172.56.219.184', '172.56.212.37', '172.56.16.222',
        '172.56.212.36', '172.56.211.124', '172.56.217.116', '172.58.129.38',
        '172.56.41.180', '172.59.169.181', '23.116.199.129', '50.79.12.241',
        '172.56.166.100', '172.56.218.219', '172.56.65.150', '24.184.193.143',
        '172.59.122.180', '172.56.168.223', '172.59.121.254', '172.59.139.9',
        '172.58.164.118', '172.56.93.76'
    ]) AS ip
),

campaigns AS (
    SELECT campaign_id, campaign_group_id, funnel_level
    FROM `dw-main-bronze.integrationprod.campaigns`
    WHERE advertiser_id = 37775
      AND deleted = FALSE AND is_test = FALSE
      AND funnel_level = 1
      AND objective_id IN (1, 5, 6)
),

-- Check event_log for vast events matching these IPs (180-day window, all S1 campaigns)
s1_vast_matches AS (
    SELECT
        el.ip,
        c.campaign_group_id,
        MIN(el.time) AS earliest_impression,
        MAX(el.time) AS latest_impression,
        COUNT(*) AS impression_count,
        el.event_type_raw
    FROM `dw-main-silver.logdata.event_log` el
    JOIN campaigns c ON c.campaign_id = el.campaign_id
    WHERE el.ip IN (SELECT ip FROM unresolved_ips)
      AND el.event_type_raw IN ('vast_start', 'vast_impression')
      AND el.time >= TIMESTAMP('2025-08-06') AND el.time < TIMESTAMP('2026-02-11')
      AND el.advertiser_id = 37775
    GROUP BY el.ip, c.campaign_group_id, el.event_type_raw
),

-- Check CIL for bid IPs matching these IPs (180-day window)
s1_cil_matches AS (
    SELECT
        cil.ip,
        c.campaign_group_id,
        MIN(cil.time) AS earliest_impression,
        MAX(cil.time) AS latest_impression,
        COUNT(*) AS impression_count
    FROM `dw-main-silver.logdata.cost_impression_log` cil
    JOIN campaigns c ON c.campaign_id = cil.campaign_id
    WHERE cil.ip IN (SELECT ip FROM unresolved_ips)
      AND cil.time >= TIMESTAMP('2025-08-06') AND cil.time < TIMESTAMP('2026-02-11')
      AND cil.advertiser_id = 37775
    GROUP BY cil.ip, c.campaign_group_id
)

SELECT
    u.ip,
    COALESCE(v.campaign_group_id, c2.campaign_group_id) AS found_in_cg,
    v.event_type_raw AS vast_event_type,
    v.earliest_impression AS vast_earliest,
    v.latest_impression AS vast_latest,
    v.impression_count AS vast_count,
    c2.earliest_impression AS cil_earliest,
    c2.latest_impression AS cil_latest,
    c2.impression_count AS cil_count
FROM unresolved_ips u
LEFT JOIN s1_vast_matches v ON v.ip = u.ip
LEFT JOIN s1_cil_matches c2 ON c2.ip = u.ip
ORDER BY u.ip;
