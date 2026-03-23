-- TI-650: Diagnostic for 85 unresolved S3 VVs
-- Q1: All-time clickpass search (back to 2022) for ANY prior S1/S2 VV matching resolved_ip + campaign_group_id
-- Q2: S1 impression pool check (event_log + viewability_log + impression_log) — 180d before audit window
-- Q3: Full campaign metadata for the 85 unresolved
-- Advertiser filter on all scans to limit cost

WITH unresolved AS (
    SELECT * FROM UNNEST([
      STRUCT('ad_served_id' AS ad_served_id, 'resolved_ip' AS resolved_ip, 0 AS campaign_group_id, 0 AS advertiser_id, TIMESTAMP('2000-01-01') AS vv_time),
  ('ad97dc6e-3d43-440f-b04b-d994cabaabd4', '172.253.254.50', 78036, 32153, TIMESTAMP('2026-02-09 22:30:20')),
  ('bc6414f3-d22b-4b63-b7a6-f8b632e7ee66', '172.253.192.119', 109000, 36702, TIMESTAMP('2026-02-05 03:57:49')),
  ('4198ecb1-65bb-4646-8e82-ee5e73d77136', '68.67.136.211', 101294, 39445, TIMESTAMP('2026-02-08 14:41:50')),
  ('eb017fca-92e9-46bd-8df2-8aa27c387873', '174.207.103.51', 64793, 32352, TIMESTAMP('2026-02-10 16:27:24')),
  ('8de95683-edea-4d10-8a12-03bca94a9cf8', '172.217.36.121', 108574, 39445, TIMESTAMP('2026-02-06 16:20:22')),
  ('84ee1e99-c7c6-40f3-bed6-899c95ed9db9', '68.67.164.194', 110454, 39445, TIMESTAMP('2026-02-07 11:39:58')),
  ('d4e67ea6-3169-4471-b6d5-4c51a52f6144', '10.105.0.118', 86247, 36507, TIMESTAMP('2026-02-04 22:35:14')),
  ('7a1f2009-e162-4111-87ad-86e2dd267421', '68.67.139.33', 109000, 36702, TIMESTAMP('2026-02-04 07:40:07')),
  ('282a7f7b-fdc8-4336-90c9-ac54f168b1c1', '68.67.136.229', 78036, 32153, TIMESTAMP('2026-02-09 12:24:21')),
  ('5d45c59f-f77b-4c61-917f-79cb6f3cee6b', '68.67.136.223', 82225, 36390, TIMESTAMP('2026-02-05 07:53:53')),
  ('b7c1271f-a1c0-4fee-92bc-b93534350e48', '64.90.68.124', 109802, 38323, TIMESTAMP('2026-02-09 20:13:49')),
  ('99315a77-c902-425f-8f46-5ee0bdfa6566', '172.253.192.251', 109000, 36702, TIMESTAMP('2026-02-04 00:06:14')),
  ('024417b8-1a80-48d3-a3b8-51796936972c', '74.125.77.6', 98512, 39397, TIMESTAMP('2026-02-08 03:26:46')),
  ('4baf1356-77bf-482a-a1e5-d75828af7430', '68.67.136.247', 60845, 32153, TIMESTAMP('2026-02-09 09:13:18')),
  ('3a488bcb-35ff-429d-a7bc-c02cfdc6dbf9', '172.253.234.154', 108870, 35672, TIMESTAMP('2026-02-04 10:35:20')),
  ('f7ecd9a8-0276-41ec-bc8f-3d9f487aec79', '68.67.139.13', 101294, 39445, TIMESTAMP('2026-02-06 16:43:04')),
  ('e70a6e4c-e46a-48bc-908d-18b3a7a0d55b', '172.253.254.48', 108994, 36702, TIMESTAMP('2026-02-10 23:37:29')),
  ('4ed32334-cd1a-4788-85db-9d756b0ff443', '74.125.77.20', 108574, 39445, TIMESTAMP('2026-02-07 01:07:56')),
  ('201202bd-c964-4c6a-a41c-e4f43aa541e4', '172.253.218.53', 108870, 35672, TIMESTAMP('2026-02-04 05:02:29')),
  ('783c5e68-0ccc-42b8-a213-64b6a4f2f5a3', '74.125.182.96', 108195, 39397, TIMESTAMP('2026-02-08 02:58:20')),
  ('e611bc47-69b5-4b2a-bd56-15456ec33215', '10.106.34.65', 107129, 39445, TIMESTAMP('2026-02-05 19:51:04')),
  ('ecba9bbc-7e26-4945-932b-088f76480521', '74.125.182.101', 108195, 39397, TIMESTAMP('2026-02-07 09:40:39')),
  ('1208fbde-7791-4e3e-b735-625c16b2fb6a', '172.253.234.137', 108870, 35672, TIMESTAMP('2026-02-04 14:01:53')),
  ('a4fb547e-724b-410f-b454-9a1d63e0b796', '68.67.139.31', 82225, 36390, TIMESTAMP('2026-02-04 07:51:30')),
  ('1bf9e4d7-ad4d-4930-8127-a090dc7ec1c3', '173.194.96.188', 98512, 39397, TIMESTAMP('2026-02-05 16:42:59')),
  ('8e38cca7-495f-4432-a382-802f51b80fde', '68.67.139.22', 101294, 39445, TIMESTAMP('2026-02-07 14:38:52')),
  ('172d8864-79a2-4f55-ad0c-cfb136642ea1', '74.125.77.29', 98512, 39397, TIMESTAMP('2026-02-06 00:15:43')),
  ('bd750515-abe5-4c0c-9069-85e39332a22a', '74.125.182.107', 108574, 39445, TIMESTAMP('2026-02-10 20:34:48')),
  ('a7204363-f64c-4176-bfcf-d541fa1148f8', '68.67.136.216', 110454, 39445, TIMESTAMP('2026-02-07 14:38:55')),
  ('63ebba04-ebe9-4256-b7e7-5bb7b031791d', '172.56.251.94', 78036, 32153, TIMESTAMP('2026-02-10 05:03:30')),
  ('5997220c-36ef-4c9b-be55-4ad835805543', '172.253.192.120', 108870, 35672, TIMESTAMP('2026-02-05 11:10:46')),
  ('18adae72-2d62-4896-87c2-6e299a885e2c', '74.125.182.143', 108870, 35672, TIMESTAMP('2026-02-04 14:24:39')),
  ('06b4885d-e6b1-4906-9dce-24f2c30eeb4b', '10.105.0.117', 32267, 33023, TIMESTAMP('2026-02-08 04:10:51')),
  ('97b357c7-f1a5-4be5-b646-607b2ccc60c1', '172.58.50.72', 107168, 33804, TIMESTAMP('2026-02-09 15:42:47')),
  ('31331779-e0ac-40fb-acc2-7c024c099937', '68.67.136.207', 82714, 32153, TIMESTAMP('2026-02-09 09:13:24')),
  ('58dd9462-09ea-4a9a-97de-7d81bddb19e1', '74.125.182.143', 108870, 35672, TIMESTAMP('2026-02-04 20:30:26')),
  ('e5360184-62d3-4c32-82d4-c8b8e9f72df4', '172.217.36.121', 67431, 36537, TIMESTAMP('2026-02-08 10:55:28')),
  ('8dbd98ed-fc42-46a2-8058-60c5d27a24b3', '68.67.139.7', 67431, 36537, TIMESTAMP('2026-02-06 16:42:41')),
  ('e333f947-78ef-44c7-8099-40f66889d79d', '74.125.77.28', 108870, 35672, TIMESTAMP('2026-02-08 18:50:07')),
  ('e40fb79e-d205-4c09-a347-4bcfb419f56f', '173.194.96.188', 78036, 32153, TIMESTAMP('2026-02-09 14:07:26')),
  ('de7219bb-d799-4131-a433-9ee024d7a6d6', '68.67.136.252', 108195, 39397, TIMESTAMP('2026-02-05 17:15:32')),
  ('994a619d-a116-4bb9-abfc-96f95c0e22d6', '68.67.139.33', 108870, 35672, TIMESTAMP('2026-02-04 05:25:45')),
  ('a26f301f-88c7-415d-9631-74009b810f52', '74.125.77.3', 82714, 32153, TIMESTAMP('2026-02-09 17:15:19')),
  ('ff53f0fe-7d70-47da-a5aa-b96103831dd5', '172.217.36.186', 78036, 32153, TIMESTAMP('2026-02-09 17:16:52')),
  ('4affff9c-c6bb-43c9-bff3-d486b1915860', '68.67.136.212', 108994, 36702, TIMESTAMP('2026-02-07 00:14:28')),
  ('6a074cdf-9bf2-418a-9ca9-5ad2e8eb558f', '74.125.80.233', 108574, 39445, TIMESTAMP('2026-02-06 16:20:25')),
  ('57da8f01-cbed-474a-84fe-e38688c57cf3', '173.194.96.185', 60845, 32153, TIMESTAMP('2026-02-09 14:07:26')),
  ('178d07a4-908f-4203-81a6-5a140cc72cca', '68.67.136.238', 82225, 36390, TIMESTAMP('2026-02-05 07:54:07')),
  ('357837ba-ef14-4aa7-8360-3b452e692925', '68.67.139.8', 60845, 32153, TIMESTAMP('2026-02-09 09:13:20')),
  ('2d040816-4d3d-4372-bd71-b2b15f047766', '74.125.182.129', 108870, 35672, TIMESTAMP('2026-02-04 05:41:25')),
  ('93bcb810-8283-4247-9f93-f19ffe6823b6', '68.67.139.27', 78036, 32153, TIMESTAMP('2026-02-09 11:35:52')),
  ('e121878e-a50c-472c-9998-352796dcfbda', '209.232.168.17', 109802, 38323, TIMESTAMP('2026-02-09 16:50:26')),
  ('ac23cd9a-2d5e-4850-8ad8-747325c818e5', '74.125.80.227', 78036, 32153, TIMESTAMP('2026-02-10 17:12:26')),
  ('3c5b4b83-2c53-4851-aff4-d89f96795e6a', '172.253.192.118', 108870, 35672, TIMESTAMP('2026-02-06 18:34:43')),
  ('0662966e-1a25-4c3b-aa8d-c6d3bc3f3103', '10.105.0.192', 108027, 36507, TIMESTAMP('2026-02-09 12:21:12')),
  ('ab083cf7-e7ea-4a39-a3ec-e985c18cc74f', '173.194.96.190', 82225, 36390, TIMESTAMP('2026-02-07 11:59:45')),
  ('f4dc3b72-82ea-4114-b29b-05bb1e84c4d0', '172.253.254.50', 82714, 32153, TIMESTAMP('2026-02-09 22:30:23')),
  ('1e95d009-8f30-464a-a753-cd7e7b7d5013', '74.125.77.24', 108870, 35672, TIMESTAMP('2026-02-05 19:28:20')),
  ('ddea6604-86b6-42e9-b6d9-1032054fe8ae', '172.217.36.116', 78036, 32153, TIMESTAMP('2026-02-09 17:16:52')),
  ('eaf71385-892b-4556-9061-956ed378515f', '172.253.254.62', 108870, 35672, TIMESTAMP('2026-02-04 14:01:29')),
  ('b22b2acc-55b0-426d-a0cc-0672072d2888', '172.253.192.119', 82714, 32153, TIMESTAMP('2026-02-10 05:30:38')),
  ('2700c4a7-dd4e-4ebd-9ced-136cd3c63e75', '173.194.96.188', 108574, 39445, TIMESTAMP('2026-02-07 15:47:47')),
  ('114e7841-5fc8-46c9-a81e-9ccf821d4e9e', '74.125.182.108', 108574, 39445, TIMESTAMP('2026-02-06 21:15:01')),
  ('85090c3e-0edc-4985-9953-b8e40e6b3d61', '172.56.176.214', 93144, 39397, TIMESTAMP('2026-02-09 16:21:28')),
  ('6f987fd6-e13a-49d3-bd35-e088dfbe6946', '74.125.114.4', 108195, 39397, TIMESTAMP('2026-02-05 17:46:17')),
  ('633eef09-d07c-4952-9501-ee4622435197', '74.125.182.135', 32267, 33023, TIMESTAMP('2026-02-07 19:15:02')),
  ('fb2cc0e4-8ab1-4d68-8e1a-c40b910e272d', '172.253.254.50', 82225, 36390, TIMESTAMP('2026-02-10 20:24:45')),
  ('49836ddf-46d3-40d1-b25d-5d60abea85ba', '193.36.225.169', 69884, 35094, TIMESTAMP('2026-02-10 05:19:21')),
  ('734ae8c5-1160-4b46-8609-0ce72b633151', '172.253.234.233', 108574, 39445, TIMESTAMP('2026-02-06 23:01:52')),
  ('6121bc30-35b1-49e4-a6dd-190c6933741d', '74.125.113.21', 32267, 33023, TIMESTAMP('2026-02-08 23:09:02')),
  ('40b59da7-6f65-4a98-ad85-b5c01a368661', '173.194.96.190', 109000, 36702, TIMESTAMP('2026-02-04 19:19:52')),
  ('e394c4f6-82f5-46d8-a773-fffd79264bf2', '172.217.36.116', 108574, 39445, TIMESTAMP('2026-02-09 21:28:23')),
  ('192312e8-3060-4818-8bfa-773d90977eae', '68.67.136.198', 78036, 32153, TIMESTAMP('2026-02-09 11:36:10')),
  ('00561390-44e9-48af-bce3-b7464c7d1a63', '74.125.114.9', 101294, 39445, TIMESTAMP('2026-02-08 13:00:31')),
  ('8bebde1b-579f-4b8d-a745-e2f451e9fd9f', '68.67.136.235', 101294, 39445, TIMESTAMP('2026-02-07 14:38:49')),
  ('2ad581e8-5c54-4c23-8a37-828ab2845fad', '74.125.77.27', 101294, 39445, TIMESTAMP('2026-02-07 14:05:25')),
  ('6f319b9f-3eb7-4933-a13d-f4775e65bb55', '172.253.228.89', 98512, 39397, TIMESTAMP('2026-02-06 17:42:41')),
  ('462cb6e5-cca6-440a-81d9-271caf2a9734', '172.253.234.215', 108195, 39397, TIMESTAMP('2026-02-05 19:05:46')),
  ('4cd78931-8fbf-4c30-b890-53845c2614d2', '68.67.139.35', 98512, 39397, TIMESTAMP('2026-02-05 17:15:20')),
  ('a4e6146f-4d5b-476a-b6f7-a5d6a14cc25f', '172.253.192.241', 109000, 36702, TIMESTAMP('2026-02-04 03:24:24')),
  ('6916db9a-f7b3-4d0a-8548-6a6182237964', '68.67.136.201', 108195, 39397, TIMESTAMP('2026-02-05 17:15:18')),
  ('5324c5e4-342a-490c-852b-1ed1e0322eef', '172.56.218.117', 109802, 38323, TIMESTAMP('2026-02-09 12:37:15')),
  ('3f226215-aa37-4c47-89e1-8add6320539f', '74.125.80.229', 108574, 39445, TIMESTAMP('2026-02-07 14:05:33')),
  ('9b0fa781-3622-49e9-8a20-dd6a3b596abe', '74.125.182.97', 109000, 36702, TIMESTAMP('2026-02-04 02:21:01')),
  ('060a80f6-05c9-414b-b4f8-cc9f570445c9', '68.67.139.34', 82225, 36390, TIMESTAMP('2026-02-04 07:51:37'))
    ]) WHERE ad_served_id != 'ad_served_id'  -- skip header row
),

-- Q1: ALL-TIME clickpass search for prior S1/S2 VVs
-- No lower bound on time — search everything
all_time_vv_matches AS (
    SELECT
        u.ad_served_id AS s3_ad_served_id,
        u.resolved_ip,
        u.campaign_group_id,
        u.vv_time AS s3_vv_time,
        cp.ad_served_id AS match_ad_served_id,
        SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] AS match_clickpass_ip,
        cp.time AS match_vv_time,
        c.funnel_level AS match_funnel_level,
        cp.campaign_id AS match_campaign_id,
        c.campaign_group_id AS match_campaign_group_id,
        TIMESTAMP_DIFF(u.vv_time, cp.time, DAY) AS days_before_s3
    FROM unresolved u
    JOIN `dw-main-silver.logdata.clickpass_log` cp
        ON SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] = u.resolved_ip
        AND cp.time >= TIMESTAMP('2022-01-01')  -- all-time floor
        AND cp.time < u.vv_time
        AND cp.advertiser_id IN (
          32153, 32352, 33023, 33804, 34671, 35094, 35672,
          36390, 36507, 36537, 36702, 38323, 39445
        )
        AND cp.ip IS NOT NULL
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = cp.campaign_id
        AND c.campaign_group_id = u.campaign_group_id
        AND c.deleted = FALSE AND c.is_test = FALSE
        AND c.funnel_level IN (1, 2)
        AND c.objective_id IN (1, 5, 6)
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY u.ad_served_id, c.funnel_level
        ORDER BY cp.time DESC
    ) = 1
),

-- Q2: S1 impression pool check (±180d to be generous)
-- event_log (CTV)
imp_event AS (
    SELECT
        u.ad_served_id AS s3_ad_served_id,
        u.resolved_ip,
        u.campaign_group_id,
        el.ad_served_id AS imp_ad_served_id,
        SPLIT(el.ip, '/')[SAFE_OFFSET(0)] AS imp_ip,
        el.time AS imp_time,
        'event_log' AS source,
        el.campaign_id AS imp_campaign_id
    FROM unresolved u
    JOIN `dw-main-silver.logdata.event_log` el
        ON SPLIT(el.ip, '/')[SAFE_OFFSET(0)] = u.resolved_ip
        AND el.event_type_raw IN ('vast_start', 'vast_impression')
        AND el.time >= TIMESTAMP('2025-08-04')  -- 180d before audit window start
        AND el.time < u.vv_time
        AND el.advertiser_id IN (
          32153, 32352, 33023, 33804, 34671, 35094, 35672,
          36390, 36507, 36537, 36702, 38323, 39445
        )
        AND el.ip IS NOT NULL
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = el.campaign_id
        AND c.campaign_group_id = u.campaign_group_id
        AND c.deleted = FALSE AND c.is_test = FALSE
        AND c.funnel_level = 1
        AND c.objective_id IN (1, 5, 6)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY u.ad_served_id ORDER BY el.time DESC) = 1
),

-- viewability_log (display viewable)
imp_viewability AS (
    SELECT
        u.ad_served_id AS s3_ad_served_id,
        u.resolved_ip,
        u.campaign_group_id,
        vw.ad_served_id AS imp_ad_served_id,
        SPLIT(vw.ip, '/')[SAFE_OFFSET(0)] AS imp_ip,
        vw.time AS imp_time,
        'viewability_log' AS source,
        vw.campaign_id AS imp_campaign_id
    FROM unresolved u
    JOIN `dw-main-silver.logdata.viewability_log` vw
        ON SPLIT(vw.ip, '/')[SAFE_OFFSET(0)] = u.resolved_ip
        AND vw.time >= TIMESTAMP('2025-08-04')
        AND vw.time < u.vv_time
        AND vw.advertiser_id IN (
          32153, 32352, 33023, 33804, 34671, 35094, 35672,
          36390, 36507, 36537, 36702, 38323, 39445
        )
        AND vw.ip IS NOT NULL
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = vw.campaign_id
        AND c.campaign_group_id = u.campaign_group_id
        AND c.deleted = FALSE AND c.is_test = FALSE
        AND c.funnel_level = 1
        AND c.objective_id IN (1, 5, 6)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY u.ad_served_id ORDER BY vw.time DESC) = 1
),

-- impression_log (all display)
imp_impression AS (
    SELECT
        u.ad_served_id AS s3_ad_served_id,
        u.resolved_ip,
        u.campaign_group_id,
        il.ad_served_id AS imp_ad_served_id,
        SPLIT(il.ip, '/')[SAFE_OFFSET(0)] AS imp_ip,
        il.time AS imp_time,
        'impression_log' AS source,
        il.campaign_id AS imp_campaign_id
    FROM unresolved u
    JOIN `dw-main-silver.logdata.impression_log` il
        ON SPLIT(il.ip, '/')[SAFE_OFFSET(0)] = u.resolved_ip
        AND il.time >= TIMESTAMP('2025-08-04')
        AND il.time < u.vv_time
        AND il.advertiser_id IN (
          32153, 32352, 33023, 33804, 34671, 35094, 35672,
          36390, 36507, 36537, 36702, 38323, 39445
        )
        AND il.ip IS NOT NULL
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = il.campaign_id
        AND c.campaign_group_id = u.campaign_group_id
        AND c.deleted = FALSE AND c.is_test = FALSE
        AND c.funnel_level = 1
        AND c.objective_id IN (1, 5, 6)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY u.ad_served_id ORDER BY il.time DESC) = 1
)

-- OUTPUT: One row per unresolved S3 VV with all diagnostic info
SELECT
    u.ad_served_id,
    u.resolved_ip,
    u.campaign_group_id,
    u.advertiser_id,
    u.vv_time AS s3_vv_time,
    adv.company_name AS advertiser_name,
    cg.name AS campaign_group_name,

    -- All-time VV matches
    vv_s2.match_ad_served_id AS alltime_s2_vv_id,
    vv_s2.match_vv_time AS alltime_s2_vv_time,
    vv_s2.days_before_s3 AS alltime_s2_days_before,

    vv_s1.match_ad_served_id AS alltime_s1_vv_id,
    vv_s1.match_vv_time AS alltime_s1_vv_time,
    vv_s1.days_before_s3 AS alltime_s1_days_before,

    -- Impression pool matches
    ie.imp_ad_served_id AS imp_event_id,
    ie.imp_time AS imp_event_time,
    iv.imp_ad_served_id AS imp_viewability_id,
    iv.imp_time AS imp_viewability_time,
    ii.imp_ad_served_id AS imp_impression_id,
    ii.imp_time AS imp_impression_time,

    -- Summary flags
    CASE
        WHEN vv_s2.match_ad_served_id IS NOT NULL THEN 'HAS_S2_VV'
        WHEN vv_s1.match_ad_served_id IS NOT NULL THEN 'HAS_S1_VV'
        WHEN ie.imp_ad_served_id IS NOT NULL OR iv.imp_ad_served_id IS NOT NULL OR ii.imp_ad_served_id IS NOT NULL THEN 'IMP_ONLY'
        ELSE 'TRULY_UNRESOLVED'
    END AS diagnostic_result

FROM unresolved u
JOIN `dw-main-bronze.integrationprod.advertisers` adv
    ON u.advertiser_id = adv.advertiser_id AND adv.deleted = FALSE
JOIN `dw-main-bronze.integrationprod.campaign_groups` cg
    ON u.campaign_group_id = cg.campaign_group_id AND cg.deleted = FALSE

LEFT JOIN all_time_vv_matches vv_s2
    ON vv_s2.s3_ad_served_id = u.ad_served_id AND vv_s2.match_funnel_level = 2
LEFT JOIN all_time_vv_matches vv_s1
    ON vv_s1.s3_ad_served_id = u.ad_served_id AND vv_s1.match_funnel_level = 1

LEFT JOIN imp_event ie ON ie.s3_ad_served_id = u.ad_served_id
LEFT JOIN imp_viewability iv ON iv.s3_ad_served_id = u.ad_served_id
LEFT JOIN imp_impression ii ON ii.s3_ad_served_id = u.ad_served_id

ORDER BY u.advertiser_id, u.vv_time;
