-- TI-650: v15 Step 2 — Forensic trace lookup for 50 unresolved S3 VVs
-- Uses hardcoded ad_served_ids from Step 1 to avoid expensive s1_pool rebuild
-- Looks up each source table via ad_served_id or auction_id (via td_impression_id bridge)
--
-- IMPORTANT: bid_logs and win_logs use Beeswax IDs — join only via auction_id, no advertiser filter

WITH unresolved_ids AS (
    SELECT ad_served_id FROM UNNEST([
        '88f455e3-f670-4360-9da8-080002418538',
        '1f5be653-cb59-4d8d-9829-44f01cc267ea',
        '80207c6e-1fb9-427b-b019-29e15fb3323c',
        '8d28c468-946f-4323-b5b5-40bec00af581',
        'c5cd082d-d839-4403-8386-d7a9dd0724ee',
        'fac53268-baa3-412e-a7c2-45fd17255e6c',
        '19c23a38-2f4c-4922-9692-c31666b45ac5',
        'f64672fe-f809-4ade-ba99-20f3d1bd5f7e',
        'c6c1b2b3-7bc1-49bc-a27e-76c6770a46ad',
        'a84d38a1-738c-4d83-9072-9c543d645537',
        '5abf1814-d934-4c53-aaed-c58e8a43eef0',
        'a882afd3-f614-4dc9-94cd-c3847523c4f5',
        '80ca9252-b28b-45f7-b702-84bbfd35c1c6',
        'b7e3f341-0aeb-4827-ba7f-77fe25d3161d',
        'd49c9ca2-c732-4b36-b4c7-305fb7a4941e',
        'ef3aebac-55e3-4d85-abf5-b66e83de870d',
        '037a6212-0cc4-4c32-b217-03b3d7ae7fdd',
        '1f1c1edb-7871-4a03-be6c-0f8db1258fd1',
        'db47ac99-35ca-43b0-9a7b-67b09039bdb7',
        '57fd010c-04d5-4168-b8f1-7b5f9ce450fc',
        '5f14702f-0939-48bb-8302-46ece30b5827',
        'f222b1ba-843b-4f94-9bb6-5542c1b10e7e',
        '068ad77f-a50a-4f83-909d-3430865e932a',
        '459945c9-b20b-4246-b4d6-7cfb8948279e',
        '15b8cd59-966c-49cb-bebe-622ada8d575c',
        '51454f62-8a07-4213-b0c1-726702317795',
        '0564dce3-af68-4b0d-a02d-6a269ea55e90',
        '0ee2e6de-7221-4786-b880-c37298408a88',
        '406784fe-b17a-4439-b2ce-bf2d8917a82f',
        'fef513bb-2d67-40ff-9f1b-06de1730e41e',
        'f5de6b01-008e-4afc-9253-0d621572674f',
        '8272ec87-4692-4458-a8ac-90f9ff32cf0d',
        '6006d84e-6fd0-4a30-8a84-1c8c5b643219',
        '2d76e557-92c5-4d78-8398-b4f2a82d165b',
        '8474b327-64ca-482a-85ee-5ab0f843dfd7',
        '58cabed2-8ac0-4331-a2c0-fa6693145c54',
        '6b7671d0-6bec-4f2e-be04-04a76ccd6e29',
        '58392068-0101-46dc-89e9-1ea4df9a9d0e',
        '0f45d993-9c4b-4cb9-8959-5dceac3f9ceb',
        'f564710d-7b16-4f7c-ae93-7af8d4b9bec1',
        '9452ce84-be10-450e-9d88-e9b6e656638d',
        '72f52cc3-7727-4a8a-a775-cd50b89c6034',
        'a0b31a96-f488-4b77-a12e-d785b328cdfe',
        'db52dac8-1cc1-4d5b-a258-3a7c00c9344b',
        'e5e2203c-3204-4b1e-86f4-c9d91c6e4123',
        'e2b7c722-e53e-4e66-bf3b-ce27aa654a02',
        'f8b5b525-7ce2-442e-ba70-838e1937f12f',
        'cb56fbeb-7ad3-43dd-b975-995a6acba5a7',
        'c3695cc0-1cec-4c3f-86e5-f0213ef62a58',
        '1feabc02-2fc2-44b7-bd75-f4966ceafa2d'
    ]) AS ad_served_id
),

-- Clickpass (VV entry)
cp AS (
    SELECT cp.ad_served_id, cp.ip AS redirect_ip, cp.guid,
           cp.campaign_id, cp.time AS vv_time, cp.is_cross_device
    FROM `dw-main-silver.logdata.clickpass_log` cp
    WHERE cp.ad_served_id IN (SELECT ad_served_id FROM unresolved_ids)
      AND cp.time >= TIMESTAMP('2026-02-04') AND cp.time < TIMESTAMP('2026-02-11')
      AND cp.advertiser_id = 37775
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),

-- Event log: vast_start (provides td_impression_id bridge to auction_id)
ev_vast_start AS (
    SELECT el.ad_served_id,
           el.ip AS vast_start_ip,
           el.bid_ip AS vs_bid_ip,
           el.original_ip AS vs_original_ip,
           el.td_impression_id,
           el.time AS vast_start_time
    FROM `dw-main-silver.logdata.event_log` el
    WHERE el.ad_served_id IN (SELECT ad_served_id FROM unresolved_ids)
      AND el.event_type_raw = 'vast_start'
      AND el.time >= TIMESTAMP('2025-11-06') AND el.time < TIMESTAMP('2026-02-11')
      AND el.advertiser_id = 37775
    QUALIFY ROW_NUMBER() OVER (PARTITION BY el.ad_served_id ORDER BY el.time ASC) = 1
),

-- Event log: vast_impression
ev_vast_imp AS (
    SELECT el.ad_served_id,
           el.ip AS vast_imp_ip,
           el.bid_ip AS vi_bid_ip,
           el.time AS vast_imp_time
    FROM `dw-main-silver.logdata.event_log` el
    WHERE el.ad_served_id IN (SELECT ad_served_id FROM unresolved_ids)
      AND el.event_type_raw = 'vast_impression'
      AND el.time >= TIMESTAMP('2025-11-06') AND el.time < TIMESTAMP('2026-02-11')
      AND el.advertiser_id = 37775
    QUALIFY ROW_NUMBER() OVER (PARTITION BY el.ad_served_id ORDER BY el.time ASC) = 1
),

-- Impression log (serve)
imp_log AS (
    SELECT il.ad_served_id,
           il.ip AS serve_ip,
           il.bid_ip AS imp_bid_ip,
           il.original_ip AS imp_original_ip,
           il.time AS serve_time
    FROM `dw-main-silver.logdata.impression_log` il
    WHERE il.ad_served_id IN (SELECT ad_served_id FROM unresolved_ids)
      AND il.time >= TIMESTAMP('2025-11-06') AND il.time < TIMESTAMP('2026-02-11')
      AND il.advertiser_id = 37775
    QUALIFY ROW_NUMBER() OVER (PARTITION BY il.ad_served_id ORDER BY il.time ASC) = 1
),

-- Cost impression log
cil_log AS (
    SELECT cil.ad_served_id,
           cil.ip AS cil_ip,
           cil.time AS cil_time
    FROM `dw-main-silver.logdata.cost_impression_log` cil
    WHERE cil.ad_served_id IN (SELECT ad_served_id FROM unresolved_ids)
      AND cil.time >= TIMESTAMP('2025-11-06') AND cil.time < TIMESTAMP('2026-02-11')
      AND cil.advertiser_id = 37775
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cil.ad_served_id ORDER BY cil.time ASC) = 1
),

-- UI visits
uv_log AS (
    SELECT CAST(uv.ad_served_id AS STRING) AS ad_served_id,
           uv.ip AS visit_ip,
           uv.impression_ip AS uv_imp_ip,
           uv.time AS visit_time
    FROM `dw-main-silver.summarydata.ui_visits` uv
    WHERE CAST(uv.ad_served_id AS STRING) IN (SELECT ad_served_id FROM unresolved_ids)
      AND uv.time >= TIMESTAMP('2026-01-28') AND uv.time < TIMESTAMP('2026-02-18')
      AND uv.from_verified_impression = TRUE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(uv.ad_served_id AS STRING) ORDER BY uv.time ASC) = 1
),

-- Bid events log (MNTN-native) — join via td_impression_id = auction_id
bid_ev AS (
    SELECT evs.ad_served_id,
           bel.ip AS bid_events_ip,
           bel.auction_id,
           bel.campaign_group_id AS bel_cg_id,
           bel.time AS bid_events_time
    FROM ev_vast_start evs
    JOIN `dw-main-silver.logdata.bid_events_log` bel
        ON bel.auction_id = evs.td_impression_id
    WHERE bel.time >= TIMESTAMP('2025-11-06') AND bel.time < TIMESTAMP('2026-02-11')
      AND bel.advertiser_id = 37775
),

-- Bid logs (Beeswax-native) — join via td_impression_id = auction_id
-- Cannot filter by MNTN advertiser_id
bid_lg AS (
    SELECT evs.ad_served_id,
           bl.ip AS bid_log_ip,
           bl.time AS bid_log_time
    FROM ev_vast_start evs
    JOIN `dw-main-silver.logdata.bid_logs` bl
        ON bl.auction_id = evs.td_impression_id
    WHERE bl.time >= TIMESTAMP('2025-11-06') AND bl.time < TIMESTAMP('2026-02-11')
),

-- Win logs (Beeswax-native) — join via td_impression_id = auction_id
-- Cannot filter by MNTN advertiser_id
win_lg AS (
    SELECT evs.ad_served_id,
           wl.ip AS win_ip,
           wl.impression_ip_address AS win_infra_ip,
           wl.time AS win_time
    FROM ev_vast_start evs
    JOIN `dw-main-silver.logdata.win_logs` wl
        ON wl.auction_id = evs.td_impression_id
    WHERE wl.time >= TIMESTAMP('2025-11-06') AND wl.time < TIMESTAMP('2026-02-11')
)

SELECT
    cp.ad_served_id,
    cp.vv_time,
    cp.is_cross_device,
    cp.campaign_id,

    -- Clickpass
    cp.redirect_ip,
    cp.guid,

    -- Event log: vast_start
    evs.vast_start_ip,
    evs.vs_bid_ip,
    evs.vs_original_ip,
    evs.td_impression_id,
    evs.vast_start_time,

    -- Event log: vast_impression
    evi.vast_imp_ip,
    evi.vi_bid_ip,
    evi.vast_imp_time,

    -- Impression log (serve)
    imp.serve_ip,
    imp.imp_bid_ip,
    imp.imp_original_ip,
    imp.serve_time,

    -- CIL
    cil.cil_ip,
    cil.cil_time,

    -- UI visits
    uv.visit_ip,
    uv.uv_imp_ip,
    uv.visit_time,

    -- Bid events (MNTN)
    be.bid_events_ip,
    be.bel_cg_id,
    be.bid_events_time,

    -- Bid logs (Beeswax)
    bl.bid_log_ip,
    bl.bid_log_time,

    -- Win logs (Beeswax)
    wl.win_ip,
    wl.win_infra_ip,
    wl.win_time,

    -- IP EQUALITY checks across tables
    (evs.vs_bid_ip = cil.cil_ip) AS event_bid_eq_cil,
    (evs.vs_bid_ip = imp.imp_bid_ip) AS event_bid_eq_imp_bid,
    (evs.vs_bid_ip = be.bid_events_ip) AS event_bid_eq_bid_events,
    (evs.vs_bid_ip = bl.bid_log_ip) AS event_bid_eq_bid_log,
    (evs.vs_bid_ip = wl.win_ip) AS event_bid_eq_win,
    (evs.vast_start_ip = evi.vast_imp_ip) AS vast_start_eq_vast_imp,
    (imp.serve_ip = evs.vs_bid_ip) AS serve_eq_bid,

    -- TABLE PRESENCE flags (NULL = no record)
    (evs.ad_served_id IS NOT NULL) AS has_vast_start,
    (evi.ad_served_id IS NOT NULL) AS has_vast_imp,
    (imp.ad_served_id IS NOT NULL) AS has_impression_log,
    (cil.ad_served_id IS NOT NULL) AS has_cil,
    (uv.ad_served_id IS NOT NULL) AS has_ui_visits,
    (be.ad_served_id IS NOT NULL) AS has_bid_events,
    (bl.ad_served_id IS NOT NULL) AS has_bid_logs,
    (wl.ad_served_id IS NOT NULL) AS has_win_logs

FROM cp
LEFT JOIN ev_vast_start evs ON evs.ad_served_id = cp.ad_served_id
LEFT JOIN ev_vast_imp evi ON evi.ad_served_id = cp.ad_served_id
LEFT JOIN imp_log imp ON imp.ad_served_id = cp.ad_served_id
LEFT JOIN cil_log cil ON cil.ad_served_id = cp.ad_served_id
LEFT JOIN uv_log uv ON uv.ad_served_id = cp.ad_served_id
LEFT JOIN bid_ev be ON be.ad_served_id = cp.ad_served_id
LEFT JOIN bid_lg bl ON bl.ad_served_id = cp.ad_served_id
LEFT JOIN win_lg wl ON wl.ad_served_id = cp.ad_served_id
ORDER BY cp.vv_time;
