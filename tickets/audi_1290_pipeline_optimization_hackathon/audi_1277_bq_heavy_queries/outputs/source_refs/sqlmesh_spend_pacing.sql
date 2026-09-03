MODEL (
  kind VIEW,
  owner 'ber',
  tags ['cil', 'dco'],
  description 'View of current and prior day of spend used for pacing. Unlinked & PSA spend excluded.',
  gateway silver,
  session_properties (
    query_label = [('ber_sqlmesh_model', 'spend_pacing')]
  ),
  formatting FALSE
);
/* NOTE: Consumed by PER for pacing. AID 9090 (PSA) Excluded. Only linked spend is included. */

WITH ct_params AS ( /* set params for view, used throughout */
  SELECT CURRENT_DATE - INTERVAL 2 DAY AS buffer_dt
  , TIMESTAMP(CURRENT_DATE - INTERVAL 1 DAY) AS start_ts
),

ct_bwn AS (
  SELECT
    bwn.advertiser_id
    , CASE
      WHEN LENGTH(CAST(bwn.auction_timestamp AS STRING)) = 16
      THEN CAST(safe.TIMESTAMP_MICROS(CAST(FLOOR(bwn.auction_timestamp) AS INT64)) AS TIMESTAMP)
      ELSE CAST(safe.TIMESTAMP_MICROS(CAST(FLOOR(bwn.auction_timestamp / 1000) AS INT64)) AS TIMESTAMP)
    END AS time
    , bwn.campaign_id
    , bwn.mntn_auction_id AS impression_id
    , safe_divide(CAST(bwn.win_price AS BIGNUMERIC), 1000000) AS media_cost
    , bwn.partner_id
    , p.start_ts
  FROM `dw-main-bronze.external.bidder_win_notifications__v1` AS bwn
  CROSS JOIN ct_params AS p
  WHERE
    (DATE(dt) >= p.buffer_dt) /* initial filter */
    AND bwn.advertiser_id <> 9090 /* no PSA needed */
    AND bwn.impression_timestamp <> -1
), 

ct_il AS ( /* for unlinked */
  SELECT
    i.ttd_impression_id
    , i.time
    , i.epoch
    , i.advertiser_id
    , i.campaign_id
    , i.creative_id
    , i.original_aid
    , i.ad_served_id
  FROM `dw-main-bronze.external.impression__v1` i 
  CROSS JOIN ct_params AS p
  WHERE
    (DATE(i.dt) >= p.buffer_dt) /* initial filter */
  UNION ALL
  SELECT
    i.ttd_impression_id
    , i.time
    , i.epoch
    , i.advertiser_id
    , i.campaign_id
    , i.creative_id
    , i.original_aid
    , i.ad_served_id
  FROM `dw-main-bronze.external.vastimpression__v1` i 
  CROSS JOIN ct_params AS p
  WHERE
    (DATE(i.dt) >= p.buffer_dt) /* initial filter */
), 

ct_impression_info AS (
  SELECT
    coalesce(i.advertiser_id, b.advertiser_id, -3) AS advertiser_id
    , coalesce(i.campaign_id, b.campaign_id, -3) AS campaign_id
    , coalesce(i.time, b.time) AS time
    , date_trunc(coalesce(i.time, b.time), HOUR) AS time_hr /* for spend calcs */
    , coalesce(i.epoch, unix_micros(b.time)) AS epoch
    , b.impression_id
    , b.media_cost
    , b.partner_id
    , coalesce(i.creative_id, -3) AS creative_id
    , i.ttd_impression_id
    , i.ad_served_id
    , FALSE::BOOL AS unlinked /* always FALSE (good), inner join w/ imp_id from imp log */
    , b.start_ts
  FROM ct_bwn AS b
  INNER JOIN (SELECT DISTINCT ttd_impression_id FROM ct_il) AS shi
    ON shi.ttd_impression_id = b.impression_id
  LEFT JOIN ct_il AS i
    ON i.ttd_impression_id = b.impression_id
    AND i.campaign_id = b.campaign_id
    AND i.original_aid <> 9090 /* no PSA */
    AND i.advertiser_id <> 9090 /* no PSA */
), 

ct_campaigns AS (
  SELECT
    advertiser_id
    , campaign_id
    , campaign_group_id
    , channel_id
    , CASE WHEN channel_id = 8 AND objective_id = 1 THEN TRUE ELSE FALSE END AS has_tpa
  FROM dw-main-bronze.integrationprod.public_campaigns
), 

ct_stg_margins AS (
  SELECT
    i.impression_id
    , i.advertiser_id
    , i.campaign_id
    , i.time
    , i.creative_id
    , i.unlinked
    , i.media_cost
    , m.take_rate
    , CASE WHEN c.has_tpa IS TRUE THEN m.data_margin ELSE 0 END AS data_margin
    , m.partner_margin
    , m.has_cpm
    , m.target_cpi
    , i.start_ts
  FROM ct_impression_info AS i
  LEFT JOIN ct_campaigns AS c
    ON c.campaign_id = i.campaign_id
  LEFT JOIN dw-main-silver.margins.margin_history AS m
    ON c.advertiser_id = m.advertiser_id
    AND c.campaign_group_id = m.campaign_group_id
    AND c.channel_id = m.channel_id
    AND i.partner_id = m.partner_id
    AND i.time_hr >= m.start_hr
    AND i.time_hr < COALESCE(m.end_hr, CURRENT_TIMESTAMP() + INTERVAL 100 DAY)
  QUALIFY row_number() OVER (
    PARTITION BY i.impression_id
    ORDER BY i.impression_id, i.epoch DESC, i.ttd_impression_id, i.ad_served_id, i.advertiser_id, m.start_hr DESC, COALESCE(m.end_hr, CURRENT_TIMESTAMP() + INTERVAL 100 DAY) DESC
  ) = 1
), 

ct_media_cost_margins AS (
  SELECT
    *
    , media_cost / (1 - (take_rate + partner_margin + data_margin)) AS media_cost_with_margins
  FROM ct_stg_margins
), 

ct_spend_calcs AS (
  SELECT
    cil.*
    , CASE
      WHEN cil.has_cpm IS TRUE
      THEN cil.target_cpi * (1 - (cil.take_rate + cil.data_margin))
      ELSE (cil.media_cost_with_margins * cil.partner_margin) + cil.media_cost
    END AS media_spend
    , CASE
      WHEN cil.has_cpm IS TRUE
      THEN cil.target_cpi * cil.data_margin
      ELSE cil.media_cost_with_margins * cil.data_margin
    END AS data_spend
    , CASE
      WHEN cil.has_cpm IS TRUE
      THEN cil.target_cpi * cil.take_rate
      ELSE cil.media_cost_with_margins * cil.take_rate
    END AS platform_spend
  FROM ct_media_cost_margins AS cil
)
/* final select - current and yesterday */
SELECT
  coalesce(advertiser_id, -4)::INT64 AS advertiser_id
  , coalesce(campaign_id, -4)::INT64 AS campaign_id
  , coalesce(impression_id, '-4')::STRING AS impression_id
  , time::TIMESTAMP AS time
  , coalesce(unlinked, TRUE)::BOOL AS unlinked
  , coalesce(creative_id, -4)::INT64 AS creative_id
  , coalesce(media_cost, 0)::NUMERIC AS media_cost
  , GREATEST(coalesce(media_spend, 0), 0)::BIGNUMERIC AS media_spend
  , GREATEST(coalesce(data_spend, 0), 0)::BIGNUMERIC AS data_spend
  , GREATEST(coalesce(platform_spend, 0), 0)::BIGNUMERIC AS platform_spend
  , 'bwn' AS source
FROM ct_spend_calcs
WHERE
  time >= start_ts /* within param date range */
UNION ALL 
SELECT
  coalesce(cil.advertiser_id, -4)::INT64 AS advertiser_id
  , coalesce(cil.campaign_id, -4)::INT64 AS campaign_id
  , coalesce(cil.impression_id, '-4')::STRING AS impression_id
  , cil.time::TIMESTAMP AS time
  , coalesce(cil.unlinked, TRUE)::BOOL AS unlinked
  , coalesce(cil.creative_id, -4)::INT64 AS creative_id
  , coalesce(cil.media_cost, 0)::NUMERIC AS media_cost
  , GREATEST(coalesce(cil.media_spend, 0), 0)::BIGNUMERIC AS media_spend
  , GREATEST(coalesce(cil.data_spend, 0), 0)::BIGNUMERIC AS data_spend
  , GREATEST(coalesce(cil.platform_spend, 0), 0)::BIGNUMERIC AS platform_spend
  , 'cil' AS source
FROM dw-main-silver.logdata.cost_impression_log AS cil
CROSS JOIN ct_params AS p
WHERE cil.time >= p.start_ts /* already processed cil spend */
AND cil.advertiser_id <> 9090 /* no PSA */
AND cil.unlinked IS FALSE /* only linked spend */
AND not exists (
  SELECT 1 FROM ct_spend_calcs AS s WHERE s.impression_id = cil.impression_id
)
