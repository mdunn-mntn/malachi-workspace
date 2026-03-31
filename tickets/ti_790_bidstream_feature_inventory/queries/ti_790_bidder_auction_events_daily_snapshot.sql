-- TI-790: bidder_auction_events daily IP snapshot
-- Aggregates per IP per day from bidder_auction_events (auctions we saw but didn't bid on)
-- Net-new signals not in guid_log or augmentor_log:
--   content_genre (87% fill), device_make (90%), content_series (37%),
--   content_channel (36%), publisher_name (100%), geo_zip (95%)
--
-- Source: bronze.raw.bidder_auction_events (90-day TTL)
-- Partition: _PARTITIONTIME (HOUR). Use exact hour timestamp for partition pruning.
-- Cost: ~17 GB per hour. Use single-hour partitions.
-- Tested: 2026-03-31

WITH bae_events AS (
  SELECT
    device_ip AS ip,
    DATE(_PARTITIONTIME) AS event_date,
    -- Content genre (breakout feature — 87% fill)
    -- Normalize: lowercase, split comma-delimited, strip prefixes
    CASE
      WHEN LOWER(content_genre) LIKE 'genre_%' THEN REPLACE(LOWER(content_genre), 'genre_', '')
      ELSE LOWER(SPLIT(content_genre, ',')[SAFE_OFFSET(0)])
    END AS genre_primary,
    content_genre AS genre_raw,
    -- Device make (90% fill — demographic proxy)
    UPPER(device_make) AS device_make,
    -- Content series (37% fill — specific show)
    content_series,
    -- Publisher (100% fill, 301 values)
    publisher_name,
    -- Geo
    geo_zip,
    geo_country,
    -- Auction metadata
    auction_dropped_reason
  FROM `dw-main-bronze.raw.bidder_auction_events`
  -- Replace with target hour partition
  WHERE _PARTITIONTIME = TIMESTAMP('2026-03-30 13:00:00')
    AND device_ip IS NOT NULL AND device_ip != ''
)

SELECT
  ip,
  event_date,
  COUNT(*) AS n_dropped_auctions,

  -- Content genre (net-new: what content they watch — not in augmentor_log or guid_log)
  COUNTIF(genre_primary IS NOT NULL AND genre_primary NOT IN ('', ' ')) AS n_events_with_genre,
  ROUND(COUNTIF(genre_primary IS NOT NULL AND genre_primary NOT IN ('', ' ')) / COUNT(*), 4) AS pct_events_with_genre,
  COUNT(DISTINCT CASE WHEN genre_primary NOT IN ('', ' ') THEN genre_primary END) AS n_distinct_genres,

  -- Top genre percentages (most valuable for vertical classification)
  ROUND(COUNTIF(genre_primary = 'entertainment') / NULLIF(COUNTIF(genre_primary IS NOT NULL AND genre_primary NOT IN ('', ' ')), 0), 4) AS pct_genre_entertainment,
  ROUND(COUNTIF(genre_primary = 'news') / NULLIF(COUNTIF(genre_primary IS NOT NULL AND genre_primary NOT IN ('', ' ')), 0), 4) AS pct_genre_news,
  ROUND(COUNTIF(genre_primary = 'drama') / NULLIF(COUNTIF(genre_primary IS NOT NULL AND genre_primary NOT IN ('', ' ')), 0), 4) AS pct_genre_drama,
  ROUND(COUNTIF(genre_primary = 'comedy') / NULLIF(COUNTIF(genre_primary IS NOT NULL AND genre_primary NOT IN ('', ' ')), 0), 4) AS pct_genre_comedy,
  ROUND(COUNTIF(genre_primary IN ('sports', 'sport')) / NULLIF(COUNTIF(genre_primary IS NOT NULL AND genre_primary NOT IN ('', ' ')), 0), 4) AS pct_genre_sports,
  ROUND(COUNTIF(genre_primary IN ('reality', 'reality-tv')) / NULLIF(COUNTIF(genre_primary IS NOT NULL AND genre_primary NOT IN ('', ' ')), 0), 4) AS pct_genre_reality,
  ROUND(COUNTIF(genre_primary = 'documentary') / NULLIF(COUNTIF(genre_primary IS NOT NULL AND genre_primary NOT IN ('', ' ')), 0), 4) AS pct_genre_documentary,
  ROUND(COUNTIF(genre_primary IN ('crime', 'thriller')) / NULLIF(COUNTIF(genre_primary IS NOT NULL AND genre_primary NOT IN ('', ' ')), 0), 4) AS pct_genre_crime_thriller,

  -- Device make (net-new: which physical device — demographic proxy)
  COUNT(DISTINCT CASE WHEN device_make != '' THEN device_make END) AS n_distinct_device_makes,
  MAX(CASE WHEN device_make = 'ROKU' THEN 1 ELSE 0 END) AS has_roku_device,
  MAX(CASE WHEN device_make = 'SAMSUNG' THEN 1 ELSE 0 END) AS has_samsung_device,
  MAX(CASE WHEN device_make = 'LG' THEN 1 ELSE 0 END) AS has_lg_device,
  MAX(CASE WHEN device_make = 'VIZIO' THEN 1 ELSE 0 END) AS has_vizio_device,
  MAX(CASE WHEN device_make = 'AMAZON' THEN 1 ELSE 0 END) AS has_amazon_device,
  MAX(CASE WHEN device_make = 'APPLE' THEN 1 ELSE 0 END) AS has_apple_device,

  -- Publisher diversity
  COUNT(DISTINCT publisher_name) AS n_distinct_publishers,

  -- Content series (specific show — after filtering garbage)
  COUNT(DISTINCT CASE
    WHEN content_series != ''
      AND content_series NOT LIKE '%{{%'
      AND LENGTH(content_series) != 32  -- filter MD5 hashes
    THEN content_series
  END) AS n_distinct_clean_series,

  -- Geo (ZIP available at 95% — higher than augmentor_log)
  COUNT(DISTINCT CASE WHEN geo_zip != '' THEN geo_zip END) AS n_distinct_zips

FROM bae_events
GROUP BY ip, event_date
;
