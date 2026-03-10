WITH bid AS (
  SELECT ad_served_id, bid_ip
  FROM `dw-main-silver.logdata.event_log`
  WHERE DATE(time) BETWEEN "2026-03-03" AND "2026-03-09"
    AND bid_ip IS NOT NULL
    AND event_type_raw IN ("vast_start","vast_impression")
  GROUP BY 1,2
),
vast_start AS (
  SELECT ad_served_id, ip AS vast_start_ip
  FROM `dw-main-silver.logdata.event_log`
  WHERE DATE(time) BETWEEN "2026-03-03" AND "2026-03-09"
    AND event_type_raw = "vast_start"
  QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),
vast_imp AS (
  SELECT ad_served_id, ip AS vast_impression_ip
  FROM `dw-main-silver.logdata.event_log`
  WHERE DATE(time) BETWEEN "2026-03-03" AND "2026-03-09"
    AND event_type_raw = "vast_impression"
  QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
)
SELECT
  COUNT(*) AS total,
  COUNTIF(b.bid_ip = vs.vast_start_ip) AS bid_eq_start,
  COUNTIF(b.bid_ip = vi.vast_impression_ip) AS bid_eq_imp,
  COUNTIF(b.bid_ip = vs.vast_start_ip AND b.bid_ip = vi.vast_impression_ip) AS bid_eq_both,
  COUNTIF(b.bid_ip = vs.vast_start_ip AND b.bid_ip != vi.vast_impression_ip) AS bid_eq_start_only,
  COUNTIF(b.bid_ip != vs.vast_start_ip AND b.bid_ip = vi.vast_impression_ip) AS bid_eq_imp_only,
  COUNTIF(b.bid_ip != vs.vast_start_ip AND b.bid_ip != vi.vast_impression_ip) AS bid_eq_neither
FROM bid b
LEFT JOIN vast_start vs ON b.ad_served_id = vs.ad_served_id
LEFT JOIN vast_imp vi ON b.ad_served_id = vi.ad_served_id
WHERE vs.vast_start_ip IS NOT NULL AND vi.vast_impression_ip IS NOT NULL
