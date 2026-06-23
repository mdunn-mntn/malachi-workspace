-- TI-1044: ghost-WIN simulation inputs. Per ghost-only household: # ghost bids + outcomes.
-- + total real bid events (win-rate denominator). win = sample ghost bids at per-bid win rate.
DECLARE d0 DATE DEFAULT '2026-06-13'; DECLARE d1 DATE DEFAULT '2026-06-22'; DECLARE o1 DATE DEFAULT '2026-06-23';
WITH bp AS (
  SELECT ip, threshold_failure_reasons AS r
  FROM `dw-main-bronze.raw.bid_price_log`
  WHERE advertiser_id=51660 AND is_ctv AND DATE(time) BETWEEN d0 AND d1 AND ip IS NOT NULL AND ip!=''
),
ghost AS (SELECT ip, COUNT(*) AS n FROM bp WHERE r='ghostBid' GROUP BY ip),
served AS (SELECT DISTINCT ip FROM `dw-main-silver.logdata.cost_impression_log`
           WHERE advertiser_id=51660 AND DATE(time) BETWEEN d0 AND d1 AND ip IS NOT NULL AND ip!=''),
go AS (SELECT g.ip, g.n FROM ghost g LEFT JOIN served s USING(ip) WHERE s.ip IS NULL),
gv AS (SELECT DISTINCT ip FROM `dw-main-silver.logdata.guid_log`
       WHERE advertiser_id=51660 AND DATE(time) BETWEEN d0 AND o1 AND ip IS NOT NULL AND ip!=''),
cv AS (SELECT DISTINCT ip FROM `dw-main-silver.logdata.conversion_log`
       WHERE advertiser_id=51660 AND DATE(time) BETWEEN d0 AND o1 AND ip IS NOT NULL AND ip!='')
SELECT LEAST(go.n, 40) AS nbid_bucket,
       COUNT(*) AS households,
       COUNTIF(gv.ip IS NOT NULL) AS visitors,
       COUNTIF(cv.ip IS NOT NULL) AS converters,
       (SELECT COUNTIF(r IS NULL OR r='') FROM bp) AS real_bid_events
FROM go LEFT JOIN gv USING(ip) LEFT JOIN cv USING(ip)
GROUP BY 1 ORDER BY 1;
