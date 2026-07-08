-- Module 09rt -- RETARGETING reach, frequency & HI recirculation (monthly).
-- Per month over retargeting delivery (objective_id = 4):
--   rt_reach / rt_imps          -> frequency (imps per IP)
--   rt_hi_reach                 -> RT-served IPs that scored HI (>= 8001) on ANY
--                                  impression in the window. RT rows carry NO
--                                  household_score — scores ride prospecting — so HI
--                                  status comes from the IP's scored impressions.
--   rt_new_hi / rt_returning_hi -> HI IPs retargeted for the FIRST month vs again
--   rt_brand_new                -> IPs whose FIRST month served by this advertiser on
--                                  ANY campaign is this month (completely new)
-- Scores exist since 2025-06 only. Month gaps = months with no RT delivery.
WITH rt AS (
  SELECT campaign_id FROM `dw-main-bronze.integrationprod.campaigns`
  WHERE advertiser_id = {{ Advertiser_ID }} AND deleted = FALSE AND objective_id = 4
),
all_base AS (
  -- advertiser-wide (any campaign): supplies scores + first-ever-contact month
  SELECT ip, FORMAT_DATE('%Y-%m', DATE(time)) AS mo, campaign_id, household_score AS hs
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE advertiser_id = {{ Advertiser_ID }}
    AND time >= TIMESTAMP(DATE_SUB(DATE('{{ Period_Start }}'), INTERVAL 1 YEAR))
    AND time <  TIMESTAMP(DATE('{{ Period_End }}'))
    AND ip IS NOT NULL AND ip != '0.0.0.0'
),
first_seen AS (SELECT ip, MIN(mo) AS first_mo FROM all_base GROUP BY ip),
hi_ips AS (
  SELECT DISTINCT ip FROM all_base WHERE hs = 10000 OR hs BETWEEN 8001 AND 9999
),
im AS (
  SELECT ip, mo, COUNT(*) AS imps
  FROM all_base
  WHERE campaign_id IN (SELECT campaign_id FROM rt)
  GROUP BY ip, mo
),
rt_first AS (SELECT ip, MIN(mo) AS first_rt_mo FROM im GROUP BY ip)
SELECT
  im.mo                                                         AS rt_mo,
  COUNT(*)                                                      AS rt_reach,
  SUM(im.imps)                                                  AS rt_imps,
  COUNTIF(h.ip IS NOT NULL)                                     AS rt_hi_reach,
  COUNTIF(h.ip IS NOT NULL AND rf.first_rt_mo = im.mo)          AS rt_new_hi,
  COUNTIF(h.ip IS NOT NULL AND rf.first_rt_mo < im.mo)          AS rt_returning_hi,
  COUNTIF(fs.first_mo = im.mo)                                  AS rt_brand_new,
  DATE_SUB(DATE('{{ Period_Start }}'), INTERVAL 1 YEAR)         AS p1_start,
  DATE_SUB(DATE('{{ Period_End }}'),   INTERVAL 1 YEAR)         AS p1_end,
  DATE('{{ Period_Start }}')                                    AS p2_start,
  DATE('{{ Period_End }}')                                      AS p2_end
FROM im
LEFT JOIN hi_ips h USING (ip)
LEFT JOIN rt_first rf USING (ip)
LEFT JOIN first_seen fs USING (ip)
GROUP BY im.mo
ORDER BY im.mo
