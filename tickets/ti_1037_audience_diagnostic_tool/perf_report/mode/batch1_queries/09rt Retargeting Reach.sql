-- Module 09rt -- RETARGETING reach, frequency & HI recirculation (monthly).
-- Per month over retargeting delivery (objective_id = 4):
--   rt_reach / rt_imps          -> frequency (imps per IP)
--   rt_hi_reach                 -> RT-served IPs KNOWN to be HI as of that month:
--                                  the IP scored >= 8001 on some impression AT OR
--                                  BEFORE the month (no borrowing from the future —
--                                  the SAME rule as the score-tier charts). RT rows
--                                  carry no score; scores ride prospecting and are
--                                  logged since 2025-06, so HI metrics start there
--                                  and earlier months read 0 (render shows "no
--                                  score data", matching module 06).
--   rt_new_hi / rt_returning_hi -> first month retargeted-while-known-HI vs again
--   rt_brand_new                -> IPs whose FIRST month served by this advertiser on
--                                  ANY campaign is this month (completely new)
-- Month gaps = months with no RT delivery.
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
hi_scored AS (
  -- first month each IP was OBSERVED scoring HI (scores exist since 2025-06)
  SELECT ip, MIN(mo) AS first_hi_mo
  FROM all_base
  WHERE hs = 10000 OR hs BETWEEN 8001 AND 9999
  GROUP BY ip
),
im AS (
  SELECT ip, mo, COUNT(*) AS imps
  FROM all_base
  WHERE campaign_id IN (SELECT campaign_id FROM rt)
  GROUP BY ip, mo
),
rt_hi_first AS (
  -- first month the IP was retargeted while already known-HI
  SELECT im.ip, MIN(im.mo) AS first_hi_rt_mo
  FROM im
  JOIN hi_scored h ON h.ip = im.ip AND h.first_hi_mo <= im.mo
  GROUP BY im.ip
)
SELECT
  im.mo                                                          AS rt_mo,
  COUNT(*)                                                       AS rt_reach,
  SUM(im.imps)                                                   AS rt_imps,
  APPROX_QUANTILES(im.imps, 100)[OFFSET(50)]                     AS rt_freq_median,
  COUNTIF(h.first_hi_mo IS NOT NULL AND h.first_hi_mo <= im.mo)  AS rt_hi_reach,
  COUNTIF(rhf.first_hi_rt_mo = im.mo)                            AS rt_new_hi,
  COUNTIF(rhf.first_hi_rt_mo < im.mo)                            AS rt_returning_hi,
  COUNTIF(fs.first_mo = im.mo)                                   AS rt_brand_new,
  DATE_SUB(DATE('{{ Period_Start }}'), INTERVAL 1 YEAR)          AS p1_start,
  DATE_SUB(DATE('{{ Period_End }}'),   INTERVAL 1 YEAR)          AS p1_end,
  DATE('{{ Period_Start }}')                                     AS p2_start,
  DATE('{{ Period_End }}')                                       AS p2_end
FROM im
LEFT JOIN hi_scored h USING (ip)
LEFT JOIN rt_hi_first rhf USING (ip)
LEFT JOIN first_seen fs USING (ip)
GROUP BY im.mo
ORDER BY im.mo
