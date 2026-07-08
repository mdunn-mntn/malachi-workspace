-- Module 09rt -- PROSPECTING HI recirculation (monthly). NOT obj=4 retargeting:
-- this measures how much of prospecting's HI delivery is re-touching IPs the
-- advertiser already served. Unless DS16 (net-new gate) is on, prospecting will
-- re-serve previously-touched IPs -- the VV (pageview) and conversion excludes
-- only remove visitors/converters. A client that scales spend while holding
-- HHST=10000 exhausts the net-new HI pool and ends up re-touching ~99%.
-- Rules (per Malachi): HI counts ONLY at a FULL 10000, and re-touch requires
-- 10000 BOTH times -- served at 10000 this month AND at 10000 in a prior month
-- (no future borrowing; scores logged since 2025-06, earlier months read
-- "no score data" in the render). Scope mirrors module 06: obj=1 / funnel=1,
-- RTC-excluded. Column aliases keep the legacy rt_ prefix so the HTML resolver
-- and render plumbing are unchanged.
--   rt_reach / rt_imps / rt_freq_median -> prospecting reach + MEDIAN imps/IP
--   rt_hi_reach       -> IPs served at 10000 this month
--   rt_new_hi         -> first-ever month the IP was served at 10000
--   rt_returning_hi   -> re-touched: also served at 10000 in an earlier month
--   rt_brand_new      -> first-ever contact on ANY campaign is this month
WITH prosp AS (
  SELECT campaign_id FROM `dw-main-bronze.integrationprod.campaigns`
  WHERE advertiser_id = {{ Advertiser_ID }} AND deleted = FALSE
    AND objective_id = 1 AND funnel_level = 1
),
all_base AS (
  -- advertiser-wide (ANY campaign): supplies first-ever-contact + prior-10000 history
  SELECT ip, FORMAT_DATE('%Y-%m', DATE(time)) AS mo, campaign_id,
         household_score AS hs,
         (model_params IS NULL OR model_params NOT LIKE '%realtime_conquest_score=10000%') AS not_rtc
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE advertiser_id = {{ Advertiser_ID }}
    AND time >= TIMESTAMP(DATE_SUB(DATE('{{ Period_Start }}'), INTERVAL 1 YEAR))
    AND time <  TIMESTAMP(DATE('{{ Period_End }}'))
    AND ip IS NOT NULL AND ip != '0.0.0.0'
),
first_seen AS (SELECT ip, MIN(mo) AS first_mo FROM all_base GROUP BY ip),
hi_first AS (
  -- first month each IP was served at a FULL 10000 (RTC rows excluded so a
  -- realtime-conquest 10000 does not mark an IP as HI)
  SELECT ip, MIN(mo) AS first_hi_mo
  FROM all_base
  WHERE hs = 10000 AND not_rtc
  GROUP BY ip
),
pim AS (
  -- prospecting (obj=1, funnel=1, RTC-excluded) delivery per ip x month
  SELECT ip, mo, COUNT(*) AS imps, LOGICAL_OR(hs = 10000) AS hi_now
  FROM all_base
  WHERE campaign_id IN (SELECT campaign_id FROM prosp) AND not_rtc
  GROUP BY ip, mo
)
SELECT
  pim.mo                                                  AS rt_mo,
  COUNT(*)                                                AS rt_reach,
  SUM(pim.imps)                                           AS rt_imps,
  APPROX_QUANTILES(pim.imps, 100)[OFFSET(50)]             AS rt_freq_median,
  COUNTIF(pim.hi_now)                                     AS rt_hi_reach,
  COUNTIF(pim.hi_now AND hf.first_hi_mo = pim.mo)         AS rt_new_hi,
  COUNTIF(pim.hi_now AND hf.first_hi_mo < pim.mo)         AS rt_returning_hi,
  COUNTIF(fs.first_mo = pim.mo)                           AS rt_brand_new,
  DATE_SUB(DATE('{{ Period_Start }}'), INTERVAL 1 YEAR)   AS p1_start,
  DATE_SUB(DATE('{{ Period_End }}'),   INTERVAL 1 YEAR)   AS p1_end,
  DATE('{{ Period_Start }}')                              AS p2_start,
  DATE('{{ Period_End }}')                                AS p2_end
FROM pim
LEFT JOIN hi_first hf USING (ip)
LEFT JOIN first_seen fs USING (ip)
GROUP BY rt_mo
ORDER BY rt_mo
