-- Dynamic param defaults (Mode date params are static-only, so sentinels map in SQL):
--   Period_Start = 1900-01-01 (the default) -> Jan 1 of the CURRENT year; any other date honored.
--   Period_End is CLAMPED to the first day of the current month (exclusive end ->
--   data through the last FULL month); the far-future default (2099-01-01) relies on this.
-- Module 09rt -- PROSPECTING HI recirculation (monthly). NOT obj=4 retargeting:
-- this measures how much of prospecting's HI delivery is re-touching IPs the
-- advertiser already served. Unless DS16 (net-new gate) is on, prospecting will
-- re-serve previously-touched IPs -- the VV (pageview) and conversion excludes
-- only remove visitors/converters. A client that scales spend while holding
-- HHST=10000 exhausts the net-new HI pool and ends up re-touching ~99%.
-- Rules (per Malachi): HI = household_score = 10000 ON THE BID -- the score
-- logged with each impression, NOT a monthly status. With HHST held at 10000 the
-- bidder only serves an IP while it scores 10000 (an IP that fell to 8000 is not
-- served unless the gate fell too). Re-touch = the IP's first-ever 10000 bid
-- predates this month -- 10000 at bid time on both touches. Month grain is an
-- approximation: scores carry a 30-day TTL that does not align to calendar
-- months, and within-month re-serves are not split out (they show in frequency).
-- Scores logged since 2025-06 (earlier months read "no score data" in the
-- render). Scope = ALL prospecting stages (obj 1/5/6 -- S1 + MT-S2 + MT-S3);
-- retargeting (4) and Ego (7) out. RTC serves COUNT here (an RTC-conquest serve
-- is still a targeted IP / a touch -- per Malachi; module 06's score DISTRIBUTION
-- stays RTC-excluded, different question). Column aliases keep the legacy
-- rt_ prefix so the HTML resolver and render plumbing are unchanged.
--   rt_reach / rt_imps / rt_freq_median -> prospecting reach + MEDIAN imps/IP
--   rt_hi_reach       -> IPs served at 10000 this month
--   rt_new_hi         -> first-ever month the IP was served at 10000
--   rt_returning_hi   -> re-touched: also served at 10000 in an earlier month
--   rt_brand_new      -> first-ever contact on ANY campaign is this month
--   rt_prosp_first    -> first month PROSPECTING served this IP (feeds the
--                        all-IP recirculation tab's cumulative-distinct line)
WITH prosp AS (
  SELECT campaign_id FROM `dw-main-bronze.integrationprod.campaigns`
  WHERE advertiser_id = {{ Advertiser_ID }} AND deleted = FALSE
    AND objective_id IN (1, 5, 6)
),
all_base AS (
  -- advertiser-wide (ANY campaign): supplies first-ever-contact + prior-10000 history
  SELECT ip, FORMAT_DATE('%Y-%m', DATE(time)) AS mo, campaign_id,
         household_score AS hs
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE advertiser_id = {{ Advertiser_ID }}
    AND time >= TIMESTAMP(DATE_SUB(IF(DATE(LEFT('{{ Period_Start }}', 10)) = DATE '1900-01-01', DATE_TRUNC(CURRENT_DATE(), YEAR), DATE(LEFT('{{ Period_Start }}', 10))), INTERVAL 1 YEAR))
    AND time <  TIMESTAMP(LEAST(DATE(LEFT('{{ Period_End }}', 10)), DATE_TRUNC(CURRENT_DATE(), MONTH)))
    AND ip IS NOT NULL AND ip != '0.0.0.0'
),
first_seen AS (SELECT ip, MIN(mo) AS first_mo FROM all_base GROUP BY ip),
hi_first AS (
  -- first month each IP had a bid at a FULL 10000 (score on the impression row)
  SELECT ip, MIN(mo) AS first_hi_mo
  FROM all_base
  WHERE hs = 10000
  GROUP BY ip
),
pim AS (
  -- prospecting (obj 1/5/6) delivery per ip x month
  SELECT ip, mo, COUNT(*) AS imps, LOGICAL_OR(hs = 10000) AS hi_now
  FROM all_base
  WHERE campaign_id IN (SELECT campaign_id FROM prosp)
  GROUP BY ip, mo
),
prosp_first AS (SELECT ip, MIN(mo) AS first_p_mo FROM pim GROUP BY ip)
SELECT
  pim.mo                                                  AS rt_mo,
  COUNT(*)                                                AS rt_reach,
  SUM(pim.imps)                                           AS rt_imps,
  APPROX_QUANTILES(pim.imps, 100)[OFFSET(50)]             AS rt_freq_median,
  COUNTIF(pim.hi_now)                                     AS rt_hi_reach,
  COUNTIF(pim.hi_now AND hf.first_hi_mo = pim.mo)         AS rt_new_hi,
  COUNTIF(pim.hi_now AND hf.first_hi_mo < pim.mo)         AS rt_returning_hi,
  COUNTIF(fs.first_mo = pim.mo)                           AS rt_brand_new,
  COUNTIF(pf.first_p_mo = pim.mo)                         AS rt_prosp_first,
  DATE_SUB(IF(DATE(LEFT('{{ Period_Start }}', 10)) = DATE '1900-01-01', DATE_TRUNC(CURRENT_DATE(), YEAR), DATE(LEFT('{{ Period_Start }}', 10))), INTERVAL 1 YEAR)   AS p1_start,
  DATE_SUB(LEAST(DATE(LEFT('{{ Period_End }}', 10)), DATE_TRUNC(CURRENT_DATE(), MONTH)),   INTERVAL 1 YEAR)   AS p1_end,
  IF(DATE(LEFT('{{ Period_Start }}', 10)) = DATE '1900-01-01', DATE_TRUNC(CURRENT_DATE(), YEAR), DATE(LEFT('{{ Period_Start }}', 10)))                              AS p2_start,
  LEAST(DATE(LEFT('{{ Period_End }}', 10)), DATE_TRUNC(CURRENT_DATE(), MONTH))                                AS p2_end
FROM pim
LEFT JOIN hi_first hf USING (ip)
LEFT JOIN first_seen fs USING (ip)
LEFT JOIN prosp_first pf USING (ip)
GROUP BY rt_mo
ORDER BY rt_mo
