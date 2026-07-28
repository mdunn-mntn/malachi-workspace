-- AUDI-1173 refined sizing / TOTAL-visit reach-frequency curve (the headline honesty fix).
-- Companion to audi_1173_delivered_freq_curve.sql (that curve uses ATTRIBUTED visits = ui_visits,
--   impression-anchored, last-touch-confounded). This curve uses ATTRIBUTION-INDEPENDENT total site
--   visits from logdata.guid_log (MNTN's own site pixel; a row = a page view regardless of whether MNTN
--   ever served an ad). Source-of-truth: enriched.lift__ghost_bid_visits uses guid_log for exactly this.
-- Household grain, delivered-frequency definition, stage split, shared-IP purge, buckets, and exclusions
--   are IDENTICAL to the attributed curve, so n_households matches the attributed JSON bucket-for-bucket
--   and the two curves overlay apples-to-apples.
-- TOTAL visits are pre-aggregated to (advertiser_id, ip) BEFORE the join -> NO fan-out (a household with
--   many page views contributes ONE gv row). visit = a page-view day (COUNT DISTINCT DATE(time)); pageviews
--   also emitted. guid_log has NO campaign_id -> visits attach to advertiser x ip (household), not stage;
--   per-stage total is "total visits of households touched by that stage"; the COMBINED scope is the headline.
-- IP format verified 2026-06-01: CIL.ip plain (0 slash), guid_log.ip plain (1 of 315M has '/'); strip guid side defensively.
-- Window: impressions 2026-05-15..2026-06-13 (30d, defines households+freq); TOTAL visits 2026-05-15..2026-07-28
--   (same calendar visit window as the attributed curve's 45d tail, so the two curves are window-matched).
-- Excludes WGU (31357) and AID 90 (PSA). Spend = media_spend+data_spend+platform_spend.
-- campaign->objective from the public_campaigns dim join, never cost_impression_log.model_params.
WITH camp AS (
  SELECT campaign_id, objective_id
  FROM `dw-main-bronze.integrationprod.public_campaigns`
  WHERE deleted = FALSE AND is_test = FALSE
),
imp AS (                                          -- the CIL scan (households, delivered freq, spend, stage)
  SELECT c.ip, c.advertiser_id, c.guid,
         (c.media_spend + c.data_spend + c.platform_spend) AS spend,
         CASE WHEN camp.objective_id IN (1,5,6) THEN 'prospecting'
              WHEN camp.objective_id = 4        THEN 'retargeting'
              ELSE 'other' END                   AS stage
  FROM `dw-main-silver.logdata.cost_impression_log` c
  JOIN camp USING (campaign_id)
  WHERE DATE(c.time) BETWEEN '2026-05-15' AND '2026-06-13'
    AND c.advertiser_id NOT IN (31357, 90)
    AND c.ip IS NOT NULL AND c.ip <> '0.0.0.0'
),
gv AS (                                           -- guid_log TOTAL visits, pre-aggregated to (advertiser_id, ip): NO fan-out
  SELECT advertiser_id, split(ip,'/')[OFFSET(0)] AS ip,
         COUNT(*)                    AS pageviews,
         COUNT(DISTINCT DATE(time))  AS visit_days
  FROM `dw-main-silver.logdata.guid_log`
  WHERE DATE(time) BETWEEN '2026-05-15' AND '2026-07-28'
    AND advertiser_id NOT IN (31357, 90)
    AND ip IS NOT NULL AND ip <> '0.0.0.0'
  GROUP BY 1, 2
),
g AS (                                            -- impression household aggregate (ip x adv x stage x guid)
  SELECT ip, advertiser_id, stage, guid,
         COUNT(*) AS imps, SUM(spend) AS spend
  FROM imp
  GROUP BY ip, advertiser_id, stage, guid
),
ip_flag AS (                                      -- per-IP shared/NAT flag over the window (IDENTICAL to attributed curve)
  SELECT ip,
         (COUNT(DISTINCT guid) >= 51 OR COUNT(DISTINCT advertiser_id) >= 121 OR SUM(imps) >= 501) AS shared_ip
  FROM g GROUP BY ip
),
hh_all AS (                                       -- households at per-stage AND combined scope in one pass
  SELECT scope, ip, advertiser_id,
         SUM(imps) AS freq, SUM(spend) AS spend
  FROM g, UNNEST([g.stage, 'combined']) AS scope
  GROUP BY scope, ip, advertiser_id
),
hh AS (                                           -- attach total visits (1:1 on adv x ip) + shared flag
  SELECT h.scope, h.ip, h.advertiser_id, h.freq, h.spend, f.shared_ip,
         COALESCE(v.pageviews, 0)  AS tot_pageviews,
         COALESCE(v.visit_days, 0) AS tot_visit_days,
         (v.advertiser_id IS NOT NULL) AS tot_visited
  FROM hh_all h
  JOIN ip_flag f USING (ip)
  LEFT JOIN gv v ON v.advertiser_id = h.advertiser_id AND v.ip = h.ip
),
bucketed AS (
  SELECT scope, p.purge, freq, spend, tot_pageviews, tot_visit_days, tot_visited,
         CASE WHEN freq=1 THEN '01_freq_1'      WHEN freq<=3  THEN '02_freq_2-3'
              WHEN freq<=7 THEN '03_freq_4-7'   WHEN freq<=12 THEN '04_freq_8-12'
              WHEN freq<=20 THEN '05_freq_13-20' WHEN freq<=40 THEN '06_freq_21-40'
              ELSE '07_freq_41+' END AS freq_bucket
  FROM hh, UNNEST([STRUCT('1_raw' AS purge, TRUE AS keep),
                   STRUCT('2_purged',       NOT shared_ip)]) AS p
  WHERE p.keep
)
SELECT scope, purge, freq_bucket,
  COUNT(*)                                               AS n_households,
  SUM(freq)                                              AS impressions,
  ROUND(SUM(spend),2)                                    AS spend,
  ROUND(AVG(freq),3)                                     AS avg_freq,
  SUM(tot_visit_days)                                    AS total_visit_days,
  SUM(tot_pageviews)                                     AS total_pageviews,
  ROUND(100*COUNTIF(tot_visited)/COUNT(*),4)             AS tot_hh_visit_rate_pct,    -- HEADLINE: % hh with any total visit
  ROUND(SAFE_DIVIDE(SUM(tot_visit_days),COUNT(*)),5)     AS total_visits_per_hh,      -- HEADLINE: total visits/hh (visit-days)
  ROUND(SAFE_DIVIDE(SUM(tot_pageviews),COUNT(*)),5)      AS total_pageviews_per_hh,
  ROUND(SAFE_DIVIDE(SUM(spend),COUNT(*)),4)              AS cost_per_hh,
  ROUND(SAFE_DIVIDE(SUM(spend),NULLIF(SUM(tot_visit_days),0)),4) AS cost_per_total_visit
FROM bucketed
GROUP BY scope, purge, freq_bucket
ORDER BY scope, purge, freq_bucket
