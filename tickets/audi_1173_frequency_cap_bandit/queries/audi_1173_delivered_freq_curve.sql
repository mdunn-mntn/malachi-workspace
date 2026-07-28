-- AUDI-1173 refined sizing / Q1: DELIVERED reach-frequency curve, household grain = (ip, advertiser_id).
-- Refinements vs Phase-0:
--   * DELIVERED frequency = COUNT(*) of served impressions per (ip, advertiser_id) over the window (not configured).
--   * Combined prospecting + retargeting + all stages, stage from public_campaigns.objective_id
--       (1/5/6 -> prospecting/last_tv_touch; 4 -> retargeting/visits; else -> other). Reported per-stage AND combined.
--   * >=45d visit tail: ui_visits joined on impression_id (the .steelhouse composite, NOT ad_served_id);
--       tail governed by ui_visits.time extending ~45d past the last impression day (NOT gated on visit_day).
--       Visits deduped on (advertiser_id, guid, epoch, impression_id). Both source_types self-align via impression_id.
--   * Shared-IP purge: raw AND purged curves. shared_ip = ndev>=51 OR nadv>=121 OR nimp>=501 (per-IP over window).
--   * Headline = household TOTAL visits + cost-per-household + hh_visit_rate by freq bucket.
--       visits_per_1k_imps is emitted but is a last-touch ARTIFACT (see summary 4d) — never the headline.
--   * Over-cap spend = spend on the (cap+1)th+ impression per household, cap in {3,8,12}
--       ("gross addressable, before incrementality" — NOT savings).
-- Window: impressions 2026-05-15..2026-06-13 (30d); visits 2026-05-15..2026-07-28 (45d tail past last imp day).
-- Excludes WGU (31357) and AID 90 (PSA). Spend = media_spend+data_spend+platform_spend.
-- Single CIL scan: campaign_group_id/objective_id from the public_campaigns dim join, never cost_impression_log.model_params.
WITH camp AS (
  SELECT campaign_id, objective_id
  FROM `dw-main-bronze.integrationprod.public_campaigns`
  WHERE deleted = FALSE AND is_test = FALSE
),
imp AS (                                          -- the one CIL scan
  SELECT c.ip, c.advertiser_id, c.impression_id, c.guid,
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
vis AS (                                          -- deduped visits, count per attributed impression_id
  SELECT impression_id, COUNT(*) AS nvis
  FROM (
    SELECT DISTINCT advertiser_id, guid, epoch, impression_id
    FROM `dw-main-silver.summarydata.ui_visits`
    WHERE DATE(time) BETWEEN '2026-05-15' AND '2026-07-28'
      AND impression_id IS NOT NULL
      AND source_type IN ('last_tv_touch_visits','visits')
  )
  GROUP BY impression_id
),
impv AS (                                         -- impression grain + visit count (1:1 on impression_id)
  SELECT i.ip, i.advertiser_id, i.stage, i.guid, i.spend, COALESCE(v.nvis,0) AS nvis
  FROM imp i LEFT JOIN vis v USING (impression_id)
),
g AS (                                            -- reusable small aggregate (ip x adv x stage x guid)
  SELECT ip, advertiser_id, stage, guid,
         COUNT(*) AS imps, SUM(spend) AS spend, SUM(nvis) AS visits
  FROM impv
  GROUP BY ip, advertiser_id, stage, guid
),
ip_flag AS (                                      -- per-IP shared/NAT flag over the whole window
  SELECT ip,
         (COUNT(DISTINCT guid) >= 51 OR COUNT(DISTINCT advertiser_id) >= 121 OR SUM(imps) >= 501) AS shared_ip
  FROM g GROUP BY ip
),
hh_all AS (                                       -- households at per-stage AND combined scope in one pass
  SELECT scope, ip, advertiser_id,
         SUM(imps) AS freq, SUM(spend) AS spend, SUM(visits) AS visits
  FROM g, UNNEST([g.stage, 'combined']) AS scope
  GROUP BY scope, ip, advertiser_id
),
hh AS (
  SELECT h.scope, h.ip, h.advertiser_id, h.freq, h.spend, h.visits, f.shared_ip
  FROM hh_all h JOIN ip_flag f USING (ip)
),
bucketed AS (
  SELECT scope, p.purge, shared_ip, freq, spend, visits, (visits > 0) AS visited,
         CASE WHEN freq=1 THEN '01_freq_1'      WHEN freq<=3  THEN '02_freq_2-3'
              WHEN freq<=7 THEN '03_freq_4-7'   WHEN freq<=12 THEN '04_freq_8-12'
              WHEN freq<=20 THEN '05_freq_13-20' WHEN freq<=40 THEN '06_freq_21-40'
              ELSE '07_freq_41+' END AS freq_bucket
  FROM hh, UNNEST([STRUCT('1_raw' AS purge, TRUE AS keep),
                   STRUCT('2_purged',       NOT shared_ip)]) AS p
  WHERE p.keep
)
SELECT scope, purge, freq_bucket,
  COUNT(*)                                              AS n_households,
  SUM(freq)                                             AS impressions,
  ROUND(100*SAFE_DIVIDE(SUM(freq),  SUM(SUM(freq))   OVER(PARTITION BY scope,purge)),3) AS imp_share_pct,
  ROUND(SUM(spend),2)                                   AS spend,
  ROUND(100*SAFE_DIVIDE(SUM(spend), SUM(SUM(spend))  OVER(PARTITION BY scope,purge)),3) AS spend_share_pct,
  SUM(visits)                                           AS visits,
  ROUND(100*SAFE_DIVIDE(SUM(visits),SUM(SUM(visits)) OVER(PARTITION BY scope,purge)),3) AS visit_share_pct,
  ROUND(AVG(freq),3)                                    AS avg_freq,
  ROUND(100*COUNTIF(visited)/COUNT(*),4)                AS hh_visit_rate_pct,     -- HEADLINE
  ROUND(SAFE_DIVIDE(SUM(visits),COUNT(*)),5)            AS visits_per_hh,          -- HEADLINE
  ROUND(SAFE_DIVIDE(SUM(spend),COUNT(*)),4)             AS cost_per_hh,            -- HEADLINE
  ROUND(SAFE_DIVIDE(SUM(spend),NULLIF(SUM(visits),0)),4) AS cpv,
  ROUND(1000*SAFE_DIVIDE(SUM(visits),SUM(freq)),4)      AS visits_per_1k_imps,     -- ARTIFACT (last-touch), not headline
  ROUND(SUM(spend*GREATEST(freq-3,0)/freq),2)           AS overcap_spend_cap3,     -- gross addressable, before incrementality
  ROUND(SUM(spend*GREATEST(freq-8,0)/freq),2)           AS overcap_spend_cap8,
  ROUND(SUM(spend*GREATEST(freq-12,0)/freq),2)          AS overcap_spend_cap12
FROM bucketed
GROUP BY scope, purge, freq_bucket
ORDER BY scope, purge, freq_bucket
