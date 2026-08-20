-- audi_1210_share_of_site_visits.sql — the share of each advertiser's own site traffic MNTN gets credit for.
--
-- The right lens is the share of the advertiser's own traffic we get credit for, not the raw
-- visit rate (Johnny Chen, 2026-08-19):
--   share_of_site_visits = MNTN verified visits / the advertiser's own reported site visits
--   match_rate     = matched IPs / IPs MNTN served
--
-- Verified visits = clicks + views + competing_views, the client-facing Reporting figure, NOT a
-- distinct count of matched IPs. Reconciled against Johnny Chen's numbers on 2026-08-19: for
-- advertiser 39510 verified visits give 1.269% (he read 1.25%) where distinct matched IPs give
-- 0.291%; for 66784, 0.404% against 0.260%. The distinct-IP version undercounts because one
-- household visiting several times counts once, and because the IP join misses cross-device.
-- The two move independently. Maurices (66784) matches 3.15% of served IPs but touches only
-- 0.26% of its site traffic; Re-Bath Cherry Hill (39510) matches 0.13% and touches 0.29%.
-- A low match rate mostly reflects a small campaign audience against a large site, which is a
-- media-plan fact, not a measurement fault. A low SHARE OF VOICE against real spend is the
-- anomaly worth chasing: we paid, and almost none of the site's audience came through us.
--
-- The share falls as a site gets bigger (corr of log site visits to log share = -0.24; median
-- share runs 1.09% for the smallest fifth of sites down to 0.39% for the largest). So it is compared
-- WITHIN a site-size peer group, not across the whole base, or the flag would just select big sites.
--
-- Universe: live, non-test advertisers that served in the trailing 30 days AND reported at least
-- 1,000 site visits. Below that the ratio is noise. Advertisers with fewer than 1,000 reported
-- visits, and those reporting none at all, are returned separately by the `coverage` column so
-- they are visible rather than silently dropped. 9090 (PSA) excluded by design.
WITH served AS (
  SELECT advertiser_id, ip,
    COUNT(*) AS impressions,
    SUM(COALESCE(media_spend,0) + COALESCE(data_spend,0) + COALESCE(platform_spend,0)) AS spend
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE DATE(time) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AND CURRENT_DATE()
    AND advertiser_id IS NOT NULL AND advertiser_id != 9090
  GROUP BY 1, 2
),
visiting AS (
  SELECT advertiser_id, ip FROM `dw-main-silver.logdata.clickpass_log`
  WHERE DATE(time) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AND CURRENT_DATE()
  GROUP BY 1, 2
),
converting AS (
  SELECT advertiser_id, ip FROM `dw-main-silver.summarydata.ui_conversions`
  WHERE DATE(time) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AND CURRENT_DATE()
  GROUP BY 1, 2
),
-- 12-month history separates a never-installed pixel from one that went dark. An opt-out never
-- reports a visit; a defect reports and then stops, and the stop date is the useful part.
history AS (
  SELECT advertiser_id,
    SUM(raw_visits)                        AS raw_visits_12mo,
    MAX(IF(raw_visits > 0, day, NULL))     AS last_day_with_a_visit
  FROM `dw-main-silver.summarydata.sum_by_advertiser_by_day`
  WHERE day BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY) AND CURRENT_DATE()
  GROUP BY 1
),
site AS (
  SELECT advertiser_id,
    SUM(raw_visits)         AS raw_visits_30d,
    SUM(raw_conversions)    AS raw_conversions_30d,
    SUM(clicks) + SUM(views) + SUM(competing_views) AS verified_visits_30d,
    COUNTIF(raw_visits > 0) AS days_with_any_visit
  FROM `dw-main-silver.summarydata.sum_by_advertiser_by_day`
  WHERE day BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AND CURRENT_DATE()
  GROUP BY 1
),
rolled AS (
  SELECT
    s.advertiser_id,
    SUM(s.spend)                    AS spend_30d,
    SUM(s.impressions)              AS impressions_30d,
    COUNT(*)                        AS served_ips_30d,
    SUM(IF(v.ip IS NOT NULL, 1, 0)) AS matched_ips_30d,
    SUM(IF(c.ip IS NOT NULL, 1, 0)) AS converting_ips_30d
  FROM served s
  LEFT JOIN visiting   v USING (advertiser_id, ip)
  LEFT JOIN converting c USING (advertiser_id, ip)
  GROUP BY 1
  HAVING SUM(s.impressions) > 0
),
joined AS (
  SELECT
    r.advertiser_id,
    adv.company_name AS advertiser_name,
    r.spend_30d, r.impressions_30d, r.served_ips_30d,
    r.matched_ips_30d, r.converting_ips_30d,
    COALESCE(t.raw_visits_30d, 0)      AS raw_visits_30d,
    COALESCE(t.verified_visits_30d, 0) AS verified_visits_30d,
    COALESCE(t.raw_conversions_30d, 0) AS raw_conversions_30d,
    COALESCE(t.days_with_any_visit, 0) AS days_with_any_visit,
    COALESCE(h.raw_visits_12mo, 0)     AS raw_visits_12mo,
    h.last_day_with_a_visit,
    SAFE_DIVIDE(r.matched_ips_30d, r.served_ips_30d)          AS match_rate,
    SAFE_DIVIDE(t.verified_visits_30d, t.raw_visits_30d)      AS share_of_site_visits,
    CASE WHEN COALESCE(t.raw_visits_30d, 0) = 0    THEN 'Pixel reported nothing'
         WHEN COALESCE(t.raw_visits_30d, 0) < 1000 THEN 'Site too quiet to score'
         ELSE 'Scored' END AS coverage
  FROM rolled r
  JOIN `dw-main-bronze.integrationprod.advertisers` adv USING (advertiser_id)
  LEFT JOIN site t USING (advertiser_id)
  LEFT JOIN history h USING (advertiser_id)
  WHERE COALESCE(adv.deleted, FALSE) = FALSE
    AND COALESCE(adv.is_test, FALSE) = FALSE
    AND adv.company_name IS NOT NULL
),
sized AS (
  SELECT j.*,
    -- percentiles are computed only over scorable advertisers, so a quiet site cannot drag them
    IF(j.coverage = 'Scored',
       NTILE(5) OVER (PARTITION BY j.coverage ORDER BY j.raw_visits_30d), NULL) AS site_size_quintile,
    IF(j.coverage = 'Scored',
       PERCENT_RANK() OVER (PARTITION BY j.coverage ORDER BY j.share_of_site_visits), NULL) AS site_visit_share_percentile
  FROM joined j
)
SELECT s.*,
  IF(s.coverage = 'Scored',
     PERCENT_RANK() OVER (PARTITION BY s.coverage, s.site_size_quintile ORDER BY s.share_of_site_visits),
     NULL) AS site_visit_share_percentile_vs_peers
FROM sized s
ORDER BY s.spend_30d DESC
