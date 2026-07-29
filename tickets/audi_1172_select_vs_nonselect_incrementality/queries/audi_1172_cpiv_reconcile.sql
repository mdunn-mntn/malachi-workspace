-- AUDI-1172 CPIV reconciliation: bridge Matt's ghost-bid pipeline visits to
-- Reporting Verified Visits + cohort-matched spend, per product.
-- Cohort: Kirsa's 93 AIDs, prospecting, clean-gated lift CGs, 2026-06-22..07-27.
WITH aids AS (
  SELECT advertiser_id FROM UNNEST([
    37983,47347,46722,33760,31276,35821,45550,33768,41034,40521,51095,40535,34185,33617,34094,
    39149,40236,36232,33666,41545,53341,41426,36743,34421,40807,37893,42097,37676,53308,45458,
    50413,45921,38363,31357,38579,53656,49868,40598,59241,40601,32863,47228,37798,33389,31460,
    53749,36794,33179,37775,35086,58469,32769,62938,33950,39207,61583,59584,36678,30238,33270,
    31441,32404,36583,37423,57418,38799,38800,33316,36507,33448,34862,34585,54196,47209,65217,
    37085,48875,34472,39834,56494,37316,62689,32153,33467,59460,32040,66784,37880,40002,44054,
    40563,39225,44339,44419]) AS advertiser_id
),
cg AS (
  SELECT r.advertiser_id, r.entity_id AS cgid,
         CASE WHEN pcg.product_id = 2 THEN 'Select' ELSE 'non_Select' END AS product,
         r.n_treatment, r.n_holdout, r.vis_treatment, r.vis_holdout,
         r.abs_itt, r.se, r.incremental_visits, r.conv_abs_itt
  FROM `dw-main-gold.reporting.lift__ghost_bid_rollup` r
  JOIN `dw-main-silver.public.campaign_groups` pcg ON r.entity_id = pcg.campaign_group_id
  WHERE r.level = 'campaign_group'
    AND r.advertiser_id IN (SELECT advertiser_id FROM aids)
    AND r.se > 0 AND NOT r.low_coverage
),
comp AS (
  SELECT campaign_group_id AS cgid, ip_compliance, holdout_won_rate
  FROM `dw-main-gold.reporting.lift__ghost_bid_results`
  WHERE stratum_type = 'overall'
),
af AS (
  SELECT campaign_group_id AS cgid,
    SUM(CAST(media_spend AS FLOAT64)+CAST(data_spend AS FLOAT64)+CAST(platform_spend AS FLOAT64)) AS total_spend,
    SUM(display_impressions + ctv_impressions) AS impressions,
    SUM(first_day_visits+second_day_visits+third_day_visits+fourth_day_visits
        +fifth_day_visits+sixth_day_visits+seventh_day_visits) AS vv_7d_daybucket,
    SUM(last_touch_visits_day0+last_touch_visits_day1+last_touch_visits_day2+last_touch_visits_day3
        +last_touch_visits_day4+last_touch_visits_day5+last_touch_visits_day6) AS vv_7d_lasttouch,
    SUM(competing_visit_assists) AS competing_visits_total,
    SUM(first_touch_visits) AS first_touch_visits_total
  FROM `dw-main-silver.summarydata.all_facts`
  WHERE hour >= DATETIME '2026-06-22' AND hour < DATETIME '2026-07-28'
    AND campaign_group_id IN (SELECT cgid FROM cg)
  GROUP BY 1
)
SELECT cg.product,
       COUNT(*) AS n_cg,
       SUM(cg.n_treatment) AS n_treatment,
       SUM(cg.vis_treatment) AS vis_treatment_pipeline,
       SUM(cg.incremental_visits) AS incr_visits,
       SUM(comp.ip_compliance * cg.n_treatment) AS households_reached,
       SUM(af.total_spend) AS spend_total,
       SUM(af.impressions) AS impressions,
       SUM(af.vv_7d_daybucket) AS vv_7d_daybucket,
       SUM(af.vv_7d_lasttouch) AS vv_7d_lasttouch,
       SUM(af.competing_visits_total) AS competing_visits_total,
       SUM(af.first_touch_visits_total) AS first_touch_visits_total
FROM cg
LEFT JOIN comp USING(cgid)
LEFT JOIN af USING(cgid)
GROUP BY cg.product
