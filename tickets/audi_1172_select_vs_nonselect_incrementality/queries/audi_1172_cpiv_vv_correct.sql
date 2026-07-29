-- AUDI-1172 CPIV on the CORRECT Verified-Visit basis.
-- UI Verified Visit = clicks + views + competing_views (AUDI-1070, reproduced to the dollar) -
-- NOT first_day..seventh_day_visits (those are last-touch day-buckets only).
-- Method (Matt's call): incremental_VV = Reporting_VV x rel_lift/(1+rel_lift), where rel_lift is
-- the volume-weighted (raw-count) pooled relative visit lift from the ghost-bid pipeline.
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
  SELECT r.entity_id AS cgid,
         CASE WHEN pcg.product_id = 2 THEN 'Select' ELSE 'non_Select' END AS product,
         r.n_treatment, r.n_holdout, r.vis_treatment, r.vis_holdout,
         r.conv_treatment, r.conv_holdout
  FROM `dw-main-gold.reporting.lift__ghost_bid_rollup` r
  JOIN `dw-main-silver.public.campaign_groups` pcg ON r.entity_id = pcg.campaign_group_id
  WHERE r.level = 'campaign_group'
    AND r.advertiser_id IN (SELECT advertiser_id FROM aids)
    AND r.se > 0 AND NOT r.low_coverage
),
pipe AS (   -- volume-weighted (raw-count) pooled pipeline rates -> rel_lift (visits + conversions)
  SELECT product,
         SUM(vis_treatment) AS vis_t, SUM(n_treatment) AS n_t,
         SUM(vis_holdout)   AS vis_h, SUM(n_holdout)   AS n_h,
         SUM(conv_treatment) AS conv_t, SUM(conv_holdout) AS conv_h
  FROM cg GROUP BY product
),
af AS (     -- Reporting Verified Visits + Conversions (authoritative UI defn), obj=1, cohort CGs, window
  SELECT c.product,
         SUM(IFNULL(a.clicks,0)) + SUM(IFNULL(a.views,0)) + SUM(IFNULL(a.competing_views,0)) AS vv_reported,
         SUM(IFNULL(a.click_conversions,0)) + SUM(IFNULL(a.view_conversions,0))
           + SUM(IFNULL(a.competing_view_conversions,0)) AS conv_reported,
         SUM(IFNULL(a.media_spend,0)+IFNULL(a.data_spend,0)+IFNULL(a.platform_spend,0)) AS spend
  FROM `dw-main-silver.summarydata.all_facts` a
  JOIN cg c ON a.campaign_group_id = c.cgid
  WHERE a.hour >= DATETIME '2026-06-22' AND a.hour < DATETIME '2026-07-28'
    AND a.objective_id = 1
  GROUP BY c.product
)
SELECT p.product,
       p.vis_t/p.n_t AS rate_treatment, p.vis_h/p.n_h AS rate_holdout,
       (p.vis_t/p.n_t)/(p.vis_h/p.n_h) - 1 AS rel_lift_raw,
       (p.conv_t/p.n_t)/NULLIF(p.conv_h/p.n_h,0) - 1 AS conv_rel_lift_raw,
       af.vv_reported, af.conv_reported, af.spend
FROM pipe p JOIN af USING(product)
