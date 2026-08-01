-- AUDI-1172 CPIV/CPIA per product-mix group (PTV-only / Select-only / Both), ALL MNTN advertisers.
-- Group-pooled cost: CPIV = spend / (Reporting_VV x L_v/(1+L_v)); L = volume-weighted raw-count lift.
-- Same clean gate + exclusions (test/deleted/WGU/MNTN) as audi_1172_aid_group_lift.sql. Spend/VV over the
-- clean-gated CGs so numerator (spend) and denominator (incremental) cover the same campaign groups.
WITH cls AS (   -- classify each advertiser by product mix (clean-gated CG rows)
  SELECT r.advertiser_id,
         MAX(CASE WHEN pcg.product_id = 2 THEN 1 ELSE 0 END) AS has_select,
         MAX(CASE WHEN pcg.product_id = 1 THEN 1 ELSE 0 END) AS has_ptv
  FROM `dw-main-gold.reporting.lift__ghost_bid_rollup` r
  JOIN `dw-main-silver.public.campaign_groups` pcg ON r.entity_id = pcg.campaign_group_id
  WHERE r.level = 'campaign_group' AND r.se > 0 AND NOT r.low_coverage
  GROUP BY 1
),
grp AS (
  SELECT c.advertiser_id,
         CASE WHEN has_select = 1 AND has_ptv = 1 THEN 'Both'
              WHEN has_select = 1 THEN 'Select-only'
              ELSE 'PTV-only' END AS grp
  FROM cls c
  JOIN `dw-main-silver.public.advertisers` adv ON c.advertiser_id = adv.advertiser_id
  WHERE adv.is_test = FALSE AND adv.deleted = FALSE
    AND c.advertiser_id != 31357              -- exclude WGU (extreme outlier, per Kirsa)
    AND adv.company_name NOT LIKE '%MNTN%'      -- belt-and-suspenders on internal accounts
),
cg AS (   -- clean-gated campaign groups tagged with their advertiser's group
  SELECT r.entity_id AS cgid, g.grp,
         r.n_treatment, r.n_holdout, r.vis_treatment, r.vis_holdout,
         r.conv_treatment, r.conv_holdout
  FROM `dw-main-gold.reporting.lift__ghost_bid_rollup` r
  JOIN grp g ON r.advertiser_id = g.advertiser_id
  WHERE r.level = 'campaign_group' AND r.se > 0 AND NOT r.low_coverage
),
pipe AS (   -- volume-weighted (raw-count) lift per group
  SELECT grp,
         SUM(vis_treatment) AS vis_t, SUM(n_treatment) AS n_t,
         SUM(vis_holdout)   AS vis_h, SUM(n_holdout)   AS n_h,
         SUM(conv_treatment) AS conv_t, SUM(conv_holdout) AS conv_h
  FROM cg GROUP BY grp
),
af AS (   -- Reporting Verified Visits + Conversions + spend per group (metered), prospecting
  SELECT c.grp,
         SUM(IFNULL(a.clicks,0)) + SUM(IFNULL(a.views,0)) + SUM(IFNULL(a.competing_views,0)) AS vv_reported,
         SUM(IFNULL(a.click_conversions,0)) + SUM(IFNULL(a.view_conversions,0))
           + SUM(IFNULL(a.competing_view_conversions,0)) AS conv_reported,
         SUM(IFNULL(a.media_spend,0)+IFNULL(a.data_spend,0)+IFNULL(a.platform_spend,0)) AS spend
  FROM `dw-main-silver.summarydata.all_facts` a
  JOIN cg c ON a.campaign_group_id = c.cgid
  WHERE a.hour >= DATETIME '2026-06-22' AND a.hour < DATETIME '2026-07-28'
    AND a.objective_id = 1
  GROUP BY c.grp
)
SELECT p.grp,
       SAFE_DIVIDE(p.vis_t/p.n_t, SAFE_DIVIDE(p.vis_h, p.n_h)) - 1 AS rel_lift_visit,
       SAFE_DIVIDE(p.conv_t/p.n_t, SAFE_DIVIDE(p.conv_h, p.n_h)) - 1 AS rel_lift_conv,
       af.vv_reported, af.conv_reported, af.spend
FROM pipe p
JOIN af USING (grp)
ORDER BY p.grp
