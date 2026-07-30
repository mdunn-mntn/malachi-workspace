-- AUDI-1172 Ask 2: advertiser-level OVERALL incrementality, all MNTN advertisers,
-- split into 3 product-mix groups (PTV-only / Select-only / Both). Rollup-only (cheap).
-- Classification from CG-level product_id; lift from level='advertiser' rows (pooled across
-- both products), re-aggregating the partner split. Test accounts + WGU excluded downstream.
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
  SELECT advertiser_id,
         CASE WHEN has_select = 1 AND has_ptv = 1 THEN 'Both'
              WHEN has_select = 1 THEN 'Select-only'
              ELSE 'PTV-only' END AS grp
  FROM cls
),
adv AS (   -- advertiser-level lift (pooled across products); IVW-combine the <=2 partner rows
  SELECT r.advertiser_id,
         SUM(r.n_treatment) AS n_t, SUM(r.vis_treatment) AS vis_t,
         SUM(r.n_holdout)   AS n_h, SUM(r.vis_holdout)   AS vis_h,
         SUM(r.conv_treatment) AS conv_t, SUM(r.conv_holdout) AS conv_h,
         SUM(r.abs_itt / POW(r.se,2)) / SUM(1.0/POW(r.se,2)) AS abs_itt,
         SQRT(1.0 / SUM(1.0/POW(r.se,2))) AS se,
         SAFE_DIVIDE(SUM(SAFE_DIVIDE(r.conv_abs_itt, POW(r.conv_se,2))),
                     SUM(SAFE_DIVIDE(1.0, POW(r.conv_se,2)))) AS conv_abs_itt,
         SQRT(SAFE_DIVIDE(1.0, SUM(SAFE_DIVIDE(1.0, POW(r.conv_se,2))))) AS conv_se
  FROM `dw-main-gold.reporting.lift__ghost_bid_rollup` r
  WHERE r.level = 'advertiser' AND r.se > 0 AND NOT r.low_coverage
  GROUP BY 1
)
SELECT g.grp, a.advertiser_id, adv2.company_name,
       a.n_t, a.vis_t, a.n_h, a.vis_h, a.conv_t, a.conv_h,
       a.abs_itt, a.se, a.conv_abs_itt, a.conv_se,
       a.vis_h / a.n_h AS holdout_vr,
       SAFE_DIVIDE(a.conv_h, a.n_h) AS conv_holdout_vr
FROM adv a
JOIN grp g USING (advertiser_id)
JOIN `dw-main-silver.public.advertisers` adv2 ON a.advertiser_id = adv2.advertiser_id
WHERE adv2.is_test = FALSE AND adv2.deleted = FALSE
  AND a.advertiser_id != 31357            -- exclude WGU (extreme outlier, per Kirsa)
  AND adv2.company_name NOT LIKE '%MNTN%'  -- belt-and-suspenders on internal accounts
