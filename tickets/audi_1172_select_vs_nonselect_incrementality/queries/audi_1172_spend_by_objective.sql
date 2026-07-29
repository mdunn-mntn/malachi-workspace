-- Diagnostic: spend + impressions by product x objective_id for the cohort CGs,
-- to confirm which objectives carry Select vs non-Select delivery (spend scope check).
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
         CASE WHEN pcg.product_id = 2 THEN 'Select' ELSE 'non_Select' END AS product
  FROM `dw-main-gold.reporting.lift__ghost_bid_rollup` r
  JOIN `dw-main-silver.public.campaign_groups` pcg ON r.entity_id = pcg.campaign_group_id
  WHERE r.level = 'campaign_group'
    AND r.advertiser_id IN (SELECT advertiser_id FROM aids)
    AND r.se > 0 AND NOT r.low_coverage
)
SELECT cg.product, af.objective_id,
       COUNT(DISTINCT af.campaign_group_id) AS n_cg,
       SUM(CAST(af.media_spend AS FLOAT64)+CAST(af.data_spend AS FLOAT64)+CAST(af.platform_spend AS FLOAT64)) AS total_spend,
       SUM(af.display_impressions + af.ctv_impressions) AS impressions
FROM `dw-main-silver.summarydata.all_facts` af
JOIN cg ON af.campaign_group_id = cg.cgid
WHERE af.hour >= DATETIME '2026-06-22' AND af.hour < DATETIME '2026-07-28'
GROUP BY cg.product, af.objective_id
ORDER BY cg.product, total_spend DESC
