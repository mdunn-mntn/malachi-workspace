-- Module 12c (B) — household reach per prospecting campaign_group + pairwise overlap vs the BASE
-- (the largest-reach prospecting group = the broad/ungated core). Structure-agnostic: works for
-- Kindred's base+3-variants and for Bouqs' national base+frequency/gated variants alike.
--   reach            = distinct households the group reaches (HLL on sum_by_campaign_by_day.uniques)
--   base_grp         = the largest-reach prospecting group (the "base")
--   union_with_base  = HLL union(group, base); intersection = reach + base_reach - union_with_base
--                      => net-new-vs-base = 1 - intersection/reach (chart computes this).
-- Groups derived dynamically (objective_id=1, funnel_level=1, deleted=FALSE); never hardcoded.
WITH prosp AS (
  SELECT DISTINCT campaign_group_id AS grp FROM `dw-main-bronze.integrationprod.campaigns`
  WHERE advertiser_id = {{AID}} AND deleted = FALSE AND objective_id = 1 AND funnel_level = 1
),
day_sk AS (
  SELECT c.campaign_group_id AS grp, g.name AS group_name, s.uniques
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` s
  JOIN `dw-main-bronze.integrationprod.campaigns` c ON c.campaign_id = s.campaign_id
  LEFT JOIN `dw-main-bronze.integrationprod.campaign_groups` g ON g.campaign_group_id = c.campaign_group_id
  WHERE s.advertiser_id = {{AID}} AND s.day BETWEEN "{{WIN_START}}" AND "{{WIN_END}}"
    AND c.campaign_group_id IN (SELECT grp FROM prosp) AND s.uniques IS NOT NULL
),
grp_sk AS (
  SELECT grp, ANY_VALUE(group_name) AS group_name,
         HLL_COUNT.MERGE_PARTIAL(uniques) AS sk, HLL_COUNT.MERGE(uniques) AS reach
  FROM day_sk GROUP BY grp
),
base AS (SELECT grp AS base_grp FROM grp_sk ORDER BY reach DESC LIMIT 1)
SELECT g.grp AS campaign_group_id, g.group_name, g.reach,
  (SELECT base_grp FROM base) AS base_grp,
  (SELECT HLL_COUNT.MERGE(sk)
     FROM UNNEST([g.sk, (SELECT sk FROM grp_sk WHERE grp = (SELECT base_grp FROM base))]) AS sk
  ) AS union_with_base
FROM grp_sk g
ORDER BY g.reach DESC
