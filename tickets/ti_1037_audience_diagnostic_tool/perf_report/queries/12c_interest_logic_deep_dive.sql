/* ============================================================================
   Module 12c — INTEREST-LOGIC deep-dive (per-campaign targeting DNA + funnel gate)
   ----------------------------------------------------------------------------
   Answers "what is UNIQUE about each prospecting campaign's audience, and does any
   campaign NARROW its reach?" — the client's core audience question.

   STRUCTURAL FINDING (Kindred, from parsing categories.where op-trees, 2026-07-02):
   All 6 prospecting campaigns share the SAME interest core:
       ( MM DS19[255 keywords]  OR  3P DS35[11-14 maternity/baby segments] )
   => MM and 3P are OR'd = ADDITIVE / broadening. NO campaign does MM AND 3P
      (the "required-3P narrows MM" pattern is NOT present anywhere).
   Plus a shared exclusion block (hygiene, identical across all 6):
       NOT ( DS47 CRM-exclusion lists  OR  DS21 own-converters  OR  DS34 own-funnel  OR  DS2 )
   Plus DS14[1] = "Beeswax Bidder" = plumbing (near-universal), not targeting.

   THE DIFFERENTIATOR — only the 3 Q1-2026 variants (Harter/Motherhood/Mom-Focus)
   add an extra AND'd DS16 FUNNEL-TAG gate the base/Mid/Low lack:
       AND ( NOT DS16[7291 Impressions, 787280 Wins]  OR  DS16[own campaign-group tag] )
   Decoded via tpa.categories (data_source_id=16 = advertiser's own funnel):
       7291   = "Impressions"  (households advertiser 35094 has served, ANY campaign)
       787280 = "Wins"         (households the advertiser won)
       1783281/1783302/1783323 = each variant's own CampaignGroupID impression tag
   Semantics: target a household iff (NEVER impressed/won by Kindred) OR (already
   served by THIS variant). => a NET-NEW-reach gate with per-variant ownership.

   THE MEASUREMENT (below) proves the gate materially narrows the variants:
   they fish the residual net-new pool the ungated base already skipped, which is
   smaller and lower-quality -> variant ROAS 1.18-1.35x vs base 2.39x.

   Reach uses BQ-native HLL sketches on sum_by_campaign_by_day (verified HLL_COUNT.MERGE
   works); overlaps via merge-union + inclusion-exclusion (no raw IP scan needed).

   Params: {{AID}} {{WIN_START}} {{WIN_END}}
   ============================================================================ */

-- ---------------------------------------------------------------------------
-- (A) Monthly delivery + distinct household reach per campaign_group — the
--     base->variants HANDOFF (base winds down, gated variants ramp up).
--     -> 12c_reach_monthly.csv
-- ---------------------------------------------------------------------------
WITH s AS (
  SELECT c.campaign_group_id AS grp, FORMAT_DATE("%Y-%m", s.day) AS mon, s.impressions, s.uniques
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` s
  JOIN `dw-main-bronze.integrationprod.campaigns` c ON c.campaign_id = s.campaign_id
  WHERE s.advertiser_id = {{AID}} AND s.day BETWEEN "{{WIN_START}}" AND "{{WIN_END}}"
    AND c.campaign_group_id IN (69884,109926,96108,115943,115945,115946)
)
SELECT grp, mon, SUM(impressions) AS imps, HLL_COUNT.MERGE(uniques) AS reach
FROM s GROUP BY grp, mon ORDER BY grp, mon;

-- ---------------------------------------------------------------------------
-- (B) Pairwise household OVERLAP — base(69884) vs each variant, and variant vs
--     variant. Conditional HLL_COUNT.MERGE (aggregates skip NULL) builds each
--     subset's union sketch in one pass; intersection = A + B - (A∪B).
--     -> feeds 12c_overlap.csv
--     Result (Jan-May'26): each variant reaches ~435K = ~26% of base's 1.64M;
--     base∩variant ~27% (=> ~72% NET-NEW vs base); variant∩variant ~9% (=> ~90%
--     mutually DISJOINT — a 3-way mutually-exclusive creative split of the residual).
-- ---------------------------------------------------------------------------
WITH s AS (
  SELECT c.campaign_group_id AS grp, s.uniques
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` s
  JOIN `dw-main-bronze.integrationprod.campaigns` c ON c.campaign_id = s.campaign_id
  WHERE s.advertiser_id = {{AID}} AND s.day BETWEEN "{{WIN_START}}" AND "{{WIN_END}}"
    AND c.campaign_group_id IN (69884,115943,115945,115946)
)
SELECT
  HLL_COUNT.MERGE(IF(grp=69884, uniques, NULL))  AS base,
  HLL_COUNT.MERGE(IF(grp=115943, uniques, NULL)) AS harter,
  HLL_COUNT.MERGE(IF(grp=115945, uniques, NULL)) AS mother,
  HLL_COUNT.MERGE(IF(grp=115946, uniques, NULL)) AS momfocus,
  HLL_COUNT.MERGE(IF(grp IN(69884,115943), uniques, NULL))  AS b_h,
  HLL_COUNT.MERGE(IF(grp IN(69884,115945), uniques, NULL))  AS b_m,
  HLL_COUNT.MERGE(IF(grp IN(69884,115946), uniques, NULL))  AS b_mf,
  HLL_COUNT.MERGE(IF(grp IN(115943,115945), uniques, NULL)) AS h_m,
  HLL_COUNT.MERGE(IF(grp IN(115943,115946), uniques, NULL)) AS h_mf,
  HLL_COUNT.MERGE(IF(grp IN(115945,115946), uniques, NULL)) AS m_mf,
  HLL_COUNT.MERGE(IF(grp IN(115943,115945,115946), uniques, NULL)) AS all3v
FROM s;

-- ---------------------------------------------------------------------------
-- (C) Category-name decode (reference; run once): the DS16 gate tags, DS35 3P
--     segments, DS14 plumbing, and the exclusion lists.
--       SELECT data_source_id, data_source_category_id, name, display_name, path_from_root
--       FROM `dw-main-bronze.tpa.categories`
--       WHERE data_source_category_id IN (<ids from the expression op-trees>);
--     NB tpa.categories key is data_source_category_id (NOT category_id); match on it.
-- ---------------------------------------------------------------------------
