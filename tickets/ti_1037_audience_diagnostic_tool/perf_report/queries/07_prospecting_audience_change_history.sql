/* ============================================================================
   Module 07 — Prospecting audience-expression CHANGE HISTORY (per campaign)
   ----------------------------------------------------------------------------
   The audience an advertiser targets can change UNDER A FIXED campaign_id: the
   segment expression is versioned. This pulls every distinct audience config a
   prospecting campaign (funnel=1/obj=1) ran over time, from the type-2 archive,
   collapsed to the moments something changed — the data-source set OR the audience_id.
   charts/07 renders it as a data-source-presence timeline so you can see exactly
   when DS13/DS19/DS35/etc. entered or left, and when the audience_id was swapped.

   Answers: "did P1 even have MM (DS13/DS19)?", "did the audience_id change?",
   "when did scoring-relevant sources appear?" — all without re-querying delivery.

   DS ids are regex-extracted from the expression JSON (`"data_source_id":N`).
   Ordering: create_time (archive `version` is NON-monotonic — never order by it).
   Restricted to campaigns that DELIVERED in [WIN_START, WIN_END) (same active set as the other
   modules) so dormant historical campaigns don't clutter the timeline.
   Source : archives.audience_segment_archives (full history; heavy scan — not date-prunable).
   Params : {{AID}} {{WIN_START}} {{WIN_END}}
   ============================================================================ */
WITH camp AS (
  SELECT campaign_id, campaign_group_id, name AS camp_name
  FROM `dw-main-bronze.integrationprod.campaigns`
  WHERE advertiser_id = {{AID}} AND deleted = FALSE
    AND objective_id = 1 AND funnel_level = 1
    AND campaign_id IN (
      SELECT DISTINCT campaign_id
      FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`
      WHERE advertiser_id = {{AID}}
        AND day >= "{{WIN_START}}" AND day < "{{WIN_END}}" AND impressions > 0
    )
),
v AS (
  SELECT
    a.campaign_id, a.audience_id, a.segment_id, a.create_time,
    ARRAY_TO_STRING(ARRAY(
      SELECT DISTINCT x FROM UNNEST(REGEXP_EXTRACT_ALL(a.expression, r'"data_source_id":([0-9]+)')) x
      ORDER BY CAST(x AS INT64)), ",")                 AS ds_ids
  FROM `dw-main-silver.archives.audience_segment_archives` a
  JOIN camp c USING (campaign_id)
  WHERE a.expression_type_id = 2 AND a.is_targeted = TRUE
),
chg AS (
  SELECT v.*,
    LAG(ds_ids)      OVER (PARTITION BY campaign_id ORDER BY create_time) AS prev_ds,
    LAG(audience_id) OVER (PARTITION BY campaign_id ORDER BY create_time) AS prev_aud
  FROM v
)
SELECT
  chg.campaign_id,
  c.campaign_group_id,
  c.camp_name,
  DATE(chg.create_time) AS changed_on,
  chg.audience_id,
  chg.segment_id,
  chg.ds_ids
FROM chg
JOIN camp c ON c.campaign_id = chg.campaign_id
WHERE prev_ds IS NULL OR chg.ds_ids != prev_ds OR chg.audience_id != prev_aud
ORDER BY chg.campaign_id, chg.create_time
