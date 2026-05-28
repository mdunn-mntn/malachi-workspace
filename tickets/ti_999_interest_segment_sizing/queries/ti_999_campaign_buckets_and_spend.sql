-- TI-999: bucket active campaigns by interest-segment usage, then roll up spend + impressions.
-- Window: 30 days ending 2026-05-28 (most recent complete day in sum_by_campaign_by_day).
-- Bucket rule:
--   INTEREST_DS = {17 ShareThis, 18 Dstillery, 35 LiveRamp IP}
--   INTERNAL_TARGETING_DS = {4 CRM-1P, 19 RTC, 38 BUK}
--   * interest_only:   uses INTEREST AND NOT INTERNAL_TARGETING
--   * interest_mixed:  uses INTEREST AND INTERNAL_TARGETING
--   * no_interest:     does not use INTEREST
-- Caveat: REGEX captures any DS reference in the expression including NOT/exclusion clauses;
--         "uses_X" therefore means "expression references X" not "actively targets X". A future
--         refinement would parse the AST to distinguish positive vs negative clauses.
-- Active campaign = had >=1 impression in the window.

WITH
window_ AS (
  SELECT DATE '2026-04-29' AS lo, DATE '2026-05-28' AS hi
),

-- 1) Active campaigns in window + spend/impression rollup
--    Notes on column choice:
--      impressions, conversions, spend  -> straight SUM on INT/NUMERIC columns
--      site_visitors is an HLL sketch (BYTES) — must use HLL_COUNT.MERGE to get a
--      campaign-window unique-visitor count. We carry the merged sketch through to
--      the bucket roll-up so the bucket-level count is also unique-deduped.
campaign_rollup AS (
  SELECT
    advertiser_id,
    campaign_id,
    SUM(impressions)                            AS impressions_30d,
    SUM(media_spend)                            AS media_spend_30d,
    SUM(data_spend)                             AS data_spend_30d,
    SUM(platform_spend)                         AS platform_spend_30d,
    SUM(click_conversions + view_conversions)   AS conversions_30d,
    HLL_COUNT.MERGE_PARTIAL(site_visitors)      AS site_visitors_sketch
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`, window_
  WHERE day BETWEEN window_.lo AND window_.hi
  GROUP BY advertiser_id, campaign_id
  HAVING SUM(impressions) > 0
),

-- 2) Extract DS ids referenced anywhere in each campaign's audience-expression(s)
campaign_ds AS (
  SELECT
    s.campaign_id,
    ARRAY_AGG(DISTINCT ds_id IGNORE NULLS) AS ds_array
  FROM `dw-main-silver.audience.audience_segments` s,
       UNNEST(REGEXP_EXTRACT_ALL(s.expression, r'"data_source_id":(\d+)')) AS ds_str_raw,
       UNNEST([SAFE_CAST(ds_str_raw AS INT64)])                            AS ds_id
  WHERE s.expression_type_id = 2
    AND s.is_targeted = TRUE
  GROUP BY s.campaign_id
),

-- 3) Bucket assignment per campaign
campaign_buckets AS (
  SELECT
    r.advertiser_id,
    r.campaign_id,
    r.impressions_30d,
    r.media_spend_30d,
    r.data_spend_30d,
    r.platform_spend_30d,
    r.site_visitors_sketch,
    r.conversions_30d,
    -- flags
    EXISTS(SELECT 1 FROM UNNEST(d.ds_array) x WHERE x IN (17, 18, 35))  AS uses_interest,
    EXISTS(SELECT 1 FROM UNNEST(d.ds_array) x WHERE x IN (4, 19, 38))   AS uses_internal_targeting,
    EXISTS(SELECT 1 FROM UNNEST(d.ds_array) x WHERE x = 17)             AS uses_sharethis,
    EXISTS(SELECT 1 FROM UNNEST(d.ds_array) x WHERE x = 18)             AS uses_dstillery,
    EXISTS(SELECT 1 FROM UNNEST(d.ds_array) x WHERE x = 35)             AS uses_liveramp,
    d.ds_array
  FROM campaign_rollup r
  LEFT JOIN campaign_ds d USING (campaign_id)
),

-- 4) Bucket label
labelled AS (
  SELECT
    *,
    CASE
      WHEN uses_interest AND NOT uses_internal_targeting THEN 'interest_only'
      WHEN uses_interest AND uses_internal_targeting     THEN 'interest_mixed'
      WHEN NOT COALESCE(uses_interest, FALSE)            THEN 'no_interest'
    END AS bucket
  FROM campaign_buckets
)

SELECT
  bucket,
  COUNT(DISTINCT campaign_id)                                                       AS n_campaigns,
  COUNT(DISTINCT advertiser_id)                                                     AS n_advertisers,
  SUM(impressions_30d)                                                              AS impressions_30d,
  SUM(media_spend_30d)                                                              AS media_spend_30d,
  SUM(media_spend_30d + data_spend_30d + platform_spend_30d)                        AS total_spend_30d,
  HLL_COUNT.MERGE(site_visitors_sketch)                                             AS unique_visitors_30d,
  SUM(conversions_30d)                                                              AS conversions_30d,
  -- weighted KPIs at the bucket level
  SAFE_DIVIDE(HLL_COUNT.MERGE(site_visitors_sketch), SUM(impressions_30d))          AS unique_visitor_rate,
  SAFE_DIVIDE(SUM(conversions_30d),                  SUM(impressions_30d))          AS conversion_rate,
  -- per-DS interest exposure breakdown
  COUNTIF(uses_sharethis)                                                           AS n_camps_uses_sharethis,
  COUNTIF(uses_dstillery)                                                           AS n_camps_uses_dstillery,
  COUNTIF(uses_liveramp)                                                            AS n_camps_uses_liveramp
FROM labelled
GROUP BY bucket
ORDER BY bucket;
