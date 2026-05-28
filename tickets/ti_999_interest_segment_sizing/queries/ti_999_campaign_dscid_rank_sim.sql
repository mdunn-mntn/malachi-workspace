-- TI-999: rank simulation — where do the dscids advertisers chose fall in the order of
-- "quality" (activity proxy)? Outputs per-campaign averages + DS-level distribution histograms.
-- Quality proxy until TI-956 ships: per-dscid distinct-IP count over one day (2026-05-26).
-- Higher rank = better (more active in IPDSC).

WITH
window_ AS (SELECT DATE '2026-04-29' AS lo, DATE '2026-05-28' AS hi),

-- Per-dscid activity (one-day IPDSC IP count) ----------------
ipdsc_unnested AS (
  SELECT data_source_id, c.element AS dscid, ip
  FROM `dw-main-bronze.external.ipdsc__v1` t,
       UNNEST(t.data_source_category_ids.list) AS c
  WHERE dt = '2026-05-26'
    AND data_source_id IN (17, 18, 35)
),
dscid_ipcount AS (
  SELECT data_source_id, dscid, COUNT(DISTINCT ip) AS n_ips_1d
  FROM ipdsc_unnested
  GROUP BY data_source_id, dscid
),
active_dscids AS (
  SELECT
    data_source_id,
    data_source_category_id AS dscid,
    DATE_DIFF(CURRENT_DATE(), COALESCE(updated_date, created_date), DAY) AS days_since_update,
    deprecated
  FROM `dw-main-bronze.tpa.categories`
  WHERE data_source_id IN (17, 18, 35) AND deprecated = FALSE
),
dscid_quality AS (
  SELECT
    a.data_source_id,
    a.dscid,
    COALESCE(d.n_ips_1d, 0) AS n_ips_1d,
    a.days_since_update,
    RANK() OVER (PARTITION BY a.data_source_id ORDER BY COALESCE(d.n_ips_1d, 0) DESC) AS rank_by_activity,
    ROUND(100.0 * (1.0 - PERCENT_RANK() OVER (PARTITION BY a.data_source_id
                                              ORDER BY COALESCE(d.n_ips_1d, 0) DESC)), 1) AS activity_pctile,
    COUNT(*) OVER (PARTITION BY a.data_source_id) AS n_active_dscids_in_ds
  FROM active_dscids a
  LEFT JOIN dscid_ipcount d USING (data_source_id, dscid)
),

-- Campaign spend rollup (prospecting only) --------------------
campaign_rollup AS (
  SELECT advertiser_id, campaign_id,
         SUM(impressions)                               AS impressions_30d,
         SUM(media_spend + data_spend + platform_spend) AS total_spend_30d,
         SUM(click_conversions + view_conversions)      AS conversions_30d
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`, window_
  WHERE day BETWEEN window_.lo AND window_.hi
  GROUP BY advertiser_id, campaign_id
  HAVING SUM(impressions) > 0
),

-- Per-campaign per-DS dscid list ------------------------------
campaign_ds_dscids AS (
  SELECT
    s.campaign_id,
    SAFE_CAST(REGEXP_EXTRACT(m, r'"data_source_id":(\d+)') AS INT64) AS ds_id,
    SAFE_CAST(REGEXP_EXTRACT(m, r'"data_source_id":\d+,"category_ids":\[(\d+)') AS INT64) AS first_dscid_in_block,
    SPLIT(REGEXP_EXTRACT(m, r'"category_ids":\[([\d,]+)\]'), ',') AS dscid_list_str
  FROM `dw-main-silver.audience.audience_segments` s,
       UNNEST(REGEXP_EXTRACT_ALL(
         s.expression,
         r'"data_source_id":\d+,"category_ids":\[[\d,]+\]')) AS m
  WHERE s.expression_type_id = 2 AND s.is_targeted = TRUE
),

-- Explode to per (campaign_id, ds_id, dscid) rows -----------
campaign_dscid_flat AS (
  SELECT
    c.campaign_id,
    c.ds_id,
    SAFE_CAST(d AS INT64) AS dscid
  FROM campaign_ds_dscids c, UNNEST(c.dscid_list_str) AS d
),

-- Dedup (campaign references same dscid in multiple expression nodes)
campaign_dscid AS (
  SELECT DISTINCT campaign_id, ds_id, dscid
  FROM campaign_dscid_flat
  WHERE ds_id IN (17, 18, 35) AND dscid IS NOT NULL
),

-- Campaign-level retargeting exclusion check ----------------
campaign_uses_retargeting AS (
  SELECT campaign_id, LOGICAL_OR(uses_retargeting) AS excludes
  FROM (
    SELECT campaign_id, ds_id IN (4, 8, 47) AS uses_retargeting
    FROM campaign_dscid_flat
  )
  GROUP BY campaign_id
),

-- Join campaign dscids to dscid quality (prospecting only) --
campaign_dscid_ranked AS (
  SELECT
    cd.campaign_id,
    cd.ds_id,
    cd.dscid,
    q.n_ips_1d,
    q.rank_by_activity,
    q.activity_pctile,
    q.n_active_dscids_in_ds
  FROM campaign_dscid cd
  JOIN dscid_quality q ON cd.ds_id = q.data_source_id AND cd.dscid = q.dscid
  WHERE cd.campaign_id IN (
    SELECT r.campaign_id
    FROM campaign_rollup r
    LEFT JOIN campaign_uses_retargeting x USING (campaign_id)
    WHERE NOT COALESCE(x.excludes, FALSE)  -- prospecting only
  )
)

-- Final output: per-DS aggregate of chosen-dscid ranks
SELECT
  ds_id,
  COUNT(DISTINCT campaign_id)                                                        AS n_prospecting_camps_using_ds,
  COUNT(*)                                                                           AS n_camp_dscid_pairs,
  AVG(activity_pctile)                                                               AS avg_activity_pctile_chosen,
  APPROX_QUANTILES(activity_pctile, 100)[OFFSET(50)]                                 AS median_activity_pctile_chosen,
  AVG(rank_by_activity)                                                              AS avg_rank_chosen,
  APPROX_QUANTILES(rank_by_activity, 100)[OFFSET(50)]                                AS median_rank_chosen,
  -- What % of chosen dscids are in top decile / quartile / median?
  ROUND(100.0 * COUNTIF(activity_pctile >= 90) / COUNT(*), 1)                        AS pct_chosen_top_decile,
  ROUND(100.0 * COUNTIF(activity_pctile >= 75) / COUNT(*), 1)                        AS pct_chosen_top_quartile,
  ROUND(100.0 * COUNTIF(activity_pctile >= 50) / COUNT(*), 1)                        AS pct_chosen_top_half,
  ROUND(100.0 * COUNTIF(activity_pctile >= 25) / COUNT(*), 1)                        AS pct_chosen_above_p25,
  -- Activity IPs at the dscid level: average n_ips per chosen dscid
  AVG(n_ips_1d)                                                                      AS avg_n_ips_chosen,
  MAX(n_ips_1d)                                                                      AS max_n_ips_chosen,
  ANY_VALUE(n_active_dscids_in_ds)                                                   AS n_active_dscids_in_ds
FROM campaign_dscid_ranked
GROUP BY ds_id
ORDER BY ds_id;
