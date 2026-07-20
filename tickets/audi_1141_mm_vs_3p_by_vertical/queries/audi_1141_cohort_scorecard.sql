/* ============================================================================
   AUDI-1141 — MM vs 3P prospecting performance by sales vertical (trailing 6mo)
   ----------------------------------------------------------------------------
   Campaign-grain cohort with bucket / cap / zip flags + KPIs. Aggregation
   (advertiser-weighted + impression-weighted) happens in Python.

   Cohort : S1 prospecting (objective_id=1, funnel_level=1), delivered
            (impressions>0) in trailing 180d. Classified at the bidder-facing
            SEGMENT level (silver.audience.audience_segments, type=2, targeted).
   Buckets: MM  = DS13/19/38/46 present, no 3P
            3P  = DS17 ShareThis / DS18 Dstillery / DS35 LiveRamp, no MM
            Mixed = both ; Neither = CRM/1P/geo-only (excluded downstream)
   Cap    : HHST gate (household_score_threshold_archives). threshold>0 = gate on.
   Zip    : zip-level (location_type_id=7) location_id in the INCLUDE block of
            geos (before the first "op":"not") = narrowing → drop except Auto/ProServ.
   Vertical: advertiser -> fpa_advertiser_verticals type=0 parent -> 8 sales buckets.
   KPIs   : visits=views+clicks, conv=click+view conv, revenue=click+view order value,
            spend=media+data+platform. Default (non-competing) attribution lens.
   ============================================================================ */
WITH
kpi AS (
  SELECT campaign_id,
    ANY_VALUE(advertiser_id) AS advertiser_id,
    SUM(impressions) AS imps,
    SUM(views) + SUM(clicks) AS visits,
    SUM(click_conversions) + SUM(view_conversions) AS conv,
    SUM(click_order_value) + SUM(view_order_value) AS revenue,
    SUM(media_spend) + SUM(data_spend) + SUM(platform_spend) AS spend
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`
  WHERE day >= DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY)
  GROUP BY campaign_id
  HAVING imps > 0
),
camp AS (
  SELECT k.*
  FROM kpi k
  JOIN `dw-main-bronze.integrationprod.campaigns` c USING (campaign_id)
  WHERE c.deleted = FALSE AND c.objective_id = 1 AND c.funnel_level = 1
),
seg AS (
  SELECT s.campaign_id,
    STRING_AGG(s.expression, "|||") AS expr_all,
    ARRAY_CONCAT_AGG(REGEXP_EXTRACT_ALL(s.expression, r'"data_source_id":([0-9]+)')) AS ds_ids
  FROM `dw-main-silver.audience.audience_segments` s
  JOIN camp USING (campaign_id)
  WHERE s.expression_type_id = 2 AND s.is_targeted = TRUE
  GROUP BY s.campaign_id
),
ds_flags AS (
  SELECT campaign_id, expr_all,
    ('13' IN UNNEST(ds_ids) OR '19' IN UNNEST(ds_ids) OR '38' IN UNNEST(ds_ids) OR '46' IN UNNEST(ds_ids)) AS has_mm,
    ('17' IN UNNEST(ds_ids) OR '18' IN UNNEST(ds_ids) OR '35' IN UNNEST(ds_ids)) AS has_3p,
    ('19' IN UNNEST(ds_ids)) AS has_ds19,
    ('13' IN UNNEST(ds_ids)) AS has_ds13,
    ('46' IN UNNEST(ds_ids)) AS has_ds46,
    ('38' IN UNNEST(ds_ids)) AS has_ds38
  FROM seg
),
inc AS (
  SELECT campaign_id,
    REGEXP_EXTRACT(expr_all, r'"geos":\{"where":\{"op":"and","value":\[(.*?)\{"op":"not"') AS inc_block,
    REGEXP_CONTAINS(expr_all, r'geo_radii') AS has_radius
  FROM ds_flags
),
inc_ids AS (
  SELECT campaign_id, CAST(TRIM(id) AS INT64) AS location_id
  FROM inc, UNNEST(REGEXP_EXTRACT_ALL(inc_block, r'"location_ids":\[([0-9,]+)\]')) l, UNNEST(SPLIT(l, ",")) id
  WHERE TRIM(id) != ""
),
zip_flag AS (
  SELECT i.campaign_id,
    LOGICAL_OR(ld.location_type_id = 7) AS zip_in_include,
    LOGICAL_OR(ld.location_type_id = 6) AS city_in_include
  FROM inc_ids i
  JOIN `dw-main-silver.geo.location_data` ld USING (location_id)
  GROUP BY 1
),
hhst AS (
  SELECT a.campaign_id,
    COUNT(*) AS hhst_writes,
    COUNTIF(a.threshold > 0) AS hhst_writes_gated,
    MAX(a.threshold) AS hhst_max,
    ARRAY_AGG(a.threshold ORDER BY a.update_time DESC LIMIT 1)[OFFSET(0)] AS hhst_latest
  FROM `dw-main-silver.archives.household_score_threshold_archives` a
  JOIN camp USING (campaign_id)
  WHERE a.update_time >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY))
  GROUP BY a.campaign_id
),
cur AS (
  SELECT campaign_id, threshold AS hhst_current
  FROM `dw-main-silver.dso.household_score_thresholds`
),
vert AS (
  SELECT advertiser_id, ANY_VALUE(vertical_id) AS vertical_id, ANY_VALUE(vertical_name) AS vertical_name
  FROM `dw-main-bronze.integrationprod.fpa_advertiser_verticals`
  WHERE type = 0
  GROUP BY advertiser_id
)
SELECT
  c.campaign_id, c.advertiser_id,
  v.vertical_id, v.vertical_name,
  CASE v.vertical_id
    WHEN 112 THEN 'ProServ' WHEN 111 THEN 'ProServ' WHEN 121 THEN 'ProServ' WHEN 128 THEN 'ProServ' WHEN 109 THEN 'ProServ'
    WHEN 107 THEN 'Education'
    WHEN 101 THEN 'Retail / Ecom' WHEN 130 THEN 'Retail / Ecom' WHEN 120 THEN 'Retail / Ecom' WHEN 116 THEN 'Retail / Ecom'
      WHEN 103 THEN 'Retail / Ecom' WHEN 132 THEN 'Retail / Ecom' WHEN 133 THEN 'Retail / Ecom' WHEN 105 THEN 'Retail / Ecom' WHEN 119 THEN 'Retail / Ecom'
    WHEN 115 THEN 'Gaming / Entertainment' WHEN 110 THEN 'Gaming / Entertainment' WHEN 102 THEN 'Gaming / Entertainment' WHEN 131 THEN 'Gaming / Entertainment'
    WHEN 104 THEN 'Telco & Tech' WHEN 108 THEN 'Telco & Tech' WHEN 136 THEN 'Telco & Tech'
    WHEN 129 THEN 'Restaurants / Dining' WHEN 114 THEN 'Restaurants / Dining'
    WHEN 117 THEN 'CPG & Health' WHEN 113 THEN 'CPG & Health' WHEN 106 THEN 'CPG & Health' WHEN 126 THEN 'CPG & Health' WHEN 127 THEN 'CPG & Health' WHEN 122 THEN 'CPG & Health'
    WHEN 137 THEN 'Auto, Travel & Hospitality' WHEN 135 THEN 'Auto, Travel & Hospitality' WHEN 134 THEN 'Auto, Travel & Hospitality' WHEN 123 THEN 'Auto, Travel & Hospitality'
    ELSE 'Other / Unmapped'
  END AS sales_vertical,
  CASE WHEN f.has_mm AND f.has_3p THEN 'Mixed'
       WHEN f.has_mm AND NOT f.has_3p THEN 'MM'
       WHEN f.has_3p AND NOT f.has_mm THEN '3P'
       ELSE 'Neither' END AS bucket,
  f.has_ds19, f.has_ds13, f.has_ds46, f.has_ds38,
  COALESCE(z.zip_in_include, FALSE) AS zip_narrow,
  COALESCE(z.city_in_include, FALSE) AS city_narrow,
  COALESCE(i.has_radius, FALSE) AS radius_narrow,
  COALESCE(h.hhst_writes, 0) AS hhst_writes,
  COALESCE(h.hhst_writes_gated, 0) AS hhst_writes_gated,
  COALESCE(h.hhst_max, 0) AS hhst_max,
  COALESCE(h.hhst_latest, 0) AS hhst_latest,
  COALESCE(cu.hhst_current, 0) AS hhst_current,
  c.imps, c.visits, c.conv, c.revenue, c.spend
FROM camp c
JOIN ds_flags f USING (campaign_id)
LEFT JOIN inc i USING (campaign_id)
LEFT JOIN zip_flag z USING (campaign_id)
LEFT JOIN hhst h USING (campaign_id)
LEFT JOIN cur cu USING (campaign_id)
LEFT JOIN vert v ON v.advertiser_id = c.advertiser_id
