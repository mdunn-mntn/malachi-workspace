-- TI-837 Phase 2 cohort selection — Stage A.5
-- Vertical / category per advertiser
-- ----------------------------------------------------------------
-- Sources:
--   bronze.integrationprod.fpa_advertiser_verticals — vertical assignment
--   bronze.integrationprod.advertisers              — current name (FPA name
--                                                      is unreliable per
--                                                      global gotcha — write-once,
--                                                      stale, sometimes empty)
--
-- One row per advertiser; if multiple FPA vertical rows exist (rare),
-- pick the most recent one.
-- ----------------------------------------------------------------

WITH
fpa_latest AS (
  SELECT
    advertiser_id,
    vertical_name,
    vertical_id,
    type AS vertical_type,
    ROW_NUMBER() OVER (
      PARTITION BY advertiser_id
      ORDER BY updated_time DESC NULLS LAST, created_time DESC NULLS LAST
    ) AS rn
  FROM `dw-main-bronze.integrationprod.fpa_advertiser_verticals`
),

advertiser_dim AS (
  SELECT
    advertiser_id,
    company_name
  FROM `dw-main-bronze.integrationprod.advertisers`
  WHERE deleted = FALSE
    AND is_test = FALSE
)

SELECT
  a.advertiser_id,
  a.company_name,
  v.vertical_name,
  v.vertical_id,
  v.vertical_type
FROM advertiser_dim a
LEFT JOIN (SELECT * FROM fpa_latest WHERE rn = 1) v
  USING (advertiser_id)
ORDER BY a.advertiser_id
