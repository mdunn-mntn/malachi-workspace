/* =============================================================================
   End-to-end validation: s1_bid_ip / s1_vast_ip population by branch
   Advertiser 37775 | 2026-02-04 to 2026-02-11 (7 days cp_dedup)
   el_all / il_all: 30-day window (2026-01-07 to 2026-02-11)
   prior_vv_pool: 90-day lookback, advertiser 37775 only (cheap)

   Goal: confirm all 4 s1_branch CASE arms fire and s1_bid_ip is populated
   for each permutation. Output: one summary row per (vv_stage, pv_stage,
   s1_pv_stage, s1_branch) showing row_count and s1_bid_ip_fill_pct.
   ============================================================================= */

WITH campaigns_stage AS (
  SELECT campaign_id, funnel_level AS stage
  FROM `dw-main-bronze`.integrationprod.campaigns
  WHERE deleted = FALSE
)
, cp_dedup AS (
  SELECT
    cp.ad_served_id
    , cp.advertiser_id
    , cp.campaign_id
    , cp.ip
    , cp.is_new
    , cp.is_cross_device
    , cp.first_touch_ad_served_id
    , cp.time
    , c.stage AS vv_stage
  FROM `dw-main-silver`.logdata.clickpass_log AS cp
  LEFT JOIN campaigns_stage AS c ON c.campaign_id = cp.campaign_id
  WHERE
    cp.time >= TIMESTAMP('2026-02-04') AND cp.time < TIMESTAMP('2026-02-11')
    AND cp.advertiser_id = 37775
  QUALIFY row_number() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
)
, el_all AS (
  SELECT
    ad_served_id
    , ip AS vast_ip
    , bid_ip
    , time
    , row_number() OVER (PARTITION BY ad_served_id ORDER BY time) AS rn
  FROM `dw-main-silver`.logdata.event_log
  WHERE
    event_type_raw = 'vast_impression'
    AND time >= TIMESTAMP('2026-01-07') AND time < TIMESTAMP('2026-02-11')
)
, il_all AS (
  SELECT
    ad_served_id
    , ip AS vast_ip
    , bid_ip
    , time
    , row_number() OVER (PARTITION BY ad_served_id ORDER BY time) AS rn
  FROM `dw-main-silver`.logdata.impression_log
  WHERE
    time >= TIMESTAMP('2026-01-07') AND time < TIMESTAMP('2026-02-11')
)
, v_dedup AS (
  SELECT
    CAST(ad_served_id AS STRING) AS ad_served_id
    , ip
    , is_new
    , impression_ip
  FROM `dw-main-silver`.summarydata.ui_visits
  WHERE
    from_verified_impression = TRUE
    AND time >= TIMESTAMP('2026-01-28') AND time < TIMESTAMP('2026-02-18')
  QUALIFY row_number() OVER (PARTITION BY CAST(ad_served_id AS STRING) ORDER BY time DESC) = 1
)
, prior_vv_pool AS (
  /* Filtered to advertiser 37775 — dramatically reduces scan size while
     still covering all IPs in the chain for this advertiser. */
  SELECT
    cp.ip
    , cp.ad_served_id AS prior_vv_ad_served_id
    , cp.campaign_id AS pv_campaign_id
    , cp.time AS prior_vv_time
    , c.stage AS pv_stage
  FROM `dw-main-silver`.logdata.clickpass_log AS cp
  LEFT JOIN campaigns_stage AS c ON c.campaign_id = cp.campaign_id
  WHERE
    cp.time >= TIMESTAMP('2025-11-08') AND cp.time < TIMESTAMP('2026-02-11')
    AND cp.advertiser_id = 37775
  QUALIFY row_number() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
)
, with_all_joins AS (
  SELECT
    cp.ad_served_id
    , cp.vv_stage
    , pv.pv_stage
    , s1_pv.pv_stage AS s1_pv_stage
    , CASE
        WHEN cp.vv_stage = 1        THEN 1
        WHEN pv.pv_stage = 1        THEN 2
        WHEN s1_pv.pv_stage = 1     THEN 3
        ELSE 4
      END AS s1_branch
    /* s1_bid_ip — resolved via 4-branch CASE */
    , CASE
        WHEN cp.vv_stage = 1        THEN COALESCE(lt.bid_ip, lt_d.bid_ip)
        WHEN pv.pv_stage = 1        THEN COALESCE(pv_lt.bid_ip, pv_lt_d.bid_ip)
        WHEN s1_pv.pv_stage = 1     THEN COALESCE(s1_lt.bid_ip, s1_lt_d.bid_ip)
        ELSE                             COALESCE(s2_lt.bid_ip, s2_lt_d.bid_ip)
      END AS s1_bid_ip
    /* s1_vast_ip — same 4-branch CASE */
    , CASE
        WHEN cp.vv_stage = 1        THEN COALESCE(lt.vast_ip, lt_d.vast_ip)
        WHEN pv.pv_stage = 1        THEN COALESCE(pv_lt.vast_ip, pv_lt_d.vast_ip)
        WHEN s1_pv.pv_stage = 1     THEN COALESCE(s1_lt.vast_ip, s1_lt_d.vast_ip)
        ELSE                             COALESCE(s2_lt.vast_ip, s2_lt_d.vast_ip)
      END AS s1_vast_ip
    /* Also capture s1_ad_served_id to verify CASE resolution */
    , CASE
        WHEN cp.vv_stage = 1        THEN cp.ad_served_id
        WHEN pv.pv_stage = 1        THEN pv.prior_vv_ad_served_id
        WHEN s1_pv.pv_stage = 1     THEN s1_pv.prior_vv_ad_served_id
        ELSE                             s2_pv.prior_vv_ad_served_id
      END AS s1_ad_served_id
    /* lt_bid_ip for reference */
    , COALESCE(lt.bid_ip, lt_d.bid_ip) AS lt_bid_ip
    , row_number() OVER (
        PARTITION BY cp.ad_served_id
        ORDER BY pv.prior_vv_time DESC NULLS LAST
                , s1_pv.prior_vv_time DESC NULLS LAST
                , s2_pv.prior_vv_time DESC NULLS LAST
      ) AS _pv_rn
  FROM cp_dedup AS cp
  LEFT JOIN el_all AS lt
    ON lt.ad_served_id = cp.ad_served_id AND lt.rn = 1
  LEFT JOIN il_all AS lt_d
    ON lt_d.ad_served_id = cp.ad_served_id AND lt_d.rn = 1
  LEFT JOIN v_dedup AS v
    ON v.ad_served_id = cp.ad_served_id
  LEFT JOIN prior_vv_pool AS pv
    ON pv.ip = COALESCE(lt.bid_ip, lt_d.bid_ip)
    AND pv.prior_vv_time < cp.time
    AND pv.prior_vv_ad_served_id != cp.ad_served_id
    AND pv.pv_stage <= cp.vv_stage
  LEFT JOIN el_all AS pv_lt
    ON pv_lt.ad_served_id = pv.prior_vv_ad_served_id AND pv_lt.rn = 1
  LEFT JOIN il_all AS pv_lt_d
    ON pv_lt_d.ad_served_id = pv.prior_vv_ad_served_id AND pv_lt_d.rn = 1
  LEFT JOIN prior_vv_pool AS s1_pv
    ON s1_pv.ip = COALESCE(pv_lt.bid_ip, pv_lt_d.bid_ip)
    AND s1_pv.pv_stage <= pv.pv_stage
    AND s1_pv.prior_vv_time < pv.prior_vv_time
    AND s1_pv.prior_vv_ad_served_id != pv.prior_vv_ad_served_id
  LEFT JOIN el_all AS s1_lt
    ON s1_lt.ad_served_id = s1_pv.prior_vv_ad_served_id AND s1_lt.rn = 1
  LEFT JOIN il_all AS s1_lt_d
    ON s1_lt_d.ad_served_id = s1_pv.prior_vv_ad_served_id AND s1_lt_d.rn = 1
  LEFT JOIN prior_vv_pool AS s2_pv
    ON s2_pv.ip = COALESCE(s1_lt.bid_ip, s1_lt_d.bid_ip)
    AND s2_pv.pv_stage = 1
    AND s2_pv.prior_vv_time < s1_pv.prior_vv_time
    AND s2_pv.prior_vv_ad_served_id != s1_pv.prior_vv_ad_served_id
  LEFT JOIN el_all AS s2_lt
    ON s2_lt.ad_served_id = s2_pv.prior_vv_ad_served_id AND s2_lt.rn = 1
  LEFT JOIN il_all AS s2_lt_d
    ON s2_lt_d.ad_served_id = s2_pv.prior_vv_ad_served_id AND s2_lt_d.rn = 1
)
SELECT
  vv_stage
  , pv_stage
  , s1_pv_stage
  , s1_branch
  , COUNT(*) AS row_count
  , COUNTIF(s1_bid_ip IS NOT NULL) AS s1_bid_ip_found
  , COUNTIF(s1_vast_ip IS NOT NULL) AS s1_vast_ip_found
  , COUNTIF(s1_ad_served_id IS NOT NULL) AS s1_ad_served_id_found
  , ROUND(100.0 * COUNTIF(s1_bid_ip IS NOT NULL) / COUNT(*), 1) AS s1_bid_ip_fill_pct
FROM with_all_joins
WHERE _pv_rn = 1
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4
