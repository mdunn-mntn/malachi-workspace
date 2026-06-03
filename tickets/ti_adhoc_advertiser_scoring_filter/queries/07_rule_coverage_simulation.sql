-- Empirical answer to Victor's question:
-- "Are the 4 rules good enough to identify campaigns running tomorrow?"
--
-- Test: of advertisers that actually spent yesterday, what % are caught by
-- each of the 4 rules applied to today's state?
-- (If today's filter catches yesterday's bidders, tomorrow's filter will
-- catch tomorrow's bidders by the same logic.)

WITH yesterday_spenders AS (
  SELECT DISTINCT advertiser_id
  FROM `dw-main-silver.summarydata.sum_by_advertiser_by_day`
  WHERE day BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
                AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    AND media_spend > 0
),
rule1 AS (
  SELECT DISTINCT advertiser_id
  FROM `dw-main-bronze.integrationprod.campaign_groups`
  WHERE deleted = FALSE AND is_test = FALSE
    AND (
      (start_time <= CURRENT_TIMESTAMP()
        AND (end_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY) OR end_time IS NULL))
      OR (start_time BETWEEN CURRENT_TIMESTAMP() AND TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 7 DAY))
    )
),
rule2 AS (
  SELECT DISTINCT advertiser_id
  FROM `dw-main-bronze.integrationprod.campaign_groups`
  WHERE deleted = FALSE AND is_test = FALSE
    AND update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
),
rule3 AS (
  SELECT advertiser_id
  FROM `dw-main-bronze.integrationprod.advertisers`
  WHERE deleted = FALSE AND is_test = FALSE
    AND create_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
),
rule4 AS (
  SELECT advertiser_id
  FROM `dw-main-bronze.integrationprod.advertisers`
  WHERE deleted = FALSE AND is_test = FALSE
    AND update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
)
SELECT
  COUNT(*) AS n_yesterday_spenders,
  COUNTIF(advertiser_id IN (SELECT advertiser_id FROM rule1)) AS in_rule1_live,
  COUNTIF(advertiser_id IN (SELECT advertiser_id FROM rule2)) AS in_rule2_cg_update,
  COUNTIF(advertiser_id IN (SELECT advertiser_id FROM rule3)) AS in_rule3_new_adv,
  COUNTIF(advertiser_id IN (SELECT advertiser_id FROM rule4)) AS in_rule4_adv_update,
  COUNTIF(
    advertiser_id IN (SELECT advertiser_id FROM rule1)
    OR advertiser_id IN (SELECT advertiser_id FROM rule2)
    OR advertiser_id IN (SELECT advertiser_id FROM rule3)
    OR advertiser_id IN (SELECT advertiser_id FROM rule4)
  ) AS in_any_rule,
  COUNTIF(
    advertiser_id NOT IN (SELECT advertiser_id FROM rule1)
    AND advertiser_id NOT IN (SELECT advertiser_id FROM rule2)
    AND advertiser_id NOT IN (SELECT advertiser_id FROM rule3)
    AND advertiser_id NOT IN (SELECT advertiser_id FROM rule4)
  ) AS missed_by_all_rules
FROM yesterday_spenders;
