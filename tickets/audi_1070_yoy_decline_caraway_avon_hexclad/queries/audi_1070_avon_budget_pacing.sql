-- AUDI-1070 — Avon (31921) budget pacing: "% to cap" (Tofer's Over/Under Spend report) vs % of nominal.
-- % to cap = daily media_cost / DSO daily budget cap (ACTIVE campaign groups only).
-- DSO daily cap: dso_campaign_group_daily_budgets.budget (latest by update_time; table retains only current row).
-- GOTCHA: paused CGs keep stale tiny caps with 0 delivery -> excluding them (media_cost>0) is required, else % understates.

-- (1) Per-CG % to cap, last 30 days
WITH cap AS (
  SELECT campaign_group_id, budget AS daily_cap
  FROM `dw-main-bronze.integrationprod.dso_campaign_group_daily_budgets`
  WHERE advertiser_id=31921
  QUALIFY ROW_NUMBER() OVER (PARTITION BY campaign_group_id ORDER BY update_time DESC)=1
),
sp AS (
  SELECT campaign_group_id, COUNT(*) active_days, AVG(media_cost) avg_daily_media_cost
  FROM `dw-main-silver.summarydata.sum_by_campaign_group_by_day`
  WHERE advertiser_id=31921 AND day >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AND media_cost>0
  GROUP BY campaign_group_id
)
SELECT campaign_group_id AS cgid, ROUND(daily_cap) dso_daily_cap, ROUND(avg_daily_media_cost) avg_daily_media_cost,
  active_days, ROUND(100*SAFE_DIVIDE(avg_daily_media_cost, daily_cap),1) pct_to_cap
FROM cap JOIN sp USING (campaign_group_id) ORDER BY avg_daily_media_cost DESC;

-- (2) Nominal vs DSO operative budget reconciliation (why 99% vs ~40%)
-- Run separately:
-- SELECT cg.campaign_group_id, cg.name, cg.budget AS nominal_budget,
--   d.budget AS dso_daily_cap, f.budget AS dso_flight_budget
-- FROM (SELECT * FROM `dw-main-bronze.integrationprod.campaign_groups`
--       WHERE campaign_group_id IN (69271,69273)
--       QUALIFY ROW_NUMBER() OVER (PARTITION BY campaign_group_id ORDER BY update_time DESC)=1) cg
-- LEFT JOIN (SELECT campaign_group_id, budget FROM `dw-main-bronze.integrationprod.dso_campaign_group_daily_budgets`
--            WHERE campaign_group_id IN (69271,69273) QUALIFY ROW_NUMBER() OVER (PARTITION BY campaign_group_id ORDER BY update_time DESC)=1) d USING(campaign_group_id)
-- LEFT JOIN (SELECT campaign_group_id, budget FROM `dw-main-bronze.integrationprod.dso_campaign_group_flight_budgets`
--            WHERE campaign_group_id IN (69271,69273) QUALIFY ROW_NUMBER() OVER (PARTITION BY campaign_group_id ORDER BY update_time DESC)=1) f USING(campaign_group_id);
