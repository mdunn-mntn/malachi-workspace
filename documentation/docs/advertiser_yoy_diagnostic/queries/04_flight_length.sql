-- Flight-length: runs of consecutive active days per campaign. Short flights (<=3d) auto-trigger HHST=0.
-- Params: {{AID}} {{WIN_START}} {{WIN_END}}
WITH days AS (
  SELECT campaign_id, day FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`
  WHERE advertiser_id={{AID}} AND day>="{{WIN_START}}" AND day<"{{WIN_END}}" AND impressions>0 GROUP BY 1,2),
grp AS (SELECT campaign_id, day,
    DATE_SUB(day, INTERVAL ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY day) DAY) run_key FROM days),
runs AS (SELECT campaign_id, MIN(day) run_start, MAX(day) run_end, COUNT(*) run_days FROM grp GROUP BY 1, run_key)
SELECT r.campaign_id, c.name, COUNT(*) n_flights, ROUND(AVG(run_days),1) avg_flight_days,
  COUNTIF(run_days<=3) short_flights_le3d, ROUND(100*COUNTIF(run_days<=3)/COUNT(*),0) pct_short
FROM runs r JOIN `dw-main-bronze.integrationprod.campaigns` c USING(campaign_id)
WHERE c.objective_id=1 AND c.funnel_level=1
GROUP BY 1,2 ORDER BY short_flights_le3d DESC
