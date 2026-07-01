-- Collapsed HHST gate-change events on prospecting campaigns (0/-1/-100=no gate; 6666=HI+PP; 10000=HI-only).
-- Params: {{AID}} {{WIN_START}} {{WIN_END}}
WITH h AS (SELECT campaign_id, threshold, update_time,
    LAG(threshold) OVER (PARTITION BY campaign_id ORDER BY update_time) prev
  FROM `dw-main-silver.archives.household_score_threshold_archives`
  WHERE advertiser_id={{AID}} AND update_time>=TIMESTAMP("{{WIN_START}}") AND update_time<TIMESTAMP("{{WIN_END}}")),
camp AS (SELECT campaign_id, name FROM `dw-main-bronze.integrationprod.campaigns`
  WHERE advertiser_id={{AID}} AND objective_id=1 AND funnel_level=1 AND deleted=FALSE)
SELECT DATE(h.update_time) change_date, h.campaign_id, c.name, h.threshold AS new_threshold
FROM h JOIN camp c USING(campaign_id) WHERE prev IS NULL OR threshold!=prev ORDER BY h.update_time
