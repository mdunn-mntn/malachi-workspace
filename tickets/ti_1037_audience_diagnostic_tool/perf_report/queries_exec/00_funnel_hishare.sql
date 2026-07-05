SELECT campaign_id, COUNT(DISTINCT ip) reach_ip,
   COUNT(DISTINCT IF(household_score>=8001, ip, NULL)) hi_ip,
   COUNT(DISTINCT IF(household_score BETWEEN 6666 AND 8000, ip, NULL)) pp_ip,
   COUNT(DISTINCT IF(household_score BETWEEN 1 AND 6665, ip, NULL)) mid_ip,
   COUNT(DISTINCT IF(household_score<=0, ip, NULL)) unscored_ip
 FROM `dw-main-silver.logdata.cost_impression_log`
 WHERE advertiser_id={{AID}} AND DATE(time) BETWEEN "{{DELIV_MONTH_START}}" AND "{{DELIV_MONTH_END}}"
   AND campaign_id IN (SELECT campaign_id FROM `dw-main-bronze.integrationprod.campaigns`
     WHERE advertiser_id={{AID}} AND deleted=FALSE AND objective_id=1)
 GROUP BY 1 ORDER BY reach_ip DESC
