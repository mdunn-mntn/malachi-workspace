SELECT a.campaign_id, APPROX_QUANTILES(a.total_audience_size,2)[OFFSET(1)] med_total,
   MAX(a.total_audience_size) max_total, APPROX_QUANTILES(a.funnel_audience_size,2)[OFFSET(1)] med_funnel
 FROM `dw-main-silver.perml.flight_cid_day_audience_sizes` a
 WHERE a.campaign_id IN (SELECT campaign_id FROM `dw-main-bronze.integrationprod.campaigns`
     WHERE advertiser_id={{AID}} AND deleted=FALSE AND objective_id=1)
   AND a.rpt_day BETWEEN "{{P2_START}}" AND "{{P2_END}}"
 GROUP BY 1 ORDER BY med_total DESC
