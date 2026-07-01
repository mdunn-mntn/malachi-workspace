/* CRUX: daily per-prospecting-campaign delivery composition JOINED to the HHST gate in effect that day. | Reveals gate flips + overnight delivery inversion. Params: {{AID}} {{WIN_START}} {{WIN_END}} */
WITH camp AS (SELECT campaign_id, name FROM `dw-main-bronze.integrationprod.campaigns`
  WHERE advertiser_id={{AID}} AND objective_id=1 AND funnel_level=1 AND deleted=FALSE),
gate_daily AS (
  SELECT campaign_id, chg_date, threshold FROM (
    SELECT campaign_id, DATE(update_time) chg_date, threshold,
      ROW_NUMBER() OVER (PARTITION BY campaign_id, DATE(update_time) ORDER BY update_time DESC) rn
    FROM `dw-main-silver.archives.household_score_threshold_archives`
    WHERE advertiser_id={{AID}} AND update_time < TIMESTAMP("{{WIN_END}}")) WHERE rn=1),
delivery AS (
  SELECT campaign_id, DATE(time) d, COUNT(*) imps,
    ROUND(100*COUNTIF(hs=10000 OR hs BETWEEN 8001 AND 9999)/COUNT(*),1) pct_hi,
    ROUND(100*COUNTIF(hs IS NULL OR hs<=0)/COUNT(*),1) pct_unscored
  FROM (SELECT campaign_id, time, COALESCE(household_score, SAFE_CAST(REGEXP_EXTRACT(model_params, r"household_score=(-?[0-9]+)") AS INT64)) hs
        FROM `dw-main-silver.logdata.cost_impression_log`
        WHERE advertiser_id={{AID}} AND time>=TIMESTAMP("{{WIN_START}}") AND time<TIMESTAMP("{{WIN_END}}")
          AND campaign_id IN (SELECT campaign_id FROM camp)
          AND (model_params IS NULL OR model_params NOT LIKE "%realtime_conquest_score=10000%")) GROUP BY 1,2)
SELECT dl.d, dl.campaign_id, c.name, g.threshold AS gate_asof, dl.imps, dl.pct_hi, dl.pct_unscored
FROM delivery dl JOIN camp c ON c.campaign_id=dl.campaign_id
LEFT JOIN gate_daily g ON g.campaign_id=dl.campaign_id AND g.chg_date<=dl.d
QUALIFY ROW_NUMBER() OVER (PARTITION BY dl.campaign_id, dl.d ORDER BY g.chg_date DESC)=1
ORDER BY dl.d, dl.campaign_id
