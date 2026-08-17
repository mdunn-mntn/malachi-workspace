-- Which sources actually enable a DS63 impression, split by touchpoint.
-- Result 2026-08-17 over 214,251 impressions (dt 2026-08-06..08-12):
--   30 augmentor_log  211,370 (98.7%)  leg1 only      FREE
--   22 Experian       208,723 (97.4%)                 flat_fee, external_reporting_required = false
--   29 deepsync       207,031 (96.6%)                 $0.50 fixed_cpm, THE only billable one
--   58 Audience Acuity 205,681 (96.0%)                not in direct_data_partners at all
--   23 guid_log       175,981 (82.1%)  leg1 mostly    FREE
-- avg vendors/impression 4.708 (leg1 auction 4.456, leg2 graph 1.684).
WITH e AS (
  SELECT ad_served_id, d AS dsid, "combined" AS leg
    FROM `dw-main-gold.reporting.ddp_crm_graph_cpm`, UNNEST(graph_dsids) d
  UNION ALL
  SELECT ad_served_id, d, "leg1_auction"
    FROM `dw-main-gold.reporting.ddp_crm_graph_cpm`, UNNEST(leg1_graph_dsids) d
  UNION ALL
  SELECT ad_served_id, d, "leg2_graph"
    FROM `dw-main-gold.reporting.ddp_crm_graph_cpm`, UNNEST(leg2_graph_dsids) d
)
SELECT e.dsid, ds.name, e.leg, COUNT(DISTINCT e.ad_served_id) AS imps
FROM e
LEFT JOIN `dw-main-bronze.integrationprod.data_sources` ds
       ON CAST(ds.data_source_id AS INT64) = e.dsid
GROUP BY 1,2,3
ORDER BY imps DESC
LIMIT 40;
