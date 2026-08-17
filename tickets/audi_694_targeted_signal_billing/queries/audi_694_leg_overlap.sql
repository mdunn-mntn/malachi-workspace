-- Is leg 2 (segment->household translation) redundant with leg 1 (auction->household)?
-- Result 2026-08-17 on ddp_crm_graph_cpm (216,409 rows): leg2 empty on 0.0% of rows, and leg2
-- contributes a vendor leg1 does NOT carry on 49,016 rows (22.7%). So the graph_version join-loss
-- concern does not reproduce in the 2026-08-13 build, and per-touchpoint vs per-vendor crediting
-- changes the credited vendor set on ~a fifth of DS63 impressions.
SELECT
  COUNT(*) AS rows_,
  COUNTIF(ARRAY_LENGTH(leg1_graph_dsids)=0) AS leg1_empty,
  COUNTIF(ARRAY_LENGTH(leg2_graph_dsids)=0) AS leg2_empty,
  ROUND(100*COUNTIF(ARRAY_LENGTH(leg2_graph_dsids)=0)/COUNT(*),1) AS pct_leg2_empty,
  COUNTIF(ARRAY_LENGTH(leg1_graph_dsids)=0 AND ARRAY_LENGTH(leg2_graph_dsids)=0) AS both_empty,
  COUNTIF((SELECT COUNT(*) FROM UNNEST(leg2_graph_dsids) d
            WHERE d NOT IN UNNEST(leg1_graph_dsids)) > 0) AS leg2_adds_new_vendor
FROM `dw-main-gold.reporting.ddp_crm_graph_cpm`
LIMIT 5;
