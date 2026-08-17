-- THE MONEY QUERY. Deepsync (DS29, the only per-impression-billable vendor on the graph path)
-- credited impression-share under the three candidate divisor rules, on real DS63 output.
-- Source: dw-main-gold.reporting.ddp_crm_graph_cpm (built 2026-08-13, dt 2026-08-06..08-12).
-- Result 2026-08-17 on 216,409 match rows / 209,076 with deepsync present:
--   A PR#24 (billable-only divisor)  209,076 shares  $104.54/wk   <- deepsync is sole billable vendor
--   B MM parity (all sources in N)    44,626.8       $ 22.31/wk   <- 4.7x less than A
--   C full preemption (AUDI-1113)        807.0       $  0.40/wk   <- 259x less than A
-- 99.6% of deepsync-credited impressions also carry a free log (guid 23 / augmentor 30).
-- Grain caveat: this is the MATCH grain, before the cross-provider winner split
-- (ddp_winners_imp.impression_cnt = 1/N over ad_served_id). Upper bound on all three.
WITH imp AS (
  SELECT ad_served_id, graph_dsids,
         (SELECT COUNT(*) FROM UNNEST(graph_dsids) d
           WHERE d IN (17,24,28,29,33,35,36,40,51)) AS n_billable,   -- external_reporting_required roster
         ARRAY_LENGTH(graph_dsids) AS n_all,
         EXISTS(SELECT 1 FROM UNNEST(graph_dsids) d WHERE d IN (23,30)) AS free_present,
         EXISTS(SELECT 1 FROM UNNEST(graph_dsids) d WHERE d = 29)       AS deepsync_present
  FROM `dw-main-gold.reporting.ddp_crm_graph_cpm`
)
SELECT
  COUNT(*) AS imps_total,
  COUNTIF(deepsync_present) AS imps_with_deepsync,
  COUNTIF(deepsync_present AND free_present) AS deepsync_and_free,
  ROUND(SUM(IF(deepsync_present, 1.0/n_billable, 0)),1) AS a_share_billable_only_pr24,
  ROUND(SUM(IF(deepsync_present, 1.0/n_all, 0)),1)      AS b_share_mm_parity,
  ROUND(SUM(IF(deepsync_present AND NOT free_present, 1.0/n_billable, 0)),1) AS c_share_full_preemption,
  ROUND(SUM(IF(deepsync_present, 1.0/n_billable, 0))/1000*0.5, 2) AS a_usd,
  ROUND(SUM(IF(deepsync_present, 1.0/n_all, 0))/1000*0.5, 2)      AS b_usd,
  ROUND(SUM(IF(deepsync_present AND NOT free_present, 1.0/n_billable, 0))/1000*0.5, 2) AS c_usd
FROM imp
LIMIT 5;
