# Draft reply to Jack re: simulation data (2026-08-19)

Most of it isn't from enriched impressions actually. The vendor and divisor numbers all come from
dw-main-gold.reporting.ddp_crm_graph_cpm, which is Wei and Maya's own build, so you already have it.
It's one row per DS63 impression with graph_dsids plus leg1_graph_dsids and leg2_graph_dsids split
out, 214k impressions for Aug 6 through 12. That's the right table to simulate on since you can vary
the rule without rebuilding anything.

Here's the query I used for the three scenarios, the only thing that changes between them is the
divisor:

WITH imp AS (
  SELECT ad_served_id, graph_dsids,
         (SELECT COUNT(*) FROM UNNEST(graph_dsids) d
           WHERE d IN (17,24,28,29,33,35,36,40,51)) AS n_billable,
         ARRAY_LENGTH(graph_dsids) AS n_all,
         EXISTS(SELECT 1 FROM UNNEST(graph_dsids) d WHERE d IN (23,30)) AS free_present,
         EXISTS(SELECT 1 FROM UNNEST(graph_dsids) d WHERE d = 29)       AS deepsync_present
  FROM `dw-main-gold.reporting.ddp_crm_graph_cpm`
)
SELECT
  COUNTIF(deepsync_present) AS imps_with_deepsync,
  SUM(IF(deepsync_present, 1.0/n_billable, 0)) AS a_billable_only,
  SUM(IF(deepsync_present, 1.0/n_all, 0))      AS b_all_sources,
  SUM(IF(deepsync_present AND NOT free_present, 1.0/n_billable, 0)) AS c_free_log_preemption
FROM imp;

That 17,24,28,... list is the current billable roster, direct_data_partners where is_current and
external_reporting_required. Worth knowing that 40 credits back to 28, so 33Across shows up as two
vendors unless you collapse them, which is the thing that bit BAE on BAE-4923.

One caveat, that's the match grain, before the winner split spreads the impression across providers.
Fine for comparing rules against each other, not for absolute dollars.

The only number that needed enriched impressions was the coverage gap, 39% of in-scope DS63
impressions have no crediting row at all. Happy to save that cohort out if you want it.
