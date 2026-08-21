# AUDI-1215 instrument coverage facts (run 2026-08-21)

AID 51660 (ElevenLabs), CGID 122748, campaign 608814. PRE <= 2026-06-30, BLACKOUT 2026-07-01..07-10, POST >= 2026-07-11.

## Instrument A: dw-main-silver.enriched.lift__ghost_bid_visits
Physical: sqlmesh__enriched.enriched__lift__ghost_bid_visits__2999749496, 4.33B rows / 470GB, partitioned dt DAY, NOT clustered, last modified 2026-08-21.
Overall (dt 2026-06-01..2026-08-21): MIN(dt)=2026-06-22, MAX(dt)=2026-08-20, 4,327,793,099 rows.
CGID 122748: partner_id=8 only (no partner 79 rows at all). Weekly detail: audi_1215_ghost_bid_coverage_combined.csv.
Entry-cohort ghost_frac (anchor=first dt per advertiser x campaign x ip, valid band 0.09-0.11):
W26 0.1025 | W27 0.0960 | W28 0.0953 | W29 0.0877 (below band) | W30 0.0917 | W31 0.0925 | W32 0.0965 | W33 0.0954 | W34 0.0947.
Pre window usable: 2026-06-22..2026-06-30 only (9 days). Post: 2026-07-11..2026-08-20.
Scan cost gotcha: no clustering, CGID-filtered scan is ~214GB minimum (dry-run is exact); combined coverage query 284GB, ran once.

## Instrument B: holdout lineage (silver views + gold reporting)
Monthly runs, begin_date 2025-09-01..2026-07-01. Silver physical tables last modified 2026-08-03; results_by_month_raw 2026-08-11. NO August run.
- lift__holdout_advertisers: 51660 in runs 2026-02-01..2026-07-01. conversion_window 2,592,000s (30d) Feb-May, 3,715,200s (43d) Jun-Jul. tz America/New_York.
- lift__holdout_campaign_groups: CGID 122748 in runs 2026-05-01/06-01/07-01, campaign 608814, type primary.
- lift__holdout_visits (82.3M rows): ip x time, control-arm visits only, advertiser grain, no CGID column. 51660: Feb 3,386 / Mar 11,115 / Apr 17,411 / May 63,482 / Jun 51,702 / Jul 34,048 (equal to control_visits in results).
- lift__holdout_households (16.83B rows, 656GB) and lift__holdout_audiences (16.86B rows, 792GB): presence check skipped, ~21GB scan even partition-filtered (no clustering); membership implied by advertisers/step1 rows.
- gold lift__holdout_conversions (3.35M rows): control-side only for 51660, campaign_group_id NULL on all rows; daily timestamps.
- gold lift__holdout_results_step1: 51660 holdout_audience_size Feb 279,632 / Mar 1,882,784 / Apr 2,060,734 / May 7,607,558 / Jun 5,068,305 / Jul 6,019,829.
- gold lift__holdout_results_step2: daily day rows but control_group=true only for 51660; campaign_group_id is a comma-joined STRING of the run's CGs.
- gold lift__results_by_month_raw / v_lift__results_by_month: grain = one row per advertiser x objective x monthly run (day=begin_date, n_days=1). 51660 rows Feb-Jul 2026, objective_id=1, all status SUCCESS.
  Jun run campaign_group_id='107892, 114635, 122748' (mixed). Jul run ='122748'. Feb-May label empty/varies.
  Jul run (2026-07-01..2026-08-01): objective_impressions 23,070,780; objective_visits 142,489; control_visits 34,048; weighted_control_visits 47,231; objective_conversions 7,024; control_conversions 1,584; users_reached 8,089,272; control_users 5,831,376; holdout_aud 5,990,678; multiplier 1.3872.
  Jun run: objective_impressions 22,765,712; objective_visits 235,723; control_visits 51,702; objective_conversions 13,311; control_conversions 3,601; users_reached 5,938,106; holdout_aud 5,021,406; multiplier 1.2016.
- gold v_lift__conversions: treatment (control=false) conversions ARE CGID-attributed with timestamps. CGID 122748: May run 5,678 (first conv 2026-05-07), Jun run 13,211, Jul run 7,019 (+probattr 2/16/5). Control conversions have NULL campaign_group_id.

## Instrument C: gold ghost-bid (all-time aggregates, refreshed 2026-08-21)
- lift__ghost_bid_results: 11 rows for CGID 122748 (campaign 608814, partner 8). stratum_types: overall(all); bid_count(1, 2-3, 4-10, 11+); score_band(High, Mid, PP, no_score); score_band_ivw(combined); score_band_mh(combined). Overall n_treatment=16,854,639, n_holdout=1,748,403, ghost_frac=0.09398, has_valid_holdout=true, meets_min_n=true, meets_min_compliance=true, ghost_frac_inflated=false.
- lift__ghost_bid_rollup: campaign_group/122748/partner 8 row present (1/1 campaigns, n=16,854,639/1,748,403); advertiser 51660 rollup spans 2 CGs (122748, 130550).
