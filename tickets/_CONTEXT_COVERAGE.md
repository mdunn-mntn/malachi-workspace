# Context Pass Coverage

One row per ticket processed by the context pass. state=carded (TL;DR written) or needs_review.

| ticket | state | facts | notes |
|---|---|---|---|
| audi_1089_ddp_vendor_evaluations | carded | 0 | epic; all durable facts already in knowledge/*.md |
| audi_1089_ddp_vendor_evaluations/ds24_justuno | carded | 1 | IPv6 19.61% feed-share delta → data_knowledge.md |
| audi_1089_ddp_vendor_evaluations/ds26_predactiv | carded | 4 | DS17/DS26 split, HEM DAG caps, HEMSignalReader inventory, ingest path |
| audi_1089_ddp_vendor_evaluations/ds28_33across | carded | 3 | 32-col raw feed, legacy device_id DAG, 41.68M sole-IP base |
| audi_1089_ddp_vendor_evaluations/ds39_klickly | carded | 1 | BUK ALS training enrichment consumer |
| audi_1089_ddp_vendor_evaluations/ds40_33across_api | carded | 3 | context-full batch 2 |
| audi_1091_augmentor_full_source | carded | 3 | context-full batch 2 |
| audi_1111_vendor_quality | carded | 4 | context-full batch 2 |
| audi_1111_vendor_quality/audi_1115_wtp_cpm | carded | 6 | context-full batch 2 |
| audi_1111_vendor_quality/audi_1116_rtc_free_logs | carded | 0 | context-full batch 2 |
| audi_1111_vendor_quality/audi_1117_ds14_svs_overlap | carded | 6 | context-full batch 2 |
| ber_2250_incrementality_overhaul/ti_1019_mde_calculator_advertiser_prefill | carded | 4 | context-full batch 2 |
| ber_2250_incrementality_overhaul/ti_1039_liftlab_design_review | carded | 0 | context-full batch 2 |
| ber_2250_incrementality_overhaul/ti_831_audience_deciles | carded | 0 | context-full batch 3; random-bucketing clarification |
| ber_2250_incrementality_overhaul/ti_835_control_group_design | carded | 0 | context-full batch 3; guid vs clickpass two-stories |
| ber_2250_incrementality_overhaul/ti_839_measure_results | carded | 0 | context-full batch 3; backlog stub |
| ber_2250_incrementality_overhaul/ti_842_present_results | carded | 0 | context-full batch 3; backlog stub |
| ber_2250_incrementality_overhaul/ti_884_power_sample_size_analysis | carded | 2 | context-full batch 3; MDE self-test + Lauren cross-val → experimentation.md |
| ber_2250_incrementality_overhaul/ti_885_mid_intent_experiment_setup | carded | 0 | context-full batch 3; design-only, Kirsa blocker |
| ber_2250_incrementality_overhaul/ti_886_uplift_model_implementation | carded | 0 | context-full batch 3; plan-only stub |
| ber_2250_incrementality_overhaul/ti_933_select_lift_analysis | carded | 3 | context-full batch 4; Select incremental pooled lift |
| dm_3118_rtc_monitor | carded | 0 | context-full batch 4; RTC monitoring SQL |
| dm_3188_comparison_rt_and_non_rt | carded | 0 | context-full batch 4; RT vs non-RT export, numbers in Drive CSV |
| goal_attainment_customer_goal_map | carded | 0 | context-full batch 4; added front-matter; Mode report data map |
| mm_44_ipdsc_hh_discrepancy | carded | 0 | context-full batch 4; stub, findings in artifacts |
| tgt_4016_ecomm_classifier_thresholds | carded | 0 | context-full batch 4; threshold in notebook |
| tgt_4103_common_crawl_coverage | carded | 0 | context-full batch 5; stub, results in notebook |
| ti_033_vertical_classification_changes | carded | 0 | context-full batch 5; results in Drive/gitignored |
| ti_1003_experiment_archive | carded | 0 | context-full batch 5; experiment archive Phase 1 |
| ti_1016_memdb_bidder_cache_optimization | carded | 2 | context-full batch 5; cache-shrink killed, segmentless-score lever |
| ti_1027_5x5_data_evaluation | carded | 1 | context-full batch 5; 5x5 DDP KEEP eval |
| ti_1033_experiment_archive_deploy | carded | 0 | context-full batch 5; archive host/ship/polish |
| ti_1037_audience_diagnostic_tool | carded | 0 | batch 6 |
| ti_1044_elevenlabs_ctv_incrementality | carded | 2 | batch 6 |
| ti_1053_elevenlabs_3p_segments | carded | 0 | batch 6 |
| ti_1058_ds13_ds19_pipeline_map | carded | 1 | batch 6 |
| ti_200_whitelist_blocklist | carded | 0 | batch 6 |
| ti_253_tpa_monitor | carded | 0 | batch 6 |
| ti_254_investigate_low_ntb_percentage | carded | 0 | batch 6 |
| ti_270_pre_post_analysis_ga | carded | 0 | batch 7; Jaguar GA pre/post, results in Drive |
| ti_310_ntb_investigations | carded | 0 | batch 7; NTB misclassification, facts already in knowledge |
| ti_34_identity_sync_freshness | carded | 1 | batch 7; membership sync freshness monitoring |
| ti_391_audience_intent_scoring | carded | 0 | batch 7; AIS pre/post, results in Drive |
| ti_501_jaguar_kpi | carded | 0 | batch 7; Jaguar KPI + causal impact, lift in notebooks |
| ti_502_ip_scoring | carded | 0 | batch 7; how-we-use-scores reference doc |
| ti_504_causal_impact_experimentation | carded | 0 | batch 7; Fangorn RCT vs CausalImpact framework |
| ti_541_ip_scoring_pipeline | carded | 0 | batch 7; DS13 pipeline documentation |
| ti_542_max_reach_causal_impact | carded | 0 | batch 8; Max Reach causal impact, results in notebook/PDF |
| ti_644_root_insurance | carded | 0 | batch 8; CRM match gap, HEM vs household |
| ti_684_missing_ip_from_ipdsc | carded | 0 | batch 8; in-progress, root cause TBD |
| ti_737_fpa_advertiser_verticals | carded | 1 | batch 8; CoreDW->BQ parity PASSED |
| ti_748_causal_impact_media_plan | carded | 1 | batch 8; Media Plan CI, config-version split |
| ti_780_campaign_ramp_up_research | carded | 1 | batch 8; 4-week ramp-up exclusion |
| ti_803_buk_value_analysis | carded | 0 | batch 9; BUK value epic, 184x per-advertiser finding |
| ti_804_keyword_visit_rate_analysis | carded | 2 | batch 9; Phase 1 keyword visit-rate 184x |
| ti_809_multiday_validation | carded | 2 | batch 9; feature-ranking day stability |
| ti_810_feature_store_pipeline | carded | 6 | batch 9; 7 Layer-1 IP feature models in prod |
| ti_811_advertiser_features | carded | 0 | batch 9; not-started backlog item |
| ti_813_buk_500_advertiser_scale | carded | 1 | batch 9; BUK scaled to 500 advs, Fangorn eval |
| ti_790_bidstream_feature_inventory | needs_review | 0 | batch 9; card fields were placeholder stubs (test/a), not summary-supported |
