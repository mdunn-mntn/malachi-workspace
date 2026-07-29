# MNTN Workspace Memory Index

> One tight line per entry (twinned topics share a line). Detail lives in each topic file + git `knowledge/data_catalog.md`/`data_knowledge.md`.

## Data docs (git source of truth)
- `knowledge/data_catalog.md` — schemas, partitions, clustering, join keys, query tips · `knowledge/data_knowledge.md` — business logic, gotchas, architecture, tribal knowledge

## Feedback — how to work
- [bq_workflow](feedback_bq_workflow.md) — bq_run.sh perf logging; no polling/cost-warnings; never preempt long · [fast_first_bq](feedback_fast_first_bq.md) — probe sample/APPROX then scale to exact
- [mntn_only](feedback_mntn_only.md) — NEVER mention IPSOS. MNTN only. · [source_table_ips](feedback_source_table_ips.md) — IPs from source table, never proxies like CIL
- [review_tone](feedback_review_tone.md) — self-review tone: rubric-forward, specific, aspirational · [self_review_habit](feedback_self_review_habit.md) — update self_review_2.md after every ticket · [attribution](feedback_attribution.md) — never misattribute Malachi's ideas (primary speaker in transcripts)
- [unresolved_not_crm](feedback_unresolved_not_crm.md) — unresolved VVs ≠ CRM graph; it's lookback/TTL/bug
- [doc_style](feedback_doc_style.md) — one doc, one ranked table, tells a story, actionable · [explain_simply_and_visually](feedback_explain_simply_and_visually.md) — deep concept → plain analogy + visuals
- [minimize_complexity](feedback_minimize_complexity.md) — simple deliverables, no invented terms/columns · [facts_not_presentation](feedback_facts_not_presentation.md) — plain facts/caveats, no Power Line · [no_unsolicited_suggestions](feedback_no_unsolicited_suggestions.md) — no invented fixes/next-steps
- [dont_extend_old_tickets](feedback_dont_extend_old_tickets.md) — don't extend stale/reassigned tickets; verify · [no_emdash_no_namedrop](feedback_no_emdash_no_namedrop.md) — no em-dashes/name-dropping · [plain_voice_internal_docs](feedback_plain_voice_internal_docs.md) — internal docs: plain human voice
- [xlsx_default_output](feedback_xlsx_default_output.md) — default output=.xlsx to Drive My Drive/Tickets/<KEY>/ · [xlsx_master_format](reference_xlsx_master_format.md) — master .xlsx lib/mntn_xlsx.py (MntnWorkbook) · [drive_mount_xlsx_delivery](reference_drive_mount_xlsx_delivery.md) — write to local Drive mount
- [mntn_brand_assets](reference_mntn_brand_assets.md) — brand kit: hex palette + fonts (Neue Haas/Inter) + logo
- [airflow_prod_safety](feedback_airflow_prod_safety.md) — never modify DAGs/push main in airflow-ti; feature flags · [dataproc_cost_awareness](feedback_dataproc_cost_awareness.md) — check Dataproc Serverless cost before batch/backfill
- [bluf_communication](feedback_bluf_communication.md) — BLUF: lead every human-facing comm with the conclusion · [terse_chat_replies](feedback_terse_chat_replies.md) — chat: answer first, cut filler
- [terse_tickets](feedback_terse_tickets.md) — Terse Comms caps (comment/desc/PR 900ch/commit subj ≤72) · [ticket_writing_rule](feedback_ticket_writing_rule.md) — tickets/PRs: fewest words, objective/task/results
- [verify_edit_scripts](feedback_verify_edit_scripts.md) — gate "shipped" on git diff; assert-abort = zero edits · [audit_ref_check_before_delete](feedback_audit_ref_check_before_delete.md) — audits: ref-check before delete
- [no_naive_pre_post](feedback_no_naive_pre_post.md) — no naive pre/post for advertiser KPIs; use CausalImpact · [rank_desc_always](feedback_rank_desc_always.md) — rank by primary metric, most on top
- [runbook_artifacts_png_sql_only](feedback_runbook_artifacts_png_sql_only.md) — runbook steps: SQL + PNG only · [adversarial_workflow_authoring](feedback_adversarial_workflow_authoring.md) — multi-agent verify: blocking gate
- [background_work_liveness](feedback_background_work_liveness.md) — never passively wait on bg/async work; arm a stall-detector Monitor; a hung task sends NO completion notification
- [sprint_ready_plan](feedback_sprint_ready_plan.md) — sprint-ready = self-contained impl plan (BLUF/Problem/Solution/Implement/Impact/Expected) + companion RFD; deliver both

## Experiment methodology
- [causal_impact_pattern](reference_causal_impact_pattern.md) — tiered-rollout: cluster-bootstrap DiD + UCM CI · [causal_impact_dashboard](reference_causal_impact_dashboard.md) — Mode dashboard for rollouts
- [no_y_lags_in_causalimpact](feedback_no_y_lags_in_causalimpact.md) — no treated-y lags as exog; freq_seasonal[7] · [causal_impact_viz_simplicity](feedback_causal_impact_viz_simplicity.md) — CI/pre-post viz aggregate-only
- [fangorn_tier_assignment](reference_fangorn_tier_assignment.md) — only Fangorn T2 random; T2=DiD, T1/3=CI-only · [wave3_selection_bias](reference_wave3_selection_bias.md) — Fangorn Wave 3 not random; CI over DiD
- [bootstrap_must_match_design](feedback_bootstrap_must_match_design.md) — bootstrap variant must match design · [cuped_needs_randomization](feedback_cuped_needs_randomization.md) — CUPED needs randomization
- [ghost_bid_lift_register](reference_ghost_bid_lift_register.md) — ghost-bid bias; gf .09-.11 · [ghost_bid_columns](reference_ghost_bid_columns.md) — filter threshold_failure_reasons='ghost-bid'; 753K/day
- [total_visit_signal](reference_total_visit_signal.md) — guid_log=page-views (dedup→visit-days per adv,ip,date; 366TB, partition-prune/Databricks), clickpass=visits; enriched.lift__ghost_bid_visits=INCR total-visit lift (7d, platform ref only); use total visits not attributed VV for freq RCTs (attribution bias); TI-835 ~0% total lift → non-inferiority-shaped

## Audience / targeting / scoring
- [crm_excluded_from_prospecting](feedback_crm_excluded_from_prospecting.md) — prospecting excludes DS4(CRM)/DS8(IP · [crm_polarity_matters_with_mm](feedback_crm_polarity_matters_with_mm.md) — CRM exclude=hygiene; include=narrows
- [rtc_hhst_gating](reference_rtc_hhst_gating.md) — RTC affects bidding only when HHST set · [mm_3p_intersection_mechanics](reference_mm_3p_intersection_mechanics.md) — 3P-include w/ HHST>0 NARROWS MM
- [fangorn_audience_overlay](reference_fangorn_audience_overlay.md) — Fangorn overlay→DS46; DS19 survives, DS13 · [mm_component_taxonomy](reference_mm_component_taxonomy.md) — DS19=MM Core, DS13=PP v1, DS46=PP v2
- [audience_intent_scoring_dag](reference_audience_intent_scoring_dag.md) — Ryan audience_intent DAG: HI 10K = DS13 · [fangorn_two_model_passes](reference_fangorn_two_model_passes.md) — Fangorn = TWO passes/IP (HI + PP)
- [bidder_scoring_reality](reference_bidder_scoring_reality.md) — 3 score fields: household/advertiser_household · [bidder_score_fields_empirically_zero](reference_bidder_score_fields_empirically_zero.md) — bidder_bid_events scores ~0
- [mm_adoption_ds19_swing](reference_mm_adoption_ds19_swing.md) — "% AIDs using MM" swings ~2x on DS19 · [mntn_1p_3p_mm_definitions](reference_mntn_1p_3p_mm_definitions.md) — 1P/3P both unscored; MM (MNTN-derived)
- [ti_956_per_pattern_application](reference_ti_956_per_pattern_application.md) — TI-956 per-segment scoring · [segment_quality_framework_ds_agnostic](reference_segment_quality_framework_ds_agnostic.md) — Alex's segment_quality
- [prospecting_scores_gcs_monitor](reference_prospecting_scores_gcs_monitor.md) — daily GCS prospecting-scores monitor
- [hhst_pacing_lever](reference_hhst_pacing_lever.md) — HHST gate thrashed daily; flips invert delivery · [hhst_efficiency_sizing](reference_hhst_efficiency_sizing.md) — PROD setter=camperbid v3/v4 (auction pop)→idso UPSERT; ddm.hhst_generate_recommendation is fenced pilot only; HI ~5x cheaper CPV (correlational); graduated lift blocked in BQ; gate-SAFE
- [frequency_capping](reference_frequency_capping.md) — fcap per campaign+campaign_group, counter key ALWAYS IP (no MNTN-id branch), NO advertiser rollup (advertiser_frequency_caps EMPTY), fails open; owned @SteelHouse/rtb (snowsignal/rogusdev) NOT Zach/Jordan; cap-arms need NEW bidder code; leakage-$ shared-IP confounded (retracted); freq-cap bandit = better first MAB
- [mm_vs_3p_scorecard](reference_mm_vs_3p_scorecard.md) — AUDI-1141: MM(gated) IVR 0.46% vs 3P 0.07% (~6.6x) · [fangorn_detection](reference_fangorn_detection.md) — detector = band CONTINUITY (6666-7999 AND 8001-9999)
- [ddp_valuation_framework](reference_ddp_valuation_framework.md) — value any 3P vendor + WTP; volume≠value · [ddp_billing_logic](reference_ddp_billing_logic.md) — REGIME: Jan-Apr fractional 1/N, May+ single-credit
- [goal_attainment_report](reference_goal_attainment_report.md) — "% customers hit goal" Mode report; ~63% · [select_vs_nonselect_incrementality](reference_select_vs_nonselect_incrementality.md) — AUDI-1172: Select ~5x more incremental (+22% vs +4%)
- [take_rates_sensitive](feedback_take_rates_sensitive.md) — take rates private; shareable = media/data_spend · [client_pricing_model](reference_client_pricing_model.md) — pricing_model_type fixed_cpm vs custom_margin
- [id164_ip_quality_scoring](reference_id164_ip_quality_scoring.md) — Identity ID-164 toxic-hub IP scoring; extend · [geo_axes](feedback_geo_axes.md) — US-only: GEO-BROAD-incl default; collapse GEO-NARROW-excl
- [stable_hi_not_stable_roas](reference_stable_hi_not_stable_roas.md) — stable HI-share ≠ stable ROAS · [within_hi_vr_discriminator](reference_within_hi_vr_discriminator.md) — within-HI VR: gate-removal holds

## Reference — infra / repos / routing / people
- [bq_location_reservation](reference_bq_location_reservation.md) — BQ jobs must be us-central1 · [databricks](reference_databricks.md) — push >4h-risk queries to Databricks; small-core for shuffle
- [airflow_ti](reference_airflow_ti.md) — feature-store repo (SteelHouse/airflow-ti) · [sqlmesh_repo](reference_sqlmesh_repo.md) — SteelHouse/sqlmesh DW repo; model=job · [airflow_ti_cross_repo_deps](reference_airflow_ti_cross_repo_deps.md) — lazy-import cross-repo deps
- [olympus_repo](reference_olympus_repo.md) — Media Plan algorithm = steelhouse/olympus (Chris Addy) · [bidder_serving_stores](reference_bidder_serving_stores.md) — Aerospike rtb (key=IP), scores GCS, price/threshold
- [idso_repo](reference_idso_repo.md) — SteelHouse/idso `dco` = SOLE writer of dso.household_score_thresholds (+recency/viewability/cpm gates); UPSERT fed by camperbid v3/v4 (auction pop)→performance.optimized_intent_thresholds; DDM is fenced pilot only
- [oncall_runbook](reference_oncall_runbook.md) — /oncall (§0 classifier); log §3+§2+incident_log.jsonl; INC-001 Bombora benign, INC-002 fangorn dataproc contention RESOLVED
- [improvements_backlog](reference_improvements_backlog.md) — improvement/tech-debt ideas → improvements_backlog.md (file, not Jira); promote when prioritized
- [backend_reporting_ber_team](reference_backend_reporting_ber_team.md) — BER owns summarydata + · [graph_usersreached_mixed_key](reference_graph_usersreached_mixed_key.md) — usersreached IP(CTV)/cookie(display)
- [audience_platform_authority](reference_audience_platform_authority.md) — Zach Schoenberger = SoT · [pixel_ops_routing](reference_pixel_ops_routing.md) — pixel bugs → Ashley Pineda Varela · [slack_channel_routing](reference_slack_channel_routing.md) — #chapter-ui/#data-platform/#dev-mode-support
- [advertiser_vertical_correction](reference_advertiser_vertical_correction.md) — fix mis-tagged vertical: Shopper · [wgu_pixel_case](reference_wgu_pixel_case.md) — WGU revenue never real ($1/lead); Sep'25 retag broke amount
- [data_eng_mcp](reference_data_eng_mcp.md) — Data Eng AI/MCP at data-eng-ai.in.mountain.com (Harvey Yau) · [compass](reference_compass.md) — Compass infra investigator + Atlas Code MCP
- [confluence_api_access](reference_confluence_api_access.md) — Jira token = Confluence REST read+WRITE; TAR space · [ti_experiment_archive](reference_ti_experiment_archive.md) — manifest-driven experiment site; one YAML per
- [deck_standards](reference_deck_standards.md) — Tufte+RevealJS; share_deck.sh githack; no matplotlib · [matplotlib_dollar_mathtext](reference_matplotlib_dollar_mathtext.md) — two $ → italic mathtext · [mode_dashboard_porting](reference_mode_dashboard_porting.md) — Mode port: window.datasets→Chart.js
- [pi5_server](reference_pi5_server.md) — Pi 5 SSH, Slack bot cron, Pi-hole, deploy keys; weekly workflow-audit cron · [workflow_audit_loop](reference_workflow_audit_loop.md) — /workflow-audit: aggregator + skill (propose-only)
- [team_name](reference_team_name.md) — team = Audience Intelligence (AUDI); Jira TI→AUDI · [chapi_ui](reference_chapi_ui.md) — client UI=CHAPI→ClickHouse; industry_standard=last-touch+competing_*
- [mntn_campaign_stages](reference_mntn_campaign_stages.md) — Stage 1/2/3 = Prospecting/Engaged/Engaged+VV; S1→S3 · [mntn_leadership](reference_mntn_leadership.md) — Kale (Dir), Paulo Black (VP Eng), Richard Girges (CTO), Alex
- [ticket_context_eval_tooling](reference_ticket_context_eval_tooling.md) — ticket cards + retrieval eval · [ticket_framing_gate](reference_ticket_framing_gate.md) — /frame skill + §0 Framing; gate blocks in_progress

## Jira / Todoist / process
- [one_spike_multi_item](feedback_one_spike_multi_item.md) — Bryce: multi-item eval = ONE spike w/ checklist · [jira_conventions](reference_jira_conventions.md) — ALL Jira conventions: wiki markup, curl v2 writes
- [astronomer_clear_with_latest_bundle](feedback_astronomer_clear_with_latest_bundle.md) — clear failed Airflow w/ latest bundle · [transcribe_shortcut](feedback_transcribe_shortcut.md) — `/transcribe` skill; log stale → cross-check ticket
- [todoist](feedback_todoist.md) — Todoist on-request ONLY; Eat That Frog ABCDE; MNTN section of MindWyre

## Leadership direction
- [paulo_direction](feedback_paulo_direction.md) — explain system behavior, be go-to for ecosystem Qs, equip Alyson
- [eng_rubric](reference_eng_rubric.md) — MNTN Eng Levels & Skills rubric (Speed/Craft/Adaptability) · [rubric_strategy](feedback_rubric_strategy.md) — optimize for all 4s at current level; every entry maps to a 4

## Project (active)
- [audi_1175_ds14_scoring_cost](project_audi_1175_ds14_scoring_cost.md) — DS14 scoring-cost opt: backlog/sprint-ready, gate-safe (HHST auction-scoped), ~$2-11k/mo; AUDI-1175 spike + AUDI-1176 impl
- [audi_1173_freq_cap_bandit](project_audi_1173_freq_cap_bandit.md) — freq-cap bandit: decision-ready RFD drafted + adversarial-gated; RCT designed (3-arm control/cap-8/cap-3, total-visit-COUNT primary via household bootstrap, 5% relative NI margin, MD5 16-hex buckets 100-999 disjoint from holdout, ~10-12wk); needs small @SteelHouse/rtb bidder feature (arms not config-only); leakage=capability gap (advertiser_frequency_caps empty), magnitude retracted; pending owner review
- [fangorn_on_mntn_id](project_fangorn_on_mntn_id.md) — AUDI-1049 epic: re-key Fangorn FS IP→household (Opt 1); Malachi lane = FS build 1166-1170 + validate 1105; folder tickets/audi_1049_*
- [self_optimizing_context](project_self_optimizing_context.md) — ticket-context: TL;DR cards + _ROUTING keywords · [ti810_ryan_answers](project_ti810_ryan_answers.md) — Ryan FS backfill/DAG/dev→prod answers (2026-04-02)
- [structured_bq_catalog](project_structured_bq_catalog.md) — per-table BQ catalog (263 tables, 57 verified) LIVE · [bq_optimization_discipline](project_bq_optimization_discipline.md) — bq/query_cookbook + optimization_playbook
- [qfai_transfer_interest](project_qfai_transfer_interest.md) — QFAI SWE move interest; Anne's candor confidential
- [audi_1111_vendor_quality](project_audi_1111_vendor_quality.md) — AUDI-1111 epic: WTP lens-invariant (no vendor · [audi_1089_ddp_evals](project_audi_1089_ddp_evals.md) — workbook DONE; 1093 preemption $274K, Klickly APPROVED · [audi_1089_template_workbook](reference_audi_1089_template_workbook.md) — LOCKED 139 rows/4 sheets
- [audi_1083_mm_classifier](project_audi_1083_mm_classifier.md) — MM classifier LIVE 2026-07-24 · [bidstream_initiative](project_bidstream_initiative.md) — TI-789 epic: bidstream feature extraction + DS13/DS19
- [incrementality_experiment](project_incrementality_experiment.md) — BER-2250 intent-shuffle; Q2 top priority · [bidder_level_ghost_bidding_approved](project_bidder_level_ghost_bidding_approved.md) — bidder-level ghost bidding · [incrementality_pivot](project_incrementality_pivot.md) — Kale narrowing Fangorn/TI toward incrementality
- [bombora_audience_design](project_bombora_audience_design.md) — AUDI owns Bombora B2B audience (ElevenLabs) · [intent_tier_pacing](project_intent_tier_pacing.md) — AUDI-1070: pace HI/PP IPs across flight
- [audi_1037_mode_dashboard](project_audi_1037_mode_dashboard.md) — AUDI-1037 perf_report → Mode dashboard · [advertiser_high_liveness_filter](project_advertiser_high_liveness_filter.md) — Victor owns live-advertiser filter
- [ti_999_strategic_goal](project_ti_999_strategic_goal.md) / [sizing](project_ti_999_interest_segment_sizing.md) — TI-999 3P curation/ranking + interest-segment sizing (awaiting Zach+Alex)
- [ti_956_paused](project_ti_956_paused.md) — TI-956 paused 2026-06-10; PR #1073 open, DAG paused · [super_structure_adoption](project_super_structure_adoption.md) — thin-kernel synthesis COMPLETE 2026-07-20
- [buk_loom_request](project_buk_loom_request.md) / [rebrand](project_buk_rebrand.md) — Kale wants ~5min BUK Loom
- [q2_ceremonies](project_q2_ceremonies.md) — weekly grooming/syncs/standups; update tickets before meetings · [review_cycle](project_review_cycle.md) / [rubric_rollout](project_rubric_rollout.md) — active review=self_review_2

## Stack, SQLMesh + critical gotchas (full detail in git knowledge/data_catalog.md + data_knowledge.md)
- **Stack:** bronze.integrationprod (Postgres CDC dims) · bronze.raw (events 10–90d TTL) → SQLMesh → silver.logdata/summarydata/aggregates · silver.core (views over integrationprod.core_*). silver=dw-main-silver, bronze=dw-main-bronze; clean name → versioned sqlmesh__*.
- **Epochs/TTL:** epochs spend_log=ns/bidder_bid_events=ms/auction_events=µs · CIL floor 2023-10-01 (scores NULL pre-2025-06); bid_events+bid_logs_enriched 90d, event_log_filtered 60d, augmentor+bid_price 10d · dims filter deleted=FALSE AND is_test=FALSE.
- **Stage/channel/holdout:** objective_id UNRELIABLE→funnel_level; Prospecting=obj IN (1,5,6) (1=Prosp/4=RT/5=MT-S2/6=MT-S3/7=Ego) · channel_id CTV8/display1 · product_id 1=PTV/2=Select/3=QF · 10% holdout MD5('{AID}:{IP}') mod 1000 0–99, ITT.
- **Joins/IP:** bid_logs dedup ROW_NUMBER, bid_ip=COALESCE(NULLIF(ip,'0.0.0.0'),impression_log.bid_ip,event_log.bid_ip) · win_logs campaign_alt_id=cg_id/line_item_alt_id=campaign_id · no IP→IP bridging in BQ.
- **Metric gotchas:** ip vs ip_raw (ui_visits/visits) · *_facts/all_facts `hour`=DATETIME not TIMESTAMP · is_new=client JS pixel (41–56% mismatch normal) · ui_conversions.order_amt (order_amt_usd=NULL) · RTC model_params~'realtime_conquest_score=10000' · audiences=templates/audience_segments=targeting · device_type INT bronze/STRING silver.
- **Sources/floors:** agg__daily_sum_by_campaign cheapest but Sep 2025+ uniques~0→sum_by_campaign_by_day for long pre-periods · 2025-01-01 floor is LOG tables only (CIL 2023-10, ui_visits 2023-01, all_facts ~2020-10) · fpa_advertiser_verticals type0 parent(37)/type1 sub(148), advertiser_name UNRELIABLE→JOIN advertisers.company_name · filter low-imp <1,000 weeks · WGU=31357.
