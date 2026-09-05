---
doc_type: glossary
title: Glossary — MNTN terms, acronyms & concepts → where the authority lives
summary: "business term / acronym / concept → one-line definition + pointer to the authoritative doc or data_knowledge.md section. Load this instead of grepping 465 KB of prose."
last_verified: 2026-09-05
keywords: [glossary, terms, acronyms, definitions, VV, HHST, HI, PP, MM, RTC, DS, BUK, funnel_level, holdout, attribution, waypoint, fangorn, ghost-bid, unattributed bq jobs, airflow-dag label, dead cohort, zero-delete sweep, superseded build gap, optimizer bq surface scope, short page, cursor pagination, deployment variable vs pod env, KubernetesPodOperator env_vars, known false failure, dbt assertion, input retention window, completion window, cohort probe, expired batch, receipt count completeness, cursor deleted mid-listing, shared default openai project, untransitioned receipt, monotonic submit, gsutil false zero]
---

# Glossary

Term / acronym / concept → a one-line definition + **→ pointer** to the authoritative source (a
per-table doc under `bq/`, a `data_knowledge.md` `##` section, `ds_catalog.md`, or `mntn_business.md`).
This is a **routing index, not the source of truth** — follow the pointer for the full, nuanced version.
For a table's schema/grain/cost, start at [`bq/_CATALOG_INDEX.md`](bq/_CATALOG_INDEX.md).

## Identity, IPs & households
- **VV (Verified Visit)** — an ad-attributed site visit (the headline funnel event); "clickpass" is the legacy name. → [`bq/logdata/clickpass_log.md`](bq/logdata/clickpass_log.md), [`bq/summarydata/visits.md`](bq/summarydata/visits.md)
- **`ip` vs `ip_raw`** — two IP columns on log tables; in most current views `ip_raw` is a literal alias of `ip` (NOT a distinct pre-enrichment IP). Use `bid_ip` for the public IP (~14% of `ip` are internal 10.x NAT). → [`bq/logdata/impression_log.md`](bq/logdata/impression_log.md), `data_knowledge.md` § Stage 3 VV Pipeline
- **CIL (cost_impression_log)** — the 33-month impression+cost+score history table (floor 2023-10-01; scores NULL before 2025-06). Not a proxy for source IPs. → [`bq/logdata/cost_impression_log.md`](bq/logdata/cost_impression_log.md)
- **guid** — MNTN first-party cookie/device id; `guid_log` = the honest total-traffic pixel (fires on every page view). → [`bq/logdata/guid_log.md`](bq/logdata/guid_log.md)
- **CGNAT / NAT** — carrier-grade NAT means many households share one IP → report per-IP frequency as **medians, never means**. → `data_knowledge.md` § Per-IP frequency
- **iCloud Private Relay** — Apple relay IPs; mapped via `summarydata.icloud_*` + `logdata.icloud_vv_log`. → [`bq/summarydata/icloud_guids.md`](bq/summarydata/icloud_guids.md)
- **No IP→IP bridging in BQ** — graph_ips self-joins are expensive + CGNAT-limited; identity bridging is external. → `data_knowledge.md` § Architecture

## Funnel, campaign & entity structure
- **funnel_level** — authoritative field for campaign stage (on `campaigns`). `objective_id` is UNRELIABLE for stage. → [`bq/integrationprod/campaigns.md`](bq/integrationprod/campaigns.md)
- **objective_id** — 1=Prospecting / 4=Retargeting / 5=MT-S2 / 6=MT-S3 / 7=Ego; prospecting = `objective_id IN (1,5,6)`. → [`bq/integrationprod/objectives.md`](bq/integrationprod/objectives.md)
- **product_id** — on `campaign_groups`: 1=PTV / 2=Select / 3=QuickFrame (immutable since 2025-07-31). → [`bq/integrationprod/campaign_groups.md`](bq/integrationprod/campaign_groups.md)
- **campaign_group vs campaign** — `campaign_group_id` = the CLIENT-facing campaign; `campaign_id` = internal funnel-stage sub-campaigns. → `data_knowledge.md` § "campaign_group_id = the CLIENT-facing campaign"
- **`*_alt_id` remap** — in `win_logs`, `campaign_alt_id` = campaign_group_id and `line_item_alt_id` = campaign_id (names map to a DIFFERENT id level). → [`bq/logdata/win_logs.md`](bq/logdata/win_logs.md)
- **channel_id** — 1=display / 8=CTV. → [`bq/integrationprod/channels.md`](bq/integrationprod/channels.md)
- **Entity hierarchy** — advertiser → campaign_group → campaign → flight; creative_group → creative. → `data_knowledge.md` § Entity Hierarchy
- **flights** — authoritative flight schedule (start/end); short flights (<72 h) push a 0 HHST gate. → [`bq/core/flights.md`](bq/core/flights.md)

## Audience, targeting & scoring
- **DS (data_source_id)** — the audience/data-source taxonomy (DSxx). Full catalog → [`ds_catalog.md`](ds_catalog.md); live graph → [`bq/integrationprod/audience_data_sources.md`](bq/integrationprod/audience_data_sources.md)
- **MM (Model Match)** — MNTN-derived, scored audience. Components: **DS19 = MM Core, DS13 = PP v1, DS46 = PP v2** (one slot). → `data_knowledge.md` § Audience System, MEMORY `reference_mm_component_taxonomy`
- **Shopper Graph** — original name for **MNTN Matched** (confirmed Alyson Lefkowitz + Brian McAdams). The `SteelHouse/shopper_graph` service backs the ENTIRE MNTN Match backend (building an MM campaign hits it directly); DS-team owned (Alyson); deployed **manually** (merge to main does nothing until a deploy workflow runs). → `data_knowledge.md` § Shopper Graph API, MEMORY `reference_shopper_graph_deploy`
- **hoteling / `fpa.mm_domain_map`** — mapping many AIDs that share one domain (franchises) to a single root AID whose autopilot profile is reused. Consulted by `/autopilot`, NEVER by `/vertical`; Postgres-only, NOT mirrored to BQ (proxy: `dw-main-gold.bae.v_aid_flagged_dup_domain`). → `data_catalog.md` § fpa.mm_domain_map, `data_knowledge.md` § Shopper Graph API
- **1P / 3P / MM** — 1P (uploaded) + 3P (bought interest) are unscored; MM (MNTN-derived) is scored. → `data_knowledge.md` § Advertising Concepts
- **HI (High Intent) / PP (Purchase Propensity) / MI (Mid Intent)** — Fangorn intent tiers from the household scoring pass. → `data_knowledge.md` § Intent Scoring Architecture
- **HHST (household_score threshold)** — the intent gate; a **pacing lever** thrashed daily that drives delivery composition. RTC bypasses it. `household_score` is BINARY (10000 or unscored). → `data_knowledge.md` § Intent Scoring, § HHST intent gate
- **RTC (Real-Time Conquest)** — a distinct conquest population that BYPASSES the HHST gate; ~47% of RTC IPs never reach HI. `model_params ~ 'realtime_conquest_score=10000'`, DS19. → `data_knowledge.md` § RTC
- **Fangorn** — the two-pass (HI + PP) household scoring system; detect via score-band continuity. → `data_knowledge.md` § Fangorn vs bucketed scoring
- **CRM exclusion** — prospecting excludes DS4 (CRM) / DS8 (IP List) / DS47 (CRM-IDG). → MEMORY `feedback_crm_excluded_from_prospecting`
- **within-HI visit rate** — THE discriminator between gate-removal (VR holds) and over-scaling (VR falls). → `data_knowledge.md` § within-HI visit rate

## Keywords / BUK
- **BUK (Bottoms-Up Keywords)** — MNTN's keyword-targeting product; internal name. Exec/buyer rebrand = "Behavior Keywords". → `data_knowledge.md` § Keyword Targeting & BUK
- **term / term_id** — a keyword; campaign-scoped numeric-STRING id in `aggregates.win_rate_*_by_term_hour` (NULL term_id = separate no-term bucket). → [`bq/aggregates/win_rate_bq_bids_by_term_hour.md`](bq/aggregates/win_rate_bq_bids_by_term_hour.md)

## Attribution & measurement
- **attribution_model_type_id** — {1=Last Touch, 2=Last TV Touch, 3=Last Touch Competing, 4=Last TV Touch Competing}; use IN (3,4) for competing. Distinct from `attribution_model_id`. → [`bq/summarydata/ui_visits.md`](bq/summarydata/ui_visits.md)
- **competing_\*** — cross-vendor "competing" attribution legs; NEVER SUM across attribution models (they are separate, overlapping views). → [`bq/summarydata/all_facts.md`](bq/summarydata/all_facts.md)
- **industry_standard attribution** — a MISNOMER; it's last-touch + competing_*, not first-touch. → `data_knowledge.md` § CORRECTION "industry_standard"
- **order_amt** — the conversion value column (`order_amt_usd` is NULL/sparse — use `order_amt`). → [`bq/summarydata/ui_conversions.md`](bq/summarydata/ui_conversions.md), [`bq/logdata/conversion_log.md`](bq/logdata/conversion_log.md)
- **10% holdout** — `MD5('{AID}:{IP}') mod 1000`, 0–99 = holdout (per-advertiser per-IP). Use ITT. → `data_knowledge.md` § Campaign Holdout
- **usersreached (graph)** — mixed-key HLL (IP for CTV / cookie for display) → ~2× served IPs; use CIL distinct ip instead. → `data_knowledge.md` § graph.usersreached gotcha
- **Waypoint** — MNTN's funnel/event product (`summarydata.waypoints_*`); a waypoint = a tracked funnel event/stage. → [`bq/summarydata/waypoints.md`](bq/summarydata/waypoints.md)

## Bidding & delivery
- **ghost-bid** — a suppressed/non-emitted bid; `threshold_failure_reasons='ghost-bid'` (~753K/day); bias register for lift studies. → `data_knowledge.md` § B2B CVR power floor + ghost-bid; MEMORY `reference_ghost_bid_lift_register`
- **has_price / price** — on bidder aggregates, `price` is the computed candidate bid (populated even when `has_price=false`); isolate emitted bids via `has_price=TRUE`, not `price>0`. → [`bq/aggregates/campaign_group_log_aggregation.md`](bq/aggregates/campaign_group_log_aggregation.md)
- **win_cost_micros_usd** — Beeswax clearing price in micros USD (÷1e6 → $ CPM after ×1000). → [`bq/logdata/spend_log.md`](bq/logdata/spend_log.md), [`bq/logdata/win_logs.md`](bq/logdata/win_logs.md)
- **bid_logs vs win_logs** — `bid_logs` is bid-grain (fans out); `win_logs` is impression-grain. Dedup bid_logs before joining. → [`bq/logdata/bid_logs.md`](bq/logdata/bid_logs.md)

## Epoch units (per-table — the recurring trap)
- Units differ PER COLUMN and per table: impression_log/cost_impression_log/clickpass/conversion/win_logs `epoch` = **µs**; `spend_log.auction_epoch` = **ns**; `bidder_bid_events` = **ms**; `cost_impression_log.batch_epoch` = **seconds**. `datastream_metadata.source_timestamp` (integrationprod) = **ms** CDC-capture, not business time. → each table's doc; `time_unit` front-matter field.

## Experiment methodology
- **DiD (Difference-in-Differences)** — report with cluster-bootstrap SE/CI/p, matched to the design. → `experimentation.md` § Standard Analysis Protocol
- **CausalImpact (UCM)** — Bayesian structural time-series counterfactual; VIF→BIC covariate selection, no treated-y lags. → `experimentation.md`; `documentation/docs/causal_impact_did_math_reference.md`
- **CUPED** — variance reduction needing randomization (not on non-random cohorts). → `data_knowledge.md` § CUPED ρ
- **Methods convergence** — when DiD and CI agree, that's the strongest informal-causal argument. → `experimentation.md` § Standard Analysis Protocol

## Architecture & data stack
- **SQLMesh** — silver.logdata/summarydata/aggregates are VIEWs over versioned `sqlmesh__*` physicals (hash drifts on rebuild → re-resolve from the view DDL). `silver.core` = thin views over `bronze.integrationprod.core_*`. → `data_knowledge.md` § Architecture, § Datastream Replication
- **Datastream / CDC** — `bronze.integrationprod` = Postgres CDC dims; filter `deleted=FALSE AND is_test=FALSE` (when those columns exist). → `data_knowledge.md` § is_test/deleted Filters
- **TTL floors** — cost_impression_log 2023-10-01 (fixed); bidder_bid_events 10-day (not 90); event_log_filtered 60-day; augmentor/bid_price 10-day. → `data_knowledge.md` § TTL / Retention Summary; each table's Cost notes
- **BQ job location** — must be us-central1 (slot reservation); dataset-less/external-table queries default to US = on-demand $. → `data_catalog.md`, `.claude/CLAUDE.md` § BigQuery
- **Unattributed BQ jobs (optimizer)** — fleet-SA jobs (`airflow-ti-prod@`, `airflow-camperbid-prod@`) in `dw-main-bronze.region-us-central1.INFORMATION_SCHEMA.JOBS_BY_PROJECT` whose `labels` carry no `airflow-dag`: python-client and Spark-BigQuery-connector submits, never the operator's. Lives only in the daily `optimizer_bq_<date>.md` report, not the ledger or Mode. → MEMORY `reference_bq_job_attribution`, `data_knowledge.md` § BigQuery Behavioral Gotchas
- **Dead cohort (OpenAI batch)** — a day's `openai_batch_submissions/dt=` receipts where 0 of N batches ever progressed (`was_submitted` all False; live status outside in_progress/finalizing/completed with 0 completed requests). After shopper_graph#305 `batch_fetch` fails with `DeadCohortError` once the youngest receipt is ≥ 12 h old (`DEAD_COHORT_MIN_AGE_HOURS`). → `data_catalog.md` § shopper_graph/openai_batch_submissions, MEMORY `reference_mntn_matched_batch_pipeline`, decision 0006
- **Zero-delete sweep (OpenAI cleanup)** — a `delete_all_storage_files.py` run that frees nothing. It is indistinguishable in the logs from a quiet day, which is why the 2026-08-28 quota outage ran silent for six days. Since shopper_graph#305 the script RAISES when it frees nothing while ≥ `STORAGE_ALARM_MIN_FILES` (default 10,000) files are still stored, and when every eligible delete fails. Normal volume is a few hundred to ~1,200 files/day. **The alarm could not fire while the sweep's file count was a partial page** (2026-09-04: `seen` peaked at 4,623 against a 10,000 threshold) — fixed by shopper_graph#308; #309 additionally suppresses the alarm on a run whose listing was truncated by a deleted cursor, since partial counts are not a total. → MEMORY `reference_openai_sdk_pagination`, `reference_mntn_matched_batch_pipeline`, ticket AUDI-1321
- **Short page (cursor pagination)** — a list-API page shorter than the requested `limit`. It is **NOT** the last page: terminate a cursor walk on an EMPTY page (`after=<last id>`), never on `len(page) < limit`. The OpenAI file sweep broke this and reported 28 files four minutes before `batch_fetch` deleted 416 it had never listed (fixed shopper_graph#308, 2026-09-04; the corrected walk then measured 129 files / 4.2 GiB of OUR files — not the whole store, see "Shared default OpenAI project"). → MEMORY `reference_openai_sdk_pagination` TRAP 3
- **Input retention window (OpenAI batch)** — how long `delete_all_storage_files.py` keeps `purpose=batch` INPUT files. It must exceed OpenAI's 24 h batch `completion_window` or the sweep deletes an input out from under a live batch: at 12 h, `batch_transition` on `dt=2026-09-02` returned `failed=119, expired=0`. Floor is **26 h** (`shopper_graph#310`, deployed 2026-09-04); outputs stay at 48 h. → MEMORY `reference_mntn_matched_batch_pipeline`, decision 0010
- **Cohort probe (OpenAI batch)** — clearing `batch_transition` on the fetch run whose `data_interval_start` is `D+1` (it reads `dt=D`) to print one `cohort dt=D: n=.. completed=.. failed=.. expired=.. ...` status line for a day's receipts. Non-destructive apart from flipping `was_submitted=True` on progressed rows, and it only examines rows with `was_downloaded=False & was_submitted=False`, so `n` is not the cohort size. Run it BEFORE deleting any receipts. → MEMORY `reference_mntn_matched_batch_pipeline`
- **Expired batch (OpenAI)** — a batch that passed its 24 h completion window unfetched. Unrecoverable, and so is a `completed` one whose output file the sweep already deleted (`files.content(output_file_id)` 404s). `dt=2026-08-27` probed expired=612 / completed=128 / in_progress=2 and all 742 had to be re-submitted. → MEMORY `reference_mntn_matched_batch_pipeline`
- **Shared default OpenAI project** — the OpenAI project the MNTN Match pipeline runs in is **company-shared**, and the 2.5 TB file-storage cap is per PROJECT while our API key can list only the files **we** uploaded. So `files.list` totals bound our own footprint and can never name who holds the rest; headroom depends on other teams' usage. Confirmed by Malachi 2026-09-05, correcting the 2026-09-04 "the 2.5 TB was ours" reading. → `data_knowledge.md` § MNTN Matched pipeline, MEMORY `reference_mntn_matched_batch_pipeline`, `feedback_scoped_credential_cannot_prove_ownership`, decision 0009
- **Untransitioned receipt (`was_submitted`)** — a receipt row in `openai_batch_submissions/dt=` that no `batch_transition` has flagged. `batch_fetch` selects `was_downloaded == False & was_submitted == True`, so an untransitioned day downloads NOTHING and still goes green; a fresh submit always writes `was_submitted=False`. After any re-submit clear `batch_transition` then `batch_fetch`. Tell: a `cohort dt=…` line in a **`batch_fetch`** log means the receipts are still untransitioned. → `data_catalog.md` § shopper_graph/openai_batch_submissions, MEMORY `reference_mntn_matched_batch_pipeline`
- **Monotonic submit (OpenAI quota `400`)** — a `batch_submit` killed by the storage quota keeps every receipt it already wrote, and the double-submission guard skips those files on the retry, so attempts accumulate instead of restarting (`dt=2026-08-28`: 866 of 1,241, then 1,241 of 1,241). Retry it; do not wait for a clean storage window. → MEMORY `reference_mntn_matched_batch_pipeline`
- **Deployment variable vs pod env (KubernetesPodOperator)** — an Astro deployment Environment-tab variable configures the Airflow component pods and `env-secrets`; it does **not** reach a pod launched by `MntnKubePodOperator`, which builds a fresh pod from the task's `env_vars` plus one named secret key. A new variable for a KPO task needs an `airflow-ti` PR; `env_vars` is a `template_field`, so `"{{ var.value.get('name','default') }}"` makes it Variable-tunable afterwards. → MEMORY `reference_airflow_ti`, `reference_astro_deploy_mechanics`
- **Known false failure (dbt test)** — a test that fails for a reason unrelated to the data. It is a property of ONE assertion, never of the task: `batch_test.test_product_categorization` is spurious only when `product_categorization__max_dt` fails ALONE. Read the assertion name before marking any test success. → MEMORY `feedback_check_which_dbt_assertion_failed`, `reference_mntn_matched_batch_pipeline`
- **Superseded-build gap (Astro)** — merging the next PR before the previous one's Astro build reaches DEPLOYED can supersede that build, so a merged change never reaches prod while main and GitHub both look correct. Land a multi-PR batch serially, one merge per DEPLOYED (~4-8 min per airflow-ti build). → MEMORY `reference_airflow_ti`, decision 0008
- **Optimizer BQ surface scope** — the optimizer's BigQuery surface is scoped by **service account** (`OPTIMIZER_BQ_SAS`, default `airflow-ti-prod@` + `airflow-camperbid-prod@`), NOT by team; the Spark surface excludes other teams by team label (`phs.TEAM`) but the BQ surface never did. That is why the sweep flags other teams' BQ jobs — by design. → MEMORY `reference_bq_job_attribution`, `project_airflow_optimizer`
- **gsutil false zero (`ReauthUnattendedError`)** — when gcloud's reauth has expired, `gsutil ls` writes the error to **stderr** and an **EMPTY listing to stdout**, so `gsutil ls … 2>/dev/null | grep -c` reports **0 objects** for a prefix that is full. Any "the data is missing" conclusion drawn that way is an auth artifact; fix with `gcloud auth login` in an interactive terminal, and prefer `gcloud storage` for listings and copies. → MEMORY `reference_gsutil_reauth_false_zero`, `reference_gcloud_storage_over_gsutil`

## Where to go next
| I need… | open |
|---|---|
| a table's schema/grain/cost | [`bq/_CATALOG_INDEX.md`](bq/_CATALOG_INDEX.md) → the table doc |
| the full nuance behind a term above | the **→ pointer** on that entry |
| a data-source `DSxx` | [`ds_catalog.md`](ds_catalog.md) |
| business logic / a metric definition | [`data_knowledge.md`](data_knowledge.md) § (see pointers) |
| experiment / causal method | [`experimentation.md`](experimentation.md) § Standard Analysis Protocol |
| products / org / industry | [`mntn_business.md`](mntn_business.md) |
