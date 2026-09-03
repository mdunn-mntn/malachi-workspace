---
name: idso_repo
description: "SteelHouse/idso `dco` = MNTN's per-campaign threshold optimize+apply service; the SOLE physical writer of dso.household_score_thresholds (HHST) org-wide, plus recency/viewability/cpm gates; fed by camperbid v3/v4 -> performance.optimized_intent_thresholds"
metadata: 
  node_type: memory
  type: reference
  originSessionId: 30b091df-9f96-4b81-bccc-78447e75e9ec
doc_type: memory
keywords: [idso, dco, household_score_thresholds, hhst, camperbid, optimized_intent_thresholds, threshold optimize apply, steelhouse idso, ddm pilot, bidder gates]
domain: [repos, bidding, audience-scoring]
lifecycle: active
last_verified: 2026-07-28
---
`SteelHouse/idso` (Kotlin, `dco` module) is MNTN's per-campaign threshold optimization + apply layer, and the **sole physical writer of CoreDB `dso.household_score_thresholds` org-wide** (verified AUDI-1175, 2026-07-28).

**Applied-table write path (3 hops, 3 repos):**
1. **COMPUTE** — `airflow-camperbid spark_scripts/intent_score_threshold_v3/pipeline.py` (non-Fangorn cohort) + `intent_score_threshold_v4/pipeline.py` (Fangorn cohort) compute the per-campaign HHST from AUCTION logs (`COUNT(DISTINCT ip)` over `raw.bid_price_log ∪ raw.bidder_bid_events`, has_price OR threshold_failure_reasons) → `camperbid.hhst_v{3,4}__prod__campaign_threshold`. Scope = MMv2 + funnel_level=1 + objective_id=1 prospecting, **Select `product_id=2` EXCLUDED via `.join(..., how="left_anti")`** (so v3/v4 ARE the PTV `product_id=1` writers; the `product_id=2` clause is an exclusion, not a filter).
2. **SYNC** — v4 DAG `sql/sync_optimized_intent_thresholds.sql` UNION-ALLs both cohorts → CoreDB `performance.optimized_intent_thresholds`.
3. **APPLY** — idso `dco/src/main/kotlin/com/steelhouse/dco/datasource/postgres/HouseholdScoreThresholdRepository.kt` UPSERT (`INSERT ... ON CONFLICT (campaign_id) DO UPDATE`), value = `coalesce(campaign preset, campaign-group preset, oit.threshold)` from `CampaignRepository.kt` LEFT JOIN `performance.optimized_intent_thresholds`. Eligibility gate `HouseholdScoreThresholdServiceImpl.kt` = `PROSPECTING && Channel.TV && funnelLevel==1 && !isMntnSelect && in audience_type_alpha`. Driven by an hourly idso-cron sidecar (`cron/`, k8s ns `prod-optimization`). Applies ~2,082 PTV camps/day.

Sibling repositories in the same `postgres` package write the OTHER bidder gates the same way: `RecencyThresholdRepository`, `RecencyFloorThreshold`, `ViewabilityThresholdRepository`, `CPMThresholdRepository`. So idso is the general "optimize-then-apply" control plane for per-campaign bidder thresholds, not just HHST.

**NOT the writer:** DDM is the fenced `ddm.test_hhst_campaigns` **pilot** only. Several DDM routines read the full scored `ext_tpa.prospecting_intent`/`advertiser_intent` (`hhst_bucket_collections`, `cache_hhst_population_filters`, `cache_hhst_win_conditions`, `cache_hhst_augmentor_volume`, `cache_hhst_augmentor_test_volume`), but all read `dso.household_score_thresholds` only as a join input and write `ddm.*` cache/bucket tables; the recommendation step (`hhst_generate_recommendation`) is fenced to `test_hhst_campaigns`. None writes `dso.household_score_thresholds`. The superseded predecessors of the compute step are `SteelHouse/airflow dags/camperbid/intent_score_threshold/main` (EMR) and `dags/performance/intent_score_threshold` (Redshift) — Redshift→EMR→BQ migration; only camperbid v3/v4 is live.

Not confirmable from code (Airflow/k8s runtime state): which compute DAG is un-paused and whether the idso-cron is running — confirm via Airflow UI / deployment. Related [[reference_hhst_efficiency_sizing]] [[reference_hhst_pacing_lever]] [[reference_bidder_serving_stores]].

**airflow-camperbid runtime (AUDI-1277, verified 2026-09-03):** Airflow 3.3.0 on Astro runtime 3.2-5. `airflow.sdk.Variable.get(key, default=..., deserialize_json=False)` and `Variable.set(key, value, description=None, serialize_json=False)` work from `@task` functions. `@task.short_circuit(ignore_downstream_trigger_rules=False)` skips only direct downstream tasks (not transitive) — later `trigger_rule='none_failed'` tasks still run and require an explicit gate. `BigQueryHook(gcp_conn_id=..., use_legacy_sql=False).insert_job(configuration, project_id, location).result()` iterates rows (works with the BigQuery REST API). DAG parsing via `DagBag('dags')` with a venv works; `astro dev parse` did not complete on this Mac (20 minutes at "Checking your DAGs for errors", no image pull or network activity). CI: `lint.yaml` runs pre-commit (gitleaks, ruff-check, ruff-format only — NOT black/isort/flake8; PR template headings are Ticket/Context/Changes/TTL/Tests/Documentation). Dev and prod deploy together on merge; there is no pre-merge dev-only deploy.

**CODEOWNERS (AUDI-1277, verified 2026-09-03):** No rule for `dags/bos*` or `dags/intent_score_threshold*`; global default applies: `@SteelHouse/pacing` + `@SteelHouse/performance-ml` review required for any PR. DAG owner (narrative): Forrest Bajbek for `bos__spend`; histogram authors Varun Jain (PRs 523/537/542) and Tony Chen (PR 572, 2026-09-01). Commit rights: malachi has `push` (not admin) on `SteelHouse/airflow-camperbid` and on `SteelHouse/sqlmesh`.
