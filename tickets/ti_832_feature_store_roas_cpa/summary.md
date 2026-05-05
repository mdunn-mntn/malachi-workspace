# TI-832: Update Feature Store with ROAS/CPA-specific features

**Jira:** https://mntn.atlassian.net/browse/TI-832
**Status:** In Progress
**Date Started:** 2026-05-05
**Date Completed:**
**Assignee:** Malachi

---

## 1. Introduction

The Feature Store (airflow-ti `feature_store_setup_model` DAG) feeds Fangorn and downstream targeting models. It currently surfaces campaign objective via `goal_type_id` (ROAS / CPA / etc.) on the `core_campaign_id` and `core_campaign_group_id` Layer-1 models, but the broader feature set is not yet aligned to the **Campaign Objectives V3** roadmap (PRO-118 / TI-639 — ROAS/CPA Audience Scoring Model).

This ticket audits and updates the Feature Store to ensure the features needed for ROAS/CPA-specific scoring (Ad Spend, Order Value, conversion signals, etc.) are available, correctly engineered, and reachable to the model training/inference pipelines.

## 2. The Problem

- Feature Store coverage is currently optimized for visit-rate scoring; ROAS/CPA-specific signals (revenue-side, conversion-value, spend-shape) are either missing or not engineered for use as model features.
- Ryan flagged (Slack, 2026-05-05) that **3 steps in `feature_store_setup_model` are failing in prod for the last few days**. None are in active use, but they need to be fixed before the campaign-level ROAS/CPA work lands. Likely cause: **column name changes after the BQ migration** — schema drift on tables the failing models read from.
- Where to source campaign objective: `core_campaign_group_id.py` / `core_campaign_id.py` — the campaign objective comes from `public.campaign_groups.goal_type_id` joined to `core.goal_types` (ROAS / CPA / etc.).
  ```sql
  SELECT DISTINCT goal_type_id, goal_type_name
  FROM public.campaign_groups
  ```

## 3. Plan of Action

1. **Identify the 3 failing tasks** in the `feature_store_setup_model` DAG (Airflow UI prod) — capture task IDs, error messages, and which underlying tables/columns are the source of the failure.
2. **Fix schema drift** — for each failing model, either remove columns that no longer exist or add the new column names (per Ryan's hypothesis of post-BQ-migration renames).
3. **Audit Feature Store for ROAS/CPA gaps** — list features Campaign Objectives V3 / TI-639 needs (Ad Spend, Order Value, conversion signals, goal-type-conditioned aggregates) and check coverage across Layer 1 / Layer 2.
4. **Engineer the missing features** as new Layer-1 / Layer-2 models in `airflow-ti/models/feature_store/`, following [naming standards](https://mntn.atlassian.net/wiki/spaces/TAR/pages/3474751523/Feature+Store+Naming+Conventions) (`docs/feature_store_naming_standards.md`).
5. **Wire into the DAG** on a feature branch (Ryan owns DAG dependency wiring per the airflow-ti prod-safety rule — feature branches only, never push to main).
6. **Validate** in dev, then hand off the branch for Ryan's DAG dep wiring + prod deploy.

## 4. Investigation & Findings

### 4.1 Repo & branch setup
- airflow-ti repo: `/Users/malachi/Developer/work/mntn/airflow-ti` (already cloned, `git@github.com:SteelHouse/airflow-ti.git`).
- Currently on `feature/ti-810-bidstream-ip-features` (TI-810 work). New branch needed for TI-832.
- Per `feedback_airflow_prod_safety`: never modify DAGs / push to main; models only on feature branches; Ryan wires DAG deps.

### 4.2 Ryan's pointers (Slack, 2026-05-05)
- Failing tasks discovered via Airflow UI (no PagerDuty yet — feature store is not in prod use; Fangorn still uses a Databricks notebook).
- Suspected cause: column renames after BQ migration → `Claude rip out the columns that aren't there any more or add missing columns`.
- Source-of-truth for campaign objective: `core_campaign_group_id` / `core_campaign_id` — `goal_type_id` comes from `public.campaign_groups`, joined to `core.goal_types`.
- Naming conventions: `docs/feature_store_naming_standards.md` in airflow-ti, also at https://mntn.atlassian.net/wiki/spaces/TAR/pages/3474751523/Feature+Store+Naming+Conventions

### 4.3 DAG inventory (`feature_store_setup_model`)
Layer-1 source tasks (19): `guid_log_ip_advertiser_id_rollup`, `guid_log_advertiser_id_dsc_id`, `conversion_log_advertiser_id_dsc_id`, `site_visit_signal_advertiser_id_dsc_id`, `aug_log_ip_vertical_id`, `core_advertiser_id`, `sentiment_advertiser_id`, `core_campaign_id`, `summary_advertiser_id`, `summary_campaign_group_id`, `salesforce_advertiser_id`, `core_campaign_group_id`, `summary_campaign_id`, `aug_log_ip`, `win_logs_ip`, `bae_ip`, `cil_ip`, `guid_log_ip`, `conv_log_ip`.
Layer-2 derived (7): `guid_log_derived_ip_vertical_id`, `guid_and_conv_log_derived_advertiser_id_dsc_id`, `guid_log_generic_penalty_derived_advertiser_id_dsc_id`, `site_visit_signal_derived_advertiser_id_dsc_id`, `core_derived_advertiser_id`, `core_derived_campaign_id`, `core_derived_campaign_group_id`.
Layer-3 pivot (1): `guid_log_pivot_ip_vertical_id`.

### 4.4 Failing tasks (last 3 prod runs, screenshot 2026-05-05)

Three Layer-1 source tasks failing — `summary_advertiser_id`, `summary_campaign_group_id`, `summary_campaign_id`. The Layer-2 `core_derived_*` cascades are upstream-blocked, not independently failing.

### 4.5 Root cause — post-BQ-migration column drop on `dw-main-silver.summarydata.sum_by_*_by_day`

All three models do `SELECT *` from BQ, then explicitly project a fixed column list. The migration dropped the `*_cost` columns (and `legacy_spend` on campaign_group), so the explicit projection now references columns that don't exist.

| Column projected in code | sum_by_advertiser_by_day | sum_by_campaign_group_by_day | sum_by_campaign_by_day |
|--------------------------|:-:|:-:|:-:|
| `data_cost`     | dropped | dropped | dropped |
| `fee_cost`      | dropped | dropped | dropped |
| `partner_cost`  | dropped | dropped | dropped |
| `legacy_spend`  | n/a     | dropped | n/a     |

The `*_spend` columns (`media_spend`, `data_spend`, `platform_spend`) **are** still present — the migration consolidated cost → spend. `legacy_spend` already has a defensive `if "legacy_spend" not in bq_df.columns` shim (lines 76–77 of `summary_campaign_group_id.py`) but the cost columns have no fallback, hence the crash.

**Fix:** drop the four columns from the explicit `.select(...)` projection in the three model files. No downstream code references them (Layer-2 `core_derived_*` only reads spend/visit/conversion fields). Verify with grep before pushing.

### 4.6 New columns in schema not yet projected (potential ROAS/CPA inputs)

Discovered via the same schema diff. These are candidates for the broader ROAS/CPA feature work in Phase 2 of this ticket — particularly `probattr_*` and `raw_*` for un-attributed and probabilistically-attributed conversion/order signals.

- **`probattr_*` (20 columns, all three views):** probabilistic-attribution variants of view/visit/conversion/order_value metrics — `probattr_views`, `probattr_view_conversions`, `probattr_view_order_value`, `probattr_site_visitors`, `probattr_new_site_visitors`, `probattr_last_touch_views`, `probattr_last_touch_view_conversions`, `probattr_last_touch_view_order_value`, plus `probattr_competing_*` mirrors of each.
- **`raw_*` (advertiser only, 5 columns):** `raw_conversions`, `raw_order_value`, `raw_visits`, `raw_new_site_visitors`, `raw_existing_site_visitors` — un-attributed conversion/visit/spend signals.
- **`new_to_file`, `visitors`** (advertiser only).

## 5. Solution

### 5.1 Phase 1 — fix the column-drift crash (2026-05-05)

Branch: `feature/ti-832-fix-summary-bq-column-drift` in airflow-ti (commit `06a98cf`). PR: [airflow-ti#1024](https://github.com/SteelHouse/airflow-ti/pull/1024) — open for review 2026-05-05.

Patched four files (-17 lines, no additions):
- `models/feature_store/feature_group_1_source/summary_advertiser_id.py` — drop `data_cost`, `fee_cost`, `partner_cost` from explicit `.select(...)`.
- `models/feature_store/feature_group_1_source/summary_campaign_id.py` — same three.
- `models/feature_store/feature_group_1_source/summary_campaign_group_id.py` — same three plus `legacy_spend`; remove the `legacy_spend` defensive shim that's now dead.
- `utils_model/feature_store_core_campaign.py` — drop the same three from `SUMMARY_OUTCOME_METRIC_COLS` so Layer-2 `core_derived_*` doesn't crash on the next run with the now-missing parquet columns.

Verified repo-wide grep returns no surviving references; all four files compile cleanly. No `dags/` changes (per prod-safety: Ryan owns DAG dep wiring).

### 5.2 Phase 2 — ROAS/CPA features (pending)

Audit `probattr_*`, `raw_*`, and goal-type-conditioned aggregates against PRO-118 / TI-639 needs; engineer net-new Layer-1 / Layer-2 models.

## 6. Questions Answered

- **Q:** Where does the campaign objective (ROAS / CPA / etc.) come from?
  **A:** `public.campaign_groups.goal_type_id` joined to `core.goal_types.goal_type_id` for the label. Currently exposed in `core_campaign_group_id.py` (and `core_campaign_id.py`) Feature Store models.

- **Q:** Where should the airflow-ti repo live?
  **A:** Already at `/Users/malachi/Developer/work/mntn/airflow-ti`, sibling to `workspace/`. Pattern: `~/Developer/work/mntn/<repo>` for all SteelHouse/MNTN GitHub repos.

## 7. Data Documentation Updates

(pending — likely additions: `core_campaign_groups.goal_type_id` semantics, `core.goal_types` lookup, post-BQ-migration column renames discovered during the fix)

## 8. Open Items / Follow-ups

- Identify the 3 failing tasks in `feature_store_setup_model` (Airflow UI).
- Confirm BQ-migration column renames affecting the failing tasks.
- Build the ROAS/CPA feature inventory from Campaign Objectives V3 / TI-639 requirements.
- PagerDuty on feature_store DAG (Ryan's IMO) — separate ticket if pursued.

## 9. Files

- `meetings/ti_832_01_ryan_feature_store_roas_cpa_2026_05_05.txt` — kickoff conversation with Ryan (transcription pending).
- `queries/` — SQL audits for goal_type_id distribution + missing-column probes.
- `outputs/` — failing-task error captures + feature inventory.
- `artifacts/` — feature-engineering scripts, naming-convention notes.

## 10. Related

- airflow-ti feature branch: TBD (likely `feature/ti-832-feature-store-roas-cpa-features`).
- PRO-118 / TI-639 (ROAS/CPA Audience Scoring Model) — primary downstream consumer.
- TI-810 (bidstream IP features) — adjacent Feature Store work, current branch.
- Naming standards: airflow-ti `docs/feature_store_naming_standards.md`.
