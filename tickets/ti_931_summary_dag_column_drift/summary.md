# TI-931: feature_store_setup_model summary_* tasks failing — *_cost cols dropped post-BQ migration

**Jira:** https://mntn.atlassian.net/browse/TI-931
**Type:** Bug
**Status:** In Progress
**Priority:** P3 — Normal
**Story Points:** 2
**Sprint:** TI Sprint 05/04/26 – 05/18/26 (id 6160)
**Date Started:** 2026-05-05
**Date Completed:**
**Assignee:** Malachi
**Blocks:** [TI-832](https://mntn.atlassian.net/browse/TI-832)

---

## 1. Introduction

Pre-existing bug in `feature_store_setup_model` (airflow-ti). Surfaced when starting TI-832 (ROAS/CPA features) — Ryan flagged the failures Slack 2026-05-05. The bug predates TI-832 and is its own scope; it blocks TI-832 because the DAG must be green before Phase-2 feature work lands.

## 2. The Problem

Three Layer-1 tasks have been failing in prod for ~3 days (5/3, 5/4, 5/5): `summary_advertiser_id`, `summary_campaign_group_id`, `summary_campaign_id`. Layer-2 `core_derived_*` cascade-blocked. Tasks aren't in active prod use (Fangorn still on Databricks), but the failure blocks the campaign-level ROAS/CPA work in TI-832.

## 3. Root Cause

The BQ migration consolidated cost → spend on `dw-main-silver.summarydata.sum_by_*_by_day`. The `*_cost` family was dropped:

| Column projected in code | sum_by_advertiser | sum_by_campaign_group | sum_by_campaign |
|--------------------------|:-:|:-:|:-:|
| `data_cost` | dropped | dropped | dropped |
| `fee_cost` | dropped | dropped | dropped |
| `partner_cost` | dropped | dropped | dropped |
| `legacy_spend` | n/a | dropped | n/a |

`*_spend` columns (`media_spend`, `data_spend`, `platform_spend`) remain and authoritatively replace the cost family. The Layer-1 models do `SELECT *` from BQ then explicitly project the now-missing columns, hence the crash. The shared `SUMMARY_OUTCOME_METRIC_COLS` in `utils_model/feature_store_core_campaign.py` also lists the dropped columns, so the Layer-2 `core_derived_*` tasks would have crashed on unblock too.

## 4. Solution

**Branch:** `feature/ti-832-fix-summary-bq-column-drift` in airflow-ti (commit `06a98cf` — branch name predates the TI-931 split; PR title now references TI-931).
**PR:** [airflow-ti#1024](https://github.com/SteelHouse/airflow-ti/pull/1024).
**Diff:** -17 lines, 0 additions, 4 files; no `dags/` changes.

Patched files:
- `models/feature_store/feature_group_1_source/summary_advertiser_id.py` — drop 3 cols from `.select()`
- `models/feature_store/feature_group_1_source/summary_campaign_id.py` — drop 3 cols from `.select()`
- `models/feature_store/feature_group_1_source/summary_campaign_group_id.py` — drop 4 cols + dead `legacy_spend` defensive shim (lines 76–77)
- `utils_model/feature_store_core_campaign.py` — drop 3 from `SUMMARY_OUTCOME_METRIC_COLS`

Why removal (not replacement with zero defaults): the columns aren't in the source any more. Replacing with `F.lit(0.0)` would silently mask the upstream change. Mapping cost→spend isn't 1:1 — they're related but not equivalent metrics; the migration explicitly chose spend as the new authoritative surface.

Repo-wide grep for the four column names returns clean. All four files `py_compile` clean.

## 5. Verification

**Pre-merge (done):**
- [x] Repo-wide grep for `data_cost` / `fee_cost` / `partner_cost` / `legacy_spend` → clean
- [x] `python3 -m py_compile` on all four files → clean
- [x] Diff size confirmed: 4 files, -17 lines, 0 additions, no `dags/` touched

**Post-merge (pending):**
- [ ] `deploy_prod.yaml` GitHub Action goes green
- [ ] Clear failed task instances for `summary_*` + `core_derived_*` for 5/3, 5/4, 5/5 in prod Astronomer/Airflow UI
- [ ] Cleared instances run green
- [ ] Next 01:03 UTC scheduled run goes green for all 6 tasks with no manual clearing
- [ ] Spot-check parquet output landed in `gs://mntn-data-archive-prod/feature_store/feature_group_1_source/summary_*/dt=YYYY-MM-DD/`

## 6. Data Documentation Updates

Already added to [knowledge/data_knowledge.md](../../knowledge/data_knowledge.md) under "Business Logic":
- Post-BQ-migration column drops on `summarydata.sum_by_*_by_day` (this bug)
- `probattr_*` (20 cols) and `raw_*` (5 cols on advertiser only) inventory — newly exposed in the same views, candidates for TI-832 Phase 2

## 7. Files

- `meetings/ti_931_01_ryan_kickoff_dag_failures_2026_05_05.txt` — Ryan's kickoff conversation; the bug-fix portion is 03:54–10:50 (rest of the meeting was TI-832 ROAS/CPA scoping, retained in TI-832 folder).
- airflow-ti PR: [#1024](https://github.com/SteelHouse/airflow-ti/pull/1024)
- airflow-ti commit: `06a98cf` on `feature/ti-832-fix-summary-bq-column-drift`

## 8. Related

- **Blocks:** [TI-832](https://mntn.atlassian.net/browse/TI-832) — ROAS/CPA feature work depends on this DAG being green.
- airflow-ti ticket scope: model code + shared utility constant only; no DAG file changes (Ryan owns DAG dep wiring).
- Why this surfaced now: Ryan was reviewing `feature_store_setup_model` while kicking off TI-832 with Malachi; not caught earlier because none of these models are consumed in prod yet (Fangorn still uses a Databricks notebook; PagerDuty not wired on this DAG).

## 9. Open Items / Follow-ups

- After Phase 1 ships: consider PagerDuty on `feature_store_setup_model` (Ryan, IMO). Separate ticket if pursued.
- Why didn't CI catch this? Schema-drift tests on Layer-1 BQ source views — separate ticket if pursued.
