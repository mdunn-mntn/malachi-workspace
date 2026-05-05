# TI-832: Update Feature Store with ROAS/CPA-specific features

**Jira:** https://mntn.atlassian.net/browse/TI-832
**Status:** In Progress (blocked on TI-931)
**Date Started:** 2026-05-05
**Date Completed:**
**Assignee:** Malachi
**Blocked by:** [TI-931](https://mntn.atlassian.net/browse/TI-931) — column-drift bug fix on `summary_*` models. Must merge + DAG green before this work proceeds.

---

## 1. Introduction

Add IP-grain conversion / spend signals to the Feature Store so **Fangorn V2** has the inputs it needs. Fangorn V2 is a parallel XGBoost classifier (Matt Brorby) trained on **conversions instead of visits** — the bidder picks Fangorn vs Fangorn V2 per campaign based on `goal_type_id` (CPV vs ROAS, etc.).

Scope clarified in the Alex Knorr + Matt Brorby meeting (2026-05-05, [meetings/ti_832_02_alex_matt_feature_store_roas_cpa_2026_05_05.txt](meetings/ti_832_02_alex_matt_feature_store_roas_cpa_2026_05_05.txt)):
- **Consumer:** Fangorn V2 (Matt building, Alex iterated on the prototype).
- **Grain:** **IP-level**, not advertiser/campaign — `conv_log_ip`, `guid_log_ip`, etc. — the same surface Fangorn already consumes.
- **Feature ask is open-ended** — Matt: *"whatever you can think of that might be helpful... limit it down to whatever you think is low-hanging fruit."* Conversion frequency, conversion order_value, possibly device-type.
- **Data leakage is the loudest constraint** — features must bind to conversion event timestamp, not feature-processing date.

## 2. The Problem

Feature Store coverage is currently optimized for visit-rate scoring (Fangorn). ROAS/CPA-specific signals (conversion volume, order_value, conversion frequency, recency) are not yet engineered at IP grain. Fangorn V2 needs them as model inputs.

## 3. Plan of Action

Two phases:

**Phase 2 — Spec lock-in for Fangorn V2 inputs (DISCOVERY, no code).**
Propose a concrete column-by-column feature list to Matt + Alex. Matt explicitly punted the list to us. We get explicit sign-off before any Layer-1 changes.

**Phase 3 — Add IP-level conversion features (one chunk, one PR).**
After Phase 2 sign-off: implement the agreed metrics in `conv_log_ip` (and any peer Layer-1), plug into the canned Layer-2 windowing in `utils_model/feature_store_core_campaign.py`. Each metric group ships as its own small PR.

(Phase 1 — the column-drift bug — moved out of TI-832 to **[TI-931](https://mntn.atlassian.net/browse/TI-931)**. PR [airflow-ti#1024](https://github.com/SteelHouse/airflow-ti/pull/1024) is open there.)

## 4. Investigation & Findings

### 4.1 Repo & branch setup
- airflow-ti: `~/Developer/work/mntn/airflow-ti`. Workspace doc: [airflow_ti_workflow.md](../../documentation/docs/airflow_ti_workflow.md).
- Naming standards: [`docs/feature_store_naming_standards.md`](https://github.com/SteelHouse/airflow-ti/blob/main/docs/feature_store_naming_standards.md). Use `_outcome_` suffix for forward-looking variables, `_7d` / `_14d` / `_30d` for lookbacks.
- Per `feedback_airflow_prod_safety`: feature branches only, no `dags/` edits, Ryan wires DAG deps.

### 4.2 Where campaign objective lives (already in Feature Store)
`public.campaign_groups.goal_type_id` joined to `core.goal_types.goal_type_id`. Already projected on `core_campaign_group_id.py` and `core_campaign_id.py` Layer-1 models. **No change needed for the dimension surface** — only the per-IP conversion metrics need adding.

### 4.3 Inputs already available for Phase 3 (don't reinvent)
- **IP-grain Layer-1 models:** `conv_log_ip`, `guid_log_ip`, `aug_log_ip`, `bae_ip`, `cil_ip`, `win_logs_ip` (all under `models/feature_store/feature_group_1_source/`, TI-810 work).
- **Layer-2 window pattern:** 7/14/30d backward + 7/14d forward outcome already canned in `utils_model/feature_store_core_campaign.py` (`SUMMARY_OUTCOME_METRIC_COLS`, `aggregate_summary_windows`, `rolling_sum_exprs`, `forward_sum_exprs`). New metrics plug into this — no new aggregation code.
- **Conversion-side surface on `summarydata.sum_by_*_by_day`** (advertiser/campaign grain — useful for cross-reference, not the IP-level Phase 3 home): `click_conversions`, `view_conversions`, `last_touch_view_conversions`, `last_tv_touch_view_conversions`, `conversions_assist_view`, plus `*_order_value` mirrors. New `probattr_*` (20 cols) and `raw_*` (5 cols, advertiser only) families also now exposed — see [knowledge/data_knowledge.md](../../knowledge/data_knowledge.md).

### 4.4 Decisions from Alex/Matt meeting
- IP grain (not advertiser/campaign).
- Open-ended feature ask — we drive the proposal.
- Data leakage: bind to event timestamp, never to processing date. *"Be super careful about how you're assigning dates for things... here's the day that I processed, but did this conversion happen today or not?"*
- Don't aggregate at DSC-id grain — that's the bottom-up-keywords pipeline, not Fangorn.
- Feature drift to be monitored once V2 is in prod.
- Local testing path: `python model_run.py conv_log_ip -a '{"run_date": "YYYY-MM-DD"}'` (Dataproc Serverless, dev bucket, branch suffix). Alex offered a walkthrough.

## 5. Phase 2 — feature spec (TO DRAFT, before Phase 3 starts)

Candidate metrics to propose to Matt + Alex (column-by-column; each gets the standard 7/14/30d backward + 7/14d forward outcome treatment via the existing helper):

- `conv_count` — conversions per IP per window
- `order_value_sum` — total revenue per IP per window
- `avg_order_value` — `order_value_sum / conv_count` (computed in Layer 2 from the two above)
- `days_since_last_conv` — recency
- `distinct_conv_advertisers_30d` — breadth (does this IP convert across many brands?)
- `view_conv_share` — view-attributed share (vs click)
- Forward outcomes for training: `conv_count_forward_outcome_{7,14}d`, `order_value_forward_outcome_{7,14}d`

**Open spec questions:**
1. Click-attributed vs view-attributed vs both vs probattr? (depends on V2 training target)
2. Strip view-attributed outliers? (advertiser variance is huge — ToS/checkout vs B2B)
3. Recency window cap (90d? 365d?)
4. Device-type as feature (Matt mentioned in passing — needs a source-table audit; might be in `bae_ip` or `aug_log_ip`)

Output of Phase 2: this section gets filled in with the final agreed list, then a Jira comment posts the spec for Matt + Alex thumb-up.

## 6. Solution

(to be filled during Phase 3)

## 7. Data Documentation Updates

Already added during the TI-931 work in [knowledge/data_knowledge.md](../../knowledge/data_knowledge.md):
- Post-BQ-migration column drops on `summarydata.sum_by_*_by_day`
- `probattr_*` (20 columns, all three views) — probabilistic-attribution metrics
- `raw_*` (5 columns, advertiser only) — un-attributed metrics

Likely Phase 3 additions: `conversion_log` IP-grain semantics, `goal_type_id` mapping table, any new Layer-1 models added.

## 8. Open Items / Follow-ups

- Phase 2 spec discovery — see Section 5.
- Local-dev walkthrough with Alex if needed.
- Phase 3 PR sizing — keep each metric group its own PR.
- Possibly extend `probattr_*` / `raw_*` to IP grain — separate ticket, defer until Matt asks.

## 9. Files

- `meetings/ti_832_02_alex_matt_feature_store_roas_cpa_2026_05_05.txt` — Alex Knorr + Matt Brorby scoping conversation.
- `meetings/ti_832_01_ryan_feature_store_roas_cpa_2026_05_05.txt` — Ryan kickoff (also covers TI-931 bug — copy in TI-931's folder).
- `queries/` — SQL audits for `conv_log_ip` source tables, schema probes.
- `outputs/` — Phase 2 spec drafts, Phase 3 verification samples.
- `artifacts/` — Phase 3 model code references.

## 10. Related

- **Blocked by:** [TI-931](https://mntn.atlassian.net/browse/TI-931) — column-drift bug; must ship before Phase 3.
- **Downstream consumer:** Fangorn V2 (Matt). Bidder routes to V2 by campaign `goal_type_id`.
- **Related epic:** PRO-118 / TI-639 (ROAS/CPA Audience Scoring Model) is the eventual primary consumer.
- **Adjacent:** TI-810 (bidstream IP features) — same Layer-1 IP-grain surface.
- **Naming standards:** airflow-ti `docs/feature_store_naming_standards.md`.
