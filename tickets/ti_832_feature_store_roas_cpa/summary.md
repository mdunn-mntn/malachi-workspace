# TI-832: Update Feature Store with ROAS/CPA-specific features

**Jira:** https://mntn.atlassian.net/browse/TI-832
**Status:** In Progress — Phase 2 (Fangorn V2 conversion-feature spec) underway. TI-931 unblocker shipped 2026-05-05.
**Date Started:** 2026-05-05
**Date Completed:**
**Assignee:** Malachi
**Was blocked by:** [TI-931](https://mntn.atlassian.net/browse/TI-931) — column-drift bug fix verified in prod 2026-05-05 (PR #1024 merged, 18 task instances re-run green). Now unblocked.

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

## 5. Phase 2 — feature spec (DRAFTED 2026-05-05, awaiting Matt + Alex sign-off)

**Source-table audit (14d, 1.0B rows of `dw-main-silver.logdata.conversion_log`):**
- `order_amt_usd` is **1.4% populated → unusable**. Use `order_amt` filtered to USD currency.
- `device_type` 96.9% / `is_mobile_device` 96.9% / `operating_system` 97.1% — viable IP-grain features.
- `conversion_type` cardinality = 6,541 → HLL only, no direct enumeration.
- 47.9M distinct IPs in 30 days — Layer-2 `(ip)` grain is tractable.

**What `conv_log_ip` produces today** (Layer-1, daily): `conversion_count`, `total_order_amount`, `order_amount_count`, `max_order_amount`, plus HLL sketches for `order_id` / `conversion_type` / `conversion_source_id` / `advertiser_id`. **No timestamps, no currency-clean revenue, no device split, no Layer-2.**

### 5.1 Layer-1 additions to `conv_log_ip` (PR-A, ~30 net lines)

| Column | Aggregation | Why |
|---|---|---|
| `conversion_time_min` | `MIN(time)` | Earliest conv on dt — first-conv recency in Layer-2 |
| `conversion_time_max` | `MAX(time)` | Last-conv recency |
| `usd_order_amount` | `SUM(CASE WHEN UPPER(COALESCE(order_curr,'USD'))='USD' THEN CAST(order_amt AS DOUBLE) ELSE 0 END)` | Currency-clean revenue (~99% coverage; non-USD zero-fills) |
| `desktop_conv_count` | `SUM(WHEN device_type='desktop' THEN 1)` | Mirror `guid_log_ip` device-class pattern |
| `mobile_conv_count` | `SUM(WHEN LOWER(device_type) IN ('mobile','phone') THEN 1)` | Same |
| `tablet_conv_count` | `SUM(WHEN device_type='tablet' THEN 1)` | Same |
| `other_device_conv_count` | `conversion_count − (desktop+mobile+tablet)` | Captures CTV / unknown |
| `os_family_hll` | `hll_sketch_agg(os_family, 12)` | Mergeable distinct-OS count in Layer-2 |

Existing columns kept as-is. No removals.

### 5.2 New Layer-2 model `conv_log_derived_ip` (PR-B, ~250 lines)

Mirrors [`guid_log_derived_ip_vertical_id.py`](../../../airflow-ti/models/feature_store/feature_group_2_derived/guid_log_derived_ip_vertical_id.py) **minus the `vertical_id` grain** (pure IP). `MultiSnapshotFileStorageBaseModel(["base","monthly"])`. Reuses `rolling_sum_exprs` / `forward_sum_exprs` / `rolling_hll_merge_exprs` / `forward_hll_merge_exprs` from [`utils_model/feature_store_core_campaign.py`](../../../airflow-ti/utils_model/feature_store_core_campaign.py) — no new aggregation code.

**Backward (lookback) columns** — windows `(7, 14, 30)`:

| Column | Source | Notes |
|---|---|---|
| `conversion_count_{7,14,30}d` | `SUM(conversion_count)` | Volume |
| `total_order_amount_{7,14,30}d` | `SUM(total_order_amount)` | Currency-mixed (parity with L1) |
| `usd_order_amount_{7,14,30}d` | `SUM(usd_order_amount)` | Primary revenue feature |
| `max_order_amount_30d` | `MAX(max_order_amount)` over window | High-value-purchase signal |
| `desktop_conv_count_{7,14,30}d` / `mobile_…` / `tablet_…` | `SUM(...)` | Device split |
| `distinct_advertisers_30d` | `hll_merge_extract_count(advertiser_id_hll)` | Multi-brand converter? |
| `distinct_conversion_types_30d` | `hll_merge_extract_count(conversion_type_hll)` | Funnel-stage diversity |
| `distinct_orders_30d` | `hll_merge_extract_count(order_id_hll)` | True-distinct order count |
| `last_conversion_time_max` | `MAX(conversion_time_max)` | Last-touch timestamp |
| `last_conversion_day` | `to_date(last_conversion_time_max)` | Date form |
| `days_since_last_conversion` | `datediff(run_date, last_conversion_day) + 1`, sentinel `999` | Mirrors `days_since_last_visit_in_vertical` |

**Forward outcome columns** (monthly snapshot only):

| Column | Source | Notes |
|---|---|---|
| `conversion_count_forward_{7,14}d_outcome` | forward `conversion_count` | Did this IP convert post-snapshot? |
| `usd_order_amount_forward_{7,14}d_outcome` | forward `usd_order_amount` | ROAS training target |
| `first_conversion_time_min_forward_outcome` | forward `MIN(conversion_time_min)` | Time-to-first-conversion |
| `first_conversion_day_forward_outcome` | `to_date(...)` | Date form |
| `days_until_first_conversion_forward_outcome` | `datediff(...)`, sentinel `999` | Time-to-conversion outcome |

**Layer-3 (pivot) — out of scope.** Only build if V2 needs a wide-format input — Matt's call later.

### 5.3 Open questions for Matt + Alex (with recommendation)

1. **Currency normalization** — FX-adjusted vs USD-only filter? **Rec: USD-only filter** (~1% non-USD; FX not in feature store).
2. **Outlier capping at L1** — cap `conversion_count` per IP per day or surface raw? **Rec: raw at L1, cap at training time** (preserve information).
3. **`vertical_id` breakdown** — pivot `(ip, vertical_id)` or pure IP? **Rec: pure IP MVP**; add `_ip_vertical_id` variant in follow-up if V2 wants vertical breadth.
4. **Attribution split (click/view/probattr)** — conversion_log has no attribution column at event row. **Rec: stay attribution-agnostic at IP grain**; attribution lives on advertiser-grain `summary_*` (already in feature store).
5. **Recency cap** — 30d window + `999` sentinel for "never converted"? **Rec: yes**, consistency with `guid_log_derived`. Longer windows = separate ticket.
6. **`probattr_*` / `raw_*` extension to IP grain** — currently advertiser-grain. **Rec: defer**; only add if Matt explicitly wants it.
7. **OS-family counts** — desktop/mobile/tablet/other only, or also mac/windows/ios/android counts at L1? **Rec: device-class only at L1**, OS-family as PR-A2 follow-up if Matt asks.

### 5.4 Sequence

PR-A (Layer-1 extension) ships first → backfill `conv_log_ip` for the 30d lookback by clearing failed/stale tasks. PR-B (Layer-2 derived) ships second. PR-C (DAG dep wiring) is Ryan's.

**STOP gate:** no PR-A / PR-B code lands until Matt + Alex thumb up the spec.

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
