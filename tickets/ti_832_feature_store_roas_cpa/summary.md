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

## 5. Phase 2 — feature spec (MEASURED 2026-05-05, awaiting Matt + Alex sign-off)

**Methodology** (mirrors [TI-790](../ti_790_bidstream_feature_inventory/summary.md) with the conversion target):
- Built (IP, advertiser_id) training set: 421,366 pairs, 1,355 positives (0.32% base rate). Bid-active IPs from `win_logs` day F=2026-04-15, 1% IP sample. Label = "this pair had a conversion in F+1..F+14" (14d).
- Feature universe: 28 pre-bid (`win_logs`/`cost_impression_log`/`bidder_auction_events`) + 21 conv-history (rolling 7/14/30d backward from `conversion_log` at IP grain + (IP, adv) pair grain).
- Trained XGBoost (`scale_pos_weight≈310`), computed SHAP (mean abs Shapley) on combined model.
- Script: [`artifacts/ti_832_xgboost_conversion_shap.py`](artifacts/ti_832_xgboost_conversion_shap.py). SQL: [`queries/ti_832_training_dataset.sql`](queries/ti_832_training_dataset.sql).

**Results:**
| Model | Features | Test AUC |
|---|---|---|
| A — pre-bid only | 28 | 0.8090 |
| B — conv-history only | 21 | **0.7485** (real standalone signal) |
| C — combined | 49 | **0.8187** (ΔAUC +0.0097 vs pre-bid alone) |

Lift @ top 1%: **18.8x** (60.6% conv rate vs 0.32% base).

### 5.1 SHAP top 25 (combined model) — what survives

| # | Feature | Source | mean abs SHAP | Action |
|---|---|---|---|---|
| 1 | ci_pct_new | cost_impression_log | 1.207 | Already in feature store (TI-810) |
| 2 | bae_pct_genre | bidder_auction_events | 0.342 | Already |
| 3 | **cv_max_amt_30d** | conv_log IP | **0.310** | ✅ ADD (single biggest order in 30d) |
| 4 | wl_avg_price | win_logs | 0.275 | Already |
| 5 | ci_pct_rtc | cost_impression_log | 0.259 | Already |
| 6 | bae_n_auctions | bidder_auction_events | 0.253 | Already |
| 7 | **cv_avg_amt_30d** | conv_log IP | **0.226** | ✅ ADD (avg order size) |
| 8 | ci_total_cost | cost_impression_log | 0.215 | Already |
| 9 | **cv_n_conv_14d** | conv_log IP | **0.197** | ✅ ADD |
| 10 | **cvp_days_since_last** | conv_log (IP, adv) | **0.195** | ✅ ADD — **per-pair grain required** |
| 11 | wl_n_adv | win_logs | 0.170 | Already |
| 12 | bae_n_pubs | bidder_auction_events | 0.170 | Already |
| 13 | ci_pct_video | cost_impression_log | 0.151 | Already |
| 14 | ci_hh_score | cost_impression_log | 0.148 | Already |
| 15 | **cv_n_conv_30d** | conv_log IP | **0.146** | ✅ ADD |
| 16 | ci_n_imp | cost_impression_log | 0.141 | Already |
| 17 | **cvp_n_conv_30d** | conv_log (IP, adv) | **0.138** | ✅ ADD — per-pair |
| 18 | wl_n_wins | win_logs | 0.135 | Already |
| 19 | **cv_n_orders_30d** | conv_log IP | **0.130** | ✅ ADD |
| 20 | **cv_days_since_last** | conv_log IP | **0.125** | ✅ ADD |
| 21 | wl_n_models | win_logs | 0.119 | Already |
| 22 | wl_vcr | win_logs | 0.113 | Already |
| 23 | **cv_usd_amt_7d** | conv_log IP | **0.108** | ✅ ADD |
| 24 | **cv_n_adv_30d** | conv_log IP | **0.104** | ✅ ADD |
| 25 | **cv_usd_amt_30d** | conv_log IP | **0.096** | ✅ ADD |

**Below cutoff** (drop or deprioritize): `cv_total_amt_30d` (outranked by USD-clean equivalent), `cv_usd_amt_14d` (#27), `cv_mobile_flag_30d`, `cv_desktop_30d`, `cv_mobile_30d`, `cv_tablet_30d`, `cv_n_types_30d`, `cv_n_sources_30d`. Device-class counts surprisingly weak — **dropping** despite Matt's explicit ask.

### 5.2 Layer-1 additions to `conv_log_ip` (PR-A) — REVISED

| Column | Aggregation | Why |
|---|---|---|
| `conversion_time_min` | `MIN(time)` | Required for L2 recency math |
| `conversion_time_max` | `MAX(time)` | Required for L2 recency math |
| `usd_order_amount` | `SUM(IF(UPPER(COALESCE(order_curr,'USD'))='USD', SAFE_CAST(order_amt AS DOUBLE), 0))` | SHAP-validated (cv_usd_amt_7d/30d top 25) |

Existing columns kept (`conversion_count`, `total_order_amount`, `order_amount_count`, `max_order_amount`, HLL sketches). Device-class additions and `os_family_hll` from inspection draft **dropped** based on SHAP — no measurable signal.

### 5.3 Two new Layer-2 derived models (PR-B, PR-B2)

**`conv_log_derived_ip`** (pure IP grain) — feeds the IP-level conversion-history features that SHAP confirmed:

| Column | Source aggregation | SHAP rank |
|---|---|---|
| `cv_max_amt_30d` | `MAX(max_order_amount)` over 30d window | #3 (0.310) |
| `cv_avg_amt_30d` | `SAFE_DIVIDE(usd_order_amount_30d, conversion_count_30d)` | #7 (0.226) |
| `cv_n_conv_{7,14,30}d` | `SUM(conversion_count)` per window | #9, #15, #23 |
| `cv_usd_amt_{7,14,30}d` | `SUM(usd_order_amount)` per window | #23, #25 |
| `cv_n_orders_30d` | `hll_merge_extract_count(order_id_hll)` | #19 (0.130) |
| `cv_n_adv_30d` | `hll_merge_extract_count(advertiser_id_hll)` | #24 (0.104) |
| `cv_days_since_last` | `datediff(run_date, MAX(conversion_time_max).date) + 1`, sentinel 999 | #20 (0.125) |
| Forward outcomes (monthly snapshot only) | `cv_n_conv_forward_{7,14}d_outcome`, `cv_usd_amt_forward_{7,14}d_outcome`, `cv_first_conv_day_forward_outcome`, `cv_days_until_first_conv_forward_outcome` | training labels |

**`conv_log_derived_ip_advertiser_id`** (per-pair grain) — SHAP forced this; 2 of top 17 are per-pair:

| Column | Source aggregation | SHAP rank |
|---|---|---|
| `cvp_n_conv_30d` | `COUNT(*)` per (ip, advertiser_id) over 30d | #17 (0.138) |
| `cvp_usd_amt_30d` | `SUM(usd_order_amount)` per pair over 30d | (probable, not in top 25 but pairs with cvp_n_conv) |
| `cvp_days_since_last` | `datediff(run_date, MAX(conversion_time_max).date) + 1`, sentinel 999 | #10 (0.195) |
| Forward outcomes (monthly snapshot only) | `cvp_n_conv_forward_{7,14}d_outcome`, `cvp_usd_amt_forward_{7,14}d_outcome` | training labels |

Both reuse `rolling_sum_exprs` / `forward_sum_exprs` / HLL helpers from [`utils_model/feature_store_core_campaign.py`](../../../airflow-ti/utils_model/feature_store_core_campaign.py) — no new aggregation code.

### 5.4 What's already in the feature store (no PR needed)

The bidstream side is dominant. SHAP top 25 includes 14 features that are **already** at IP grain in the feature store from TI-810 (`ci_pct_new`, `bae_pct_genre`, `wl_avg_price`, etc). **Confirm V2 consumes them** via the existing pivot — that may be a bigger lever than the conv-history adds.

### 5.5 Open questions for Matt + Alex

1. **Per-pair grain (`conv_log_derived_ip_advertiser_id`) is new** — pivot output may need per-(ip, advertiser) rows instead of one wide row per IP. Is V2's input shape compatible? **Need confirmation before PR-B2.**
2. **Drop the device-class columns from PR-A?** SHAP says yes; Matt explicitly mentioned they might be useful. Recommend follow Matt's read of why he expected signal — if it's about cross-device household resolution, may need a different feature shape.
3. **Currency normalization** — USD-only filter sufficient (~99% covered)? **Rec: yes** (FX rates not in feature store).
4. **Training-set scale** — 1,355 positives is statistically thin. Top-10 SHAP rankings are robust; bottom-of-25 has noise. Run with 5% sample (~6,800 positives) before PR-B if extra confidence wanted? **Rec: no — top-25 cliff is clear.**

### 5.6 Sequence

1. PR-A — Layer-1 `conv_log_ip` extension (3 cols only: `conversion_time_min/max`, `usd_order_amount`). ~15 net lines.
2. Backfill `conv_log_ip` for the 30d lookback by clearing tasks (`feature_store_setup_model`).
3. PR-B — Layer-2 `conv_log_derived_ip` (pure IP). ~250 lines.
4. PR-B2 — Layer-2 `conv_log_derived_ip_advertiser_id` (per-pair). ~250 lines.
5. PR-C — Ryan wires DAG deps.

**STOP gate:** no PR-A / PR-B / PR-B2 until Matt + Alex thumb up the measured spec.

### 5.7 Artifacts

- [outputs/ti_832_training_data.csv](outputs/ti_832_training_data.csv) — 421K-row training set (gitignored, 83MB)
- [outputs/ti_832_shap_combined.csv](outputs/ti_832_shap_combined.csv) — full ranked list, all 49 features
- [outputs/ti_832_shap_combined.png](outputs/ti_832_shap_combined.png) — SHAP summary plot (top 25)
- [outputs/ti_832_importance_*.csv](outputs/) — XGBoost gain/weight/cover for each model split
- [outputs/ti_832_lift_combined.csv](outputs/ti_832_lift_combined.csv) — lift table at top 1/5/10/25/50%
- [outputs/ti_832_shap_run.log](outputs/ti_832_shap_run.log) — full training run log

### 5.8 Inspection-driven (pre-SHAP) spec — superseded

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
