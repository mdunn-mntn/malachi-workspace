# TI-1313: Query Scope & Data Availability

**Status:** Scoping complete | **Date:** 2026-09-01

## Lift Metrics (Source)

**Table:** `dw-main-gold.sqlmesh__reporting.reporting__lift__ghost_bid_rollup__*` (VIEW, physical table refreshed daily)
**Filter:** `level='campaign_group'` (2,287 rows as of 2026-08-03)
**Grain:** One row per campaign_group_id, all-time aggregate (no period/dt column)

**Columns available:**
- `entity_id` = campaign_group_id
- `advertiser_id`, `partner_id` (8=Beeswax, 79=Rust)
- `n_treatment`, `n_holdout` — sample size per arm
- `vis_treatment`, `vis_holdout` — visit counts
- `incremental_visits` = abs_itt × n_treatment (raw count of incremental visits)
- `base_holdout_rate` — holdout visit rate (typically ~0% when n_holdout is small)
- `abs_itt`, `rel_itt` — absolute/relative lift (on bid-grain ITT basis, diluted by win rate)
- `se` — standard error
- `abs_ci_low`, `abs_ci_high` — 95% CI bounds
- `z`, `significant_95` — z-stat and significance flag
- `compliance_wt` — IP compliance fraction (fraction of bid IPs that won an impression, ~50%)
- Conversion columns: `conv_treatment`, `conv_holdout`, `conv_abs_itt`, `conv_se`, `conv_z`, `conv_rel_itt`, `conv_significant_95`
- `coverage_frac_treated`, `low_coverage` — flag to filter underpowered campaigns
- `n_campaigns_incl`, `n_campaigns_total` — campaign inclusion counts

**Key gotcha:** All-time only; filter `se>0` and `NOT low_coverage` for clean gate.

---

## Campaign Attributes (Sources)

### 1. **Campaign Groups** (`dw-main-silver.public.campaign_groups`)
**Grain:** One row per campaign_group_id (PK = campaign_group_id, not id)
**Available columns:**
- `campaign_group_id`, `advertiser_id`
- `objective_id` (1=Prospecting, 4=Retargeting, etc.)
- `product_id` (1=PTV, 2=Select, 3=QF)
- `budget`, `budget_type_id`
- `start_time`, `end_time`, `active_flight_id`
- `frequency_cap_impressions`, `frequency_cap_duration` — fcap settings
- `has_audience`, `has_audience_raw` — audience flags
- `ctv_creatives_status_id`, `display_creatives_status_id`, `ui_creatives_status_id` — creative status
- `deleted`, `is_test`
- `campaign_goal_id`, `delivery_mode_id`, `platform_id`

**Attribution / window fields:** NOT present in campaign_groups; would need to come from flight or campaign level.

### 2. **Advertiser Metadata** (`dw-main-bronze.integrationprod.advertisers`)
**Join key:** `advertiser_id`
**Available columns:**
- `advertiser_vertical_id` → **requires lookup to vertical names** (not in this table)
- `company_size`, `industry` — company metadata
- `account_health` — health status string
- `monthly_muv` — monthly unique visitors
- `apply_budget_to_total_spend` — budget setting flag
- `uses_household_id`, `uses_mntn_id` — identifier preferences
- `data_tier_id` — data tier

**Missing:** CVR, AOV, tenure/launch_date, CRM exclusion status, Display MT flag — these may be in other tables or derived from impressions/conversions.

### 3. **Bid Logs & Impressions** (`dw-main-silver.logdata.cost_impression_log`, `event_log`, etc.)
**Join key:** `campaign_group_id` (in impression_log as `group_id`)
**Available attributes (per-impression, needs aggregation by campaign_group_id):**
- `household_score`, `advertiser_household_score` — intent scores (100% NULL before 2025-06-01, populated from 2025-06-01 onward; recoverable from model_params back to 2025-05-06)
- `device` → can derive device_type percentages (%TV, %Desktop, %Mobile)
- `geoname_id`, `metro_id`, `region` → geo breakouts (national vs regional)
- Spend: `cpi`, `cpm` — cost per impression and cost per mille
- Impressions: row count per campaign_group
- Frequency: `bid_count` in lift table (or need to aggregate from event logs)

**Partition gotcha:** cost_impression_log partitioned by DATE(time), starting 2025-01-01 (actually 2023-10-01 for cost_impression_log per catalog).

### 4. **Vertical Lookup** (MISSING — need to find)
**Known:** `advertiser_vertical_id` exists in advertisers table. Need to find the dimension table with vertical_id → vertical_name mapping.
- Likely in: `dw-main-bronze.integrationprod.advertiser_verticals` or similar
- Or in: `dw-main-gold.ape_core` or another fact/dimension set

**Action:** Query INFORMATION_SCHEMA to locate the vertical lookup table.

---

## Coverage & Filtering

**Powered campaigns (eligible for analysis):**
- `n_holdout >= 100` (minimum holdout power per user frame)
- `coverage_frac_treated` > some threshold (compliance gate)
- `NOT low_coverage` (baked-into rollup)
- `se > 0` (exclude zero-variance rows)
- `objective_id = 1` (prospecting only; holdout is prospecting-only by construction)
- `partner_id = 8` (Beeswax only; partner_id=79 is unreliable per data catalog §8)
- 30-day trailing window: 2026-08-02 (MAX(dt) in lift table) minus 30 days = 2026-07-03 start

**Expected count:** 950+ campaigns with 100+ holdout visits (per Matt Brorby query reference in frame).

---

## Output Schema (Draft)

**Sheet 1: Raw Data (per-campaign rows)**

| Column | Source | Type | Notes |
|--------|--------|------|-------|
| campaign_group_id | lift rollup | INT | Primary key |
| campaign_name | campaign_groups.name | STR | Campaign group name |
| advertiser_id | lift rollup | INT | Advertiser PK |
| advertiser_name | advertisers.company_name | STR | Advertiser name |
| vertical | advertiser_vertical lookup | STR | Vertical category |
| product | campaign_groups.product_id | STR | PTV / Select / QF |
| objective | campaign_groups.objective_id | INT | Prospecting=1 |
| **Lift Metrics** | | | |
| visit_lift_pct | (rel_itt * 100) | FLOAT | Relative lift % |
| visit_ci_low_pct | (abs_ci_low * 100) / base_holdout_rate | FLOAT | CI lower bound as % |
| visit_ci_high_pct | (abs_ci_high * 100) / base_holdout_rate | FLOAT | CI upper bound as % |
| visit_p_value | calc from z | FLOAT | p-value (2-tailed) |
| visit_significant | significant_95 | BOOL | Significance at 95% |
| baseline_visit_rate | base_holdout_rate | FLOAT | Holdout visit rate |
| incremental_visits | incremental_visits | INT | Raw count |
| cost_per_incremental_visit | spend / incremental_visits | FLOAT | CPIV (pipeline basis) |
| conv_lift_pct | (conv_rel_itt * 100) | FLOAT | Conversion lift % |
| conv_ci_low_pct | (conv_abs_itt - 1.96*conv_se) * 100 | FLOAT | |
| conv_ci_high_pct | (conv_abs_itt + 1.96*conv_se) * 100 | FLOAT | |
| conv_p_value | calc from conv_z | FLOAT | |
| conv_significant | conv_significant_95 | BOOL | |
| baseline_conv_rate | conv_holdout / n_holdout | FLOAT | |
| incremental_conversions | conv_abs_itt * n_treatment | INT | Raw count |
| cost_per_incremental_conversion | spend / incremental_conversions | FLOAT | CPIA |
| **Campaign Attributes: Audience & Targeting** | | | |
| avg_household_score | agg from bid logs | FLOAT | Mean intent score |
| pct_high_intent | agg from bid logs | FLOAT | % impressions with HS >= 8001 |
| pct_peak_intent | agg from bid logs | FLOAT | % impressions with HS 6666-8000 |
| pct_mid_intent | agg from bid logs | FLOAT | % impressions with HS 3333-6665 |
| pct_max_reach_intent | agg from bid logs | FLOAT | % impressions with HS 1-3332 |
| pct_unscored | agg from bid logs | FLOAT | % impressions with HS=-1 or NULL |
| avg_hhst | (derived or from model) | FLOAT | Avg household score threshold |
| has_audience | campaign_groups.has_audience | BOOL | Uses audience targeting |
| frequency_cap_impressions | campaign_groups.frequency_cap_impressions | INT | Fcap setting |
| frequency_cap_duration | campaign_groups.frequency_cap_duration | STR | Fcap window |
| **Campaign Attributes: Media Mix** | | | |
| pct_ctv | agg from bid logs (device filter) | FLOAT | % CTV |
| pct_display | agg from bid logs (device filter) | FLOAT | % Display (desktop) |
| pct_mobile | agg from bid logs (device filter) | FLOAT | % Mobile |
| **Campaign Attributes: Geography** | | | |
| is_national | logic on geo | BOOL | National vs regional |
| **Campaign Attributes: Spend & Volume** | | | |
| spend_usd | agg from cost_impression_log.cpi * impressions | FLOAT | Total spend (30d window) |
| impressions | count from cost_impression_log | INT | Total impressions |
| unique_households | n_treatment + n_holdout | INT | Treatment + holdout IPs |
| **Advertiser Attributes** | | | |
| advertiser_muvs | advertisers.monthly_muv | INT | Monthly unique visitors |
| company_size | advertisers.company_size | STR | Company size category |
| **Derived / TBD** | | | |
| crm_exclusion | ??? | BOOL | CRM exclusion applied |
| display_mt_enabled | ??? | BOOL | Display multi-touch enabled |
| stage_2_pct_spend | ??? | FLOAT | % spend to S2 |
| stage_3_pct_spend | ??? | FLOAT | % spend to S3 |
| media_plan_enabled | ??? | BOOL | Media plan active |
| attribution_window | ??? | INT | Attribution window days |

---

## Known Gotchas & Workarounds

1. **Vertical lookup:** Not found in initial scan of advertisers table. Next step: search INFORMATION_SCHEMA or query ape_core for vertical dimension.

2. **Campaign attributes not in rollup:** The lift__ghost_bid_rollup table has NO campaign-level detail — only lift metrics + counts. All attributes must be joined from campaign_groups, advertisers, and bid logs.

3. **Spend / impressions window:** Lift metrics are all-time; spend/impressions from bid logs can be filtered to a 30d window, but there's a temporal mismatch. Trade-off: use all-time spend for all-time lift OR extract a 30d subset of lift (would require silver enriched.lift__ghost_bid_visits, a different table).

4. **Intent scores:** NULL before 2025-06-01 in CIL; recoverable from model_params back to 2025-05-06. For campaigns launched before that, scores will be NULL/unavailable.

5. **Device mix:** Requires aggregation from cost_impression_log by device_type; initial queries will tell if device_type is populated across all impressions.

6. **Stage mix (S2/S3):** Not evident in campaign_groups. Likely needs to join campaign → campaign_group → objective to infer, or pull from campaign-level configs.

7. **Attribution windows:** Not in campaign_groups; may be in flight or campaign config. TBD.

8. **CRM exclusion, Display MT, media_plan:** Unknown table locations. May need Jira/PM inquiry.

---

## Next Steps

1. ✅ Confirm lift table location and schema → **DONE**
2. ✅ Confirm campaign_groups location and schema → **DONE**
3. ✅ Confirm advertiser metadata location → **DONE**
4. ⚠️ **Find vertical dimension table** → Query INFORMATION_SCHEMA
5. ⚠️ **Verify bid logs have device_type**, **household_score**, etc.
6. ⚠️ **Map Stage/objective relationships** → understand S2/S3 split
7. Draft main SQL join query
8. Aggregate per-campaign-group attributes from bid logs
9. Build .xlsx with all sheets
