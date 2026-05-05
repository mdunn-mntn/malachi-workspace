# Fangorn Lift Evaluation — Handoff to Alex Knorr

**Author:** Malachi (handing off; OOO ~2 weeks starting 2026-05-09)
**Audience:** Alex Knorr
**Companion files (all in this folder unless noted):**
- [`databricks_fangorn_lift.py`](databricks_fangorn_lift.py) — Databricks-ready notebook (run it end-to-end)
- [`wave_config.csv`](wave_config.csv) — manually-maintained per-AID flip date table (source of truth for waves)
- [`mode_dashboard_plan.md`](mode_dashboard_plan.md) — what we want the Mode dashboard to look like
- [`../queries/ti_921_pre_post_per_aid.sql`](../queries/ti_921_pre_post_per_aid.sql) — wave-aware pre/post (Method 1)
- [`../queries/ti_921_daily_panel.sql`](../queries/ti_921_daily_panel.sql) — daily KPI panel feeding both methods
- [`../queries/ti_921_flip_date_detection.sql`](../queries/ti_921_flip_date_detection.sql) — best-effort flip-date detection from CDC
- [`../../ti_849_fangorn_score_monitoring/`](../../ti_849_fangorn_score_monitoring/) — predecessor ticket; queries + CausalImpact pipeline live here

---

## 0. TL;DR — what you actually need to do

Goal: keep producing per-advertiser KPI movement readouts as new Fangorn cohorts flip, and feed a Mode dashboard once it's built.

| Step | What | Where | Cadence |
|---|---|---|---|
| 1 | Run discovery query → grab newly-flipped AIDs | [`../queries/ti_921_discover_new_flips.sql`](../queries/ti_921_discover_new_flips.sql) | Each Slack rollout announcement |
| 2 | Append discovered AIDs (with cohort label, pixel/dollar status) to `wave_config.csv` | this folder | After step 1 |
| 3 | Run the Databricks notebook | `databricks_fangorn_lift.py` | Weekly (or on demand) |
| 4 | Eyeball results | notebook outputs + `outputs/` | After each run |
| 5 | Post Slack/Jira summary if anything moves >20% | `#tar-ti` | After each run |
| 6 | Once cohort reaches 4 weeks post-flip, do final readout | one-pager | Per cohort |
| 7 | (Later) wire to Mode | `mode_dashboard_plan.md` | One-time + weekly maintenance |

Everything else in this doc is to give you the mental model so steps 1-5 make sense.

---

## 1. Fangorn primer (5 min read)

I know you sat in on the TI-832 conversation — here's the 5-minute version of "what does the model do, and what does flipping it on do to KPIs."

### The model
- Fangorn is an ML model that scores `(advertiser_id, IP)` pairs with a **probability of visiting the advertiser's site in the next 14 days** (output range 0-1).
- One model, scored daily, in the bidder. Fangorn V2 (Matt's variant) predicts conversions instead of visits — that's a different rollout, not what we're measuring here.

### How "rolling out Fangorn" works (DS13 → DS46)
- The audience targeting layer historically used **DS13** (vertical-based intent — visits and conversions in the advertiser's vertical). DS13 is rule-based.
- Fangorn rolls out as **DS46** (ML-based intent score for that advertiser).
- Per-advertiser switch: `audience_advertiser_configurations.vertical_data_source` is set to `46`. From that moment on, the Audience Service swaps DS13 → DS46 in segment-breakdown expressions at query time. The persisted base expression doesn't change (UI audience sizes don't change), but the bidder eligibility set flips.
- **Rollback** is a single column update — no audience re-ingestion needed.

### What we expect to see
A "successful" Fangorn rollout should show:
- **IVR up** (impression → visit rate) — better-targeted impressions become visits more often.
- **VVR up** (uniques → visit rate) — same direction, denominator-controlled.
- **CVR up** for advertisers with conversion pixels — more visits → more conversions.
- **CPV / CPA down** — fewer wasted impressions per visit.
- **ROAS up** for advertisers with `$`-conversion values.
- **Spend / impression volume largely unchanged** (Fangorn re-allocates within budget, doesn't increase it).

What we won't see, intentionally:
- Audience size doesn't change in the UI (DS46 swap is at query time only).
- Conversion pixels don't change.
- Last_touch advertisers behave the same as industry_standard for our purposes — all 3 launch AIDs are industry_standard, simplifying attribution math.

### The rollout schedule (as currently planned)
- **Tier 1: 369 advertisers / 44% of fleet** — staged. May 1 launch was 3 of those 369.
- **Tier 2: ~40% of fleet** — TBD.
- **Tier 3: ~16%** — excluded from initial rollout.
- Tier 1 expansion happens in waves. Each wave gets a flip date. Our infrastructure has to handle this.

---

## 2. What we measure

### Two methods, paired

You'll see both in the notebook. They answer different questions:

| Method | What it answers | Strengths | Weaknesses |
|---|---|---|---|
| **(1) Pre/post KPIs per AID** | "Did this advertiser's IVR/CVR/etc. move after we flipped them?" | Easy to read; matches TI-221/TI-270 (Jaguar) precedent that leadership trusts. | Confounded by spend swings, seasonality, calendar effects. NOT a lift claim — volume context only. |
| **(2) CausalImpact synthetic control per (AID, metric)** | "What WOULD this advertiser's IVR have been without the flip, and how does the actual compare?" | Absorbs platform-wide trends, seasonality, spend confounds. Produces 95% credible intervals + p-values. | Computationally heavier; harder to explain. Needs ≥60 days pre-period. |

The user-facing headline is method 1. Method 2 is the defensible-against-skeptics check we ship in the appendix.

### KPI suite
- **Volume:** impressions, uniques (HLL), VVs (clicks + views + competing_views), conversions, order_value, spend.
- **Rates:** IVR (vv/imp), VVR (vv/uniques), CVR (conv/vv), ROAS (rev/spend), CPV (spend/vv), CPA (spend/conv), AOV (rev/conv).
- **Filters:** `funnel_level = 1` (prospecting only — Fangorn is a prospecting-layer intervention), `deleted = FALSE AND is_test = FALSE`.

### What's reliable per-advertiser
- All 3 launch AIDs are `industry_standard` reporting (NOT last_touch) — simplifies attribution math (COALESCE-includes-competing branch).
- **Big Blue Bubble (38659):** no conversion pixel — CVR/ROAS/AOV are not meaningful for them. Only IVR/VVR matter.
- **UNW Ohio (32233) & Biz2Credit (32320):** lead-gen, no $-value per conversion → AOV/ROAS not meaningful. CVR is the right rate metric.

If you don't trust a number in the dashboard for an advertiser, check whether their pixel actually fires `$` values.

---

## 3. Architecture (where it runs, what it hits)

```
                         ┌──────────────────────────────────────────────────┐
                         │ wave_config.csv (manual, in this folder)         │
                         │  advertiser_id, advertiser_name, flip_date,      │
                         │  cohort, vertical, has_conversion_pixel, notes   │
                         └────────────────────┬─────────────────────────────┘
                                              │
                       ┌──────────────────────▼──────────────────────┐
                       │ Databricks notebook: databricks_fangorn_    │
                       │ lift.py                                      │
                       │  1. Reads wave_config.csv                    │
                       │  2. Pulls daily KPI panel from BQ            │
                       │  3. Builds per-AID pre/post (Method 1)       │
                       │  4. Builds CausalImpact (Method 2)           │
                       │  5. Writes outputs/ + charts                 │
                       └──────────────────┬───────────────────────────┘
                                          │
        ┌─────────────────────────────────┼────────────────────────────────┐
        ▼                                 ▼                                ▼
   outputs/ti_921_pre_post.csv     outputs/ti_921_ci_*.png          outputs/ti_921_panel.csv
   outputs/ti_921_ci_results.csv   (per AID, per metric)            (raw daily panel — feeds Mode)
```

### BQ tables we hit (read-only, all in `dw-main-silver` and `dw-main-bronze`)

| Table | Used for | Critical notes |
|---|---|---|
| `dw-main-bronze.integrationprod.audience_advertiser_configurations` | Detects current treated AIDs (`vertical_data_source = 46`) | Snapshot table from CDC; current state only. **`TIMESTAMP_MILLIS(datastream_metadata.source_timestamp)` gives the moment vds was last set** — reliable for flip-time detection. (Ignore the `update_time` column — frequently NULL.) |
| `dw-main-bronze.integrationprod.advertisers` | AID → company name. Filter `deleted = FALSE AND is_test = FALSE`. | `company_name` is the right column (current name). |
| `dw-main-bronze.integrationprod.campaigns` | Funnel-level filter. We use `funnel_level = 1` (prospecting). | `objective_id` is **not** authoritative for stage — use `funnel_level`. |
| `dw-main-silver.summarydata.impression_facts` | Daily impressions + uniques (HLL) | **Fresh through current day.** Use `DATE(hour)` to date-filter. |
| `dw-main-silver.summarydata.visit_facts` | Daily VVs (clicks + views + competing_views) | Same freshness as above. |
| `dw-main-silver.summarydata.conversion_facts` | Daily conversions + order_value | Same. Use `click_conversions + view_conversions + COALESCE(competing_view_conversions, 0)`. |
| `dw-main-silver.summarydata.spend_facts` | Daily spend (media + data + platform) | Same. |
| `dw-main-silver.fpa.advertiser_verticals` | AID → vertical name. Filter `type = 1`. | `advertiser_name` here is unreliable (TI-849 finding). Use `advertisers.company_name`. |

### Tables we **don't** use (and why)

| Table | Why not |
|---|---|
| `silver.summarydata.sum_by_campaign_group_by_day` | **Stale at 2026-04-14** (17+ days behind; this is a known data-platform issue inherited from TI-849). The TI-221 GP query used these rollups; we pivoted to underlying facts. If they come back fresh, simplifying off them is a future optimization. |
| `silver.aggregates.agg__daily_sum_by_campaign` | Empty since 2026-03-31. |
| `dw-main-bronze.tpa.fangorn_advertiser_inclusion` | **Source-of-truth, but lives in TPA-service Postgres, not BQ.** Has columns `advertiser_id` + `fangorn_advertiser_inclusion_date` (= the planned flip date PT). Updated when Matt/Ryan run the rollout. The downstream effect — `audience_advertiser_configurations.vertical_data_source = 46` in BQ — propagates after the nightly household-scoring run (midnight-1am PT). For our purposes the BQ flag is what matters; if you need the canonical Postgres view, ask Ryan Kleck. |

---

## 4. How to run it (three ways)

### A) The fastest answer: pre/post BQ query
For a 5-minute "did rates move?" sanity check, run [`../queries/ti_921_pre_post_per_aid.sql`](../queries/ti_921_pre_post_per_aid.sql) directly in BQ. It auto-detects flipped AIDs from `vertical_data_source = 46`, and uses a 30-day pre-period anchored to each AID's flip date (read from `wave_config.csv` if mounted, or from a `WITH wave_config AS ...` literal block at the top — open the file and adjust if you've added new waves).

### B) Databricks notebook (the primary handoff path)
[`databricks_fangorn_lift.py`](databricks_fangorn_lift.py). Open in Databricks, attach to a cluster with the BQ connector, hit run-all. Sections:
1. Setup + imports
2. Load `wave_config.csv` (small, manual)
3. Pull daily KPI panel from BQ → Spark DataFrame → pandas (panel fits in memory comfortably for any plausible cohort size)
4. Method 1: per-AID pre/post summary
5. Method 2: CausalImpact per (AID, metric)
6. Write outputs + per-metric plots

I copied the CausalImpact pipeline from TI-849 verbatim — same VIF → BIC → CI flow, same covariate set, same scaling. The only change is per-AID flip date.

### C) The standalone Python pipeline
[`../../ti_849_fangorn_score_monitoring/artifacts/ti_849_method3_causal_impact.py`](../../ti_849_fangorn_score_monitoring/artifacts/ti_849_method3_causal_impact.py) is the original. Runs on your laptop with `gcloud auth application-default login`. Use this if Databricks is unavailable for some reason.

### Auth
- **BQ from Databricks:** the workspace already has a BQ service account configured for read access — confirm with Ryan or the data-platform channel if you hit a permissions error.
- **BQ from laptop:** `gcloud auth application-default login` once, then use the `google-cloud-bigquery` Python client.

---

## 5. Wave-awareness — the only meaningful change vs TI-849

TI-849 hard-coded `pre = 2026-03-31 → 2026-04-29`, `post = 2026-05-01 → today`. That works only when all advertisers flip on the same day. For any future cohort that flips on a different date, we have to compute pre/post per-AID.

Two changes to make this work:

1. **`wave_config.csv` is the source of truth for flip dates.** Maintain it manually. When a cohort flips, append rows (one per AID) before re-running the pipeline. The notebook reads this CSV; the queries reference it. Schema:
   ```
   advertiser_id,advertiser_name,flip_date,cohort,vertical,has_conversion_pixel,notes
   32320,Biz2Credit,2026-05-01,Tier1-Wave1,Lending & Brokerage,true,
   38659,Big Blue Bubble Inc.,2026-05-01,Tier1-Wave1,Games & Comics,false,no conversion pixel
   32233,University of Northwestern Ohio,2026-05-01,Tier1-Wave1,Colleges & Universities,true,lead-gen no $ value
   ```

2. **Pre / post are computed per AID:**
   - Pre = `flip_date − 31 → flip_date − 1` (30 days)
   - Post = `flip_date + 1 → CURRENT_DATE - 1` (grows daily; flip day itself excluded per TI-221 convention)
   - `days_since_flip` is included in the daily panel so trends can be aligned across cohorts that flipped on different dates (essential for Mode).

### Discovery workflow (run after each wave)

When a new cohort flips:

1. Run [`../queries/ti_921_discover_new_flips.sql`](../queries/ti_921_discover_new_flips.sql). It returns every AID currently flipped (`vertical_data_source = 46`) that's *not* in `wave_config.csv`, with the precise UTC flip moment (`TIMESTAMP_MILLIS(datastream_metadata.source_timestamp)`) and PT date.
2. For each row returned, append a line to `wave_config.csv` with the cohort label (e.g., `Tier1-Wave3`), pixel/dollar status, and any vertical-specific notes.
3. Re-run the Databricks notebook.

There's also [`../queries/ti_921_flip_date_detection.sql`](../queries/ti_921_flip_date_detection.sql) — a best-effort detector against `audience_advertiser_configurations_archive` (Datastream history). Use it if the archive table exists in your environment; otherwise stick with the discovery query above (which uses the live snapshot's source_timestamp, no archive needed).

### What's already known (as of 2026-05-05 PM PT)

| AID | Advertiser | flip_date PT | Cohort | Notes |
|---|---|---|---|---|
| 32320 | Biz2Credit | 2026-05-01 | Tier1-Wave1 | Lead-gen |
| 38659 | Big Blue Bubble | 2026-05-01 | Tier1-Wave1 | No conversion pixel |
| 32233 | UNW Ohio | 2026-05-01 | Tier1-Wave1 | Lead-gen |
| 46538 | authenTEAK | 2026-05-05 | Tier1-Wave2 | E-commerce furniture (full KPI suite) |
| **+50 incoming** | TBD | **2026-05-06** | Tier1-Wave2 | Per Matt 2026-05-05 PM. Run discovery query tomorrow morning after household scoring completes. Ryan/Jaime API push already done; `tpa.fangorn_advertiser_inclusion` has `fangorn_advertiser_inclusion_date = 2026-05-06` for these. |

---

## 6. Mode dashboard (separate doc)

The full plan lives in [`mode_dashboard_plan.md`](mode_dashboard_plan.md). Quick version:
- **Live cohort view:** all currently-active cohorts, KPI movement vs pre-period, days-since-flip trends.
- **Advertiser drill-down:** select an AID, see its full daily series (pre + post), CausalImpact panels.
- **Archive view:** every past Fangorn experiment + cohort closes out into a frozen result row. This is the "shut it off but keep results" pattern that addresses Kale's concern about results disappearing into random notebooks.

You'll likely want to build the live + drill-down views first; archive is a slightly different schema and can wait until we've closed out wave 1.

---

## 7. Gotchas (things that will bite you if you don't know)

These are mostly inherited from TI-849 — flagged here to keep them from biting you.

1. **`sum_by_*_by_day` rollups are stale.** Use the `*_facts` tables. (Logged in `knowledge/data_catalog.md`.)
2. **AOV/ROAS are unreliable for lead-gen advertisers** (UNW Ohio, Biz2Credit) and for advertisers without conversion pixels (Big Blue Bubble). Display these but caveat clearly. The `has_conversion_pixel` column in `wave_config.csv` lets us mute them in the dashboard.
3. **`fpa.advertiser_verticals.advertiser_name` is unreliable** — write-once and stale. Always join to `advertisers.company_name` for the current display name.
4. **`objective_id` is unreliable as a stage indicator** — use `campaigns.funnel_level = 1` for prospecting.
5. **Epoch units differ across log tables.** Not relevant for the *_facts tables (they have `hour` as a TIMESTAMP), but if you go to raw logs for any reason, check `data_knowledge.md` per-table.
6. **CVR=0 doesn't mean Fangorn failed** — Big Blue Bubble has no conversion pixel and will always show CVR=0. Show pixel-having advertisers separately in any aggregate view.
7. **Spend confound is real.** If an advertiser's spend doubled the day we flipped them on, CVR almost certainly drops — they're not pacing through their best inventory. The CausalImpact spend covariate is what catches this. Don't make a lift claim on pre/post alone.
8. **The post-period grows daily.** Each run gives you a slightly longer post window. Most cohorts shouldn't be considered "mature" until 4 weeks post-flip (TI-780 maturity rule). Interpret D+1..D+7 numbers as directional, not final.
9. **Volume floor for rate metrics:** filter weeks/days with <1,000 impressions before computing rate metrics — VV attribution lag after a campaign pause produces e.g. 7 impressions + 2,564 VVs, which makes IVR explode. The notebook has this filter; preserve it if you fork the SQL.
10. **`hour` column is a TIMESTAMP in the *_facts tables** — date-filter via `DATE(hour) BETWEEN start AND end`, not via partition pseudo-columns.

---

## 8. Where to get help

| Question | Person | Channel |
|---|---|---|
| "Is this AID actually flipped to Fangorn?" | Ryan Kleck | `#dev_fangorn-model_ex` |
| "What does this audience expression do?" | Zach Schoenberger | `#chapter-data-engineering` |
| "Did the data pipeline run?" | data-platform | `#data-platform` |
| "What's the rollout schedule?" | Mike Dolt | `#tar-ti` |
| "Is the conversion pixel firing for advertiser X?" | Matt / advertiser ops | `#tar-ti` |
| "Mode dashboard access / queries" | data-eng-ai team / Harvey Yau | `#data-engineering` |
| "Anything CausalImpact-method-related" | TI-748 / TI-542 / TI-803 / TI-504 / TI-849 — pattern is canonical, see `reference_causal_impact_pattern.md` in memory | — |

---

## 9. Open questions for our meeting tomorrow

1. **Cadence:** weekly run, or trigger-on-new-cohort-flip? My default is weekly; lighter ops, plus we want trend lines anyway.
2. **Mode dashboard ownership:** you take it on, or pair with whoever owns Mode for the team?
3. **Cohort definition:** treat all 3 May-1 AIDs as a single cohort, or each AID a cohort of one? My default is single cohort per flip date — easier to compare cohorts to each other.
4. **Failure threshold:** what move triggers a Slack escalation? My default: pre→post change >20% in any direction on IVR or CVR for an advertiser with `has_conversion_pixel = true`.
5. **What to put in the archive view's frozen schema:** I have a draft in `mode_dashboard_plan.md` — would value your input on which fields are load-bearing.
6. **Tier 2 timing:** do we have any visibility on when Tier 2 starts flipping? Affects how soon the wave-aware infra has to be battle-tested.

---

## 10. If something breaks while I'm out

- **Pipeline errors:** rerun. Most failures are transient BQ slot contention.
- **CausalImpact errors on a specific (AID, metric):** the pipeline catches per-fit exceptions and continues. Other AIDs/metrics still produce. Check the log.
- **A new AID was flipped but doesn't appear in results:** check `wave_config.csv` first — almost certainly missing a row.
- **Numbers look wrong:** sanity-check against the raw daily panel CSV. If `*_facts` tables are stale, that propagates.
- **Truly stuck:** Ryan Kleck or Zach Schoenberger. The lift methodology is well-precedented (TI-748 is the canonical writeup); methodology questions can also go to the references in §8.
