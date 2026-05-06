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

Goal: keep producing per-advertiser CausalImpact lift estimates as new Fangorn cohorts flip, and feed a Mode dashboard once it's built.

**If you've never touched this before, jump to §A First-time setup.** It's a numbered walkthrough — do it end-to-end before anything else.

| Cadence | Procedure | Where in this doc |
|---|---|---|
| One-time | Set up your environment (clone repo, BQ auth, optional venv, smoke test) | **§A First-time setup** |
| One-time | Import the notebook into Databricks | **§B5** |
| Each Slack rollout | Discover new flips → update `wave_config.csv` → commit | **§B1** |
| Weekly | Run the notebook on Databricks → review output | **§B2** + **§B3** |
| Weekly | Post Slack/Jira summary if material moves | **§B4** |
| Per-cohort | At 4 weeks post-flip, write the final readout | **§B6** |
| (Later) | Build the Mode dashboard | [`mode_dashboard_plan.md`](mode_dashboard_plan.md) |

**Headline framing reminder:** CausalImpact is the lift claim. Pre/post is reported alongside as the naive comparison and as a backstop. See §2 for the why; §C for how to read the output.

---

## §A First-time setup (do this once, in order)

### A1. Get the repo on your laptop (skip if you already have it)
```bash
git clone git@github.com:mdunn-mntn/malachi-workspace.git
cd malachi-workspace
```
**Verify:** `ls tickets/ti_921_fangorn_lift_dashboard/` should show `summary.md`, `artifacts/`, `queries/`, `outputs/`, `meetings/`.

### A2. Auth to BigQuery
The notebook reads BQ via `google-cloud-bigquery`. Auth once:
```bash
gcloud auth application-default login
```
A browser opens. Log in with your `@mountain.com` account. Done.

**Verify:** `bq query --use_legacy_sql=false --project_id=dw-main-bronze 'SELECT 1 AS ok'` returns `ok = 1`.

### A3. (Local-only path) Install Python deps with a pinned-pandas venv
**Skip this step if you'll only run on Databricks.** On Databricks, the ML runtime ships everything pre-installed.

```bash
cd /Users/<you>/Developer/  # or wherever
python3 -m venv .ti921-venv
source .ti921-venv/bin/activate
pip install 'pandas<2.0' causalimpact google-cloud-bigquery statsmodels matplotlib numpy
```

The pandas pin matters — see §4 CI compat note. This venv is only for laptop work.

**Verify:** `python3 -c "from causalimpact import CausalImpact; print('ok')"` prints `ok`.

### A4. Smoke-test the discovery script
This script reads `wave_config.csv`, runs a 2-second BQ query, and prints any advertiser flipped to Fangorn that's not yet logged:
```bash
cd /path/to/malachi-workspace
python3 tickets/ti_921_fangorn_lift_dashboard/artifacts/discover_new_flips.py
```
**Expected today (2026-05-06):** stderr says `Known AIDs in wave_config.csv: 52` and `No new flips — wave_config.csv is up to date.` (since we already logged Wave 2's 48 + the original 4 yesterday). If you see new rows, follow §B1 to add them.

If it errors with "permission denied" or "google.auth", you either haven't run §A2 (`gcloud auth application-default login`) or don't have BQ read access on `dw-main-bronze` — ping Ryan Kleck or `#data-platform`.

### A5. Read [`alex_handoff.md`](alex_handoff.md) §1-§3 for the mental model
- §1 — what Fangorn is and what flipping it does to KPIs
- §2 — the two methods (CausalImpact headline + pre/post comparison) and why
- §3 — data flow + every BQ table we read

You don't need to memorize §3, but glance through it once so the column names aren't surprises.

---

## §B Recurring operations (do this every Slack rollout, and weekly)

### B1. After each Slack rollout announcement: discover new flips and update wave_config

Whenever Matt, Ryan, or Bryce posts something like *"rolled out the next 50 advertisers"*, the new AIDs land in BQ within ~24 hours (after the next nightly household-scoring run). Then:

**Step 1 — Run the discovery script:**
```bash
python3 tickets/ti_921_fangorn_lift_dashboard/artifacts/discover_new_flips.py
```
Reads `wave_config.csv`, runs the discovery query against BQ, prints a CSV to stdout with one row per newly-flipped AID — including a `suggested_csv_row` column that's already formatted to paste into wave_config.csv.

If you'd rather run the raw SQL in the BQ console, [`../queries/ti_921_discover_new_flips.sql`](../queries/ti_921_discover_new_flips.sql) has the same logic but with the known-AID list hardcoded inline (it gets stale; the Python script reads the CSV fresh every run).

**Step 2 — Edit `tickets/ti_921_fangorn_lift_dashboard/artifacts/wave_config.csv`:**
- Open the file.
- For each new AID returned by the query, paste the `suggested_csv_row` value as a new line at the bottom.
- Update the cohort label (replace `TierX-WaveY` with e.g. `Tier1-Wave3`).
- Override `has_conversion_pixel` / `has_dollar_value` / `notes` if the auto-detection got it wrong (check by spot-querying `silver.summarydata.conversion_facts` for a sample of the AIDs).
- The `flip_date` from the discovery query is the day `vertical_data_source` was set in BQ. **Effective Fangorn targeting starts the morning after the next household-scoring run.** If the BQ flip happened during the day, set `flip_date` to the next day. (Heuristic: if `flip_ts_utc` time is between 07:00-08:00 UTC, that's the morning after scoring → use that calendar day. If it's later in the day, use the next calendar day.)

**Step 3 — Commit and push:**
```bash
git add -f tickets/ti_921_fangorn_lift_dashboard/artifacts/wave_config.csv
git commit -m "TI-921: log new flips for [cohort name]"
git push origin main
```
Note the `-f` — `*.csv` is gitignored repo-wide; this file is the documented exception.

**Step 4 — Run the notebook (see §B2).**

### B2. Weekly: run the notebook and review

This is the headline workflow. Do it Monday mornings (or after each new wave is logged in B1).

**On Databricks (the supported path):**
1. Open your Databricks workspace.
2. If you already imported the notebook, open it. If not, see §B5 below for the one-time import.
3. Make sure the notebook is attached to a cluster with the BigQuery connector and the ML runtime (≤14.x).
4. Click **Run all**. Takes ~5-10 minutes for the panel pull + all CI fits.
5. Outputs land in `tickets/ti_921_fangorn_lift_dashboard/outputs/` in the cluster's mounted git workspace:
   - `ti_921_combined.csv` — the headline table (CI rel_effect + p-value, with pre/post side-by-side)
   - `ti_921_pre_post.csv` — pre/post only (backstop for AIDs where CI couldn't fit)
   - `ti_921_ci_results.csv` — CI fit details (covariates picked, CrI bounds)
   - `ti_921_panel.csv` — raw daily panel (large; feeds Mode)
   - `ti_921_ci_<aid>_<metric>.png` — per-(AID, metric) CausalImpact plot

**On laptop (fallback only — CI may fail):**
```bash
source ~/.ti921-venv/bin/activate    # the venv from A3
python3 tickets/ti_921_fangorn_lift_dashboard/artifacts/databricks_fangorn_lift.py
```
The pre/post path always works. CausalImpact may or may not — depends on your local pandas. If you hit errors, run on Databricks instead.

### B3. After the notebook: scan the output (see §C for what to look at)

### B4. Post a Slack/Jira summary if anything moves materially

In `#tar-ti`, post:
- Any (AID, metric) where CausalImpact `rel_effect` is ±15% or more *and* `p_value < 0.10`
- Any case where pre/post Δ% > 20% but CI says no effect (means a confound — flag for investigation)
- Anomalies (e.g., post_days = 0, no data, etc.)

Keep it short — 3-5 lines max. Example: *"Fangorn day-7 readout: CausalImpact says Big Blue Bubble IVR +18% (p=0.04), Biz2Credit IVR -3% (p=0.6, no effect), Lulus too early (D+1). Full table in TI-921 outputs."*

### B5. (One-time) Import the notebook into Databricks

You only need to do this once. Two ways:

**Way 1 — Via Databricks Repos (preferred, picks up updates automatically):**
1. In the Databricks left nav, click **Workspace** → **Repos** → your user folder → **Add Repo**.
2. Repo URL: `git@github.com:mdunn-mntn/malachi-workspace.git`. Branch: `main`.
3. Browse into `tickets/ti_921_fangorn_lift_dashboard/artifacts/` and double-click `databricks_fangorn_lift.py`. Databricks auto-detects the cell separators.
4. **Pull** the repo periodically (top-right of the Repos UI) to pick up changes from the laptop side.

**Way 2 — Manual import (snapshot — won't auto-update):**
1. In the Databricks left nav, click **Workspace** → your user folder → **Import**.
2. Choose **File** → upload `databricks_fangorn_lift.py` from your laptop.
3. Format = **Python notebook**. Databricks reads the `# COMMAND ----------` markers as cell separators.
4. The notebook appears as a regular Databricks notebook. Attach to a cluster, hit Run all.

Way 1 is preferred because all the supporting files (`wave_config.csv`, the SQL queries) live in the same repo and the notebook references them by relative path.

### B6. When a cohort reaches 4 weeks post-flip: final readout

Per the TI-780 maturity rule, a Fangorn cohort's lift is "real" at 4 weeks post-flip. For each cohort that hits maturity:

1. Run the notebook (B2) with that cohort included.
2. Open `outputs/ti_921_combined.csv` filtered to that cohort.
3. For the 3 most-moved metrics (highest absolute CI rel_effect), grab the corresponding CI plots from `outputs/ti_921_ci_*.png`.
4. Write a one-pager: cohort summary, the 3 plots, the CI-vs-pre/post comparison table, two-sentence interpretation.
5. Post in `#tar-ti`, link from Jira TI-921, archive in `tickets/ti_921_fangorn_lift_dashboard/artifacts/cohort_readouts/`.

For Wave 1 (flipped 2026-05-01), maturity hits ~2026-05-29.
For Wave 2 main (flipped 2026-05-06), maturity hits ~2026-06-03.

---

## §C Reading the output

Open `tickets/ti_921_fangorn_lift_dashboard/outputs/ti_921_combined.csv`. Scan in this order:

1. **Filter rows where `post_days > 0`.** Anything with 0 post days hasn't started yet — ignore.
2. **Look at `ci_rel_effect_ivr` and `ci_p_value_ivr` first.** This is the headline IVR lift claim per advertiser.
   - `rel_effect` between -1 and 1 (i.e., -100% to +100%). Multiply by 100 mentally for percent.
   - `p_value < 0.05` = strong signal. `0.05-0.10` = directional. `> 0.10` = inconclusive (often means insufficient post-period).
3. **Compare to `ivr_pct_change`** (the pre/post column right next to it). If they agree in direction and rough magnitude, you're confident. If they disagree by >10pp, the synthetic control is correcting for a confound — flag this as the more interesting story.
4. **Repeat for CVR, ROAS, CPA, CPV columns.** Note: muted/blank for advertisers without conversion pixels (`has_conversion_pixel = false` in `wave_config.csv`).
5. **`ci_post_n_days_*`** tells you how many days of post-data the CI fit used. Below 7 → wide CrI; trust the direction but not the magnitude.
6. **Volume context columns (`impressions_pre`, `impressions_post`, `spend_pre`, `spend_post`)** are far right. Check them if a rate-metric move looks weird — e.g., post impressions dropped 90%? Then the post-period is too short or the campaign paused.

### Reading the CausalImpact plots
Each `ti_921_ci_<aid>_<metric>.png` has three panels (top to bottom):
- **Original:** actual KPI series (solid) vs counterfactual prediction (dashed), with shaded 95% CrI. Vertical line = flip date. After the flip, if the solid is consistently above the dashed, that's the lift.
- **Pointwise:** per-day estimated effect (actual minus counterfactual), with CrI shading. Above zero = positive lift that day.
- **Cumulative:** running sum of the pointwise effect. Up-and-to-the-right = sustained positive lift.

These plots are what leadership wants to see. Copy straight into a deck.

---

## §D Worked example — Wave 1 day-3 readout (data as of 2026-05-04)

Today (2026-05-06), the Wave 1 advertisers (flipped 2026-05-01) have ~5 post days. The smoke test from yesterday produced this for the first 3 days:

| Advertiser | IVR pre→post (Δ%) | CVR pre→post (Δ%) | Interpretation |
|---|---|---|---|
| Biz2Credit | 1.06% → 0.97% (-8%) | 4.92% → 5.55% (+13%) | IVR slightly down, CVR up — Fangorn may be filtering toward higher-converting eyeballs even at slightly lower visit rate. Wait for CI at maturity. |
| Big Blue Bubble | 0.96% → 1.26% (+31%) | n/a (no pixel) | Clean IVR win on the only KPI that matters for them. CausalImpact at maturity should confirm or kill this. |
| UNW Ohio | 0.41% → 0.43% (+5%) | 1.37% → 0.00% | CVR=0 in 3 post days for a lead-gen advertiser is a flag — could be conversion-event lag, could be a pixel issue. Check Mike Dolt. |
| authenTEAK | n/a (Wave 2 vanguard, post starts today) | n/a | Will appear in next run. |

**The pattern to notice:** these are pre/post-only numbers. CausalImpact will refine each of them. Today's 31% IVR jump for Big Blue Bubble could be 31%, or could be 18% with the rest being platform tailwind — we don't know without the synthetic control. That's why CI is the headline at maturity.

**Today's run (2026-05-06):** the Wave 2 main cohort (50 AIDs) has 0 post days because they flipped at midnight last night. They'll start showing post-period numbers tomorrow. Wave 1 is now D+5; CI fits should start producing meaningful CrIs.

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

### Two methods — CausalImpact is the headline, pre/post is the comparison

You'll see both in the notebook. They answer related questions; we report both because the *gap between them* is itself informative.

| Method | What it answers | Role in the readout |
|---|---|---|
| **(1) CausalImpact synthetic control per (AID, metric)** *(headline)* | "What WOULD this advertiser's IVR have been without the flip, and how does the actual compare?" Uses non-Fangorn advertisers + holiday/lag/spend covariates as a synthetic control. Produces a relative effect, a 95% credible interval, and a p-value. | **The lift claim.** This is what we report up. Controls for spend swings, seasonality, and platform-wide trends that contaminate naive comparisons. |
| **(2) Pre/post KPIs per AID** *(comparison)* | "Did this advertiser's IVR/CVR/etc. move after we flipped them, ignoring everything else?" | **The naive baseline.** What stakeholders would see if they did the math themselves. We show it next to the CI number so the audience can see how much the synthetic control changes the answer. |

**Why this framing matters:** if pre/post says +25% IVR but CausalImpact says +8%, the gap is the story — most of what looks like Fangorn lift was actually platform-wide tailwind. Conversely, if pre/post says -5% but CausalImpact says +12%, Fangorn worked despite a headwind. The two together are more persuasive than either alone.

**Pre/post also fills gaps where CI can't fit.** When an advertiser has constant data (Big Blue Bubble's CVR is always 0 because they have no pixel), CausalImpact errors out. Pre/post still reports "0% pre, 0% post" — which is the honest answer. So pre/post doubles as a backstop.

**CausalImpact early-window caveat:** with only 3-7 days post-flip, credible intervals will be wide and most p-values won't clear 0.05. That's expected and honest. Treat early reads as directional; the proper readout is at 4 weeks post (TI-780 maturity rule), when CI's CrI tightens.

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

## 4. Reference — how the notebook is organized

*Procedural runbook is in §B. This section is for someone who wants to know what's inside the notebook before running it.*

[`databricks_fangorn_lift.py`](databricks_fangorn_lift.py) is structured as 7 cells (separated by `# COMMAND ----------`):

1. **Setup + imports** — including monkeypatch shims for pandas 2.x compat in CausalImpact.
2. **Load `wave_config.csv`** — reads the manual source-of-truth flip table.
3. **Pull daily KPI panel from BQ** — runs `queries/ti_921_daily_panel.sql` with the wave_config injected at runtime (replaces the inline CTE). Returns one row per (advertiser_id, day) for every active prospecting advertiser, with `is_treated` / `aid_in_treatment_group` flags and `days_since_flip`.
4. **Method 2 — CausalImpact per (AID, metric)** — for each flipped AID and each metric: build per-AID feature frame, drop high-VIF covariates, BIC-search for the best subset, fit CausalImpact, save plot + summary stats. The headline lift claim.
5. **Method 1 — pre/post per AID** — straightforward aggregation from the daily panel for context/comparison.
6. **Combined readout** — joins Method 1 + Method 2 outputs into `ti_921_combined.csv` with columns ordered CI-first per metric (rel_effect, p_value, post_n_days), then pre/post side-by-side.
7. **What to share** — markdown cell with the rules of thumb for posting Slack/Jira summaries.

The CausalImpact pipeline is copied from TI-849 verbatim (same VIF → BIC → CI flow, same covariate set, same scaling). The only material change vs TI-849 is per-AID flip date and runtime SQL injection.

### Alternative: standalone Python pipeline
[`../../ti_849_fangorn_score_monitoring/artifacts/ti_849_method3_causal_impact.py`](../../ti_849_fangorn_score_monitoring/artifacts/ti_849_method3_causal_impact.py) is the original TI-849 version. Runs on a laptop with `gcloud auth application-default login`. Useful for spot-checks; use the TI-921 notebook for production.

### CausalImpact compatibility note — IMPORTANT (CI is the headline; this has to work)

The published `causalimpact` 0.1.1 (PyPI) was written against pandas 1.x APIs and breaks under pandas ≥2.1 with errors like `KeyError: 0`, `applymap`, or `cannot concatenate object of type ndarray`. **On Databricks ML runtimes (≤14.x) it works out of the box** because those runtimes ship pandas 1.5 / 2.0. **Run this on Databricks** — that's the supported path.

If you need to run locally for any reason, use a pinned venv:
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install 'pandas<2.0' causalimpact google-cloud-bigquery statsmodels matplotlib numpy
```

The notebook has small monkeypatch shims for the most common breakages (`DataFrame.applymap`, positional Series indexing). Anything beyond those — go to the venv.

**If CI fails for a specific (AID, metric):** the notebook catches the exception per-fit and continues. Pre/post for that row still reports — that's what makes pre/post the backstop. Common reasons CI fails:
- "Input response cannot be constant" — the metric series is all zeros (e.g., Big Blue Bubble's CVR; no pixel firing). Expected; just rely on pre/post.
- "exog contains inf or nans" — covariate quality issue, usually for AIDs with sparse history. Skip and note.
- Insufficient pre-days — fewer than 30 daily observations before flip. Either extend the panel window in the SQL (`window_start = '2026-01-01'`) or accept the AID as not-yet-evaluable.

---

## 5. Reference — how wave-awareness works

*Procedural runbook is in §B1. This section is for someone who wants the conceptual mechanics.*

TI-849 hard-coded `pre = 2026-03-31 → 2026-04-29`, `post = 2026-05-01 → today`. That works only when all advertisers flip on the same day. For any future cohort that flips on a different date, pre/post has to be computed per-AID. Two pieces make this work:

1. **`wave_config.csv` is the source of truth for flip dates.** Maintained manually (per §B1). The notebook reads this CSV at runtime and injects the values into the daily-panel SQL (replacing the inline `WITH wave_config AS (...)` block). Schema:
   ```
   advertiser_id,advertiser_name,flip_date,cohort,vertical,has_conversion_pixel,has_dollar_value,notes
   ```

2. **Per-AID pre/post + `days_since_flip`:**
   - Pre = `flip_date − 31 → flip_date − 1` (30 days)
   - Post = `flip_date + 1 → CURRENT_DATE - 1` (grows daily; flip day excluded per TI-221 convention)
   - `days_since_flip` is on every panel row — negative for pre, positive for post. Lets the dashboard align cohorts on the x-axis even when they flipped weeks apart.

### Cross-check tool

[`../queries/ti_921_flip_date_detection.sql`](../queries/ti_921_flip_date_detection.sql) is a best-effort detector against `audience_advertiser_configurations_archive` (Datastream history) — use if the archive table is available in your environment. Most of the time the discovery query in §B1 (which uses live snapshot `source_timestamp`) is enough.

### What's already known (as of 2026-05-06 morning PT)

| AID | Advertiser | flip_date | Cohort | Notes |
|---|---|---|---|---|
| 32320 | Biz2Credit | 2026-05-01 | Tier1-Wave1 | Lead-gen |
| 38659 | Big Blue Bubble | 2026-05-01 | Tier1-Wave1 | No conversion pixel |
| 32233 | UNW Ohio | 2026-05-01 | Tier1-Wave1 | Lead-gen |
| 46538 | authenTEAK | 2026-05-05 | Tier1-Wave2 | E-commerce furniture (full KPI suite) |
| 48 advertisers | (full list in `wave_config.csv`) | 2026-05-06 | Tier1-Wave2 | Effective today after last night's household-scoring run. Includes Lulus ($2.7M/30d), Casper, NBF, US Sports Camp, etc. |

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
4. **Escalation threshold:** what triggers a Slack ping? My default: CausalImpact `rel_effect` outside ±15% on IVR or CVR with p < 0.10, OR pre/post moves more than 20% with no CI-confirmed direction (means a confound we don't understand).
5. **What to put in the archive view's frozen schema:** I have a draft in `mode_dashboard_plan.md` — would value your input on which fields are load-bearing.
6. **Tier 2 timing:** do we have any visibility on when Tier 2 starts flipping? Affects how soon the wave-aware infra has to be battle-tested.

---

## 10. If something breaks while I'm out

- **Pipeline errors:** rerun. Most failures are transient BQ slot contention.
- **CausalImpact errors on a specific (AID, metric):** the pipeline catches per-fit exceptions and continues. Other AIDs/metrics still produce. Check the log.
- **A new AID was flipped but doesn't appear in results:** check `wave_config.csv` first — almost certainly missing a row.
- **Numbers look wrong:** sanity-check against the raw daily panel CSV. If `*_facts` tables are stale, that propagates.
- **Truly stuck:** Ryan Kleck or Zach Schoenberger. The lift methodology is well-precedented (TI-748 is the canonical writeup); methodology questions can also go to the references in §8.
