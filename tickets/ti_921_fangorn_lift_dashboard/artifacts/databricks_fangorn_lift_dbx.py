# Databricks notebook source
# MAGIC %md
# MAGIC # Fangorn Lift Evaluation — TI-921
# MAGIC
# MAGIC **Audience:** Alex Knorr (after handoff). Original author: Malachi.
# MAGIC **Companion files:**
# MAGIC - [`alex_handoff.md`](./alex_handoff.md) — full step-by-step handoff doc (start there if you've never run this)
# MAGIC - [`wave_config.csv`](./wave_config.csv) — manual source of truth for which advertisers flipped to Fangorn and when
# MAGIC - [`mode_dashboard_plan.md`](./mode_dashboard_plan.md) — the Mode dashboard design (separate, follow-on work)
# MAGIC
# MAGIC ## What this notebook does
# MAGIC
# MAGIC For every advertiser flipped to Fangorn (DS46), it computes two views of KPI movement:
# MAGIC
# MAGIC 1. **CausalImpact synthetic control per (AID, metric)** — *the headline lift claim.*
# MAGIC    What WOULD this advertiser's IVR/CVR/ROAS have been *without* the flip? Uses non-Fangorn
# MAGIC    advertisers as a synthetic control plus holiday/lag/spend covariates. Produces relative
# MAGIC    effect, 95% credible interval, p-value.
# MAGIC 2. **Pre/post per AID** — *the naive comparison.*
# MAGIC    Simple before-vs-after on each metric. Reported alongside CausalImpact so stakeholders
# MAGIC    can see how much the synthetic control changes the answer.
# MAGIC
# MAGIC The **gap between the two** is itself informative. If pre/post says +25% IVR but CausalImpact
# MAGIC says +8%, most of the apparent lift was platform tailwind. If they agree, the lift is real.
# MAGIC
# MAGIC ## How to run this
# MAGIC
# MAGIC - **On Databricks (canonical path):** import the repo as a Repo, open this notebook, attach
# MAGIC   to an ML runtime cluster, click *Run all*. CausalImpact requires pandas <2.1 which Databricks
# MAGIC   ML runtimes ship pinned.
# MAGIC - **On a laptop:** the pre/post path works on any pandas version. The CausalImpact path needs
# MAGIC   `pip install 'pandas<2.0'` in a venv. See `alex_handoff.md` §A3.
# MAGIC
# MAGIC The Section 5 cells below will fail locally on pandas 3.x. Skip them, or use a pinned venv.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup — imports, paths, BigQuery client
# MAGIC
# MAGIC Standard imports plus two compatibility shims for the published `causalimpact` 0.1.1
# MAGIC package, which calls APIs that pandas removed in 2.1+. The shims let the library find
# MAGIC `DataFrame.applymap` and patch its positional Series indexing.
# MAGIC
# MAGIC We also resolve paths relative to the notebook so it runs identically locally and on
# MAGIC Databricks (under Repos).

# COMMAND ----------

# MAGIC %pip install google-cloud-bigquery db-dtypes causalimpact
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os, sys, warnings
from itertools import combinations
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import statsmodels.api as sm
from statsmodels.stats.outliers_influence import variance_inflation_factor

# ---- pandas 2.x / 3.x compatibility shims for `causalimpact` 0.1.1 ----
if not hasattr(pd.DataFrame, "applymap"):
    pd.DataFrame.applymap = lambda self, fn, *a, **kw: self.map(fn, *a, **kw)

from google.cloud import bigquery

warnings.filterwarnings("ignore")

# Resolve paths so this runs identically on Databricks and locally
NOTEBOOK_DIR = Path.cwd()
# When running interactively in Databricks the cwd is the notebook dir; locally
# we may be running from anywhere — resolve to the artifacts/ folder of TI-921.
if not (NOTEBOOK_DIR / "wave_config.csv").exists():
    candidate = Path("/Users/malachi/Developer/work/mntn/workspace/tickets/ti_921_fangorn_lift_dashboard/artifacts")
    if candidate.exists():
        NOTEBOOK_DIR = candidate
TICKET_DIR = NOTEBOOK_DIR.parent
WAVE_CONFIG_CSV = NOTEBOOK_DIR / "wave_config.csv"
DAILY_PANEL_SQL = TICKET_DIR / "queries" / "ti_921_daily_panel.sql"
OUTPUT_DIR = TICKET_DIR / "outputs"
OUTPUT_DIR.mkdir(exist_ok=True)

BQ_PROJECT = "dw-main-bronze"

print(f"Notebook dir: {NOTEBOOK_DIR}")
print(f"Wave config:  {WAVE_CONFIG_CSV}")
print(f"Output dir:   {OUTPUT_DIR}")

# COMMAND ----------

NOTEBOOK_DIR = Path("/Workspace/Users/malachi@mountain.com/malachi-workspace/tickets/ti_921_fangorn_lift_dashboard/artifacts")
TICKET_DIR = NOTEBOOK_DIR.parent
WAVE_CONFIG_CSV = NOTEBOOK_DIR / "wave_config.csv"
DAILY_PANEL_SQL = TICKET_DIR / "queries" / "ti_921_daily_panel.sql"
OUTPUT_DIR = TICKET_DIR / "outputs"
OUTPUT_DIR.mkdir(exist_ok=True)
print(f"NOTEBOOK_DIR: {NOTEBOOK_DIR}")
print(f"wave_config exists: {WAVE_CONFIG_CSV.exists()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load `wave_config.csv` — which advertisers flipped, when, and to which cohort
# MAGIC
# MAGIC `wave_config.csv` is the manually-maintained source of truth. Every time a new cohort flips
# MAGIC to Fangorn, we append rows here (see `alex_handoff.md` §B1). The notebook reads this CSV at
# MAGIC runtime; the SQL queries get the AID list injected from it via the cell below.
# MAGIC
# MAGIC Columns:
# MAGIC - `advertiser_id` — MNTN AID
# MAGIC - `advertiser_name` — `advertisers.company_name` (current display name)
# MAGIC - `flip_date` — the day Fangorn-targeted bidding started for this AID, in PT
# MAGIC - `cohort` — label like `Tier1-Wave1`, `Tier1-Wave2` for grouping
# MAGIC - `vertical` — for filtering and for understanding lead-gen vs e-commerce expectations
# MAGIC - `has_conversion_pixel` — auto-detected; if false, CVR/CPA are not meaningful
# MAGIC - `has_dollar_value` — auto-detected; if false, ROAS/AOV are not meaningful
# MAGIC - `notes` — free text

# COMMAND ----------

wave = pd.read_csv(WAVE_CONFIG_CSV, parse_dates=["flip_date"])
wave["advertiser_id"] = wave["advertiser_id"].astype(int)
print(f"{len(wave)} treated AIDs across {wave['cohort'].nunique()} cohort(s)")
print()
print(wave.groupby("cohort").size().to_frame("n_advertisers"))
print()
wave[["advertiser_id", "advertiser_name", "flip_date", "cohort", "vertical",
      "has_conversion_pixel", "has_dollar_value"]].head(20)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Pull the daily KPI panel from BigQuery
# MAGIC
# MAGIC This is the analytical foundation. One row per `(advertiser_id, day)` for *every* active
# MAGIC prospecting advertiser — both the Fangorn-flipped ones and all others. The non-flipped
# MAGIC advertisers form the **synthetic control pool** used by CausalImpact in Section 5.
# MAGIC
# MAGIC ### What's in the panel
# MAGIC
# MAGIC - Identifying: `advertiser_id`, `company_name`, `vertical_id`, `vertical_name`, `cohort`, `flip_date`
# MAGIC - Treatment flags: `is_treated` (per-day — TRUE only for treated AIDs *after* their flip),
# MAGIC   `aid_in_treatment_group` (TRUE for any AID ever flipped — used to exclude from control pool)
# MAGIC - Time alignment: `day`, `days_since_flip` (negative = pre, positive = post)
# MAGIC - KPIs: `impressions`, `uniques`, `vv`, `conversions`, `order_value`, `spend`, `vast_start`, `vast_complete`, `active_cgs`
# MAGIC
# MAGIC ### The full SQL (also lives at `queries/ti_921_daily_panel.sql`)
# MAGIC
# MAGIC ```sql
# MAGIC /* ========================================================================
# MAGIC    TI-921 — Daily KPI panel (wave-aware)
# MAGIC
# MAGIC    One row per (advertiser_id, day) for ALL active prospecting advertisers
# MAGIC    in the window. Used by:
# MAGIC      - Mode dashboard trend charts (filtered to treated AIDs by `is_treated`)
# MAGIC      - CausalImpact pipeline (treated rows + non-treated as platform pool)
# MAGIC      - Days-since-flip alignment for cross-cohort comparison
# MAGIC
# MAGIC    Differences vs TI-849 ti_849_method3_covariate_pull.sql:
# MAGIC      - is_treated derived from wave_config flip_date (not just current state),
# MAGIC        so pre-flip rows for a treated AID are still labeled is_treated = FALSE
# MAGIC        on those days (correct for synthetic-control covariate building).
# MAGIC      - days_since_flip column added (negative = pre, 0 excluded, positive = post)
# MAGIC      - cohort label propagated for cross-cohort grouping
# MAGIC
# MAGIC    Source tables (all fresh through current day):
# MAGIC      silver.summarydata.{impression,visit,conversion,spend}_facts
# MAGIC    ======================================================================== */
# MAGIC
# MAGIC DECLARE window_start DATE DEFAULT DATE '2026-01-01';   -- pre-period headroom; CausalImpact wants ≥30 pre-days, more is better
# MAGIC DECLARE window_end   DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
# MAGIC
# MAGIC WITH wave_config AS (
# MAGIC   -- KEEP IN SYNC WITH artifacts/wave_config.csv
# MAGIC   SELECT 32320 AS advertiser_id, DATE '2026-05-01' AS flip_date, 'Tier1-Wave1' AS cohort UNION ALL
# MAGIC   SELECT 38659,                  DATE '2026-05-01',                'Tier1-Wave1' UNION ALL
# MAGIC   SELECT 32233,                  DATE '2026-05-01',                'Tier1-Wave1' UNION ALL
# MAGIC   SELECT 46538,                  DATE '2026-05-05',                'Tier1-Wave2'
# MAGIC ),
# MAGIC
# MAGIC prospecting_campaigns AS (
# MAGIC   SELECT campaign_id, campaign_group_id, advertiser_id
# MAGIC   FROM `dw-main-bronze.integrationprod.campaigns`
# MAGIC   WHERE deleted = FALSE AND is_test = FALSE
# MAGIC     AND funnel_level = 1
# MAGIC ),
# MAGIC
# MAGIC imp AS (
# MAGIC   SELECT
# MAGIC     pc.advertiser_id, DATE(i.hour) AS day,
# MAGIC     SUM(i.display_impressions + i.ctv_impressions) AS impressions,
# MAGIC     HLL_COUNT.MERGE(i.uniques) AS uniques,
# MAGIC     COUNT(DISTINCT pc.campaign_group_id) AS active_cgs,
# MAGIC     SUM(i.vast_start) AS vast_start,
# MAGIC     SUM(i.vast_complete) AS vast_complete
# MAGIC   FROM `dw-main-silver.summarydata.impression_facts` i
# MAGIC   JOIN prospecting_campaigns pc USING (campaign_id, advertiser_id)
# MAGIC   WHERE DATE(i.hour) BETWEEN window_start AND window_end
# MAGIC   GROUP BY pc.advertiser_id, day
# MAGIC ),
# MAGIC
# MAGIC vis AS (
# MAGIC   SELECT
# MAGIC     pc.advertiser_id, DATE(v.hour) AS day,
# MAGIC     SUM(v.clicks + v.views + COALESCE(v.competing_views, 0)) AS vv
# MAGIC   FROM `dw-main-silver.summarydata.visit_facts` v
# MAGIC   JOIN prospecting_campaigns pc USING (campaign_id, advertiser_id)
# MAGIC   WHERE DATE(v.hour) BETWEEN window_start AND window_end
# MAGIC   GROUP BY pc.advertiser_id, day
# MAGIC ),
# MAGIC
# MAGIC con AS (
# MAGIC   SELECT
# MAGIC     pc.advertiser_id, DATE(c.hour) AS day,
# MAGIC     SUM(c.click_conversions + c.view_conversions + COALESCE(c.competing_view_conversions, 0)) AS conversions,
# MAGIC     SUM(c.click_order_value + c.view_order_value + COALESCE(c.competing_view_order_value, 0)) AS order_value
# MAGIC   FROM `dw-main-silver.summarydata.conversion_facts` c
# MAGIC   JOIN prospecting_campaigns pc USING (campaign_id, advertiser_id)
# MAGIC   WHERE DATE(c.hour) BETWEEN window_start AND window_end
# MAGIC   GROUP BY pc.advertiser_id, day
# MAGIC ),
# MAGIC
# MAGIC sp AS (
# MAGIC   SELECT
# MAGIC     pc.advertiser_id, DATE(s.hour) AS day,
# MAGIC     SUM(s.media_spend + s.data_spend + s.platform_spend) AS spend
# MAGIC   FROM `dw-main-silver.summarydata.spend_facts` s
# MAGIC   JOIN prospecting_campaigns pc USING (campaign_id, advertiser_id)
# MAGIC   WHERE DATE(s.hour) BETWEEN window_start AND window_end
# MAGIC   GROUP BY pc.advertiser_id, day
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   imp.advertiser_id,
# MAGIC   a.company_name,
# MAGIC   v.vertical_id,
# MAGIC   v.vertical_name,
# MAGIC   wc.flip_date,
# MAGIC   wc.cohort,
# MAGIC   -- is_treated is per-(advertiser, day): a treated AID's pre-flip days are FALSE.
# MAGIC   -- This is the correct flag for synthetic-control covariate construction.
# MAGIC   CASE
# MAGIC     WHEN wc.flip_date IS NULL THEN FALSE
# MAGIC     WHEN imp.day > wc.flip_date THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC   END AS is_treated,
# MAGIC   -- Always-treated flag (regardless of day) — used to scope the "pool" for
# MAGIC   -- platform-covariate aggregation (exclude any AID that's ever treated).
# MAGIC   (wc.flip_date IS NOT NULL) AS aid_in_treatment_group,
# MAGIC   imp.day,
# MAGIC   -- days_since_flip: negative for pre, 0 = flip day (excluded), positive for post
# MAGIC   CASE
# MAGIC     WHEN wc.flip_date IS NULL THEN NULL
# MAGIC     ELSE DATE_DIFF(imp.day, wc.flip_date, DAY)
# MAGIC   END AS days_since_flip,
# MAGIC   imp.impressions,
# MAGIC   imp.uniques,
# MAGIC   imp.active_cgs,
# MAGIC   imp.vast_start,
# MAGIC   imp.vast_complete,
# MAGIC   COALESCE(vis.vv, 0)            AS vv,
# MAGIC   COALESCE(con.conversions, 0)   AS conversions,
# MAGIC   COALESCE(con.order_value, 0)   AS order_value,
# MAGIC   COALESCE(sp.spend, 0)          AS spend
# MAGIC FROM imp
# MAGIC LEFT JOIN vis ON imp.advertiser_id = vis.advertiser_id AND imp.day = vis.day
# MAGIC LEFT JOIN con ON imp.advertiser_id = con.advertiser_id AND imp.day = con.day
# MAGIC LEFT JOIN sp  ON imp.advertiser_id = sp.advertiser_id  AND imp.day = sp.day
# MAGIC JOIN `dw-main-bronze.integrationprod.advertisers` a
# MAGIC   ON imp.advertiser_id = a.advertiser_id
# MAGIC   AND a.deleted = FALSE AND a.is_test = FALSE
# MAGIC LEFT JOIN `dw-main-silver.fpa.advertiser_verticals` v
# MAGIC   ON imp.advertiser_id = v.advertiser_id AND v.type = 1
# MAGIC LEFT JOIN wave_config wc ON imp.advertiser_id = wc.advertiser_id
# MAGIC WHERE imp.impressions > 0
# MAGIC   AND (imp.day != wc.flip_date OR wc.flip_date IS NULL)   -- exclude flip day per TI-221 convention
# MAGIC ORDER BY aid_in_treatment_group DESC, imp.advertiser_id, imp.day;
# MAGIC
# MAGIC ```
# MAGIC
# MAGIC Filters used: `funnel_level = 1` (prospecting only), `deleted = FALSE AND is_test = FALSE`.
# MAGIC Source tables: `silver.summarydata.{impression,visit,conversion,spend}_facts` (all fresh
# MAGIC through current day; the `sum_by_*_by_day` rollups are stale at 2026-04-14 — see knowledge/data_catalog.md).
# MAGIC
# MAGIC ### Wave-config injection
# MAGIC
# MAGIC The SQL has a `WITH wave_config AS (...)` block with a hardcoded AID list. The cell below
# MAGIC replaces that block at runtime with values from `wave_config.csv`, so we maintain the AID
# MAGIC list in only one place.

# COMMAND ----------

import re

def _build_wave_config_cte_body(wave_df):
    rows = [
        f"  SELECT {int(r['advertiser_id'])} AS advertiser_id, "
        f"DATE '{r['flip_date'].strftime('%Y-%m-%d')}' AS flip_date, "
        f"'{r['cohort']}' AS cohort"
        for _, r in wave_df.iterrows()
    ]
    return "wave_config AS (\n" + "\n  UNION ALL\n".join(rows) + "\n)"

sql = DAILY_PANEL_SQL.read_text()
new_body = _build_wave_config_cte_body(wave)
sql_runtime, n = re.subn(r"wave_config AS \([\s\S]*?\)", new_body, sql, count=1)
assert n == 1, "wave_config CTE not found in SQL"

print("Running daily-panel query against BigQuery...")
client = bigquery.Client(project=BQ_PROJECT)
panel = client.query(sql_runtime).to_dataframe()
panel["day"] = pd.to_datetime(panel["day"])
panel["flip_date"] = pd.to_datetime(panel["flip_date"])
for col in ["impressions", "uniques", "active_cgs", "vv", "conversions",
            "order_value", "spend", "vast_start", "vast_complete", "days_since_flip"]:
    panel[col] = pd.to_numeric(panel[col], errors="coerce")

print(
    f"Panel: {len(panel):,} rows | "
    f"treated AIDs: {panel[panel['aid_in_treatment_group']]['advertiser_id'].nunique()} | "
    f"non-treated (control pool): {panel[~panel['aid_in_treatment_group']]['advertiser_id'].nunique()} | "
    f"window: {panel['day'].min().date()} → {panel['day'].max().date()}"
)
panel.to_csv(OUTPUT_DIR / "ti_921_panel.csv", index=False)
panel.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Method 1 — Pre/post per advertiser (the *naive comparison*)
# MAGIC
# MAGIC For each treated AID, sum each KPI over the 30 days before its flip date and over all the days
# MAGIC after. Compute rates (IVR, CVR, ROAS, etc.) for each period. Report Δ%.
# MAGIC
# MAGIC This is *not* a lift claim. Spend, seasonality, day-of-week effects, holidays — anything that
# MAGIC moved at the same time as the flip — gets attributed to Fangorn here. We ship it because:
# MAGIC
# MAGIC 1. It's what stakeholders would compute themselves if they did the math.
# MAGIC 2. It's a backstop for AIDs/metrics where CausalImpact can't fit (e.g., constant-zero series).
# MAGIC 3. The *gap* between pre/post and the CI counterfactual is itself a valuable signal.
# MAGIC
# MAGIC ### Period definitions per AID
# MAGIC
# MAGIC - **Pre** = `flip_date − 31 → flip_date − 1` (30 days)
# MAGIC - **Post** = `flip_date + 1 → CURRENT_DATE − 1` (grows daily; flip day excluded per TI-221 convention)
# MAGIC
# MAGIC ### Sanity floor
# MAGIC
# MAGIC We don't compute rate metrics on days with fewer than 1,000 impressions — VV attribution lag
# MAGIC after a campaign pause produces e.g. 7 impressions + 2,564 VVs, which makes IVR meaningless.

# COMMAND ----------

def compute_pre_post(panel, wave_df):
    rows = []
    for _, w in wave_df.iterrows():
        aid = int(w["advertiser_id"])
        flip = w["flip_date"]
        pre_start = flip - pd.Timedelta(days=31)
        pre_end = flip - pd.Timedelta(days=1)
        post_start = flip + pd.Timedelta(days=1)
        post_end = panel["day"].max()

        adv = panel[panel["advertiser_id"] == aid]
        for label, lo, hi in [("pre", pre_start, pre_end), ("post", post_start, post_end)]:
            slice_ = adv[(adv["day"] >= lo) & (adv["day"] <= hi)]
            agg = slice_[["impressions", "uniques", "vv", "conversions",
                          "order_value", "spend"]].sum()
            rows.append({
                "advertiser_id": aid,
                "advertiser_name": w["advertiser_name"],
                "cohort": w["cohort"],
                "flip_date": flip,
                "period": label,
                "period_days": (hi - lo).days + 1,
                **agg.to_dict(),
            })

    df = pd.DataFrame(rows)
    df["ivr"]  = df["vv"] / df["impressions"].replace(0, np.nan)
    df["vvr"]  = df["vv"] / df["uniques"].replace(0, np.nan)
    df["cvr"]  = df["conversions"] / df["vv"].replace(0, np.nan)
    df["roas"] = df["order_value"] / df["spend"].replace(0, np.nan)
    df["cpv"]  = df["spend"] / df["vv"].replace(0, np.nan)
    df["cpa"]  = df["spend"] / df["conversions"].replace(0, np.nan)
    df["aov"]  = df["order_value"] / df["conversions"].replace(0, np.nan)

    out = []
    for aid, sub in df.groupby("advertiser_id"):
        pre_row = sub[sub["period"] == "pre"].iloc[0]
        post_row = sub[sub["period"] == "post"].iloc[0]
        rec = {
            "advertiser_id": aid,
            "advertiser_name": pre_row["advertiser_name"],
            "cohort": pre_row["cohort"],
            "flip_date": pre_row["flip_date"],
            "pre_days": int(pre_row["period_days"]),
            "post_days": int(post_row["period_days"]),
        }
        for m in ["impressions", "vv", "conversions", "spend", "order_value",
                  "ivr", "vvr", "cvr", "roas", "cpv", "cpa", "aov"]:
            pre_val = pre_row[m]
            post_val = post_row[m]
            rec[f"{m}_pre"] = pre_val
            rec[f"{m}_post"] = post_val
            rec[f"{m}_pct_change"] = (
                (post_val - pre_val) / pre_val if pre_val and pre_val != 0 else np.nan
            )
        out.append(rec)
    return pd.DataFrame(out).sort_values(["cohort", "advertiser_id"])

pre_post = compute_pre_post(panel, wave)
pre_post.to_csv(OUTPUT_DIR / "ti_921_pre_post.csv", index=False)
print(f"Wrote {OUTPUT_DIR / 'ti_921_pre_post.csv'} ({len(pre_post)} rows)")

# Show only AIDs with post-period data
active = pre_post[pre_post["post_days"] > 0]
display_cols = ["advertiser_name", "cohort", "post_days",
                "ivr_pre", "ivr_post", "ivr_pct_change",
                "cvr_pre", "cvr_post", "cvr_pct_change",
                "roas_pre", "roas_post", "roas_pct_change"]
active[display_cols].round({c: 4 for c in display_cols if 'pre' in c or 'post' in c or 'change' in c})

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Method 2 — CausalImpact synthetic control (the *headline lift claim*)
# MAGIC
# MAGIC For each treated AID and each metric, fit a Bayesian structural time-series model on the
# MAGIC pre-period using non-Fangorn advertisers' KPIs as covariates. Predict what the post-period
# MAGIC *would* have been without the flip. Compare to actual.
# MAGIC
# MAGIC ### The procedure per (AID, metric)
# MAGIC
# MAGIC 1. **Build the per-AID feature frame.** Daily metric value, lag-1 + lag-2, spend change, and
# MAGIC    platform-aggregate covariates (the non-treated AIDs' summed KPIs scaled for stability).
# MAGIC 2. **VIF drop.** Iteratively remove the highest-multicollinearity covariate until all VIFs < 10.
# MAGIC 3. **BIC subset search.** Among the survivors, pick the size-≤5 subset with lowest BIC on the
# MAGIC    pre-period — this avoids overfitting.
# MAGIC 4. **Fit CausalImpact.** Bayesian local-linear-trend + regression model. Produces a per-day
# MAGIC    counterfactual prediction, a relative effect, a 95% credible interval on cumulative effect,
# MAGIC    and a p-value.
# MAGIC 5. **Save plot + summary.**
# MAGIC
# MAGIC ### Covariates considered
# MAGIC
# MAGIC `platform_ivr`, `platform_cvr`, `platform_vcr`, `platform_roas`, `platform_cpa`,
# MAGIC `platform_impressions`, `platform_spend`, `platform_active_advertisers`, `platform_avg_cgs`,
# MAGIC `holiday`, `metric_lag1`, `metric_lag2`, `spend_change_pct`, `adv_active_cgs`.
# MAGIC
# MAGIC VIF + BIC pick the winning subset per (AID, metric). Most fits use 2-4 covariates.
# MAGIC
# MAGIC ### Why this is the headline
# MAGIC
# MAGIC It controls for everything pre/post can't: secular platform trends, weekend/weekday cycles,
# MAGIC holidays, spend swings, supply shifts. If `platform_ivr` rose 10% during the post-period for
# MAGIC non-Fangorn advertisers too, that 10% gets *subtracted* from the apparent lift — leaving only
# MAGIC the part attributable to the flip.
# MAGIC
# MAGIC ### Caveats
# MAGIC
# MAGIC - **Requires pandas <2.1** (the published `causalimpact` 0.1.1 has API breakages on newer pandas).
# MAGIC   On Databricks ML runtimes (≤14.x) this works out of the box. On a fresh laptop you'll see
# MAGIC   errors like `KeyError: 0` or `cannot concatenate object of type ndarray` — use a pinned venv.
# MAGIC - **Wide credible intervals when post is short.** With 3-7 days post, most p-values won't clear
# MAGIC   0.10. That's expected and honest. Treat early reads as directional; the proper readout is at
# MAGIC   4 weeks post (TI-780 maturity rule).
# MAGIC - **Some metrics can't fit.** "Input response cannot be constant" → metric is all zeros (e.g.,
# MAGIC   Big Blue Bubble's CVR; no pixel firing). The script catches the per-fit exception and
# MAGIC   continues to the next; pre/post still reports for that row.

# COMMAND ----------

# Try to import. If pandas is too new, this fails — skip Section 5 and rely on pre/post.
CI_AVAILABLE = False
try:
    from causalimpact import CausalImpact
    import causalimpact.main as _ci_main

    def _standardize_pre_post_data_patched(self):
        from causalimpact.misc import standardize
        self.normed_pre_data, (mu, sig) = standardize(self.pre_data)
        self.normed_post_data = (self.post_data - mu) / sig
        self.mu_sig = (mu.iloc[0], sig.iloc[0])  # was: (mu[0], sig[0])
    _ci_main.CausalImpact._standardize_pre_post_data = _standardize_pre_post_data_patched
    CI_AVAILABLE = True
    print("CausalImpact available — Section 5 will fit models.")
except Exception as e:
    print(f"CausalImpact NOT available: {type(e).__name__}: {e}")
    print("Section 5 cells will be skipped. Run on Databricks for CI fits.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5a. Build platform covariates
# MAGIC
# MAGIC The non-treated advertisers' daily KPIs are summed/averaged into a "platform" baseline that
# MAGIC captures secular trends. We exclude any AID that's *ever* in the treatment group from the
# MAGIC control pool so the platform isn't contaminated by treated AIDs' pre-flip data.

# COMMAND ----------

MIN_DAILY_IMPRESSIONS = 1000
HOLIDAY_DATES = pd.to_datetime([
    "2025-11-27", "2025-11-28", "2025-12-24", "2025-12-25", "2025-12-26",
    "2025-12-31", "2026-01-01", "2026-02-14", "2026-04-20",
])

def build_platform_covariates(panel):
    base = panel[~panel["aid_in_treatment_group"]].copy()
    base = base[base["impressions"] >= MIN_DAILY_IMPRESSIONS]
    plat = base.groupby("day").agg(
        platform_impressions=("impressions", "sum"),
        platform_uniques=("uniques", "sum"),
        platform_vv=("vv", "sum"),
        platform_conversions=("conversions", "sum"),
        platform_order_value=("order_value", "sum"),
        platform_spend=("spend", "sum"),
        platform_vast_start=("vast_start", "sum"),
        platform_vast_complete=("vast_complete", "sum"),
        platform_active_advertisers=("advertiser_id", "nunique"),
        platform_avg_cgs=("active_cgs", "mean"),
    ).reset_index()
    plat["platform_ivr"]  = plat["platform_vv"]          / plat["platform_impressions"].replace(0, np.nan)
    plat["platform_cvr"]  = plat["platform_conversions"] / plat["platform_vv"].replace(0, np.nan)
    plat["platform_vcr"]  = plat["platform_vast_complete"] / plat["platform_vast_start"].replace(0, np.nan)
    plat["platform_roas"] = plat["platform_order_value"] / plat["platform_spend"].replace(0, np.nan)
    plat["platform_cpa"]  = plat["platform_spend"]       / plat["platform_conversions"].replace(0, np.nan)
    plat["holiday"] = plat["day"].isin(HOLIDAY_DATES).astype(float)
    plat["platform_spend"] /= 1e6
    plat["platform_impressions"] /= 1e9
    plat["platform_active_advertisers"] /= 1000.0
    return plat

plat = build_platform_covariates(panel)
print(f"Platform covariates: {len(plat)} days, "
      f"avg {plat['platform_active_advertisers'].mean()*1000:.0f} active advertisers/day")
plat[["day", "platform_ivr", "platform_cvr", "platform_roas",
      "platform_active_advertisers", "platform_spend"]].tail(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5b. Fit CausalImpact for every (AID, metric)
# MAGIC
# MAGIC This loop is the heaviest part of the notebook — ~5 seconds per fit, so for 50 AIDs × 5 metrics
# MAGIC that's roughly 20 minutes. The script catches per-fit exceptions and continues, so a few failures
# MAGIC don't take down the run.
# MAGIC
# MAGIC **On a laptop with modern pandas, all fits will fail.** The cell below is a no-op in that case
# MAGIC (it checks `CI_AVAILABLE`). Run on Databricks for actual results.

# COMMAND ----------

MIN_PRE_DAYS = 30
MIN_POST_DAYS = 1
ALL_CANDIDATES = [
    "platform_ivr", "platform_cvr", "platform_vcr", "platform_roas", "platform_cpa",
    "platform_impressions", "platform_spend", "platform_active_advertisers",
    "platform_avg_cgs", "holiday", "metric_lag1", "metric_lag2",
    "spend_change_pct", "adv_active_cgs",
]
METRIC_DEFS = {
    "ivr":  {"direction": "higher", "label": "Impression-to-Visit Rate"},
    "cvr":  {"direction": "higher", "label": "Conversion Rate"},
    "roas": {"direction": "higher", "label": "Return on Ad Spend"},
    "cpa":  {"direction": "lower",  "label": "Cost per Acquisition"},
    "cpv":  {"direction": "lower",  "label": "Cost per Visit"},
}

def compute_metrics(df):
    out = df.copy()
    out["ivr"]  = out["vv"]          / out["impressions"].replace(0, np.nan)
    out["cvr"]  = out["conversions"] / out["vv"].replace(0, np.nan)
    out["roas"] = out["order_value"] / out["spend"].replace(0, np.nan)
    out["cpa"]  = out["spend"]       / out["conversions"].replace(0, np.nan)
    out["cpv"]  = out["spend"]       / out["vv"].replace(0, np.nan)
    out["vcr"]  = out["vast_complete"] / out["vast_start"].replace(0, np.nan)
    for m in ["ivr", "cvr", "roas", "cpa", "cpv", "vcr"]:
        vals = out[m].dropna()
        if len(vals) > 2:
            lo, hi = np.nanpercentile(vals, [1, 99])
            out[m] = out[m].clip(lower=lo, upper=hi)
    return out

def drop_high_vif(features, threshold=10.0):
    keep = list(features.columns)
    while len(keep) > 1:
        X = features[keep].fillna(0.0)
        X_const = sm.add_constant(X, has_constant="add")
        try:
            vifs = [variance_inflation_factor(X_const.values, i + 1) for i in range(len(keep))]
        except Exception:
            break
        if max(vifs) < threshold:
            break
        keep.remove(keep[vifs.index(max(vifs))])
    return keep

def best_subset_by_bic(target, features, max_size=5):
    cols = list(features.columns)
    best_bic, best_subset = np.inf, []
    y = target.dropna()
    for k in range(1, min(max_size, len(cols)) + 1):
        for subset in combinations(cols, k):
            X = sm.add_constant(features[list(subset)].loc[y.index].fillna(0.0), has_constant="add")
            try:
                bic = sm.OLS(y, X).fit().bic
            except Exception:
                continue
            if bic < best_bic:
                best_bic, best_subset = bic, list(subset)
    return best_subset

def fit_one(panel, plat, adv_id, adv_name, cohort, flip_date, metric):
    adv = panel[panel["advertiser_id"] == adv_id].copy()
    adv = compute_metrics(adv)
    adv["adv_active_cgs"] = adv["active_cgs"].astype(float)
    df = adv.merge(plat, on="day", how="inner").sort_values("day")
    df["metric_lag1"] = df[metric].shift(1)
    df["metric_lag2"] = df[metric].shift(2)
    df["spend_change_pct"] = df["spend"].pct_change().fillna(0).clip(-1, 5)
    df = df.dropna(subset=["metric_lag1", "metric_lag2"]).set_index("day").sort_index()
    pre = df[df.index < flip_date]
    post = df[df.index > flip_date]
    if len(pre) < MIN_PRE_DAYS or len(post) < MIN_POST_DAYS:
        return None
    pre_period = [pre.index[0].strftime("%Y-%m-%d"), pre.index[-1].strftime("%Y-%m-%d")]
    post_period = [post.index[0].strftime("%Y-%m-%d"), post.index[-1].strftime("%Y-%m-%d")]
    candidates = [c for c in ALL_CANDIDATES if c in df.columns]
    pre_df = df.loc[pre_period[0]:pre_period[1]]
    feats = pre_df[candidates].fillna(0.0)
    target = pre_df[metric].ffill().bfill()
    keep = drop_high_vif(feats)
    winning = best_subset_by_bic(target, feats[keep])
    if not winning:
        winning = keep[:3]
    ci_data = pd.DataFrame({"y": df[metric].ffill().bfill()})
    ci_data = ci_data.join(df[winning].fillna(0.0))
    ci = CausalImpact(ci_data, pre_period, post_period)
    s = ci.summary_data
    fig = ci.plot()
    fig.savefig(OUTPUT_DIR / f"ti_921_ci_{adv_id}_{metric}.png", dpi=150, bbox_inches="tight")
    plt.close()
    return {
        "advertiser_id": adv_id, "advertiser_name": adv_name,
        "cohort": cohort, "flip_date": flip_date, "metric": metric,
        "pre_n_days": (pd.Timestamp(pre_period[1]) - pd.Timestamp(pre_period[0])).days + 1,
        "post_n_days": (pd.Timestamp(post_period[1]) - pd.Timestamp(post_period[0])).days + 1,
        "covariates": ",".join(winning),
        "avg_actual_post":   s.loc["actual", "average"],
        "avg_predicted_post":s.loc["predicted", "average"],
        "abs_effect":        s.loc["actual","average"] - s.loc["predicted","average"],
        "rel_effect":        (s.loc["actual","average"] - s.loc["predicted","average"]) / s.loc["predicted","average"]
                              if s.loc["predicted","average"] else np.nan,
        "cum_effect_95_lower": s.loc["actual","cumulative"] - s.loc["predicted_upper","cumulative"],
        "cum_effect_95_upper": s.loc["actual","cumulative"] - s.loc["predicted_lower","cumulative"],
        "p_value": ci.p_value,
    }

ci_rows = []
if CI_AVAILABLE:
    for _, w in wave.iterrows():
        for metric in METRIC_DEFS:
            try:
                r = fit_one(panel, plat, int(w["advertiser_id"]), w["advertiser_name"],
                            w["cohort"], w["flip_date"], metric)
            except Exception as e:
                continue
            if r is not None:
                ci_rows.append(r)

ci_results = pd.DataFrame(ci_rows)
if not ci_results.empty:
    ci_results.to_csv(OUTPUT_DIR / "ti_921_ci_results.csv", index=False)
    print(f"Wrote {len(ci_results)} CI fits → {OUTPUT_DIR / 'ti_921_ci_results.csv'}")
    print(ci_results[["advertiser_name","cohort","metric","rel_effect","p_value","post_n_days"]].head(20))
else:
    print("No CI fits produced. Run on Databricks (pre/post-only readout follows).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Side-by-side: pre/post Δ% vs CausalImpact rel_effect
# MAGIC
# MAGIC This is the headline view for stakeholders. For each (AID, metric), one row showing:
# MAGIC
# MAGIC - **Pre/post Δ%** — the naive answer
# MAGIC - **CausalImpact rel_effect** — the lift claim, controlling for platform trends
# MAGIC - **Gap** — pre/post minus CI. If big, the synthetic control is correcting for a confound; the
# MAGIC   pre/post number was misleading. If small, the two methods agree.
# MAGIC - **CI p-value** — strength of the lift signal
# MAGIC
# MAGIC If CausalImpact didn't fit (CI_AVAILABLE=False or "constant response" errors), we show
# MAGIC pre/post-only. The gap column will be empty. Run on Databricks to populate.

# COMMAND ----------

def build_comparison_table(pre_post_df, ci_results_df):
    pp = pre_post_df[pre_post_df["post_days"] > 0].copy()
    rows = []
    metrics = ["ivr", "cvr", "roas", "cpa", "cpv"]
    for _, r in pp.iterrows():
        for m in metrics:
            ppc = r.get(f"{m}_pct_change")
            ci_match = ci_results_df[
                (ci_results_df["advertiser_id"] == r["advertiser_id"])
                & (ci_results_df["metric"] == m)
            ] if not ci_results_df.empty else pd.DataFrame()
            ci_rel = ci_match["rel_effect"].iloc[0] if len(ci_match) else np.nan
            ci_p   = ci_match["p_value"].iloc[0] if len(ci_match) else np.nan
            ci_n   = ci_match["post_n_days"].iloc[0] if len(ci_match) else np.nan
            gap = (ppc - ci_rel) if pd.notna(ppc) and pd.notna(ci_rel) else np.nan
            rows.append({
                "advertiser": r["advertiser_name"],
                "cohort": r["cohort"],
                "metric": m,
                "post_days": r["post_days"],
                "pre_post_pct_change": ppc,
                "ci_rel_effect": ci_rel,
                "ci_p_value": ci_p,
                "ci_n_post_days": ci_n,
                "gap_prepost_minus_ci": gap,
            })
    return pd.DataFrame(rows)

comparison = build_comparison_table(pre_post, ci_results if not ci_results.empty else pd.DataFrame())
comparison.to_csv(OUTPUT_DIR / "ti_921_comparison.csv", index=False)

# Pretty display: highlight rows where pre/post and CI disagree by >10 percentage points
def fmt_pct(v):
    return f"{v*100:+.1f}%" if pd.notna(v) else "—"

display = comparison.copy()
for col in ["pre_post_pct_change", "ci_rel_effect", "gap_prepost_minus_ci"]:
    display[col] = display[col].apply(fmt_pct)
display["ci_p_value"] = display["ci_p_value"].apply(lambda v: f"{v:.3f}" if pd.notna(v) else "—")
display

# COMMAND ----------

# DBTITLE 1,Section 6.5 — Pre/post bar charts
# MAGIC %md
# MAGIC ## 6.5 Pre/post bar charts — every KPI per advertiser
# MAGIC
# MAGIC Bar chart per metric (IVR, CVR, ROAS, CPV, CPA), one row per metric. Each metric subplot shows pre value (gray) and post value (blue) side by side per advertiser. Quick visual scan of which KPIs moved.

# COMMAND ----------

# DBTITLE 1,Pre/post bar charts per KPI
import matplotlib.pyplot as plt
import numpy as np

active_ap = pre_post[pre_post["post_days"] > 0].copy()
metric_specs = [
    ("ivr",  "IVR (visit rate)",        "rate"),
    ("cvr",  "CVR (conversion rate)",   "rate"),
    ("roas", "ROAS",                    "scalar"),
    ("cpv",  "CPV ($ per visit)",       "money"),
    ("cpa",  "CPA ($ per acquisition)", "money"),
]

fig, axes = plt.subplots(len(metric_specs), 1, figsize=(11, 3 * len(metric_specs)))
for ax, (m, label, kind) in zip(axes, metric_specs):
    df = active_ap[[f"{m}_pre", f"{m}_post", "advertiser_name", "cohort"]].dropna(subset=[f"{m}_pre", f"{m}_post"])
    df = df[(df[f"{m}_pre"] > 0) | (df[f"{m}_post"] > 0)]
    if df.empty:
        ax.text(0.5, 0.5, f"No {m.upper()} data", ha="center", va="center", transform=ax.transAxes)
        ax.set_title(label); continue
    x = np.arange(len(df))
    w = 0.4
    ax.bar(x - w/2, df[f"{m}_pre"],  w, label="Pre",  color="#999999")
    ax.bar(x + w/2, df[f"{m}_post"], w, label="Post", color="#1f77b4")
    ax.set_xticks(x)
    ax.set_xticklabels([n[:18] for n in df["advertiser_name"]], rotation=40, ha="right")
    ax.set_title(label)
    ax.legend(loc="upper right", fontsize=9)
    if kind == "rate":
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v*100:.1f}%"))
    elif kind == "money":
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"${v:,.0f}"))
    ax.grid(axis="y", alpha=0.3)

fig.tight_layout()
fig.savefig(OUTPUT_DIR / "ti_921_pre_post_charts.png", dpi=150, bbox_inches="tight")
plt.show()

# COMMAND ----------

# DBTITLE 1,Section 6.6 — Forest plot
# MAGIC %md
# MAGIC ## 6.6 CausalImpact vs pre/post — forest plot per metric
# MAGIC
# MAGIC For each metric, dot-and-error-bar chart showing CausalImpact rel_effect with 95% credible interval (blue circle + bars) vs naive pre/post pct_change (red ×) per advertiser. Where the two diverge, the synthetic control is correcting for a confound. Where they agree, the lift is robust.

# COMMAND ----------

# DBTITLE 1,CausalImpact vs pre/post forest plot
if not ci_results.empty:
    metrics_to_plot = ["ivr", "cvr", "roas", "cpv", "cpa"]
    fig, axes = plt.subplots(1, len(metrics_to_plot), figsize=(4 * len(metrics_to_plot), 6), sharey=True)

    for ax, m in zip(axes, metrics_to_plot):
        ci_m = ci_results[ci_results["metric"] == m].copy()
        pp_m = pre_post[pre_post["post_days"] > 0][["advertiser_id", "advertiser_name", f"{m}_pct_change"]]
        pp_m = pp_m.rename(columns={f"{m}_pct_change": "pp_pct"})
        merged = ci_m.merge(pp_m, on=["advertiser_id", "advertiser_name"], how="left")

        if merged.empty:
            ax.text(0.5, 0.5, f"No {m.upper()} fits", ha="center", va="center", transform=ax.transAxes)
            ax.set_title(m.upper()); continue

        merged = merged.sort_values("rel_effect")
        y = np.arange(len(merged))

        lower = merged["rel_effect"] - (merged["cum_effect_95_lower"] / merged["avg_predicted_post"].abs() / merged["post_n_days"])
        upper = (merged["cum_effect_95_upper"] / merged["avg_predicted_post"].abs() / merged["post_n_days"]) - merged["rel_effect"]
        ax.errorbar(
            merged["rel_effect"], y,
            xerr=[lower.abs().fillna(0), upper.abs().fillna(0)],
            fmt="o", color="#1f77b4", ecolor="#1f77b4", capsize=3, label="CausalImpact rel_effect ± 95% CrI"
        )
        ax.scatter(merged["pp_pct"], y, color="#d62728", marker="x", s=60, label="Pre/post Δ%")

        ax.axvline(0, color="black", linewidth=0.5)
        ax.set_yticks(y)
        ax.set_yticklabels([n[:22] for n in merged["advertiser_name"]], fontsize=8)
        ax.set_title(m.upper())
        ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v*100:+.0f}%"))
        ax.grid(axis="x", alpha=0.3)
        if ax is axes[0]:
            ax.legend(loc="upper left", fontsize=8)

    fig.suptitle("CausalImpact (●) vs naive pre/post (×) — divergence shows what the synthetic control corrects for", fontsize=11)
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "ti_921_lift_comparison.png", dpi=150, bbox_inches="tight")
    plt.show()
else:
    print("CI not run yet — run Section 5 first to populate this chart.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Before & after: full KPI table per advertiser
# MAGIC
# MAGIC A wider view of the same data — for each treated AID with post-data, the pre and post values
# MAGIC of every KPI side by side. Useful for stakeholders who want to see the absolute numbers, not
# MAGIC just rates of change.
# MAGIC
# MAGIC Columns are grouped by metric: pre value | post value | Δ%.
# MAGIC Rows that are dimmed/empty for ROAS/CPA/AOV indicate advertisers without `$`-conversion values
# MAGIC (per `wave_config.csv`'s `has_dollar_value` flag).
# MAGIC
# MAGIC Numbers are rounded for readability. Full-precision values are in `outputs/ti_921_pre_post.csv`.

# COMMAND ----------

active = pre_post[pre_post["post_days"] > 0].copy()
# Bring in pixel/dollar flags so we can show what's meaningful
active = active.merge(
    wave[["advertiser_id","has_conversion_pixel","has_dollar_value"]],
    on="advertiser_id", how="left"
)

def fmt_int(v):
    return f"{int(v):,}" if pd.notna(v) and v != 0 else "—"
def fmt_money(v):
    return f"${v:,.0f}" if pd.notna(v) and v != 0 else "—"
def fmt_rate(v):
    return f"{v*100:.2f}%" if pd.notna(v) else "—"
def fmt_pct(v):
    return f"{v*100:+.1f}%" if pd.notna(v) else "—"

table = pd.DataFrame()
table["advertiser"] = active["advertiser_name"]
table["cohort"]    = active["cohort"]
table["d+"]        = active["post_days"]

# Volume metrics
table["imp_pre"]   = active["impressions_pre"].apply(fmt_int)
table["imp_post"]  = active["impressions_post"].apply(fmt_int)
table["imp_Δ%"]    = active["impressions_pct_change"].apply(fmt_pct)

table["spend_pre"]  = active["spend_pre"].apply(fmt_money)
table["spend_post"] = active["spend_post"].apply(fmt_money)
table["spend_Δ%"]   = active["spend_pct_change"].apply(fmt_pct)

# Rate metrics
table["IVR_pre"]   = active["ivr_pre"].apply(fmt_rate)
table["IVR_post"]  = active["ivr_post"].apply(fmt_rate)
table["IVR_Δ%"]    = active["ivr_pct_change"].apply(fmt_pct)

# CVR — gate on has_conversion_pixel
def cvr_or_dash(row, col, fmt):
    return fmt(row[col]) if row.get("has_conversion_pixel") else "n/a"
table["CVR_pre"]   = active.apply(lambda r: fmt_rate(r["cvr_pre"])  if r["has_conversion_pixel"] else "n/a", axis=1)
table["CVR_post"]  = active.apply(lambda r: fmt_rate(r["cvr_post"]) if r["has_conversion_pixel"] else "n/a", axis=1)
table["CVR_Δ%"]    = active.apply(lambda r: fmt_pct(r["cvr_pct_change"]) if r["has_conversion_pixel"] else "n/a", axis=1)

# ROAS — gate on has_dollar_value
table["ROAS_pre"]  = active.apply(lambda r: f"{r['roas_pre']:.2f}"  if r["has_dollar_value"] and pd.notna(r["roas_pre"]) else "n/a", axis=1)
table["ROAS_post"] = active.apply(lambda r: f"{r['roas_post']:.2f}" if r["has_dollar_value"] and pd.notna(r["roas_post"]) else "n/a", axis=1)
table["ROAS_Δ%"]   = active.apply(lambda r: fmt_pct(r["roas_pct_change"]) if r["has_dollar_value"] else "n/a", axis=1)

# CPA — gate on has_conversion_pixel
table["CPA_pre"]   = active.apply(lambda r: f"${r['cpa_pre']:.2f}"  if r["has_conversion_pixel"] and pd.notna(r["cpa_pre"]) else "n/a", axis=1)
table["CPA_post"]  = active.apply(lambda r: f"${r['cpa_post']:.2f}" if r["has_conversion_pixel"] and pd.notna(r["cpa_post"]) else "n/a", axis=1)
table["CPA_Δ%"]    = active.apply(lambda r: fmt_pct(r["cpa_pct_change"]) if r["has_conversion_pixel"] else "n/a", axis=1)

print(f"{len(table)} treated AIDs with post-period data:")
table

# COMMAND ----------

# DBTITLE 1,Section 7.5 — Daily IVR trends
# MAGIC %md
# MAGIC ## 7.5 Daily trend per advertiser — IVR over time with flip date marker
# MAGIC
# MAGIC Small multiples — one panel per treated advertiser showing daily IVR over the panel window. The red dashed vertical line marks each advertiser’s Fangorn flip date. Useful for spotting whether the flip produced a visible step-change vs continuing a pre-existing trend.

# COMMAND ----------

# DBTITLE 1,Daily IVR trend small multiples
treated_aids = wave["advertiser_id"].tolist()
panel_treated = panel[panel["advertiser_id"].isin(treated_aids)].copy()
panel_treated["ivr_d"] = panel_treated["vv"] / panel_treated["impressions"].replace(0, np.nan)

ncols = 3
nrows = (len(treated_aids) + ncols - 1) // ncols
nrows = min(nrows, 18)
fig, axes = plt.subplots(nrows, ncols, figsize=(4 * ncols, 2.2 * nrows), sharex=False)
axes = axes.flatten() if hasattr(axes, "flatten") else [axes]

i = 0
for aid, sub in panel_treated.groupby("advertiser_id"):
    if i >= len(axes): break
    ax = axes[i]
    sub = sub.sort_values("day")
    flip = sub["flip_date"].iloc[0]
    ax.plot(sub["day"], sub["ivr_d"], color="#1f77b4", linewidth=1)
    ax.axvline(flip, color="red", linestyle="--", linewidth=1, alpha=0.7)
    name = wave[wave["advertiser_id"] == aid]["advertiser_name"].iloc[0]
    ax.set_title(f"{name[:22]} (flip {flip.strftime('%m-%d')})", fontsize=9)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v*100:.1f}%"))
    ax.tick_params(axis="x", labelsize=7, rotation=30)
    ax.tick_params(axis="y", labelsize=7)
    ax.grid(alpha=0.3)
    i += 1

for j in range(i, len(axes)):
    axes[j].axis("off")

fig.suptitle("Daily IVR per advertiser — red dashed line = Fangorn flip date", fontsize=11)
fig.tight_layout()
fig.savefig(OUTPUT_DIR / "ti_921_daily_trends.png", dpi=150, bbox_inches="tight")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. CausalImpact plots (per AID, per metric)
# MAGIC
# MAGIC If Section 5 produced fits, the plots are saved as PNGs in `outputs/`. The cell below shows them
# MAGIC inline. Each plot has three stacked panels:
# MAGIC
# MAGIC 1. **Original** — actual KPI series (solid) vs counterfactual prediction (dashed) with 95% CrI shading.
# MAGIC    Vertical line = flip date. After the flip, if the solid stays above the dashed, that's positive lift.
# MAGIC 2. **Pointwise** — per-day estimated effect (actual − counterfactual). Above zero = positive lift that day.
# MAGIC 3. **Cumulative** — running sum of pointwise effect. Up-and-to-the-right = sustained lift.
# MAGIC
# MAGIC These plots are exec-credible. Drop straight into a deck or Slack.

# COMMAND ----------

from IPython.display import Image, display, Markdown

plot_paths = sorted(OUTPUT_DIR.glob("ti_921_ci_*.png"))
if not plot_paths:
    print("No CausalImpact plots in outputs/. Run Section 5 successfully (Databricks) to generate them.")
else:
    for p in plot_paths[:20]:  # cap at 20 to keep the notebook reasonable
        # Filename pattern: ti_921_ci_<aid>_<metric>.png
        parts = p.stem.split("_")
        aid, metric = parts[3], parts[4]
        match = wave[wave["advertiser_id"] == int(aid)]
        name = match["advertiser_name"].iloc[0] if len(match) else f"AID {aid}"
        display(Markdown(f"### {name} — {metric.upper()}"))
        display(Image(filename=str(p)))

# COMMAND ----------

print(f"OUTPUT_DIR: {OUTPUT_DIR}")
print(f"OUTPUT_DIR exists: {OUTPUT_DIR.exists()}")
print(f"\nFiles in OUTPUT_DIR:")
for f in sorted(OUTPUT_DIR.iterdir()):
    print(f"  {f.name}  ({f.stat().st_size:,} bytes)")
print(f"\nCI_AVAILABLE: {CI_AVAILABLE}")
print(f"ci_results rows: {len(ci_results)}")
if len(ci_results) > 0:
    print(f"ci_results columns: {list(ci_results.columns)}")
    print(ci_results[['advertiser_name','metric','rel_effect','p_value','post_n_days']].head(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. What to share with the team
# MAGIC
# MAGIC When this notebook produces fresh output, the team-facing summary follows this template:
# MAGIC
# MAGIC > **Fangorn lift readout — [date], [cohort name] (D+[N])**
# MAGIC >
# MAGIC > CausalImpact rel_effect ± 95% CrI on the headline metrics:
# MAGIC > - [Advertiser A] IVR: **+X% (p=Y, D+[N])** — pre/post Δ% was Z% (gap = ...)
# MAGIC > - [Advertiser B] CVR: **+X% (p=Y, D+[N])** — pre/post Δ% was Z%
# MAGIC > - [Advertiser C] not yet fittable (D+1)
# MAGIC >
# MAGIC > Full table + plots in TI-921 outputs.
# MAGIC
# MAGIC ### When to escalate
# MAGIC - CI rel_effect outside ±15% with p < 0.10 → flag in `#tar-ti`
# MAGIC - Pre/post moves >20% but CI shows no effect → confound we don't understand; investigate
# MAGIC - An AID's post-period impressions dropped >50% → campaign paused; results not interpretable
# MAGIC
# MAGIC ### When to write the final readout
# MAGIC A cohort hits maturity at 4 weeks post-flip (TI-780 rule). At that point:
# MAGIC 1. Run this notebook with the cohort included
# MAGIC 2. Pick the 3 most-moved metrics (highest absolute CI rel_effect)
# MAGIC 3. Save the corresponding plots
# MAGIC 4. Write a one-pager: cohort summary, 3 plots, comparison table, two-sentence interpretation
# MAGIC 5. Archive in `tickets/ti_921_fangorn_lift_dashboard/artifacts/cohort_readouts/`
# MAGIC
# MAGIC For Wave 1 (flipped 2026-05-01): maturity ≈ 2026-05-29.
# MAGIC For Wave 2 main (flipped 2026-05-06): maturity ≈ 2026-06-03.
# MAGIC
# MAGIC ### Mode dashboard (follow-on)
# MAGIC
# MAGIC Once weekly runs of this notebook are stable, the outputs feed a Mode dashboard with three views:
# MAGIC live cohort overview, advertiser drill-down, archive of past experiments. Plan in
# MAGIC `mode_dashboard_plan.md`.
