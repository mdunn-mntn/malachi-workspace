# Databricks notebook source
# MAGIC %md
# MAGIC # TI-933: MNTN Select Lift Analysis (Spark port of the BQ query that timed out)
# MAGIC
# MAGIC See [`ti_933_databricks_explainer.md`](ti_933_databricks_explainer.md) for context.
# MAGIC
# MAGIC **What this does:** computes pooled ATT-style visit-rate lift for MNTN Select campaigns.
# MAGIC Replicates TI-917 v5 methodology, but cohort-filtered to `campaign_groups.product_id = 2`.
# MAGIC Reads `prospecting_intent` and `augmentor_log` directly from GCS parquet archives;
# MAGIC reads other tables via BigQuery Storage API.
# MAGIC
# MAGIC **Output:** per-(advertiser, arm) DataFrame written to DBFS, plus pooled lift numbers
# MAGIC printed at the end.

# COMMAND ----------

# MAGIC %md ## 1. Parameters

# COMMAND ----------

# --- Run mode ---
# SMOKE_TEST = True  → single day, exercises the full pipeline in 3-5 min. Run this FIRST
#                      to catch auth / schema / path issues before committing to a long run.
# SMOKE_TEST = False → real analysis at the WINDOW below.
SMOKE_TEST = True

# Real analysis window. Toggle between "7d" and "14d".
WINDOW = "7d"

# --- Output location ---
# DBFS FileStore is simplest (browser download URL: /files/ti_933/...).
# Swap to a GCS bucket (e.g., "gs://mntn-data-archive-prod/ti_933/") or a Unity
# Catalog Volume (e.g., "/Volumes/main/default/ti_933/") if your org prefers
# persistent storage. Path must end with a slash.
OUT_BASE = "/dbfs/FileStore/ti_933/"

# ---------------- derived ----------------
if SMOKE_TEST:
    # Single day, both impression and visit windows. Tiny but exercises every code path.
    WINDOW_LABEL = "smoke"
    WINDOW_START = "2026-05-04"
    WINDOW_END   = "2026-05-05"   # exclusive
    VISIT_END    = "2026-05-08"   # +3 days
elif WINDOW == "7d":
    WINDOW_LABEL = "7d"
    WINDOW_START = "2026-04-29"
    WINDOW_END   = "2026-05-06"
    VISIT_END    = "2026-05-09"
elif WINDOW == "14d":
    WINDOW_LABEL = "14d"
    WINDOW_START = "2026-04-22"
    WINDOW_END   = "2026-05-06"
    VISIT_END    = "2026-05-09"
else:
    raise ValueError(f"Unknown WINDOW: {WINDOW}")

OUT_DIR = f"{OUT_BASE}{WINDOW_LABEL}"
import os; os.makedirs(OUT_DIR, exist_ok=True) if OUT_DIR.startswith("/dbfs") or OUT_DIR.startswith("/Volumes") else None

print(f"MODE:    {'SMOKE TEST (single day)' if SMOKE_TEST else 'PRODUCTION (' + WINDOW + ')'}")
print(f"WINDOW:  {WINDOW_START} -> {WINDOW_END}  (visits +3d to {VISIT_END})")
print(f"OUT_DIR: {OUT_DIR}")
print(f"Spark:   {spark.version}")
if SMOKE_TEST:
    print()
    print("→ Smoke test produces a real but tiny lift number (wide CIs).")
    print("→ Set SMOKE_TEST = False after this run completes successfully.")

# COMMAND ----------

# MAGIC %md ## 1b. Smoke checks — auth + path validation
# MAGIC
# MAGIC Run this cell first. Confirms (a) BQ Spark connector + scratch dataset works,
# MAGIC (b) GCS parquet paths exist for both archives. Each check should return a non-zero
# MAGIC count in <30s. If any fails, the rest of the notebook will fail too — fix here first.

# COMMAND ----------

print("[1/3] BQ Spark connector + scratch dataset...")
try:
    cnt = (spark.read.format("bigquery")
           .option("table", "dw-main-bronze.integrationprod.campaign_groups")
           .option("filter", "product_id = 2 AND deleted = false AND is_test = false")
           .load()
           .count())
    print(f"      OK — {cnt} active Select campaign_groups")
except Exception as e:
    print(f"      FAIL — {type(e).__name__}: {e}")
    print("      Check: cluster service account has BQ Data Viewer + Job User on dw-main-bronze;")
    print("             scratch_ti933 dataset exists or SA has BQ Data Editor to create it.")
    raise

print("\n[2/3] GCS parquet — prospecting_intent...")
try:
    p = f"gs://household-scoring-prod/output/scoring/prospecting_intent/year={WINDOW_START[:4]}/month={WINDOW_START[5:7]}/day={WINDOW_START[8:10]}"
    cnt = spark.read.parquet(p).count()
    print(f"      OK — {cnt} rows in {p}")
except Exception as e:
    print(f"      FAIL — {type(e).__name__}: {e}")
    print("      Check: cluster SA has Storage Object Viewer on gs://household-scoring-prod/")
    raise

print("\n[3/3] GCS parquet — augmentor_log archive...")
try:
    p = f"gs://mntn-data-archive-prod/augmentor_log/region=east/dt={WINDOW_START}"
    cnt = spark.read.parquet(p).count()
    print(f"      OK — {cnt} rows in {p}")
except Exception as e:
    print(f"      FAIL — {type(e).__name__}: {e}")
    print("      Check: cluster SA has Storage Object Viewer on gs://mntn-data-archive-prod/")
    raise

print("\nAll smoke checks passed. Proceed.")

# COMMAND ----------

# MAGIC %md ## 2. Source: BigQuery dimension + smaller log tables
# MAGIC
# MAGIC Tables read via the BQ Spark connector with predicate pushdown (DATE filter on `time`).
# MAGIC Connector resolves SQLMesh views in `silver.logdata` and `silver.summarydata` transparently.

# COMMAND ----------

from datetime import date, timedelta

def bq(table: str, filter_clause: str = None, columns: list = None):
    """Read a BigQuery table via the Spark connector with optional pushdown filter + projection."""
    rdr = (spark.read.format("bigquery")
           .option("table", table)
           .option("viewsEnabled", "true")
           .option("materializationDataset", "scratch_ti933"))  # required for view reads
    if filter_clause:
        rdr = rdr.option("filter", filter_clause)
    df = rdr.load()
    if columns:
        df = df.select(*columns)
    return df

# Cohort: Select campaign_groups (product_id = 2)
groups_df = (bq("dw-main-bronze.integrationprod.campaign_groups",
                filter_clause="product_id = 2 AND deleted = false AND is_test = false",
                columns=["campaign_group_id", "advertiser_id"]))
groups_df.cache().createOrReplaceTempView("select_groups")
print(f"select_groups: {groups_df.count()} rows")

# All campaigns (we'll INNER JOIN to select_groups to get Select campaigns)
campaigns_df = (bq("dw-main-bronze.integrationprod.campaigns",
                   filter_clause="deleted = false AND is_test = false",
                   columns=["campaign_id", "advertiser_id", "campaign_group_id",
                            "objective_id", "funnel_level"]))
campaigns_df.createOrReplaceTempView("all_campaigns")

# COMMAND ----------

# MAGIC %md ## 3. Source: GCS parquet — `prospecting_intent` and `augmentor_log`
# MAGIC
# MAGIC Direct reads, no BQ involved. Both have richer GCS retention than BQ TTL.

# COMMAND ----------

# Build daily partition paths for the analysis window.
def daily_paths(base: str, start: str, end_exclusive: str, fmt) -> list:
    paths = []
    d = date.fromisoformat(start)
    end = date.fromisoformat(end_exclusive)
    while d < end:
        paths.append(fmt(base, d))
        d += timedelta(days=1)
    return paths

# prospecting_intent: gs://household-scoring-prod/output/scoring/prospecting_intent/year=YYYY/month=MM/day=DD/
prosp_base = "gs://household-scoring-prod/output/scoring/prospecting_intent"
prosp_paths = daily_paths(
    prosp_base, WINDOW_START, WINDOW_END,
    lambda b, d: f"{b}/year={d.year}/month={d.month:02d}/day={d.day:02d}"
)
print(f"prospecting_intent: {len(prosp_paths)} day partitions")

prosp_df = (spark.read
            .option("basePath", prosp_base)
            .parquet(*prosp_paths)
            .select("advertiser_id", "ip"))
prosp_df.createOrReplaceTempView("prospecting_raw")

# augmentor_log archive: gs://mntn-data-archive-prod/augmentor_log/region={east,west}/dt=YYYY-MM-DD/hh=HH/
# Both regions, all hours per day. Spark globs through hh=*.
aug_base = "gs://mntn-data-archive-prod/augmentor_log"
aug_paths = []
d0 = date.fromisoformat(WINDOW_START)
d1 = date.fromisoformat(WINDOW_END)
while d0 < d1:
    for region in ("east", "west"):
        aug_paths.append(f"{aug_base}/region={region}/dt={d0.isoformat()}")
    d0 += timedelta(days=1)
print(f"augmentor_log: {len(aug_paths)} (region, day) partitions")

aug_df = (spark.read
          .option("basePath", aug_base)
          .parquet(*aug_paths)
          .select("ip")
          .filter("ip IS NOT NULL AND ip <> '0.0.0.0'"))
aug_df.createOrReplaceTempView("augmentor_raw")

# COMMAND ----------

# MAGIC %md ## 4. Source: BQ log tables for the impression + visit windows

# COMMAND ----------

# Impression-window tables: cost_impression_log
ci_filter = (f"DATE(time) >= '{WINDOW_START}' AND DATE(time) < '{WINDOW_END}' "
             f"AND ip IS NOT NULL AND ip != '0.0.0.0'")
ci_df = bq("dw-main-silver.logdata.cost_impression_log",
           filter_clause=ci_filter,
           columns=["advertiser_id", "campaign_id", "ip"])
ci_df.createOrReplaceTempView("cost_imp_raw")

# Visit-window tables: clickpass_log, guid_log, ui_conversions (window extends +3 days)
v_filter = (f"DATE(time) >= '{WINDOW_START}' AND DATE(time) < '{VISIT_END}' "
            f"AND ip IS NOT NULL AND ip != '0.0.0.0'")
cp_df = bq("dw-main-silver.logdata.clickpass_log",
           filter_clause=v_filter,
           columns=["advertiser_id", "campaign_id", "ip"])
cp_df.createOrReplaceTempView("clickpass_raw")

gv_df = bq("dw-main-silver.logdata.guid_log",
           filter_clause=v_filter,
           columns=["advertiser_id", "ip"])
gv_df.createOrReplaceTempView("guid_visits_raw")

uc_df = bq("dw-main-silver.summarydata.ui_conversions",
           filter_clause=v_filter,
           columns=["advertiser_id", "ip"])
uc_df.createOrReplaceTempView("ui_conv_raw")

print("All source temp views registered.")

# COMMAND ----------

# MAGIC %md ## 5. The lift query (Spark SQL — same logic as BQ v3)
# MAGIC
# MAGIC One row per (advertiser, arm). Pooled stats reconstructed in pandas in the next cell.
# MAGIC Hash buckets use `xxhash64` instead of MD5-first-16-hex for performance — uniform mod-1000
# MAGIC distribution, so the holdout/targeted split is statistically equivalent to BQ's. NOT
# MAGIC IP-by-IP identical — this is a fresh randomization per Spark run.

# COMMAND ----------

result = spark.sql("""
WITH
-- Build the cohort
select_cohort AS (
  SELECT DISTINCT advertiser_id FROM select_groups
),
campaign_dim AS (
  SELECT c.campaign_id, c.advertiser_id, c.objective_id, c.funnel_level
  FROM all_campaigns c
  INNER JOIN select_groups g ON c.campaign_group_id = g.campaign_group_id
),

-- Prospecting universe filtered to Select cohort
prospecting AS (
  SELECT DISTINCT
    cast(advertiser_id as bigint) AS advertiser_id,
    ip
  FROM prospecting_raw
  WHERE cast(advertiser_id as bigint) IN (SELECT advertiser_id FROM select_cohort)
    AND ip IS NOT NULL AND ip <> '0.0.0.0'
),

-- Hash bucketing for holdout/targeted assignment
ip_assigned AS (
  SELECT
    advertiser_id, ip,
    pmod(xxhash64(concat_ws(':', cast(advertiser_id as string), ip)), 1000)        AS bucket,
    pmod(xxhash64(concat_ws(':wr:', cast(advertiser_id as string), ip)), 100000)   AS wr_bucket
  FROM prospecting
),
holdouts AS (
  SELECT advertiser_id, ip, wr_bucket FROM ip_assigned WHERE bucket BETWEEN 0 AND 99
),
targeted AS (
  SELECT advertiser_id, ip FROM ip_assigned WHERE bucket BETWEEN 100 AND 999
),

-- Augmentor IPs (biddability filter)
augmentor_ips AS (
  SELECT DISTINCT ip FROM augmentor_raw
),
biddable_holdouts AS (
  SELECT h.advertiser_id, h.ip, h.wr_bucket
  FROM holdouts h
  INNER JOIN augmentor_ips a USING (ip)
),

-- Cost-impression pairs (treated arm population)
cost_imp_pairs AS (
  SELECT DISTINCT
    cast(ci.advertiser_id as bigint) AS advertiser_id,
    ci.ip
  FROM cost_imp_raw ci
  INNER JOIN campaign_dim c ON ci.campaign_id = c.campaign_id
  WHERE cast(ci.advertiser_id as bigint) IN (SELECT advertiser_id FROM select_cohort)
),
served_treatment AS (
  SELECT DISTINCT t.advertiser_id, t.ip
  FROM targeted t INNER JOIN cost_imp_pairs c USING (advertiser_id, ip)
),

-- Per-advertiser empirical win-rate (denominator-matching)
served_n_per_adv AS (
  SELECT advertiser_id, COUNT(DISTINCT ip) AS served_n FROM served_treatment GROUP BY advertiser_id
),
biddable_n_per_adv AS (
  SELECT advertiser_id, COUNT(DISTINCT ip) AS bh_n FROM biddable_holdouts GROUP BY advertiser_id
),
win_rates AS (
  SELECT
    s.advertiser_id, s.served_n, b.bh_n,
    try_divide(s.served_n, b.bh_n * 9) AS wr
  FROM served_n_per_adv s INNER JOIN biddable_n_per_adv b USING (advertiser_id)
),

-- Subsample biddable_holdouts to match per-advertiser win-rate
bh_subsampled AS (
  SELECT bh.advertiser_id, bh.ip
  FROM biddable_holdouts bh
  INNER JOIN win_rates wr ON wr.advertiser_id = bh.advertiser_id
  WHERE wr.wr > 0 AND bh.wr_bucket < cast(wr.wr * 100000 as bigint)
),

-- Visit / conversion universes filtered to Select cohort
cp_pairs AS (
  SELECT DISTINCT cast(cp.advertiser_id as bigint) AS advertiser_id, cp.ip
  FROM clickpass_raw cp
  INNER JOIN campaign_dim c ON cp.campaign_id = c.campaign_id
  WHERE cast(cp.advertiser_id as bigint) IN (SELECT advertiser_id FROM select_cohort)
),
guid_visits AS (
  SELECT DISTINCT cast(advertiser_id as bigint) AS advertiser_id, ip
  FROM guid_visits_raw
  WHERE cast(advertiser_id as bigint) IN (SELECT advertiser_id FROM select_cohort)
),
ui_conv AS (
  SELECT DISTINCT cast(advertiser_id as bigint) AS advertiser_id, ip
  FROM ui_conv_raw
  WHERE cast(advertiser_id as bigint) IN (SELECT advertiser_id FROM select_cohort)
),

-- Two-arm subjects table
subjects AS (
  SELECT 'holdout_biddable' AS arm, advertiser_id, ip FROM bh_subsampled
  UNION ALL
  SELECT 'treated_served'   AS arm, advertiser_id, ip FROM served_treatment
)

SELECT
  s.advertiser_id,
  s.arm,
  COUNT(DISTINCT s.ip)                                     AS n_ips,
  COUNT(DISTINCT cp.ip)                                    AS clickpass_visitors,
  COUNT(DISTINCT gv.ip)                                    AS guid_visitors,
  COUNT(DISTINCT uc.ip)                                    AS ui_converters,
  try_divide(COUNT(DISTINCT cp.ip), COUNT(DISTINCT s.ip))  AS clickpass_rate,
  try_divide(COUNT(DISTINCT gv.ip), COUNT(DISTINCT s.ip))  AS guid_rate,
  try_divide(COUNT(DISTINCT uc.ip), COUNT(DISTINCT s.ip))  AS ui_conv_rate
FROM subjects s
LEFT JOIN cp_pairs   cp ON cp.advertiser_id = s.advertiser_id AND cp.ip = s.ip
LEFT JOIN guid_visits gv ON gv.advertiser_id = s.advertiser_id AND gv.ip = s.ip
LEFT JOIN ui_conv    uc ON uc.advertiser_id = s.advertiser_id AND uc.ip = s.ip
GROUP BY s.advertiser_id, s.arm
ORDER BY s.advertiser_id, s.arm
""")

# Materialize to driver
import pandas as pd
result_pd = result.toPandas()
print(f"Got {len(result_pd)} rows ({result_pd['advertiser_id'].nunique()} advertisers × 2 arms)")
result_pd.head(10)

# COMMAND ----------

# MAGIC %md ## 6. Pool in pandas + write CSVs

# COMMAND ----------

import math

# Per-advertiser CSV — name matches the local chart-gen script (window-suffixed)
per_adv_csv = f"{OUT_DIR}/ti_933_per_advertiser_lift_{WINDOW_LABEL}.csv"
result_pd.to_csv(per_adv_csv, index=False)
print(f"Wrote {per_adv_csv}")

# Pool: SUM of counts then divide. Mathematically identical to a BQ-side pooled CTE
# because (advertiser_id, ip) pairs are unique across advertisers.
pooled = (result_pd
    .groupby("arm", as_index=False)
    .agg(n_ips=("n_ips", "sum"),
         clickpass_visitors=("clickpass_visitors", "sum"),
         guid_visitors=("guid_visitors", "sum"),
         ui_converters=("ui_converters", "sum"))
)
pooled["clickpass_rate"] = pooled["clickpass_visitors"] / pooled["n_ips"]
pooled["guid_rate"]      = pooled["guid_visitors"]      / pooled["n_ips"]
pooled["ui_conv_rate"]   = pooled["ui_converters"]      / pooled["n_ips"]

pooled_csv = f"{OUT_DIR}/ti_933_pooled_lift_{WINDOW_LABEL}.csv"
pooled.to_csv(pooled_csv, index=False)
print(f"Wrote {pooled_csv}")

print("\n=== POOLED TABLE ===")
print(pooled.to_string(index=False))

# Also display in Databricks UI — click the result panel's "Download" button to
# pull these CSVs directly to your laptop without the Databricks CLI.
print("\n--- per-advertiser results (click Download in the result panel below) ---")
display(spark.createDataFrame(result_pd))
print("--- pooled results (click Download in the result panel below) ---")
display(spark.createDataFrame(pooled))

# COMMAND ----------

# MAGIC %md ## 7. Headline lift numbers (95% Wald CI)

# COMMAND ----------

def lift_with_ci(p_t: float, n_t: int, p_h: float, n_h: int):
    """Two-proportion difference with 95% Wald CI. Returns (lift_pp, ci_low_pp, ci_high_pp)."""
    if not n_t or not n_h or p_t is None or p_h is None:
        return None, None, None
    se = math.sqrt(p_t * (1 - p_t) / n_t + p_h * (1 - p_h) / n_h)
    diff = p_t - p_h
    return diff * 100, (diff - 1.96 * se) * 100, (diff + 1.96 * se) * 100

t = pooled[pooled["arm"] == "treated_served"].iloc[0]
h = pooled[pooled["arm"] == "holdout_biddable"].iloc[0]

print(f"=== POOLED LIFT — MNTN Select, {WINDOW_LABEL} window ===")
print(f"  Treated:  n_ips={t['n_ips']:>12,}  cp_v={t['clickpass_visitors']:>10,}  gv_v={t['guid_visitors']:>10,}  uc={t['ui_converters']:>8,}")
print(f"  Holdout:  n_ips={h['n_ips']:>12,}  cp_v={h['clickpass_visitors']:>10,}  gv_v={h['guid_visitors']:>10,}  uc={h['ui_converters']:>8,}")
print()

for label, rate_col, t_col, h_col in [
    ("Visit rate (guid)",     "guid_rate",     "guid_visitors",     "guid_visitors"),
    ("Visit rate (clickpass)", "clickpass_rate", "clickpass_visitors", "clickpass_visitors"),
    ("Conversion rate",       "ui_conv_rate",  "ui_converters",     "ui_converters"),
]:
    lift, lo, hi = lift_with_ci(t[rate_col], t["n_ips"], h[rate_col], h["n_ips"])
    if lift is None:
        print(f"  {label:24s}  n/a (insufficient data)")
    else:
        sig = "*" if (lo > 0 or hi < 0) else " "
        print(f"  {label:24s}  treated={t[rate_col]:.4%}  holdout={h[rate_col]:.4%}  "
              f"lift={lift:+.3f}pp  95% CI [{lo:+.3f}, {hi:+.3f}] {sig}")

# COMMAND ----------

# MAGIC %md ## 8. (Optional) Drop the temp materialization dataset
# MAGIC
# MAGIC The BQ Spark connector created a scratch dataset (`scratch_ti933`) for view materialization.
# MAGIC Set autoexpiration if that dataset doesn't already have one — otherwise the temp results
# MAGIC accumulate over time. Run once after first execution.

# COMMAND ----------

# from google.cloud import bigquery
# client = bigquery.Client(project="dw-main-silver")
# ds = client.get_dataset("dw-main-silver.scratch_ti933")
# ds.default_table_expiration_ms = 86_400_000  # 1 day
# client.update_dataset(ds, ["default_table_expiration_ms"])
