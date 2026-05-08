# Databricks notebook source
# MAGIC %md
# MAGIC # TI-933 v3 — MNTN Select Lift (per-impression attribution)
# MAGIC
# MAGIC **What's new vs v2 (Victor's original):**
# MAGIC
# MAGIC 1. **Per-IP anchor date for visit attribution.** v2 used a fixed calendar
# MAGIC    visit window (e.g. 4/29-5/8) which gave each impression a different number
# MAGIC    of attribution days (9 for early-window impressions, 3 for late-window).
# MAGIC    v3 anchors each IP at its first eligibility moment and counts visits in
# MAGIC    a fixed `ATTRIBUTION_DAYS` window from there, symmetric across arms:
# MAGIC      - Treated anchor = first cost_impression date for that (advertiser, IP)
# MAGIC      - Holdout anchor = first augmentor appearance date for that IP
# MAGIC    Both are "first day we could have served this IP." Visits are attributed
# MAGIC    in `[anchor, anchor + ATTRIBUTION_DAYS - 1]` inclusive — exactly N calendar
# MAGIC    days starting from the anchor day. So ATTRIBUTION_DAYS=7 means May 4
# MAGIC    anchor → counts visits on May 4 through May 10 (7 days total). Every IP
# MAGIC    gets the same window length.
# MAGIC
# MAGIC 2. **`ATTRIBUTION_DAYS` parameter** (default 7). 7d gives each impression a
# MAGIC    full week of attributable visits — more inclusive read than the previous
# MAGIC    3-day post-period convention.
# MAGIC
# MAGIC 3. **14d-ready.** Same WINDOW="14d" toggle as v2; output goes to the same
# MAGIC    GCS path but with `_v3` suffix on the result subdirectory so v2 and v3
# MAGIC    don't collide.
# MAGIC
# MAGIC Everything else (cohort filter, holdout hashing, win-rate subsampling,
# MAGIC source tables) is identical to v2. Run as a Databricks Job on the same
# MAGIC cluster spec Victor used for v2 (~400 cores, c3d-highmem).

# COMMAND ----------

# MAGIC %md ## 1. Parameters

# COMMAND ----------

WINDOW = "14d"         # "7d", "14d", or "1d" (smoke)
ATTRIBUTION_DAYS = 7   # per-IP visit attribution window length, in days

# Spark conf (carried from v2)
spark.conf.set("spark.sql.files.maxPartitionBytes", "536870912")  # 512 MB
spark.conf.set("spark.sql.shuffle.partitions", "auto")

OUTPUT_ROOT = f"gs://mntn-data-archive-dev/victor/ti_933_2/window_id={WINDOW}_v3"

# ---------------- derived ----------------
if WINDOW == "1d":
    WINDOW_LABEL = "smoke"
    WINDOW_START = "2026-05-04"
    WINDOW_END   = "2026-05-05"
elif WINDOW == "7d":
    WINDOW_LABEL = "7d"
    WINDOW_START = "2026-04-29"
    WINDOW_END   = "2026-05-06"
elif WINDOW == "14d":
    WINDOW_LABEL = "14d"
    WINDOW_START = "2026-04-22"
    WINDOW_END   = "2026-05-06"
else:
    raise ValueError(f"Unknown WINDOW: {WINDOW}")

# Visit window now extends ATTRIBUTION_DAYS past the impression window so the
# latest impression has a full lookahead. Per-IP attribution further narrows
# this in the SQL below.
from datetime import date, timedelta
VISIT_END = (date.fromisoformat(WINDOW_END) + timedelta(days=ATTRIBUTION_DAYS)).isoformat()

print(f"WINDOW:               {WINDOW_START} -> {WINDOW_END}  ({WINDOW_LABEL})")
print(f"ATTRIBUTION_DAYS:     {ATTRIBUTION_DAYS}")
print(f"VISIT_END (calendar): {VISIT_END}  (per-IP attribution further narrows this)")
print(f"Spark:                {spark.version}")
print(f"Output root:          {OUTPUT_ROOT}")

# COMMAND ----------

# MAGIC %md ## 2. Source: BigQuery dimension + log tables
# MAGIC
# MAGIC Same BQ Spark connector setup as v2.

# COMMAND ----------

def bq_query(query: str):
    rdr = (
        spark.read
        .format("bigquery")
        .option("viewsEnabled", "true")
        .option("parentProject", "dw-main-bronze")
        .option("billingProject", "dw-main-bronze")
        .option("project", "dw-main-bronze")
        .option("materializationDataset", "external")
        .option("bigNumericDefaultPrecision", "38")
        .option("bigNumericDefaultScale", "9")
    )
    df = rdr.load(query)
    return df

def bq(table: str, filter_clause: str = None, columns: list = None):
    rdr = (
        spark.read
        .format("bigquery")
        .option("viewsEnabled", "true")
        .option("parentProject", "dw-main-bronze")
        .option("billingProject", "dw-main-bronze")
        .option("project", "dw-main-bronze")
        .option("materializationDataset", "external")
        .option("bigNumericDefaultPrecision", "38")
        .option("bigNumericDefaultScale", "9")
        .option("table", table))
    df = rdr.load()
    if filter_clause:
        df = df.where(filter_clause)
    if columns:
        df = df.select(*columns)
    return df

# Cohort: Select campaign_groups (product_id = 2)
groups_df = (bq("dw-main-bronze.integrationprod.campaign_groups",
                filter_clause="product_id = 2 AND deleted = false AND is_test = false",
                columns=["campaign_group_id", "advertiser_id"]))
groups_df.cache().createOrReplaceTempView("select_groups")
print(f"select_groups: {groups_df.count()} rows")

campaigns_df = (bq("dw-main-bronze.integrationprod.campaigns",
                   filter_clause="deleted = false AND is_test = false",
                   columns=["campaign_id", "advertiser_id", "campaign_group_id",
                            "objective_id", "funnel_level"]))
campaigns_df.createOrReplaceTempView("all_campaigns")

# COMMAND ----------

# MAGIC %md ## 3. Source: GCS parquet — `prospecting_intent` and `aug_log_ip` (with date)
# MAGIC
# MAGIC v3 adds the partition date columns so we can compute per-IP first-eligibility timestamps.

# COMMAND ----------

def daily_paths(base: str, start: str, end_exclusive: str, fmt) -> list:
    paths = []
    d = date.fromisoformat(start)
    end = date.fromisoformat(end_exclusive)
    while d < end:
        paths.append(fmt(base, d))
        d += timedelta(days=1)
    return paths

# prospecting_intent: same as v2 (we don't need a date here, just the IP universe)
prosp_base = "gs://household-scoring-prod/output/scoring/prospecting_intent"
prosp_paths = daily_paths(
    prosp_base, WINDOW_START, WINDOW_END,
    lambda b, d: f"{b}/year={d.year}/month={d.month:02d}/day={d.day:02d}/"
)
print(f"prospecting_intent: {len(prosp_paths)} day partitions")

prosp_df = (spark.read
            .option("basePath", prosp_base)
            .parquet(*prosp_paths)
            .select("advertiser_id", "ip"))
prosp_df.createOrReplaceTempView("prospecting_raw")

# augmentor feature-store: now SELECT dt as well so we can compute per-IP first-seen date
aug_base = "gs://mntn-data-archive-prod/feature_store/feature_group_1_source/aug_log_ip"
aug_paths = []
d0 = date.fromisoformat(WINDOW_START)
d1 = date.fromisoformat(WINDOW_END)
while d0 < d1:
    aug_paths.append(f"{aug_base}/dt={d0.isoformat()}")
    d0 += timedelta(days=1)
print(f"augmentor (aug_log_ip): {len(aug_paths)} day partitions")

aug_df = (spark.read
          .option("basePath", aug_base)
          .parquet(*aug_paths)
          .select("ip", "dt")  # dt comes from partition discovery
          .filter("ip IS NOT NULL AND ip <> '0.0.0.0'"))
aug_df.createOrReplaceTempView("augmentor_raw")

# COMMAND ----------

# MAGIC %md ## 4. Source: BQ log tables — keep timestamps for per-impression attribution

# COMMAND ----------

# cost_impression_log: NOW INCLUDES `time` so we can compute per-(advertiser, IP) first impression date
ci_filter = (f"DATE(time) >= '{WINDOW_START}' AND DATE(time) < '{WINDOW_END}' "
             f"AND ip IS NOT NULL AND ip != '0.0.0.0'")
ci_df = bq_query(f"""
    SELECT advertiser_id, campaign_id, ip, time
    FROM dw-main-silver.logdata.cost_impression_log
    WHERE {ci_filter}
""")
ci_df.createOrReplaceTempView("cost_imp_raw")

# Visit-window tables: include `time` for the temporal join
v_filter = (f"DATE(time) >= '{WINDOW_START}' AND DATE(time) < '{VISIT_END}' "
            f"AND ip IS NOT NULL AND ip != '0.0.0.0'")
cp_df = bq("dw-main-silver.logdata.clickpass_log",
           filter_clause=v_filter,
           columns=["advertiser_id", "campaign_id", "ip", "time"])
cp_df.createOrReplaceTempView("clickpass_raw")

gv_df = bq("dw-main-silver.logdata.guid_log",
           filter_clause=v_filter,
           columns=["advertiser_id", "ip", "time"])
gv_df.createOrReplaceTempView("guid_visits_raw")

uc_df = bq_query(f"""
    SELECT advertiser_id, ip, time
    FROM dw-main-silver.summarydata.ui_conversions
    WHERE {v_filter}
""")
uc_df.createOrReplaceTempView("ui_conv_raw")

print("All source temp views registered with timestamps.")

# COMMAND ----------

# MAGIC %md ## 5. Cohort + holdout / targeted assignment (same as v2)

# COMMAND ----------

select_cohort_df = spark.sql("SELECT DISTINCT advertiser_id FROM select_groups")
select_cohort_df.createOrReplaceTempView("select_cohort")

ip_assigned_df = spark.sql("""
WITH prospecting AS (
  SELECT DISTINCT
    cast(advertiser_id as bigint) AS advertiser_id,
    ip
  FROM prospecting_raw
  WHERE cast(advertiser_id as bigint) IN (SELECT advertiser_id FROM select_cohort)
    AND ip IS NOT NULL AND ip <> '0.0.0.0'
)
SELECT
    advertiser_id, ip,
    pmod(xxhash64(concat_ws(':', cast(advertiser_id as string), ip)), 1000)        AS bucket,
    pmod(xxhash64(concat_ws(':wr:', cast(advertiser_id as string), ip)), 100000)   AS wr_bucket
  FROM prospecting
""").sortWithinPartitions("bucket")

(ip_assigned_df.write.mode("overwrite")
        .option("overwriteSchema", "true")
        .format("parquet")
        .option("path", f"{OUTPUT_ROOT}/ip_assigned")).save()

spark.read.parquet(f"{OUTPUT_ROOT}/ip_assigned").createOrReplaceTempView("ip_assigned")

# COMMAND ----------

# MAGIC %md ## 6. Per-IP anchor dates + per-impression attribution (NEW IN v3)
# MAGIC
# MAGIC The methodology change. For each IP we compute one "anchor" date — the first
# MAGIC moment that IP was eligible to be served (or actually served) — and count a
# MAGIC visit/conversion only if it falls within `[anchor, anchor + ATTRIBUTION_DAYS]`.
# MAGIC
# MAGIC Symmetric across arms:
# MAGIC * Treated anchor: first cost_impression date for that (advertiser, IP)
# MAGIC * Holdout anchor: first augmentor (aug_log_ip) appearance date for that IP
# MAGIC
# MAGIC Each IP gets exactly the same number of attribution days regardless of where
# MAGIC its anchor falls in the impression window — fixes the v2 attribution-window
# MAGIC asymmetry.

# COMMAND ----------

result_df = spark.sql(f"""
WITH
campaign_dim AS (
  SELECT c.campaign_id, c.advertiser_id, c.objective_id, c.funnel_level
  FROM all_campaigns c
  INNER JOIN select_groups g ON c.campaign_group_id = g.campaign_group_id
),

holdouts AS (
  SELECT advertiser_id, ip, wr_bucket FROM ip_assigned WHERE bucket BETWEEN 0 AND 99
),
targeted AS (
  SELECT advertiser_id, ip FROM ip_assigned WHERE bucket BETWEEN 100 AND 999
),

-- Augmentor with first-seen date per IP (NEW)
augmentor_ip_first AS (
  SELECT ip, MIN(to_date(dt)) AS first_aug_date
  FROM augmentor_raw
  GROUP BY ip
),

-- Biddable holdouts joined with their per-IP anchor
biddable_holdouts AS (
  SELECT h.advertiser_id, h.ip, h.wr_bucket, a.first_aug_date AS anchor_date
  FROM holdouts h
  INNER JOIN augmentor_ip_first a USING (ip)
),

-- Cost-impression with per-(advertiser, IP) FIRST impression date as anchor (NEW)
cost_imp_first AS (
  SELECT
    cast(ci.advertiser_id as bigint) AS advertiser_id,
    ci.ip,
    MIN(to_date(ci.time)) AS anchor_date
  FROM cost_imp_raw ci
  INNER JOIN campaign_dim c ON ci.campaign_id = c.campaign_id
  WHERE cast(ci.advertiser_id as bigint) IN (SELECT advertiser_id FROM select_cohort)
  GROUP BY cast(ci.advertiser_id as bigint), ci.ip
),

-- Treated arm = targeted ∩ cost_imp_first (carries the anchor date)
served_treatment AS (
  SELECT t.advertiser_id, t.ip, c.anchor_date
  FROM targeted t
  INNER JOIN cost_imp_first c USING (advertiser_id, ip)
),

-- Per-advertiser empirical win rate (denominators)
served_n_per_adv AS (
  SELECT advertiser_id, COUNT(DISTINCT ip) AS served_n FROM served_treatment GROUP BY advertiser_id
),
biddable_n_per_adv AS (
  SELECT advertiser_id, COUNT(DISTINCT ip) AS bh_n FROM biddable_holdouts GROUP BY advertiser_id
),
win_rates AS (
  SELECT s.advertiser_id, s.served_n, b.bh_n,
         try_divide(s.served_n, b.bh_n * 9) AS wr
  FROM served_n_per_adv s INNER JOIN biddable_n_per_adv b USING (advertiser_id)
),

-- Subsample biddable_holdouts to per-advertiser win-rate (carries anchor date)
bh_subsampled AS (
  SELECT bh.advertiser_id, bh.ip, bh.anchor_date
  FROM biddable_holdouts bh
  INNER JOIN win_rates wr ON wr.advertiser_id = bh.advertiser_id
  WHERE wr.wr > 0 AND bh.wr_bucket < cast(wr.wr * 100000 as bigint)
),

-- Visit / conversion universes — KEEP timestamps for the temporal predicate join
cp_pairs AS (
  SELECT DISTINCT
    cast(cp.advertiser_id as bigint) AS advertiser_id,
    cp.ip,
    to_date(cp.time) AS visit_date
  FROM clickpass_raw cp
  INNER JOIN campaign_dim c ON cp.campaign_id = c.campaign_id
  WHERE cast(cp.advertiser_id as bigint) IN (SELECT advertiser_id FROM select_cohort)
),
guid_visits AS (
  SELECT DISTINCT
    cast(advertiser_id as bigint) AS advertiser_id,
    ip,
    to_date(time) AS visit_date
  FROM guid_visits_raw
  WHERE cast(advertiser_id as bigint) IN (SELECT advertiser_id FROM select_cohort)
),
ui_conv AS (
  SELECT DISTINCT
    cast(advertiser_id as bigint) AS advertiser_id,
    ip,
    to_date(time) AS visit_date
  FROM ui_conv_raw
  WHERE cast(advertiser_id as bigint) IN (SELECT advertiser_id FROM select_cohort)
),

-- Two-arm subjects table — both arms now carry an anchor_date
subjects AS (
  SELECT 'holdout_biddable' AS arm, advertiser_id, ip, anchor_date FROM bh_subsampled
  UNION ALL
  SELECT 'treated_served'   AS arm, advertiser_id, ip, anchor_date FROM served_treatment
)

-- THE TEMPORAL JOIN: a visit counts only if visit_date is within [anchor, anchor + ATTRIBUTION_DAYS]
SELECT
  s.advertiser_id,
  s.arm,
  COUNT(DISTINCT s.ip)                                     AS n_ips,
  COUNT(DISTINCT CASE WHEN cp.visit_date BETWEEN s.anchor_date AND date_add(s.anchor_date, {ATTRIBUTION_DAYS - 1}) THEN cp.ip END) AS clickpass_visitors,
  COUNT(DISTINCT CASE WHEN gv.visit_date BETWEEN s.anchor_date AND date_add(s.anchor_date, {ATTRIBUTION_DAYS - 1}) THEN gv.ip END) AS guid_visitors,
  COUNT(DISTINCT CASE WHEN uc.visit_date BETWEEN s.anchor_date AND date_add(s.anchor_date, {ATTRIBUTION_DAYS - 1}) THEN uc.ip END) AS ui_converters
FROM subjects s
LEFT JOIN cp_pairs   cp ON cp.advertiser_id = s.advertiser_id AND cp.ip = s.ip
LEFT JOIN guid_visits gv ON gv.advertiser_id = s.advertiser_id AND gv.ip = s.ip
LEFT JOIN ui_conv    uc ON uc.advertiser_id = s.advertiser_id AND uc.ip = s.ip
GROUP BY s.advertiser_id, s.arm
ORDER BY s.advertiser_id, s.arm
""")

# Compute rates in pandas after the heavy lift completes (matches v2 output schema)
result_df.createOrReplaceTempView("result_raw")
result_df = spark.sql("""
SELECT *,
  try_divide(clickpass_visitors, n_ips) AS clickpass_rate,
  try_divide(guid_visitors,      n_ips) AS guid_rate,
  try_divide(ui_converters,      n_ips) AS ui_conv_rate
FROM result_raw
""")

result_df.explain()

(result_df.write.mode("overwrite")
        .option("overwriteSchema", "true")
        .format("parquet")
        .option("path", f"{OUTPUT_ROOT}/result")).save()

# COMMAND ----------

# MAGIC %md ## 7. Quick sanity print

# COMMAND ----------

display(spark.read.parquet(f"{OUTPUT_ROOT}/result"))
