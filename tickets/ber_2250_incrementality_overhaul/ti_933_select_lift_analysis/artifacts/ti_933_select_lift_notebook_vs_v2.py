# Databricks notebook source
# MAGIC %md ## 1. Parameters

# COMMAND ----------

# Real analysis window. Toggle between "7d" and "14d".
WINDOW = "7d"

# --- Small-file coalescing for GCS parquet reads ---
# prospecting_intent has ~20k tiny part files per day (upstream Spark writes
# without coalescing). Without this, Spark creates one task per part file and
# task scheduling overhead dominates the actual scan. 512MB target = combine
# many small files into one Spark partition.
spark.conf.set("spark.sql.files.maxPartitionBytes", "536870912")  # 512 MB
spark.conf.set("spark.sql.shuffle.partitions", "auto")  # 512 MB
# spark.conf.set("spark.sql.files.openCostInBytes", "33554432")     # 32 MB (default 4 MB; raises threshold for "should I combine")

OUTPUT_ROOT = f"gs://mntn-data-archive-dev/victor/ti_933_2/window_id={WINDOW}"

# ---------------- derived ----------------
if WINDOW == "1d":
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

print(f"WINDOW:  {WINDOW_START} -> {WINDOW_END}  (visits +3d to {VISIT_END})")
print(f"Spark:   {spark.version}")
print(f"Output root:   {OUTPUT_ROOT}")

# COMMAND ----------

# MAGIC %md ## 2. Source: BigQuery dimension + smaller log tables
# MAGIC
# MAGIC Tables read via the BQ Spark connector with predicate pushdown (DATE filter on `time`).
# MAGIC Connector resolves SQLMesh views in `silver.logdata` and `silver.summarydata` transparently.

# COMMAND ----------

from datetime import date, timedelta

def bq_query(query: str):
    rdr = (
        spark.read
        .format("bigquery")
        .option("viewsEnabled", "true")
        .option("parentProject", "dw-main-bronze")  # it is important to set all 3 project related properties!!
        .option("billingProject", "dw-main-bronze")
        .option("project", "dw-main-bronze")
        .option("materializationDataset", "external")
        .option("bigNumericDefaultPrecision", "38")
        .option("bigNumericDefaultScale", "9")
    )
    df = rdr.load(query)
    return df

def bq(table: str, filter_clause: str = None, columns: list = None):
    """Read a BigQuery table via the Spark connector with optional pushdown filter + projection.

    parentProject / materializationProject / materializationDataset / viewsEnabled
    are set globally via spark.conf.set in cell 1. No per-call overrides needed.
    """
    rdr = (
        spark.read
        .format("bigquery")
        .option("viewsEnabled", "true")
        .option("parentProject", "dw-main-bronze")  # it is important to set all 3 project related properties!!
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

# All campaigns (we'll INNER JOIN to select_groups to get Select campaigns)
campaigns_df = (bq("dw-main-bronze.integrationprod.campaigns",
                   filter_clause="deleted = false AND is_test = false",
                   columns=["campaign_id", "advertiser_id", "campaign_group_id",
                            "objective_id", "funnel_level"]))
campaigns_df.createOrReplaceTempView("all_campaigns")

# COMMAND ----------

campaigns_df.count()

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
    lambda b, d: f"{b}/year={d.year}/month={d.month:02d}/day={d.day:02d}/"
)
print(f"prospecting_intent: {len(prosp_paths)} day partitions")
for p_path in prosp_paths:
    print(p_path)

prosp_df = (spark.read
            .option("basePath", prosp_base)
            .parquet(*prosp_paths)
            .select("advertiser_id", "ip"))
prosp_df.createOrReplaceTempView("prospecting_raw")

# augmentor_log archive: gs://mntn-data-archive-prod/augmentor_log/region={east,west}/dt=YYYY-MM-DD/hh=HH/
# Both regions, all hours per day. Spark globs through hh=*.
aug_base = "gs://mntn-data-archive-prod/feature_store/feature_group_1_source/aug_log_ip"
aug_paths = []
d0 = date.fromisoformat(WINDOW_START)
d1 = date.fromisoformat(WINDOW_END)
while d0 < d1:
    aug_paths.append(f"{aug_base}/dt={d0.isoformat()}")
        
    d0 += timedelta(days=1)
print(f"augmentor_log: {len(aug_paths)} (region, day) partitions")

for p_path in aug_paths:
    print(p_path)

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
ci_df = bq_query(f"""
                 select advertiser_id, campaign_id, ip 
                 from dw-main-silver.logdata.cost_impression_log
                 where {ci_filter}
                 """)
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

uc_df = bq_query(
    f"""select advertiser_id, ip 
    from dw-main-silver.summarydata.ui_conversions
    where {v_filter}
    """
)
uc_df.createOrReplaceTempView("ui_conv_raw")

print("All source temp views registered.")

# COMMAND ----------

for df_i in [ci_df, cp_df, gv_df, uc_df]:
    print(df_i.count())

# COMMAND ----------

select_cohort_df = spark.sql("""
    SELECT DISTINCT advertiser_id FROM select_groups                    
""")
select_cohort_df.createOrReplaceTempView("select_cohort")

ip_assigned_df = spark.sql("""
    -- Prospecting universe filtered to Select cohort
with prospecting AS (
  SELECT DISTINCT
    cast(advertiser_id as bigint) AS advertiser_id,
    ip
  FROM prospecting_raw
  WHERE cast(advertiser_id as bigint) IN (SELECT advertiser_id FROM select_cohort)
    AND ip IS NOT NULL AND ip <> '0.0.0.0'
)

-- Hash bucketing for holdout/targeted assignment

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


# COMMAND ----------

spark.read.parquet(f"{OUTPUT_ROOT}/ip_assigned").createOrReplaceTempView("ip_assigned")

# COMMAND ----------

# MAGIC %md ## 5. The lift query (Spark SQL — same logic as BQ v3)
# MAGIC
# MAGIC One row per (advertiser, arm). Pooled stats reconstructed in pandas in the next cell.
# MAGIC Hash buckets use `xxhash64` instead of MD5-first-16-hex for performance — uniform mod-1000
# MAGIC distribution, so the holdout/targeted split is statistically equivalent to BQ's. NOT
# MAGIC IP-by-IP identical — this is a fresh randomization per Spark run.

# COMMAND ----------

result_df = spark.sql("""
WITH
-- Build the cohort
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

result_df.explain()

(result_df.write.mode("overwrite")
        .option("overwriteSchema", "true")
        .format("parquet")
        .option("path", f"{OUTPUT_ROOT}/result")).save()