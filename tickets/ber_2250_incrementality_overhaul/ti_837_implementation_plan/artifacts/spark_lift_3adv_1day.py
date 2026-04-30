#!/usr/bin/env python3
"""
TI-837 v5 lift analysis — Spark port (Phase 0 benchmark)

Mirrors queries/ti_837_lift_3adv_1day_v5_bq_baseline.sql.

Sources:
  augmentor_log              -> GCS direct (gs://mntn-data-archive-prod/augmentor_log/)
  household_scoring__prosp.. -> BQ connector
  campaigns                  -> BQ connector
  cost_impression_log        -> BQ connector
  clickpass_log              -> BQ connector
  guid_log                   -> BQ connector

Hash decisions (Phase 0):
  holdout_bucket  = MD5(adv:ip)[:16] mod 1000          (matches BQ exactly — same MD5)
  wr_bucket       = MD5(adv:'wr':ip)[:16] mod 100000   (DRIFTS from BQ's FARM_FINGERPRINT —
                                                        causes ~0.1pp expected per-cell sampling
                                                        noise; OK for port validation phase)

Phase 1 fix (deferred): use a Python UDF wrapping pyfarmhash to match BQ exactly.

Usage:
  ~/.databricks-py312/bin/python tickets/.../artifacts/spark_lift_3adv_1day.py
  # Output JSON written to outputs/ti_837_benchmark_phase0_spark.json
  # Wall + per-stage timings printed to stdout.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
from pathlib import Path

WORKSPACE_HOST = "https://1262887251702944.4.gcp.databricks.com"
CLUSTER_ID = "5428-215533-4jodkdfs"
KEYCHAIN_SERVICE = "databricks-ti837"

ADVERTISERS = (31276, 31455, 38422)
# Default to v5 7-day window so the run is directly comparable to v5 BQ output
DT_IMPRESSION_START = "2026-04-20"
DT_IMPRESSION_END_EXCL = "2026-04-27"  # exclusive — covers 04-20 through 04-26
DT_VISIT_END_EXCL = "2026-04-30"       # +3 day visit post-period

# v5 win_rates for these 3 advertisers (carried verbatim from v5 SQL)
WIN_RATES = {
    31276: dict(wr_all=0.093003, wr_prosp=0.062059, wr_stage1=0.042767, wr_rtg=0.032330),
    31455: dict(wr_all=0.131959, wr_prosp=0.125461, wr_stage1=0.072254, wr_rtg=0.006904),
    38422: dict(wr_all=0.062331, wr_prosp=0.051848, wr_stage1=0.036034, wr_rtg=0.010877),
}

REPO_ROOT = Path(__file__).resolve().parents[4]
OUTPUT = REPO_ROOT / "tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/outputs/ti_837_benchmark_phase0_spark.json"


def get_pat() -> str:
    return subprocess.run(
        ["security", "find-generic-password", "-s", KEYCHAIN_SERVICE, "-w"],
        capture_output=True, text=True, check=True,
    ).stdout.strip()


def make_session():
    from databricks.connect import DatabricksSession
    return DatabricksSession.builder.remote(
        host=WORKSPACE_HOST, cluster_id=CLUSTER_ID, token=get_pat()
    ).getOrCreate()


def register_sources(spark, advertisers, dt_imp_start, dt_imp_end_excl, dt_visit_excl):
    """Register all 6 input sources as Spark temp views.

    `dt_imp_start` (inclusive) and `dt_imp_end_excl` (exclusive) define the
    impression window. Visit window extends +3 days past `dt_imp_end_excl`.
    """
    adv_csv = ",".join(str(a) for a in advertisers)

    # Build prospecting partition-day list: 04-20 through 04-26 → ('20','21',...,'26')
    from datetime import date, timedelta
    d0 = date.fromisoformat(dt_imp_start)
    d1 = date.fromisoformat(dt_imp_end_excl)
    days = []
    d = d0
    while d < d1:
        days.append(f"'{d.day:02d}'")
        d += timedelta(days=1)
    days_csv = ",".join(days)
    # Assume single (year, month) for the window — true for our 7-day windows
    year_str = f"'{d0.year}'"
    month_str = f"'{d0.month:02d}'"

    # 1. Augmentor — GCS direct read with EXPLICIT partition paths.
    #    Reading the parent path forces Spark to enumerate every region/dt
    #    partition at plan time (~60 partitions × N parquet files), which
    #    overloads the driver and drops the Spark Connect session
    #    (SESSION_NOT_FOUND on first temp view registration). Building the
    #    full set of explicit per-(region, dt) paths bypasses partition
    #    discovery — Spark only lists the paths we hand it.
    aug_paths = []
    dd = d0
    while dd < d1:
        for region in ("east", "west"):
            aug_paths.append(
                f"gs://mntn-data-archive-prod/augmentor_log/region={region}/dt={dd.isoformat()}/"
            )
        dd += timedelta(days=1)
    print(f"[spark_lift]   augmentor partition paths: {len(aug_paths)} (region × dt)")
    aug = (
        spark.read
        .option("basePath", "gs://mntn-data-archive-prod/augmentor_log/")
        .parquet(*aug_paths)
        .filter("ip IS NOT NULL AND ip != '0.0.0.0'")
        .select("ip")
        .distinct()
    )
    aug.createOrReplaceTempView("augmentor_ips")

    # 2. campaigns — BQ
    print(f"[spark_lift]   loading campaign_dim...", flush=True)
    camp = (
        spark.read.format("bigquery")
        .option("parentProject", "dw-main-bronze")
        .option("billingProject", "dw-main-bronze")
        .option("bigNumericDefaultPrecision", "38")
        .option("bigNumericDefaultScale", "9")
        .option("selectedFields",
                "campaign_id,advertiser_id,objective_id,funnel_level,deleted,is_test")
        .load("dw-main-bronze.integrationprod.campaigns")
        .filter(
            f"deleted = FALSE AND is_test = FALSE AND advertiser_id IN ({adv_csv})"
        )
        .select("campaign_id", "advertiser_id", "objective_id", "funnel_level")
    )
    camp.createOrReplaceTempView("campaign_dim")
    print(f"[spark_lift]   campaign_dim temp view registered", flush=True)

    # 3. prospecting — BQ external table backed by GCS Parquet.
    # The connector cannot read external tables via Storage API directly
    # ("Only external tables with connections can be read with the Storage API").
    # Use query mode → BQ runs the SELECT, materializes the result to
    # dw-main-bronze.external, Spark reads that materialized table.
    pros_sql = f"""
        SELECT DISTINCT
          CAST(advertiser_id AS INT64) AS advertiser_id,
          ip,
          CAST(household_score AS INT64) AS household_score
        FROM `dw-main-bronze.external.household_scoring__prospecting_intent__v1`
        WHERE CAST(advertiser_id AS INT64) IN ({adv_csv})
          AND year={year_str} AND month={month_str}
          AND day IN ({days_csv})
          AND ip IS NOT NULL AND ip != '0.0.0.0'
    """
    print(f"[spark_lift]   loading prospecting (query mode)...", flush=True)
    pros = (
        spark.read.format("bigquery")
        .option("parentProject", "dw-main-bronze")
        .option("billingProject", "dw-main-bronze")
        .option("project", "dw-main-bronze")
        .option("viewsEnabled", "true")
        .option("materializationDataset", "external")
        .option("bigNumericDefaultPrecision", "38")
        .option("bigNumericDefaultScale", "9")
        .load(pros_sql)
    )
    pros.createOrReplaceTempView("prospecting")
    print(f"[spark_lift]   prospecting temp view registered", flush=True)

    # 4-6. Silver tables — BQ in `query` mode (Victor's pattern, 2026-04-30).
    #
    # `query` pushes the entire SELECT + WHERE down to BigQuery; the result
    # is materialized to a temp table in `materializationDataset`, then
    # read by Spark. The Spark schema converter only sees the result
    # columns we project — never the source schema's INTERVAL /
    # wide-BIGNUMERIC columns that broke the connector when we tried
    # `.load(table)` style.
    #
    # Three project-related options ALL must be set (parentProject +
    # billingProject + project) — Victor explicitly flagged this. The
    # connector resolves auth/quota differently depending on which of
    # these are set, and missing any of the three can cause silent failures
    # or wrong-quota billing.
    #
    # `materializationDataset=external` in `dw-main-bronze` is the
    # sanctioned location (Terragrunt-managed bronze layer dataset). We
    # have write access here. Cleaner than creating a per-user scratch.
    SILVER_OPTS = dict(
        parentProject="dw-main-bronze",
        billingProject="dw-main-bronze",
        project="dw-main-bronze",
        viewsEnabled="true",
        materializationDataset="external",
        bigNumericDefaultPrecision="38",
        bigNumericDefaultScale="9",
    )

    def _bq_query(opts: dict, sql: str):
        rd = spark.read.format("bigquery")
        for k, v in opts.items():
            rd = rd.option(k, v)
        return rd.option("query", sql).load()

    cost_imp_sql = f"""
        SELECT
          CAST(advertiser_id AS INT64) AS advertiser_id,
          ip,
          campaign_id
        FROM `dw-main-silver.sqlmesh__logdata.logdata__cost_impression_log__2498930125`
        WHERE DATE(time) >= DATE('{dt_imp_start}')
          AND DATE(time) <  DATE('{dt_imp_end_excl}')
          AND advertiser_id IN ({adv_csv})
          AND ip IS NOT NULL AND ip != '0.0.0.0'
    """
    print(f"[spark_lift]   loading cost_impression_log (query mode)...", flush=True)
    ci = _bq_query(SILVER_OPTS, cost_imp_sql)
    ci.createOrReplaceTempView("cost_impression_log")
    print(f"[spark_lift]   cost_impression_log temp view registered", flush=True)

    clickpass_sql = f"""
        SELECT
          CAST(advertiser_id AS INT64) AS advertiser_id,
          ip,
          campaign_id
        FROM `dw-main-silver.sqlmesh__logdata.logdata__clickpass_log__755100014`
        WHERE DATE(time) >= DATE('{dt_imp_start}')
          AND DATE(time) <  DATE('{dt_visit_excl}')
          AND advertiser_id IN ({adv_csv})
          AND ip IS NOT NULL AND ip != '0.0.0.0'
    """
    print(f"[spark_lift]   loading clickpass_log (query mode)...", flush=True)
    cp = _bq_query(SILVER_OPTS, clickpass_sql)
    cp.createOrReplaceTempView("clickpass_log")
    print(f"[spark_lift]   clickpass_log temp view registered", flush=True)

    guid_sql = f"""
        SELECT DISTINCT
          CAST(advertiser_id AS INT64) AS advertiser_id,
          ip
        FROM `dw-main-silver.sqlmesh__logdata.logdata__guid_log__1352168581`
        WHERE DATE(time) >= DATE('{dt_imp_start}')
          AND DATE(time) <  DATE('{dt_visit_excl}')
          AND advertiser_id IN ({adv_csv})
          AND ip IS NOT NULL AND ip != '0.0.0.0'
    """
    print(f"[spark_lift]   loading guid_log (query mode)...", flush=True)
    g = _bq_query(SILVER_OPTS, guid_sql)
    g.createOrReplaceTempView("guid_log")
    print(f"[spark_lift]   guid_log temp view registered", flush=True)

    # Win rates — small static table
    wr_rows = [
        (a, r["wr_all"], r["wr_prosp"], r["wr_stage1"], r["wr_rtg"])
        for a, r in WIN_RATES.items()
    ]
    wr_df = spark.createDataFrame(
        wr_rows,
        ["advertiser_id", "wr_all", "wr_prosp", "wr_stage1", "wr_rtg"],
    )
    wr_df.createOrReplaceTempView("win_rates")


# Spark SQL implementing the v5 logic. Hash details:
#   holdout_bucket: cast(conv(substr(md5(adv||':'||ip),1,16),16,10) as decimal(20,0)) % 1000
#                   matches BQ exactly (both use MD5 of the same input).
#   wr_bucket    : cast(conv(substr(md5(adv||':wr:'||ip),1,16),16,10) as decimal(20,0)) % 100000
#                   DIFFERS from BQ (BQ uses FARM_FINGERPRINT). Expected to drift the
#                   subsample by ~0.1pp on per-cell ATTs.
LIFT_SQL = """
WITH ip_max_score AS (
  SELECT advertiser_id, ip, MAX(household_score) AS max_household_score
  FROM prospecting GROUP BY advertiser_id, ip
),
ip_assigned AS (
  SELECT
    s.advertiser_id, s.ip,
    CASE
      WHEN s.max_household_score = 10000 THEN 'high'
      WHEN s.max_household_score BETWEEN 7000 AND 9999 THEN 'peak'
      WHEN s.max_household_score BETWEEN 3333 AND 6999 THEN 'mid'
      ELSE 'max_reach'
    END AS intent_tier,
    CAST(
      conv(substr(md5(concat(cast(s.advertiser_id as string), ':', s.ip)), 1, 16), 16, 10)
      AS decimal(20,0)
    ) % 1000 AS bucket,
    CAST(
      conv(substr(md5(concat(cast(s.advertiser_id as string), ':wr:', s.ip)), 1, 16), 16, 10)
      AS decimal(20,0)
    ) % 100000 AS wr_bucket
  FROM ip_max_score s
),
holdouts AS (
  SELECT advertiser_id, ip, intent_tier, wr_bucket FROM ip_assigned WHERE bucket BETWEEN 0 AND 99
),
targeted AS (
  SELECT advertiser_id, ip, intent_tier FROM ip_assigned WHERE bucket BETWEEN 100 AND 999
),
biddable_holdouts_full AS (
  SELECT h.advertiser_id, h.ip, h.intent_tier, h.wr_bucket,
         wr.wr_all, wr.wr_prosp, wr.wr_stage1, wr.wr_rtg
  FROM holdouts h
  INNER JOIN augmentor_ips a USING (ip)
  INNER JOIN win_rates wr USING (advertiser_id)
),
cost_imp_pairs AS (
  SELECT DISTINCT ci.advertiser_id, ci.ip, c.objective_id, c.funnel_level
  FROM cost_impression_log ci
  INNER JOIN campaign_dim c ON ci.campaign_id = c.campaign_id
),
served_treatment_all    AS (SELECT DISTINCT t.advertiser_id, t.ip, t.intent_tier FROM targeted t INNER JOIN cost_imp_pairs c USING (advertiser_id, ip)),
served_treatment_prosp  AS (SELECT DISTINCT t.advertiser_id, t.ip, t.intent_tier FROM targeted t INNER JOIN cost_imp_pairs c USING (advertiser_id, ip) WHERE c.objective_id IN (1, 5, 6)),
served_treatment_stage1 AS (SELECT DISTINCT t.advertiser_id, t.ip, t.intent_tier FROM targeted t INNER JOIN cost_imp_pairs c USING (advertiser_id, ip) WHERE c.objective_id IN (1, 5, 6) AND c.funnel_level = 1),
served_treatment_rtg    AS (SELECT DISTINCT t.advertiser_id, t.ip, t.intent_tier FROM targeted t INNER JOIN cost_imp_pairs c USING (advertiser_id, ip) WHERE c.objective_id = 4),
cp_pairs AS (
  SELECT DISTINCT cp.advertiser_id, cp.ip, c.objective_id, c.funnel_level
  FROM clickpass_log cp
  INNER JOIN campaign_dim c ON cp.campaign_id = c.campaign_id
),
clickpass_all    AS (SELECT DISTINCT advertiser_id, ip FROM cp_pairs),
clickpass_prosp  AS (SELECT DISTINCT advertiser_id, ip FROM cp_pairs WHERE objective_id IN (1, 5, 6)),
clickpass_stage1 AS (SELECT DISTINCT advertiser_id, ip FROM cp_pairs WHERE objective_id IN (1, 5, 6) AND funnel_level = 1),
clickpass_rtg    AS (SELECT DISTINCT advertiser_id, ip FROM cp_pairs WHERE objective_id = 4),

bh_all    AS (SELECT advertiser_id, ip, intent_tier FROM biddable_holdouts_full WHERE wr_all    > 0 AND wr_bucket < CAST(wr_all    * 100000 AS BIGINT)),
bh_prosp  AS (SELECT advertiser_id, ip, intent_tier FROM biddable_holdouts_full WHERE wr_prosp  > 0 AND wr_bucket < CAST(wr_prosp  * 100000 AS BIGINT)),
bh_stage1 AS (SELECT advertiser_id, ip, intent_tier FROM biddable_holdouts_full WHERE wr_stage1 > 0 AND wr_bucket < CAST(wr_stage1 * 100000 AS BIGINT)),
bh_rtg    AS (SELECT advertiser_id, ip, intent_tier FROM biddable_holdouts_full WHERE wr_rtg    > 0 AND wr_bucket < CAST(wr_rtg    * 100000 AS BIGINT)),

all_subjects AS (
  SELECT 'all' AS segment, 'holdout_biddable' AS group_name, advertiser_id, ip, intent_tier FROM bh_all
  UNION ALL SELECT 'all', 'treated_served', advertiser_id, ip, intent_tier FROM served_treatment_all
  UNION ALL SELECT 'prosp', 'holdout_biddable', advertiser_id, ip, intent_tier FROM bh_prosp
  UNION ALL SELECT 'prosp', 'treated_served', advertiser_id, ip, intent_tier FROM served_treatment_prosp
  UNION ALL SELECT 'stage1', 'holdout_biddable', advertiser_id, ip, intent_tier FROM bh_stage1
  UNION ALL SELECT 'stage1', 'treated_served', advertiser_id, ip, intent_tier FROM served_treatment_stage1
  UNION ALL SELECT 'rtg', 'holdout_biddable', advertiser_id, ip, intent_tier FROM bh_rtg
  UNION ALL SELECT 'rtg', 'treated_served', advertiser_id, ip, intent_tier FROM served_treatment_rtg
),
clickpass_with_segment AS (
  SELECT 'all'    AS segment, advertiser_id, ip FROM clickpass_all
  UNION ALL SELECT 'prosp',  advertiser_id, ip FROM clickpass_prosp
  UNION ALL SELECT 'stage1', advertiser_id, ip FROM clickpass_stage1
  UNION ALL SELECT 'rtg',    advertiser_id, ip FROM clickpass_rtg
)

SELECT
  s.segment, s.advertiser_id, s.group_name, s.intent_tier,
  COUNT(DISTINCT s.ip)  AS n_ips,
  COUNT(DISTINCT cv.ip) AS clickpass_visitors,
  COUNT(DISTINCT gv.ip) AS guid_visitors,
  COUNT(DISTINCT cv.ip) / NULLIF(COUNT(DISTINCT s.ip), 0) AS clickpass_visit_rate,
  COUNT(DISTINCT gv.ip) / NULLIF(COUNT(DISTINCT s.ip), 0) AS guid_visit_rate
FROM all_subjects s
LEFT JOIN clickpass_with_segment cv
  ON cv.segment = s.segment AND cv.advertiser_id = s.advertiser_id AND cv.ip = s.ip
LEFT JOIN guid_log gv
  ON gv.advertiser_id = s.advertiser_id AND gv.ip = s.ip
GROUP BY s.segment, s.advertiser_id, s.group_name, s.intent_tier
ORDER BY s.segment, s.advertiser_id, s.group_name, s.intent_tier
"""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default=DT_IMPRESSION_START,
                        help="Impression window start (inclusive) YYYY-MM-DD")
    parser.add_argument("--end-excl", default=DT_IMPRESSION_END_EXCL,
                        help="Impression window end (exclusive) YYYY-MM-DD")
    parser.add_argument("--visit-end-excl", default=DT_VISIT_END_EXCL,
                        help="Visit window end (exclusive) YYYY-MM-DD — typically impression-end + 3d")
    args = parser.parse_args()

    print(f"[spark_lift] cohort={ADVERTISERS}")
    print(f"[spark_lift] impression window: {args.start} -> {args.end_excl}")
    print(f"[spark_lift] visit window:      {args.start} -> {args.visit_end_excl}")
    timings = {}

    t0 = time.time()
    spark = make_session()
    timings["session_start_s"] = time.time() - t0
    print(f"[spark_lift] session up in {timings['session_start_s']:.1f}s")

    t1 = time.time()
    register_sources(spark, ADVERTISERS, args.start, args.end_excl, args.visit_end_excl)
    timings["register_sources_s"] = time.time() - t1
    print(f"[spark_lift] sources registered in {timings['register_sources_s']:.1f}s")

    print(f"[spark_lift] running lift SQL...")
    t2 = time.time()
    result_df = spark.sql(LIFT_SQL)
    rows = [r.asDict() for r in result_df.collect()]
    timings["lift_query_s"] = time.time() - t2
    print(f"[spark_lift] lift SQL completed in {timings['lift_query_s']:.1f}s — {len(rows)} cells")

    timings["total_wall_s"] = time.time() - t0
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT.open("w") as f:
        json.dump(
            {
                "cohort": list(ADVERTISERS),
                "dt_impression_start": args.start,
                "dt_impression_end_excl": args.end_excl,
                "dt_visit_end_excl": args.visit_end_excl,
                "engine": "databricks-connect-17.3.7",
                "timings": timings,
                "rows": rows,
            },
            f, indent=2, default=str,
        )

    print(f"\n[spark_lift] WROTE {OUTPUT}")
    print(f"[spark_lift] total wall: {timings['total_wall_s']:.1f}s")


if __name__ == "__main__":
    main()
