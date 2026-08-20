#!/usr/bin/env python3
"""
TI-837 Databricks Connect smoke test.

Validates:
  A. BQ Spark connector reads dw-main-bronze.integrationprod.campaigns
  B. GCS Parquet read on augmentor_log/region=east/dt=YYYY-MM-DD

Usage:
    ~/.databricks-py312/bin/python .claude/scripts/databricks_smoke.py
    ~/.databricks-py312/bin/python .claude/scripts/databricks_smoke.py --dt 2026-04-26

Prereqs:
  - an authenticated `databricks` CLI profile (`databricks auth login --host <workspace>`)
  - ~/.databricks-py312 venv with databricks-connect==17.3.*
  - Cluster 5428-215533-4jodkdfs RUNNING (will not auto-start; check the UI)

If the cluster is TERMINATED, start via:
    databricks clusters start --cluster-id 5428-215533-4jodkdfs
"""

import argparse
import os
import sys
import time

WORKSPACE_HOST = "https://1262887251702944.4.gcp.databricks.com"
CLUSTER_ID = "5428-215533-4jodkdfs"
# OAuth profile, not a stored PAT. A long-lived local token is the pattern MNTN
# decommissioned with the Slack bot; override for a service principal on OAuth M2M.
PROFILE = os.environ.get("DATABRICKS_PROFILE", "malachi@mountain.com")


def make_session():
    from databricks.connect import DatabricksSession
    from databricks.sdk.core import Config

    cfg = Config(profile=PROFILE, host=WORKSPACE_HOST, cluster_id=CLUSTER_ID)
    return DatabricksSession.builder.sdkConfig(cfg).getOrCreate()


def test_a_bq_campaigns(spark) -> int:
    df = (
        spark.read.format("bigquery")
        .option("parentProject", "dw-main-bronze")
        .option("billingProject", "dw-main-bronze")
        .option("project", "dw-main-bronze")
        .load("dw-main-bronze.integrationprod.campaigns")
        .filter("deleted = FALSE AND is_test = FALSE")
    )
    return df.count()


def test_b_gcs_augmentor(spark, dt: str) -> dict:
    path = f"gs://mntn-data-archive-prod/augmentor_log/region=east/dt={dt}/"
    df = spark.read.parquet(path)
    schema_cols = [f.name for f in df.schema.fields]
    n10 = df.limit(10).count()
    return {"path": path, "schema_cols": schema_cols, "n10": n10}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dt", default="2026-04-23", help="augmentor partition date (YYYY-MM-DD)")
    args = parser.parse_args()

    print(f"[smoke] starting Databricks Connect session as profile {PROFILE}...")
    t0 = time.time()
    spark = make_session()
    print(f"[smoke]   session up in {time.time() - t0:.1f}s")

    print("\n[A] BQ connector: dw-main-bronze.integrationprod.campaigns ...")
    t1 = time.time()
    n_campaigns = test_a_bq_campaigns(spark)
    print(f"[A]   count={n_campaigns:,}  ({time.time() - t1:.1f}s)")

    print(f"\n[B] GCS augmentor: region=east/dt={args.dt} ...")
    t2 = time.time()
    res = test_b_gcs_augmentor(spark, args.dt)
    print(f"[B]   path={res['path']}")
    print(
        f"[B]   schema cols ({len(res['schema_cols'])}): {', '.join(res['schema_cols'][:8])}, ..."
    )
    print(f"[B]   limit(10).count()={res['n10']}  ({time.time() - t2:.1f}s)")

    if n_campaigns > 100_000 and res["n10"] == 10:
        print("\n[smoke] PASS")
        sys.exit(0)
    print("\n[smoke] FAIL")
    sys.exit(1)


if __name__ == "__main__":
    main()
