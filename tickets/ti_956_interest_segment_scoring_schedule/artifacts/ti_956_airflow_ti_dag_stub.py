"""TI-956 — airflow-ti DAG stub for segment-quality scoring.

Template for Victor to drop into the airflow-ti repo. Submits the Dataproc
Serverless PySpark batch weekly, writes results to an Iceberg table via the
BigQuery Metastore catalog, BQ reads the table via the catalog.

Conventions mirror `airflow-ti/utils_runner/dataproc.py` +
`airflow-ti/utils_model/base_model/compute_component.py`:
  - Iceberg jars pre-staged in GCS (NOT spark.jars.packages — Maven resolution at
    runtime causes startup latency and CVE drift)
  - Catalog name = uppercased project_id with underscores; type=bigquery
  - Runtime env passed via MNTN_RUNTIME_ENV
  - batch_id format: <model-id-dashes>-local-YYYYMMDD-HHMM
  - Labels {team, application}

Path in airflow-ti (suggested): `dags/ti/ti_956_segment_quality_scoring.py`.

Per `[[feedback_airflow_prod_safety]]`: feature branch, never push to main;
Ryan wires DAG deps after review.

OPEN — confirm with Victor:
  - Prod service account + subnet (dev values below; agent couldn't find prod)
  - Whether airflow-ti has an Iceberg-compatible spark-bigquery-connector jar
    pre-staged in `ti_resources/spark/drivers/`. If not, our job's BQ reads
    (seg_meta, targetable_ips, performance, operative_3p) will fail until we
    either add it or refactor those reads to go through the Iceberg catalog.
  - Whether to refactor onto the IcebergBigqueryMntnPrjDevModel base class
    (more idiomatic) or keep the raw DataprocCreateBatchOperator submission
    (simpler, what's below).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)


# --- Run config (dev) — flip to prod by changing the env-keyed values below --
MODEL_ID         = "ti_956_segment_quality_scoring"
RUNTIME_ENV      = "dev"   # "dev" | "prod"

GCS_JOB_PATH     = "gs://mntn-data-archive-{env}/airflow_vs/{env}/code/ti_956/ti_956_segment_quality_scoring_job.py".format(env=RUNTIME_ENV)
GCS_PY_BUNDLE    = "gs://mntn-data-archive-{env}/airflow_vs/{env}/code/ti_956/utils.zip".format(env=RUNTIME_ENV)

PROJECT_ID       = "mntn-prj-dev-00"    # prod: confirm with Victor
REGION           = "us-central1"
SUBNET           = "projects/mntn-host-ntwrk-nonprod-00/regions/us-central1/subnetworks/mntn-dev-prj-snet-central1"  # prod: confirm
SERVICE_ACCOUNT  = "airflow-ti-dev@mntn-prj-dev-00.iam.gserviceaccount.com"  # prod: confirm
NETWORK_TAGS     = ["dataproc-dev"]
DATAPROC_VERSION = "2.3"

# Iceberg + GCP filesystem jars (pre-staged in GCS — airflow-ti convention)
ICEBERG_VERSION = "1.10.2"
SPARK_SCALA     = "3.5_2.13"  # runtime 2.3 → 3.5_2.13;  runtime 3.0 → 4.0_2.13
DRIVER_BASE     = "gs://mntn-data-archive-prod/ti_resources/spark/drivers"
ICEBERG_JARS    = ",".join([
    f"{DRIVER_BASE}/iceberg-bigquery-{ICEBERG_VERSION}.jar",
    f"{DRIVER_BASE}/iceberg-gcp-{ICEBERG_VERSION}.jar",
    f"{DRIVER_BASE}/iceberg-gcp-bundle-{ICEBERG_VERSION}.jar",
    f"{DRIVER_BASE}/iceberg-spark-runtime-{SPARK_SCALA}-{ICEBERG_VERSION}.jar",
    # spark-bigquery-connector — confirm exact filename with Victor; placeholder:
    # f"{DRIVER_BASE}/spark-{SPARK_SCALA.split('_')[0]}-bigquery-0.34.0.jar",
])

# Iceberg BigQuery Metastore catalog config (matches build_spark() in the job)
ICEBERG_CATALOG_NAME = "DW_MAIN_BRONZE" if RUNTIME_ENV == "prod" else "MNTN_PRJ_DEV_00"
ICEBERG_META_PROJECT = "dw-main-bronze" if RUNTIME_ENV == "prod" else "mntn-prj-dev-00"

WINDOW_DAYS = 30
SAMPLE_RATE = 0.0001

DEFAULT_ARGS = {
    "owner":            "malachi",
    "depends_on_past":  False,
    "email":            ["malachi@mountain.com"],
    "email_on_failure": True,
    "retries":          1,
    "retry_delay":      timedelta(minutes=30),
}


with DAG(
    dag_id="ti_956_segment_quality_scoring",
    description="Weekly LiveRamp segment-quality scoring → Iceberg via Dataproc Serverless",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 6, 8),  # first Sunday after agreement
    schedule_interval="0 6 * * 0",    # every Sunday 06:00 UTC
    catchup=False,
    max_active_runs=1,
    tags=["ti", "ti-956", "segment-quality", "weekly"],
) as dag:

    submit_scoring_job = DataprocCreateBatchOperator(
        task_id="submit_segment_quality_scoring_batch",
        project_id=PROJECT_ID,
        region=REGION,
        # batch_id format mirrors airflow-ti convention: <model-id-dashes>-local-YYYYMMDD-HHMM
        batch_id="ti-956-segment-quality-scoring-{{ ds_nodash }}-{{ ts_nodash[8:12] }}",
        batch={
            "labels": {
                "team":        "ti",
                "application": "ti_956_segment_quality",
            },
            "pyspark_batch": {
                "main_python_file_uri": GCS_JOB_PATH,
                "python_file_uris":     [GCS_PY_BUNDLE],
                "args": [
                    "--as_of_date={{ ds }}",
                    f"--window_days={WINDOW_DAYS}",
                    f"--sample_rate={SAMPLE_RATE}",
                ],
            },
            "runtime_config": {
                "version": DATAPROC_VERSION,
                "properties": {
                    # GCS-staged jars (NOT Maven Central — airflow-ti convention)
                    "spark.jars": ICEBERG_JARS,
                    # Iceberg catalog (matches the SparkSession config in the job)
                    f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}":                       "org.apache.iceberg.spark.SparkCatalog",
                    f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.type":                  "bigquery",
                    f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.gcp.bigquery.project-id": ICEBERG_META_PROJECT,
                    f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.gcp.bigquery.location":   REGION,
                    # Runtime-env propagation to driver + executors
                    "spark.dataproc.driverEnv.MNTN_RUNTIME_ENV": RUNTIME_ENV,
                    "spark.executorEnv.MNTN_RUNTIME_ENV":         RUNTIME_ENV,
                    # Prevent Dataproc's bundled Iceberg jar from conflicting with ours
                    "dataproc.artifacts.remove":                  "iceberg",
                },
            },
            "environment_config": {
                "execution_config": {
                    "subnetwork_uri":  SUBNET,
                    "service_account": SERVICE_ACCOUNT,
                    "network_tags":    NETWORK_TAGS,
                },
            },
        },
    )
