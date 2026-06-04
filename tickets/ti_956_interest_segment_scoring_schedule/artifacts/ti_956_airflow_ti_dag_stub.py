"""TI-956 — airflow-ti DAG stub for segment-quality scoring.

Template for Victor to drop into the airflow-ti repo. Submits the Dataproc
Serverless PySpark batch weekly, then registers the Iceberg partition
through BigLake so it's queryable in BQ.

Path in airflow-ti (suggested): `dags/ti/ti_956_segment_quality_scoring.py`.

Per `[[feedback_airflow_prod_safety]]`: feature branch, never push to main;
Ryan wires DAG deps after review.

This file is a TEMPLATE — Victor owns the airflow-ti integration. Tweak
operators / connection IDs / cluster image to match your conventions.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)


# --- Run config -------------------------------------------------------------
PROJECT_ID    = "mntn-targeting-prj-prod"     # <-- confirm
REGION        = "us-central1"
GCS_JOB_PATH  = "gs://mntn-targeting-jobs/ti_956/ti_956_segment_quality_scoring_job.py"
PY_FILES      = ["gs://mntn-targeting-jobs/ti_956/utils.zip"]  # bundle targeting-infra-ml utils/
SPARK_VERSION = "2.2"

WINDOW_DAYS = 30
SAMPLE_RATE = 0.0001

ICEBERG_JAR  = "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2"
BIGLAKE_JAR  = "org.apache.iceberg:iceberg-gcp:1.4.2"
BQ_CONNECTOR = "com.google.cloud.spark:spark-3.4-bigquery:0.34.0"

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
        batch_id="ti-956-{{ ds_nodash }}",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": GCS_JOB_PATH,
                "python_file_uris":     PY_FILES,
                "args": [
                    "--as_of_date={{ ds }}",
                    f"--window_days={WINDOW_DAYS}",
                    f"--sample_rate={SAMPLE_RATE}",
                ],
                "jar_file_uris": [
                    # Bundled at submit time so Dataproc Serverless can run Iceberg + BQ I/O
                ],
            },
            "runtime_config": {
                "version": SPARK_VERSION,
                "properties": {
                    "spark.jars.packages": ",".join([ICEBERG_JAR, BIGLAKE_JAR, BQ_CONNECTOR]),
                    # SQL catalogs already configured inside the job's SparkSession.
                    # Override here if Victor wants centralized config.
                },
            },
            "environment_config": {
                "execution_config": {
                    # Confirm subnet / service account with Victor
                    # "subnetwork_uri": "projects/<>/regions/us-central1/subnetworks/<>",
                    # "service_account": "ti-956-scoring@<project>.iam.gserviceaccount.com",
                },
            },
        },
    )
