"""TI-956 — Weekly LiveRamp interest-segment quality scoring.

Runs the `segment_quality_scoring` model (models/machine_learning/) on a weekly
cadence (Sunday 06:00 UTC). Output: Iceberg table
`dw-main-bronze.household_scoring.segment_quality_daily`, partitioned by
`as_of_date`.

Cadence rationale (per Victor 1:1 2026-06-05): LiveRamp segment metadata
refreshes roughly every two weeks; weekly is the safe default for v1 and can
be tuned to biweekly after first runs.

No upstream sensor needed — all 5 inputs come from BQ (ipdsc external,
tpa.categories, impression_log, sum_by_campaign_by_day, audience_segments)
which are always current.

References:
- TI-956: https://mntn.atlassian.net/browse/TI-956
- Model file: models/machine_learning/segment_quality_scoring.py
- Cross-repo dep: targeting-infra-ml 0.1.0 wheel installed at batch startup
  via spark.dataproc.driverPipPackages (see model file's runtime_properties)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator

from include.job_config import JobTeamConfig
from include.models.operators import ModelPysparkBatchOperator

TEAM = JobTeamConfig.TGT.value

ENV = Variable.get("ENV")
GCP_PROJECT = f"mntn-prj-{ENV}-00"
REGION = "us-central1"


with DAG(
    dag_id="segment_quality_scoring_weekly",
    description="Weekly LiveRamp interest-segment quality scoring → Iceberg",
    start_date=datetime(2026, 6, 7),  # Sunday — first scheduled run will be the next Sunday after deploy
    schedule="0 6 * * 0",             # Every Sunday 06:00 UTC
    catchup=False,
    max_active_runs=1,
    **TEAM.make_dag_args(
        severity=2,  # v1 is for UI surfacing, not delivery-critical; tune up later if needed
        tags=["ti-956", "ml", "segment_quality", "household_scoring", "weekly"],
        default_args={"retries": 1},
    ),
) as dag:

    run_segment_quality_scoring = ModelPysparkBatchOperator(
        task_id="segment_quality_scoring",
        model_id="segment_quality_scoring",
        project_id=GCP_PROJECT,
        region=REGION,
        pyspark_batch_args=[
            "--as_of_date",
            "{{ ds }}",  # Airflow DAG run logical date — passed to the model's argparse
        ],
        deferrable=False,
        polling_interval_seconds=60,
        timeout=5 * 60 * 60,
        execution_timeout=timedelta(hours=5),
    )

    end = EmptyOperator(task_id="end")

    run_segment_quality_scoring >> end
