from datetime import datetime

from job_config import JobTeamConfig

from airflow import DAG
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

TEAM = JobTeamConfig.TGT.value

with DAG(
    dag_id="create_ip_verticals",
    description="Create Advertiser Verticals",
    start_date=datetime(2024, 1, 7),
    schedule_interval="5 0 * * *",
    catchup=False,
    **TEAM.make_dag_args(severity=5, tags=[], default_args={"retries": 1, "max_active_runs": 1}),
) as dag:
    aws_region = "us-west-2"
    run_date = """{{ ds }}"""
    yesterday = """{{ yesterday_ds }}"""
    env = Variable.get("ENV")
    s3_output_path = (
        f"s3://mntn-data-archive-{env}/vertical_categorizations/ip_vertical_associations/"
    )

    dbt_runner_image_name = (
        f"077854988703.dkr.ecr.us-west-2.amazonaws.com/generic_dbt_runner_ml:{env}"
    )

    default_env_vars = {
        "AWS_DEFAULT_REGION": aws_region,
        "run_date": run_date,
        "env": env,
        "yesterday": yesterday,
    }

    kube_config_path = f"/usr/local/airflow/dags/kube_config-{env}.yaml"

    ddp_url_classification = KubernetesPodOperator(
        task_id="ddp_url_classification",
        name="ddp_url_classification",
        image_pull_policy="Always",
        cmds=[
            "dbt",
            "build",
            "--select",
            "models/vertical_categorization/ddp_url_verticals.py",
            "--target",
            env + "_warehouse_small",
        ],
        config_file=kube_config_path,
        cluster_context="aws",
        image=dbt_runner_image_name,
        env_vars=default_env_vars,
    )

    write_verticals = KubernetesPodOperator(
        task_id="write_verticals",
        name="write_verticals",
        image_pull_policy="Always",
        cmds=[
            "dbt",
            "build",
            "--select",
            "models/vertical_categorization/ip_vertical_associations.py",
            "--target",
            env + "_warehouse_small",
        ],
        config_file=kube_config_path,
        cluster_context="aws",
        image=dbt_runner_image_name,
        env_vars=default_env_vars,
    )

    add_redshift_partition_task = SQLExecuteQueryOperator(
        task_id="add_partition_redshift_task",
        conn_id="redshift_coredw",
        autocommit=True,
        sql=f"""ALTER TABLE ext_tpa.ip_vertical_associations ADD IF NOT EXISTS """
        """PARTITION(dt='{{ ds }}')"""
        f"""LOCATION '{s3_output_path}dt="""
        """{{ ds }}';""",
        dag=dag,
    )

    (ddp_url_classification >> write_verticals >> add_redshift_partition_task)