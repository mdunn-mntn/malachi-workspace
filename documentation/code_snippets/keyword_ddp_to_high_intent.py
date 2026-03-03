import os
from datetime import datetime

from job_config import JobTeamConfig

from airflow import DAG
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

TEAM = JobTeamConfig.TGT.value

with DAG(
    dag_id="keyword_ddp_to_high_intent",
    description="DDP Domains to high intent",
    start_date=datetime(2024, 11, 17),
    schedule_interval="0 15 * * *",
    max_active_runs=1,
    catchup=True,
    **TEAM.make_dag_args(severity=5, tags=[], default_args={"retries": 1, "max_active_runs": 1}),
) as dag:
    aws_region = "us-west-2"
    run_date = """{{ ds }}"""
    yesterday = """{{ yesterday_ds }}"""
    env = Variable.get("ENV")

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

    write_valid_domains = KubernetesPodOperator(
        task_id="write_valid_domains",
        name="write_valid_domains",
        image_pull_policy="Always",
        cmds=[
            "dbt",
            "build",
            "--select",
            "models/vertical_categorization/verticals_valid_domains.py",
            "--target",
            env,
        ],
        config_file=kube_config_path,
        cluster_context="aws",
        image=dbt_runner_image_name,
        env_vars=default_env_vars,
    )

    write_domain_mappings = KubernetesPodOperator(
        task_id="write_domain_mappings",
        name="write_domain_mappings",
        image_pull_policy="Always",
        cmds=[
            "dbt",
            "build",
            "--select",
            "models/vertical_categorization/domain_vertical_mappings.py",
            "--target",
            env,
        ],
        config_file=kube_config_path,
        cluster_context="aws",
        image=dbt_runner_image_name,
        env_vars=default_env_vars,
    )

    (write_valid_domains >> write_domain_mappings)
