import os
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

with DAG(
    dag_id=os.path.basename(__file__).replace(".py", ""),
    # These args will get passed on to each operator
    default_args={
        "retries": 3,
        "email": ["machine-learning-squad@mountain.com"],
        "email_on_failure": True,
    },
    description="mntn-match-incrementals",
    start_date=datetime(2024, 7, 24),
    schedule_interval="0 9 * * *",
    catchup=False,
) as dag:
    aws_region = "us-west-2"
    run_date = """{{ ds }}"""
    yesterday = """{{ yesterday_ds }}"""
    env = Variable.get("ENV")
    dbt_runner_image_name = (
        f"077854988703.dkr.ecr.us-west-2.amazonaws.com/mntn_matched_data_pipeline:{env}"
    )
    openai_batch_runner_image_name = (
        f"077854988703.dkr.ecr.us-west-2.amazonaws.com/openai_batch_runner:{env}"
    )

    default_env_vars = {
        "AWS_DEFAULT_REGION": aws_region,
        "run_date": run_date,
        "env": env,
        "yesterday": yesterday,
    }

    kube_config_path = f"/usr/local/airflow/dags/kube_config-{env}.yaml"

    batch_prep = KubernetesPodOperator(
        task_id="batch_prep",
        name="mntn-matched-data-pipeline-pre",
        image_pull_policy="Always",
        cmds=["dbt", "run", "--select", "models/mntn_matched/pre_batch", "--target", env],
        config_file=kube_config_path,
        cluster_context="aws",
        image=dbt_runner_image_name,
        env_vars=default_env_vars,
    )

    batch_validate = KubernetesPodOperator(
        task_id="batch_validate",
        name="openai_batch_runner",
        image_pull_policy="Always",
        cmds=["python", "validate_files.py"],
        config_file=kube_config_path,
        cluster_context="aws",
        image=openai_batch_runner_image_name,
        env_vars=default_env_vars,
    )

    batch_submit = KubernetesPodOperator(
        task_id="batch_submit",
        name="openai_batch_runner",
        image_pull_policy="Always",
        config_file=kube_config_path,
        cmds=["python", "submit_batch.py"],
        cluster_context="aws",
        image=openai_batch_runner_image_name,
        env_vars=default_env_vars,
    )

    batch_cleanup_1 = KubernetesPodOperator(
        task_id="batch_cleanup_1",
        name="openai_batch_runner_cleanup",
        image_pull_policy="Always",
        cmds=["python", "delete_all_storage_files.py"],
        image=openai_batch_runner_image_name,
        config_file=kube_config_path,
        cluster_context="aws",
        env_vars=default_env_vars,
    )
    batch_cleanup_2 = KubernetesPodOperator(
        task_id="batch_cleanup_2",
        name="openai_batch_runner_cleanup",
        image_pull_policy="Always",
        cmds=["python", "delete_all_storage_files.py"],
        image=openai_batch_runner_image_name,
        config_file=kube_config_path,
        cluster_context="aws",
        env_vars=default_env_vars,
    )

(batch_cleanup_1 >> batch_prep >> batch_validate >> batch_submit >> batch_cleanup_2)
