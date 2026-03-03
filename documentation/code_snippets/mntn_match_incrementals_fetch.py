import os
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.utils.task_group import TaskGroup

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

    batch_transition = KubernetesPodOperator(
        task_id="batch_transition",
        name="openai_batch_runner",
        image_pull_policy="Always",
        cmds=["python", "transition_batch.py"],
        config_file=kube_config_path,
        cluster_context="aws",
        image=openai_batch_runner_image_name,
        env_vars=default_env_vars,
    )

    batch_fetch = KubernetesPodOperator(
        task_id="batch_fetch",
        name="openai_batch_runner",
        image_pull_policy="Always",
        cmds=["python", "fetch_results.py"],
        config_file=kube_config_path,
        cluster_context="aws",
        image=openai_batch_runner_image_name,
        env_vars=default_env_vars,
    )

    with TaskGroup(group_id="batch_post") as batch_post:
        openai_batch_joined = KubernetesPodOperator(
            task_id="openai_batch_joined",
            name="openai_batch_joined",
            image_pull_policy="Always",
            cmds=[
                "dbt",
                "run",
                "--select",
                "openai_batch_results_joined",
                "--target",
                env + "_warehouse_small",
            ],
            image=dbt_runner_image_name,
            config_file=kube_config_path,
            cluster_context="aws",
            env_vars=default_env_vars,
        )

        taxonomy_vector = KubernetesPodOperator(
            task_id="taxonomy_vector",
            name="taxonomy_vector",
            image_pull_policy="Always",
            cmds=[
                "dbt",
                "run",
                "--select",
                "mntn_matched_taxonomy_vector",
                "--target",
                env + "_warehouse_small",
            ],
            image=dbt_runner_image_name,
            config_file=kube_config_path,
            cluster_context="aws",
            env_vars=default_env_vars,
        )

        categorization_temp = KubernetesPodOperator(
            task_id="categorization_temp",
            name="categorization_temp",
            image_pull_policy="Always",
            cmds=[
                "dbt",
                "run",
                "--select",
                "product_categorization_temp",
                "--target",
                env + "_warehouse_small",
            ],
            image=dbt_runner_image_name,
            config_file=kube_config_path,
            cluster_context="aws",
            env_vars=default_env_vars,
        )

        ([openai_batch_joined, taxonomy_vector] >> categorization_temp)

        mm_taxonomy_update = KubernetesPodOperator(
            task_id="mm_taxonomy_update",
            name="mm_taxonomy_update",
            image_pull_policy="Always",
            cmds=[
                "dbt",
                "run",
                "--select",
                "mntn_matched_taxonomy",
                "--target",
                env + "_warehouse_small",
            ],
            image=dbt_runner_image_name,
            config_file=kube_config_path,
            cluster_context="aws",
            env_vars=default_env_vars,
        )
        categorization_temp >> mm_taxonomy_update

        product_categorization = KubernetesPodOperator(
            task_id="product_categorization",
            name="product_categorization",
            image_pull_policy="Always",
            cmds=[
                "dbt",
                "run",
                "--select",
                "product_categorization",
                "--target",
                env + "_warehouse_small",
            ],
            image=dbt_runner_image_name,
            config_file=kube_config_path,
            cluster_context="aws",
            env_vars=default_env_vars,
        )
        mm_taxonomy_update >> product_categorization

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

    with TaskGroup(group_id="batch_test") as batch_test:
        test_mm_taxonomy = KubernetesPodOperator(
            task_id="test_mm_taxonomy",
            name="test_mm_taxonomy",
            image_pull_policy="Always",
            cmds=[
                "dbt",
                "test",
                "--select",
                "mntn_matched_taxonomy",
                "--indirect-selection=buildable",
                "--target",
                env + "_warehouse_small",
            ],
            image=dbt_runner_image_name,
            config_file=kube_config_path,
            cluster_context="aws",
            env_vars=default_env_vars,
        )

        test_product_categorization = KubernetesPodOperator(
            task_id="test_product_categorization",
            name="test_product_categorization",
            image_pull_policy="Always",
            cmds=[
                "dbt",
                "test",
                "--select",
                "product_categorization",
                "--indirect-selection=buildable",
                "--target",
                env + "_warehouse_small",
            ],
            image=dbt_runner_image_name,
            config_file=kube_config_path,
            cluster_context="aws",
            env_vars=default_env_vars,
        )

        test_categorization_temp = KubernetesPodOperator(
            task_id="test_categorization_temp",
            name="test_categorization_temp",
            image_pull_policy="Always",
            cmds=[
                "dbt",
                "test",
                "--select",
                "product_categorization_temp",
                "--indirect-selection=buildable",
                "--target",
                env + "_warehouse_small",
            ],
            image=dbt_runner_image_name,
            config_file=kube_config_path,
            cluster_context="aws",
            env_vars=default_env_vars,
        )

(batch_cleanup_1 >> batch_transition >> batch_fetch >> batch_post >> [batch_test, batch_cleanup_2])
