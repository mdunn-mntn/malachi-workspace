Python-client BigQuery jobs from the attribution DAGs now carry the airflow-dag and airflow-task labels the BigQuery operator writes, so the optimizer's BigQuery report attributes them.

**What**
- New `include/util/bq_job_labels.py`: `airflow_job_labels()` returns the label pair from the task context, or `{}` outside a task.
- `url_pattern_pipeline.py`: both query helpers take `labels=` and default to the task's labels.
- tmobile ip and guid workflows: labels on the advertiser query.
- gaclid flag, blocked ip and guid exports: labels through `get_df(configuration=...)`.

**Why**
About 150 airflow-ti jobs a day show as unattributed. The transform mirrors the provider's, so operator and client jobs share a key.

**Validation**
- pytest, two modules: 34 passed, 2 failed. Both failures exist on main (the AST test resolves the repo root one level too high).
- ruff: nothing new.
