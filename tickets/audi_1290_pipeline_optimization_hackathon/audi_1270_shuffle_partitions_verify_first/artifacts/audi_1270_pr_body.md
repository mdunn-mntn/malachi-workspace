Raises spark.sql.shuffle.partitions on vertical_size_monitor from 128 to 600, the only one of the 15 DAGs on AUDI-1270 whose event log puts the spill on the shuffle-read side.

What: models/monitoring/vertical_size_monitor.py, decorator and builder 128 to 600 (both sites, as #1231); dags/model_task_config.json regenerated.

Why: stages 11 and 17 read 25.6 GiB of shuffle in 128 tasks and spill 16 GiB to disk from 136 GiB in memory. At 600 each task holds 232 MiB in memory and 44 MiB of shuffle, above the 32 MiB where adaptive coalescing merges partitions back. Of the other 14 DAGs, 10 spill while reading input or BigQuery, a stage the partition count does not govern; guid_log_advertiser_id_dsc_id belongs to AUDI-1269; three no longer spill.

Validation: dryrun clean, tests/models 145 passed, ruff findings pre-exist on main.
