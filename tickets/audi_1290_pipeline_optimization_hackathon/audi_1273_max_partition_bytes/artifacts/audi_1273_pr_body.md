Lower the input read size on two DAGs that spill while reading; ipdsc_ds_67 dropped, its input files cannot be split.

What
- ipdsc_ds_49: add spark.sql.files.maxPartitionBytes = 64 MiB (default was 128 MiB).
- conv_log_derived_ip: same setting, 256 MiB to 128 MiB.
- ipdsc_ds_67: no change. Its 160 input files hold one 60 MiB row group each, so a smaller read size creates empty tasks, not smaller ones (checked on Spark 3.5.3).

Why (2026-08-05 read stage)
- ipdsc_ds_49: 583 tasks, 47 GiB input, 18 GiB spilled to disk.
- conv_log_derived_ip: 91 tasks, 14.5 GiB input, 5 GiB spilled to disk.

Validation
- model_upload.py --dryrun clean, generated task config unchanged.
- tests/models: 145 passed.
- Post-merge: first scheduled run's event log must show more read tasks, spill near zero.
