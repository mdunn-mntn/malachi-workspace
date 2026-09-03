Caps the size Spark's adaptive query execution merges shuffle partitions to at 16 MiB on the two guid pivot models, so the pivot stage stops spilling.

**What:** one builder line per model, `spark.sql.adaptive.advisoryPartitionSizeInBytes=16m`, in guid_log_pivot_ip_vertical_id and guid_conv_log_pivot_ip_vertical_id. No DAG or schedule change.

**Why:** the merge target today is total shuffle bytes divided by registered cores (800), so raising shuffle.partitions changes nothing. The pivot stage reads 49.1 GiB in 800 tasks and spills 862.5 GiB to memory and 19.0 GiB to disk per run (08-26 event logs). The cap is session wide: every merged shuffle goes from 800 partitions to 1,100 to 4,000.

**Validation:** model_upload.py --dryrun passes, generated config unchanged, no new ruff findings. Next scheduled run must show 3,100 to 4,000 pivot tasks and zero disk spill.
