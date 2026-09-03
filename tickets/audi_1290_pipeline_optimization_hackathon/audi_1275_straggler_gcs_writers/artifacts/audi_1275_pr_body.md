Spark speculation on site_network_hourly, the canary for the 13 straggler DAGs (AUDI-1275).

**What**
- `spark.speculation=true` on the model; `dags/model_task_config.json` regenerated.

**Why**
- The model writes through Hadoop's file committer v2 under Spark's commit coordinator: one attempt per partition commits, the rest are denied and delete their files.
- audience_intent scoring batches have run this since Aug 2025: 38 clean runs last week, 454 duplicates killed, no failures.
- The Nov 2025 FileNotFound incident was in the manifest committer's rename phase, not used here.

**Validation**
- `model_upload.py --dryrun` clean, config diff is one key; ruff adds no finding; no local dev batch.
- After merge: property in the batch log, `_SUCCESS` and files per hour in the 7-day band, no commit errors.

Reviewer: @rkleck-mntn. All 13 verdicts: AUDI-1275 `audi_1275_decision_memo.md`.
