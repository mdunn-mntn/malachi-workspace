Broadcast the JDBC joins on four DAGs and compute the DS42 monitor's comparison once, not three times.

What
- Three feature-store models: F.broadcast() on both joins.
- ipdsc_42_monitor: BROADCAST hint on the deal join, comparison cached once.

Why
JDBC tables carry no size statistics; the planner shuffles the big side on the join key. AQE later broadcasts it, but the aggregate inherits the skewed shuffle; AQE skew handling never fires on broadcast joins.

Validation
- 15 prod event logs: flagged stages 3-15x data-skewed.
- Local Spark 3.5.3: plans shuffle on the full aggregate key; split SQL returns identical rows.
- ruff and model_upload.py --dryrun clean; model_task_config.json unchanged.
- Not run on dev (needs a push to the shared dev branch); first prod cron validates.
