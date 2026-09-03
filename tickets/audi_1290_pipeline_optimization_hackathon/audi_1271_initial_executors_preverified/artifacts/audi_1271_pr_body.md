initialExecutors 100 to 200 on aug_log_ip_vertical_id_hourly. Adds cost, removes no fetch wait; shipped for the optimizer ledger record.

**What**
* `aug_log_ip_vertical_id_hourly.py` L72: initialExecutors 100 to 200 (maxExecutors is already 200). `dags/model_task_config.json` regenerated with `model_upload.py --dryrun`.

**Why**
* Stage 11 waits 27 to 53% on shuffle fetch in all 20 runs profiled (2026-08-31 to 09-03).
* In 12 of 20 runs the driver prologue (runtime pip install) outlasts the 60 s idle timeout; the fleet is 50 by the map stage, and 200 gets the same cut.

**Validation**
* Cost: 100 extra idle executors for 60 s, 1.7 executor-hours (17 DCU-hours, 12% per run) against 0.03 to 0.13 executor-hours of stage 11 wait.
* DCU-hours per run up after 7 days: revert. Reviewer: Ryan Kleck.
