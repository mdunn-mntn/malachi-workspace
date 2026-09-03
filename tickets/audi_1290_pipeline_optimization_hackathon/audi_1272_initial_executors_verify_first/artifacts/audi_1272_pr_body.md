Raise initialExecutors on advertiser_mid (90) and ipdsc_42_monitor (7); the other 8 fetch-wait DAGs are unchanged.

**What**
- `models/audience_intent/advertiser_mid.py`: initialExecutors 90 (min 25, max 90).
- `models/monitoring/ipdsc_42_monitor.py`: initialExecutors 7 beside executor.instances 2.
- `dags/model_task_config.json` regenerated (2 lines).

**Why**
- Serverless delivers the scale-up 100 to 160 s after the first backlog; both jobs run their map stages inside that window, so the reduce stages read from 3 to 25 servers while the run reaches 7 to 90.
- The other 8 already spread the map output over the full fleet, or the boot costs more than the wait. Table: `outputs/audi_1272_verdicts.md` in the ticket folder.

**Validation**
- `model_upload.py --dryrun` exit 0; diff is 2 model lines plus 2 JSON lines.
- ruff 0.16.1: 10 findings on both files, identical on main.
