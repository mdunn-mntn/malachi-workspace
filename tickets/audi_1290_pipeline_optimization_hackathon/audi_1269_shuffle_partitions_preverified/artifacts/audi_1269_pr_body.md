Raises `spark.sql.shuffle.partitions` on 6 jobs whose reduce stages spill to disk every run. Config only.

**What**
- `ipdsc_ds_2` 2048 -> 8192, `ipdsc_third_party_audience_builder` 512 -> 2240, `advertiser_score_distribution_monitor` 128 -> 916 (decorator and builder).
- `conversion_log` / `site_visit_signal` / `guid_log` `_advertiser_id_dsc_id`: builder 3508 / 3392 / 3400 (was the 1000 default).
- `dags/model_task_config.json` regenerated.

**Why**
- Reduce stages spill 218 to 814 GiB to disk per run; new counts put each task at 80 to 300 MiB compressed.

**Validation**
- Latest prod event log per job: reducer at configured count, no AQE coalescing, blocks above 13 KiB.
- `model_upload.py --dryrun`; no dev run, same as #1231.
- Left out: `intent_score_map` (blocks under 8 KiB), `prospecting_join` (no spill now), `household_score_distribution_monitor` (driver out-of-memory history).
