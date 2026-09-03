The BigQuery profiler now sums top-level jobs only, which ends a double count of script jobs.

What: `PROFILE_SQL` in `include/spark_optimizer/bq_profile.py` adds `AND parent_job_id IS NULL`; one new test asserts it.

Why: a script's parent row already carries its children's slot-ms and bytes, and the children are rows too, so `campaign_summary_hourly-create` and `population_histogram` were counted about twice. On deploy the BigQuery cost table drops about 40 percent for those two tasks with no real saving. Both stay above the 50 slot-hour finding threshold, so nothing resolves falsely.

Validation: corrected SQL for 2026-09-02 gives `campaign_summary_hourly-create` 96 jobs, 517 slot-hours (report said 240 jobs, 845) and `population_histogram` 1 job, 528 (report said 4 jobs, 1,057). Tests: 164 passed, 1 pre-existing `test_phs.py` failure unrelated. AUDI-1277.
