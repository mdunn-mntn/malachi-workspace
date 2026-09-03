`bos__spend` skips rebuilding `flight_metrics_per2388` when its inputs are unchanged, and `population_histogram` dedups on an INT64 key. Reviewers: @SteelHouse/pacing and @SteelHouse/performance-ml (CODEOWNERS).

What: a fingerprint task (all_facts last modified, active flight rows, date), a short-circuit against Airflow Variable `bos__flight_metrics_per2388_source_fingerprint`, and a recorder after `create`; a missing Variable or a failed fingerprint query rebuilds. `campaign_performance.drop` gets `trigger_rule=none_failed`. The histogram SQL groups on `FARM_FINGERPRINT(ip)`; output unchanged.

Why: the rebuild recomputes an unchanged 14 TiB result 96 times a day (945 slot-hours); about half the runs now skip. The histogram saves 31 percent slot time on a pinned one-hour A/B.

Validation: DagBag parse in the Airflow 3.3.0 venv clean, `pre-commit` clean, SQL dry-runs valid. AUDI-1277.
