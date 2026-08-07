# Optimization finding: `Update Vertical Categorization` chronic Spark skew

**What:** the AUDI-1191 optimizer crawled real production Spark event logs
(`gs://mntn-data-archive-prod/spark-events`) and flagged the `Update Vertical Categorization` model as
the #1 optimization target.

**The problem:** **Stage 0 is severely data-skewed on every run** — max task time is **up to 242x the
median** (and 10-20x on the other runs). One partition holds nearly all the data, so one task runs while
the rest sit idle, stretching wall-clock. The job also shows GC pressure (executors memory-starved).

**The fix (code):**
- Salt the skewed group/join key, or enable AQE skew join: `spark.sql.adaptive.enabled=true` +
  `spark.sql.adaptive.skewJoin.enabled=true`. A plain repartition will NOT fix a value-skewed key.
- Secondary: raise executor memory (or cut per-task data with more partitions) for the GC pressure.

**Impact:** skew this severe means most of the cluster is idle for most of the stage — a direct
wall-clock + cost win once the hot key is spread.

**Owner:** ~~Sean Yang / DDP~~ **CORRECTED 2026-08-07: Ryan Kleck / targeting** — the job is `spark/vertical_classification/update_website_verticals.py` in airflow-ti, launched by the `vertical_classification_fetch` DAG (`JobTeamConfig.TGT`, all commits rkleck). Backlog: IMP-024.

*(Found autonomously by the tool from production event logs — the first real optimization it surfaced.)*

---
**2026-08-07 re-assessment (before owner handoff):**
- **The DAG is `schedule=None` (manual trigger only)** and has no run in the last 2,000 prod batches — this is a when-you-next-run-it note, NOT a chronic daily cost. The "every run" evidence is 2025-era logs; they reached the 2026-08-04 crawl because the weekly cron's newest-N selection is lexically ordered (defect, hardening pass).
- **The 10-242x figure is duration skew from the OLD detector, which could not distinguish a hot partition from an IO-stalled straggler** (the intent_score_map case proved that failure mode the same day). If it's a straggler, the fix is `spark.speculation=true`, not salting. No Update VC event log was locatable in the peeked archive windows to re-discriminate; profile the next manual run with the new `straggler`/`skew` detectors before anyone codes a fix.
- GC-pressure signal stands as reported.
