Log each OpenAI batch's status and error on every check; fail batch_fetch on a dead cohort so the next outage shows day one.

What
- batch_status.py (new): per-batch line (status, age, counts, first error), cohort summary, dead-cohort check.
- batch_transitioner.py: logs every batch; finalizing now flags was_submitted.
- batch_fetcher.py, fetch_results.py: raise DeadCohortError when no batch progressed and the youngest is over DEAD_COHORT_MIN_AGE_HOURS old (default 12); existing routing posts Slack and email.
- Dockerfile: PYTHONUNBUFFERED=1.

Why
- Aug 27-30: every batch failed at OpenAI; batch_transition and batch_fetch stayed green.

Validation
- 16 unit tests; isort, flake8, mypy clean.
- Container run on a seeded dead cohort (dev bucket): DeadCohortError, exit 1; higher threshold: warning, exit 0.
- Until AUDI-1301 fixes the org-side error, batch_fetch fails daily by design.
