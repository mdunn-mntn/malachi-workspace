# Ryan enablement proposal (DRAFT: Malachi to send on Slack)

> Slack voice, BLUF. Sits in the ticket; not sent by the agent.

---

Hey Ryan — can we turn Spark event logs on for one batch + one dbt model?

It's the one blocker for a Spark optimization tool I built: it reads a job's event log and returns ranked
fixes (query/PR: missing stats, skew, shuffle partitions; compute: memory/on-demand with the reason;
plus real failures to route). Validated on real event logs.

Completed jobs don't expose the plan/metrics any other key-free way (driver proxy is running-only, no log
delivery, get-run-output has none). Turning event logs on fixes it:

- Dataproc: `spark.eventLog.enabled=true` + a GCS `spark.eventLog.dir` in the batch properties (the
  framework already warns it's off). Add `logBlockUpdates.enabled=true` for cache stats.
- Databricks: `cluster_log_conf` on the job cluster, or enforce it on the cluster policy.

Storage is cheap. Once it's flowing I point the crawler at the GCS prefix for a ranked cross-job backlog.
No prod code from me, just the flip, your call where. Config + rationale:
`audi_1191_optimization_data_enablement.md`.
