# Ryan enablement proposal (DRAFT — Malachi to send on Slack)

> A short message to unblock the optimization half of AUDI-1191. Human/Slack voice, no bullets-heavy
> Jira shape. Sits in the ticket so it's ready to send; not sent by the agent.

---

Hey Ryan — quick ask that unblocks the Spark optimization side of the debugger.

I built the analyzer that reads a job's Spark event log and spits out ranked recommendations across
three buckets: query/PR fixes (missing stats → ANALYZE TABLE, de-skew, right-size shuffle partitions),
compute fixes (bump memory/on-demand, with the reason), and real failures to route. I validated it on
real Spark event logs — it correctly catches skew, spill, spot-preemption cost, cache eviction, and pulls
the per-operator SQL metrics.

The one thing missing is the fuel. Completed job clusters don't expose the plan/metrics anywhere I can
read key-free (driver proxy is running-only, no log delivery, get-run-output has none of it). The fix is
just turning event logs on:

- Dataproc: set `spark.eventLog.enabled=true` + a GCS `spark.eventLog.dir` (+ `logBlockUpdates.enabled=true`
  for cache stats) in the batch properties. The framework already warns when it's off.
- Databricks: add `cluster_log_conf` to the job cluster (or enforce it on the cluster policy) so the
  event log lands in GCS after the run.

Could we pilot it on one low-risk batch and one dbt model? Event-log storage is cheap; I'd flag any
history-server cost to Zach before standing one up. Once it's flowing I point the crawler at the GCS prefix
and we get a ranked cross-job backlog of the biggest wins. Full config + rationale is in the ticket
(`audi_1191_optimization_data_enablement.md`). No prod code from me — just the config flip, your call on
where.
