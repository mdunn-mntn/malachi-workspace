---
name: feedback_no_laptop_crons
description: The debugger and optimizer run in Astro only; no laptop cron or launchd agent may run them (Malachi, 2026-09-06).
metadata:
  node_type: memory
  type: feedback
doc_type: memory
keywords: [laptop cron, launchd, LaunchAgents, com.mntn.daily-spark-optimizer, com.mntn.oncall-daily-rca, oncall_daily_optimizer.sh, oncall_daily_rca.sh, second code copy, drifted duplicate, airflow_optimizer workspace copy, spark_optimizer astro, debugger schedule, no local scheduler]
domain: [workflow, infra, project]
lifecycle: active
last_verified: 2026-09-06
---

**"We don't want laptop crons on this at all"** (Malachi, 2026-09-06). The debugger and optimizer run
**in Astro only**. No launchd agent, no crontab entry, no local scheduler runs either tool.

**Why:** a laptop schedule runs the WORKSPACE copy of the code, which has drifted from the deployed
bundle (the deployed bundle has ten modules the copy lacks; the copy has five the bundle lacks; shared
files differ by up to 309 lines). So it produces a second, different answer to the same question, on a
machine that is asleep half the time. On 2026-09-05 the local diagnosis job wrote 9 diagnoses against
production's 14. The optimizer agent had been crashing at import for eleven days
(`digest.py:16`, `Callable[[str], str] | None` on an old interpreter) AFTER downloading its logs.

**How to apply:** removed 2026-09-06 — `com.mntn.daily-spark-optimizer` (11:00, ran
`.claude/scripts/oncall_daily_optimizer.sh`) and `com.mntn.oncall-daily-rca` (10:00,
`.claude/scripts/oncall_daily_rca.sh`), both unloaded and their plists moved out of
`~/Library/LaunchAgents/`. **Do not recreate either, and do not add a new one.** If a capability needs
a schedule it belongs in an Astro DAG. `com.mntn.daily-gap-check` and `com.mntn.engine.loop` are
unrelated and were left alone.

This is the local-scheduler sibling of the no-API-keys-on-a-server rule.
[[reference_pi5_server]] [[project_airflow_optimizer]] [[project_airflow_debugger]]
