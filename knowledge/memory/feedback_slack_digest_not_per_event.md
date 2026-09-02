---
name: feedback_slack_digest_not_per_event
description: Automated Slack posters batch to one digest parent + threaded replies with duplicate collapsing — per-event channel posts read as spam (user feedback 2026-08-31).
metadata:
  node_type: memory
  type: feedback
doc_type: memory
keywords: [slack digest, spammy channel, per-event posts, threaded replies, duplicate collapse, bot posting shape, airflow-debugger channel, fallback channel digest, PR 1251, cluttered channel]
domain: [workflow, infra]
lifecycle: active
last_verified: 2026-09-02
---
**An automated poster gets ONE digest parent per run, threads the detail, and collapses duplicates — never one channel post per event.** Malachi's feedback on `#airflow-debugger` (2026-08-31): the per-event fallback posts were too spammy/cluttered.

**Why:** a channel of one-post-per-event trains readers to skip the whole channel — the same mechanism as the cost-report-next-to-pages rule in [[reference_slack_debugger_app]].

**How to apply:**
- One parent message per sweep/run; every item is a threaded reply under it.
- Collapse duplicates: the debugger folds `(dag, task, signature)` repeats into a counted line + a single reply (PR #1251, the user-confirmed shape).
- Write exactly-once markers only AFTER the group reply lands, so a failed digest retries instead of dropping.
- Ranked digest rows must ALIGN in the Slack client: emoji + number prefixes render ragged (variable emoji width shifts the numbers - user ask 2026-09-02). Reformat queued for the post-merge digest pass.
- Applies to any bot/poster I build, not just the debugger. [[project_airflow_debugger]] [[feedback_slack_channel_one_liners]]
