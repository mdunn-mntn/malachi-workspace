---
name: feedback_slack_digest_not_per_event
description: Automated Slack posters batch to one digest parent + threaded replies with duplicate collapsing — per-event channel posts read as spam (user feedback 2026-08-31).
metadata:
  node_type: memory
  type: feedback
doc_type: memory
keywords: [slack digest, spammy channel, per-event posts, threaded replies, duplicate collapse, bot posting shape, airflow-debugger channel, fallback channel digest, PR 1251, cluttered channel, rich_text_list ordered, ranked rows alignment, mrkdwn numbers ragged, PR 1260]
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
- Ranked digest rows must ALIGN in the Slack client, and the ONLY construct that aligns is a `rich_text` block with `rich_text_list style=ordered` — the client renders the numbers in the gutter and hanging-indents wrapped lines. Hand-numbered mrkdwn (`*1.*`) + emoji prefixes can NEVER align (variable emoji width shifts the numbers - user ask 2026-09-02). Shipped in airflow-ti PR #1260 commit `dd53939`, preview confirmed by the user 2026-09-02. Trade: context-block small-grey styling is not expressible inside `rich_text`, so the meta line became italic text. This is the reusable recipe for ANY bot ranked list.
- Applies to any bot/poster I build, not just the debugger. [[project_airflow_debugger]] [[feedback_slack_channel_one_liners]]
