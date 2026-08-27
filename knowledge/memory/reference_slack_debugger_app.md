---
name: reference_slack_debugger_app
description: The Airflow Failure Debugger Slack app - scopes, bot identity, channel IDs, and why the alert channels need groups:* rather than channels:*.
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [slack, airflow-debugger, SLACK_BOT_TOKEN, SLACK_ALERT_CHANNEL, OPTIMIZER_SLACK_CHANNEL, groups:history, alerts-tpa-pipeline, monitor-tpa, C08CURMGNMQ, AUDI-1191, robin fox]
domain: [infra, workflow]
lifecycle: active
last_verified: 2026-08-27
---

**App "Airflow Failure Debugger", bot `@airflow-debugger` (`U0BTU0FA8N4`), created 2026-08-26, approved by Robin Fox.** Bot token scopes: `chat:write`, `channels:history`, `channels:read`, `groups:history`, `groups:read`. No user token scopes.

**Both alert channels are PRIVATE, which is the trap.** `#alerts-tpa-pipeline` (`C08CURMGNMQ`) and `#monitor-tpa` (`C067ZM2EC5S`) look public but are not, so `channels:history` / `channels:read` do not reach them and every call returns `channel_not_found` or `missing_scope`. Only `groups:history` is load-bearing for the code (threading via `conversations.history`); `groups:read` is for verifying membership before anything posts.

**Diagnosing the failure mode:** `channel_not_found` = the bot is NOT in the channel. `missing_scope` = the bot IS in the channel but lacks the scope. The error flipping from the first to the second is how you confirm an invite landed. Approval is not installation, and installation is not membership: all three are separate steps.

**The optimizer digest reuses this same app.** No second Slack app was needed: `chat:write` already
covers it. The digest posts to **`#spark-optimizer` (`C0BSTH6E84T`)**, a PRIVATE channel created
2026-08-26 for it, so `groups:*` is again the load-bearing scope. Deliberately not
`#alerts-tpa-pipeline`: a daily cost report next to `*FAILURE*` pages trains people to skip both.
`airflow_optimizer/notify.py` reads `SLACK_BOT_TOKEN` + `OPTIMIZER_SLACK_CHANNEL` and posts Block
Kit: the parent is the ranked DAG list, each DAG's fix is a threaded reply.

**`notify.py` reads one `SLACK_ALERT_CHANNEL`.** Two channels needs a code change (open as of 2026-08-26). `#monitor-tpa` carries forwarded emails, not Airflow alerts; the real `*FAILURE*` posts are in `#alerts-tpa-pipeline`, and `vertical_classification_api` does not alert there at all.

**Env var trap (found live 2026-08-27):** the DEBUGGER reads `SLACK_ALERT_CHANNEL`, the OPTIMIZER reads `OPTIMIZER_SLACK_CHANNEL` — separate vars, do not confuse. `SLACK_ALERT_CHANNEL` was missing from the Astro deployment and debugger delivery silently did not post; after it was added, end-to-end delivery verified in prod 2026-08-27 (3 diagnoses posted, threaded).

**Token storage:** an Astro deployment env var marked secret on airflow-ti, plus Vault. Locally it is `security find-generic-password -s slack_bot_token -w`, never a dotfile. See [[reference_pi5_server]] for why a local key is banned but a prod-held token is not, and [[reference_anthropic_api_key_keychain]] for the same pattern.
