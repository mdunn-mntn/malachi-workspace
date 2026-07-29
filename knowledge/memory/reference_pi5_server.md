---
name: raspberry-pi-5-server
description: "Pi 5 SSH access, credentials, and services running on it (Slack knowledge bot, Pi-hole)"
metadata: 
  node_type: memory
  type: reference
  originSessionId: 6b830d36-17fc-4962-b8c0-c9c838b6e689
doc_type: memory
keywords: [pi5 server, raspberry pi, pihole5, ssh key, slack knowledge bot, workflow audit cron, deploy key, unbound, decommissioned bot, tailscale]
domain: [infra]
lifecycle: active
last_verified: 2026-07-24
---
## Raspberry Pi 5

- **Hostname:** pihole5 / pihole5.local
- **User:** pi5
- **SSH from Mac:** `ssh -i ~/.ssh/pi5 pi5@192.168.10.177` (local) or `ssh -i ~/.ssh/pi5 pi5@100.107.165.3` (Tailscale)
- **SSH key:** `~/.ssh/pi5` (generated 2026-04-08, comment: malachi-mac-to-pi5)
- **GitHub deploy key:** `pi5-slack-bot` (read-write, on mdunn-mntn/malachi-workspace)
- **OS:** Debian 12 (Bookworm), aarch64
- **Python:** 3.11.2, venv at `~/slack_bot_env`
- **Disk:** 59GB total, ~52GB free

## Services Running

### Slack Knowledge Bot — DECOMMISSIONED 2026-06-10 (do NOT recreate)
- **Retired by security policy.** Robin Fox confirmed MNTN no longer allows local Slack apps or API keys in local env. App was deliberately deleted (not an accident). Recreating it just gets it deleted again. Compliant replacement = rebuild as a **Compass** agent (Harvey Yau's group). See `slack_bot/RECOVERY.md` (reframed to migration) + `knowledge/mntn_business.md` security-policy note.
- **Cron DISABLED** 2026-06-10 (line commented in crontab). Code kept as reference for the Compass port. Last good run 2026-06-10 00:00 (commit 22043f6) — no data lost.
- **Wrapper:** `~/run_slack_bot.sh` — sources env, activates venv, pulls latest code, runs pipeline
- **Code:** `~/workspace/slack_bot/`
- **Env vars:** `~/slack_bot_env.sh` (chmod 600) — SLACK_BOT_TOKEN, ANTHROPIC_API_KEY
- **Logs:** `~/workspace/slack_bot/logs/cron.log`
- **Venv:** `~/slack_bot_env/`

### Workflow Audit — System-retro loop (ACTIVE, key-free) — added 2026-07-24
- **Cron:** `0 8 * * 1` (Mon 08:00 America/Los_Angeles) → `~/run_workflow_audit.sh`. MAILTO="" set.
- **What it does:** git pull → runs `.claude/scripts/workflow_audit.sh` (the DETERMINISTIC aggregator — pure Python + git, **NO API key, NO model**) → commits a dated `claude-prompts/workflow_audits/signals_<date>.md` → pushes via deploy key. flock-locked, log at `~/workflow_audit.log`.
- **Compliant by design:** no Claude credential on the Pi (that is what got the Slack bot killed). The reasoning/report half runs on the **Mac** via `/workflow-audit` (reads the signals, writes `audit_<date>.md`). See [[reference_workflow_audit_loop]].
- **Git auth on the Pi:** repo at `~/workspace`, remote `git@github.com:mdunn-mntn/malachi-workspace.git`; auth via `~/.ssh/config` (`Host github.com` → `IdentityFile ~/.ssh/github_pi5`, the `pi5-slack-bot` deploy key). git user = Malachi Dunn / malachi@mountain.com. TZ = America/Los_Angeles (cron fires in local time).
- **Source of truth:** `.claude/scripts/pi_run_workflow_audit.sh` in the repo; deployed OUTSIDE the checkout (self-pull safety). Update = edit repo copy, `scp` to `~/run_workflow_audit.sh`.

### Pi-hole + Unbound
- **Admin:** http://192.168.10.177/admin
- **DNS resolver:** Unbound on 127.0.0.1#5335

## Also on the network: Raspberry Pi Zero 2 W
- **Hostname:** pihole / pihole.local
- **User:** pi
- **Local IP:** 192.168.10.129
- **Tailscale IP:** 100.122.89.40
- **Role:** Original Pi-hole (still running)

## Credentials file
Full credentials at `/Users/malachi/Downloads/credentials.txt`
