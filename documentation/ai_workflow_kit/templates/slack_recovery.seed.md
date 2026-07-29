# Slack Knowledge Bot — setup + recovery

Generic skeleton for the optional nightly Slack knowledge-extraction bot. Job-neutral; wire it to your own
Slack workspace and model provider, or delete `slack_bot/` if you don't want it.

## Setup
1. Create a Slack app (see `app_manifest.yaml`), install it to your workspace, invite it to the channels you
   want extracted.
2. Put your secrets in an env file (e.g. `~/slack_bot_env.sh`, `chmod 600`): `SLACK_BOT_TOKEN` and your model
   provider key. Never commit these.
3. `pip install -r slack_bot/requirements.txt`.
4. Schedule `run_daily.py` on an always-on host (`setup_cron.py` writes the schedule).

## What it does
Scrapes the configured channels, extracts durable knowledge with an LLM, and appends candidates to a review
queue in your knowledge base. Low-confidence items go to a manual review queue rather than straight into docs.

## Recovery
- Bot not running → check the env file is sourced and the token is valid; re-run `setup_cron.py`.
- Missing a day → re-run `run_daily.py` for the date; extraction is idempotent per channel/day.
- Decommission → remove the schedule and revoke the Slack token.

## Compliance
Keep extraction to channels you're authorized to read. If your org provides a sanctioned knowledge/AI
platform, prefer adopting it over a personal nightly scraper.
