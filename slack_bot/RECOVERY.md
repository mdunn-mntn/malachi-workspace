# Slack Knowledge Bot — DECOMMISSIONED (migrate to Compass)

> **STATUS 2026-06-10: This local-Pi architecture is retired by security policy. Do NOT recreate it.**
> Robin Fox (security) confirmed the app deletion was intentional: MNTN is **no longer
> allowing local Slack apps or API keys held in local env**. The recreated app
> (App ID `A0B9MCD6Y6R`) was deleted again. The compliant replacement is to rebuild the
> nightly knowledge-extraction as a **Compass** agent (MNTN's internal AI/MCP platform —
> contact Harvey Yau's group; see `knowledge/mntn_business.md` "Atlas Code MCP / MNTN Prod MCP").
>
> Pi cron disabled 2026-06-10 (line commented in crontab). Code kept as reference for the
> Compass port. Last good run: 2026-06-10 00:00 (commit `22043f6`). No data lost.

## Migration to Compass (replaces the steps below)
The pipeline logic is unchanged — only the host + auth model move:
- **Scrape** ([scraper.py](scraper.py)) → Compass reads Slack via its sanctioned, centrally-managed
  integration instead of a local bot token. No `xoxb` token in local env.
- **Extract** ([extractor.py](extractor.py)) → same Claude prompt; runs as a Compass agent task.
- **Update** ([updater.py](updater.py)) → still commits to `knowledge/*.md` (confirm Compass has repo write access).
- **Open question:** does Compass support a scheduled (nightly) agent run + git push? Confirm with Harvey Yau.

---

## Historical recovery steps (OBSOLETE — kept for reference only; do not follow)

**Original context:** On 2026-06-10 the "Knowledge Extractor" Slack app was deleted.
Initially believed to be accidental cleanup of departed-employee bots; Robin Fox later
clarified it was a deliberate policy enforcement (no local Slack apps / no local-env keys).

## Recovery steps

1. **Recreate the app** — api.slack.com/apps → Create New App → *From a manifest* →
   pick the MNTN workspace → paste `app_manifest.yaml`. Needs the same admin approval
   flow Dustin/Jason use (Install to Workspace → "sent for review" → Jason Whiting approves).
   Speed it up via an ITS support ticket (Tim Harrison's tip).

2. **Grab the token** — once approved & installed: OAuth & Permissions →
   copy **Bot User OAuth Token** (`xoxb-...`).

3. **Update the Pi** — `ssh -i ~/.ssh/pi5 pi5@192.168.10.177`, edit `~/slack_bot_env.sh`
   (chmod 600), replace `SLACK_BOT_TOKEN`. Verify:
   `source ~/slack_bot_env.sh && curl -s -H "Authorization: Bearer $SLACK_BOT_TOKEN" https://slack.com/api/auth.test`
   → expect `"ok":true` with the Knowledge Extractor bot_id.

4. **Re-invite to channels** — a new app starts in zero channels. In each channel:
   `/invite @Knowledge Extractor`. The 22 channels it was in (2026-06-10):

   ask-incremental-lift-tests, chapter-data-analytics, chapter-data-engineering,
   data-platform, dev_fangorn-model_ex, dev-incremental-lift, dev-platform-discussion,
   engineering-culture, engineering-team, fangorn_launch_day, iamt-x-departments,
   identity_core, identity_core_dev, incremental-lift-stakeholders, mission-control,
   production-ops, q1-2026-performance-churn-investigation-..., reporting_helpdesk_ask_anything,
   sales, targeting_helpdesk_ask_anything, targeting-squad, tgt-infrastructure-squad

5. **Smoke test** — on the Pi: `~/run_slack_bot.sh` (or `python -m slack_bot.run_daily`).
   Confirm it scrapes, extracts, and pushes a commit. Cron (`0 0 * * *`) resumes automatically.

## Prevention
- Register the app under a team/shared owner, not a personal account, so the next
  departed-employee sweep doesn't catch it.
- Don't reuse another app's token (the bad pattern Dustin flagged). One app = one token.
