# Knowledge Base — MNTN Data Documentation

This folder is the **shared, org-accessible** data documentation layer. It lives in the git repo so all team members and Claude sessions have access.

## Files

| File | Purpose |
|------|---------|
| `data_catalog.md` | Table-level reference: schemas, partitions, clustering, join keys, query tips, known gotchas per table |
| `data_knowledge.md` | Tribal knowledge, business logic, architecture patterns, disambiguation, and cross-ticket insights |
| `mntn_business.md` | General MNTN business knowledge — products, strategy, org structure, industry context, terminology |

## Rules

- **These are living documents.** Update them whenever a ticket reveals new schema knowledge, gotchas, or business logic.
- **Propose before writing.** When working with Claude, show the proposed update and get approval before committing.
- **Authoritative copy lives here.** This git repo is the source of truth. Claude's memory files are kept in sync automatically.
- **Every ticket's `summary.md` should reference what was added here** in its "Data Documentation Updates" section.

## Update Protocol (for Claude sessions)

At the end of any BigQuery investigation:
1. Review findings for new table knowledge, join key confirmations, or gotchas
2. Draft proposed additions to `data_catalog.md` or `data_knowledge.md`
3. Show the user the proposed additions
4. Write to these files after approval
5. Commit with the ticket ID: `git commit -m "TI-XXX: update data_catalog with <table> findings"`

## Slack Knowledge Extraction Bot

A passive bot scrapes Slack channels nightly and uses Claude API to extract institutional knowledge into these docs automatically.

**How it works:**
- Runs at **midnight PST** on a Raspberry Pi 5 (`pihole5.local`)
- Scrapes all channels the bot is invited to → extracts knowledge via Claude Sonnet → updates these docs → commits and pushes
- **High confidence** items are auto-applied with `<!-- slack-extracted: YYYY-MM-DD -->` markers
- **Medium confidence** items go to `slack_review_queue.md` for manual review
- Each entry includes attribution: who said it, which channel, what date

**To add a new channel:** In Slack, type `/invite @Knowledge Extractor` in the channel. It will be auto-discovered on the next run.

**Code:** `slack_bot/` in the workspace root. Config: `slack_bot/config.yaml`.

**Logs:** `ssh -i ~/.ssh/pi5 pi5@192.168.10.177 'tail -30 ~/workspace/slack_bot/logs/cron.log'`

**Raw data:** `knowledge/slack_raw/` (gitignored) — daily JSON dumps of scraped messages.

## Entry Templates

See `data_catalog.md` header for the table entry format.
See `data_knowledge.md` header for the knowledge entry format.
