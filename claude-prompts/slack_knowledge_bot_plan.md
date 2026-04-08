# Slack Knowledge Bot — Implementation Plan Prompt

Use this prompt in a new LLM session to build the Slack bot end-to-end.

---

## Prompt

You are helping me build a **Slack Knowledge Extraction Bot** for my workplace (MNTN). The bot will passively collect messages from Slack channels I care about, run them through an LLM to extract valuable institutional knowledge, and automatically update my local knowledge documentation files.

### Context

I maintain a personal workspace at `/Users/malachi/Developer/work/mntn/workspace/` that contains knowledge documentation files I've been building over time. These docs capture tribal knowledge about our data stack, business logic, architecture patterns, gotchas, and general company knowledge. The problem is that **tons of valuable information gets shared in Slack conversations and is lost** — people answer questions about table schemas, clarify business logic, share architectural decisions, debug data issues, etc. I want to capture all of that automatically.

### Existing Knowledge Docs (target files for updates)

| File | Purpose | What Slack might add |
|------|---------|---------------------|
| `knowledge/data_catalog.md` | Table schemas, partitions, join keys, query tips, gotchas per table | New tables discovered, schema changes, partition info, performance tips shared in data channels |
| `knowledge/data_knowledge.md` | Business logic, tribal knowledge, architecture patterns, disambiguation | How fields are used, why things work certain ways, edge cases people discover, data quality issues |
| `knowledge/mntn_business.md` | MNTN products, strategy, org structure, industry context, terminology | Org changes, product updates, strategy shifts, new terminology, team responsibilities |
| `knowledge/experimentation.md` | Experiment methodology, covariate selection, test design lessons | Experiment results shared, methodology discussions, statistical approaches mentioned |
| `knowledge/strategic_north_star.md` | Q2 OKRs, leadership priorities, team direction | Priority shifts, new initiatives announced, leadership direction changes |

A new file may also be created if a category of knowledge doesn't fit the above (e.g., `knowledge/slack_insights.md` for miscellaneous but valuable tidbits).

### What I Need You To Build

#### 1. Slack Bot / App Setup

- Walk me through creating a Slack App in the MNTN workspace
- The bot needs these OAuth scopes:
  - `channels:history` — read messages from public channels
  - `channels:read` — list channels
  - `groups:history` — read messages from private channels (if I'm invited)
  - `groups:read` — list private channels
  - `users:read` — resolve user IDs to names
- The bot will be added to specific channels (I'll configure which ones)
- It should be a **passive reader only** — it never posts messages, never reacts, never interacts. Invisible.
- Store the bot token and any secrets in environment variables (never hardcoded)

#### 2. Daily Message Scraper

- A Python script that runs once per day (via cron or launchd on my Mac)
- For each configured channel:
  - Fetch all messages from the last 24 hours (using Slack's `conversations.history` API with `oldest`/`latest` timestamps)
  - Include thread replies (use `conversations.replies` for any message with `reply_count > 0`)
  - Resolve user IDs to display names (cache the user list to avoid rate limits)
  - Store raw messages as structured JSON: `{channel, timestamp, user, text, thread_ts, replies: [...]}`
- Save the raw daily dump to: `knowledge/slack_raw/YYYY-MM-DD.json` (gitignored — raw Slack data should not be committed)
- Handle Slack API rate limits gracefully (respect `Retry-After` headers)
- Handle pagination for channels with many messages

#### 3. LLM Knowledge Extraction

- Take the daily message dump and send it through the **Claude API** (Anthropic) for knowledge extraction
- I have an Anthropic API key — use the `anthropic` Python SDK
- The extraction prompt should:
  - Analyze all messages from the day across all channels
  - Identify **only high-value institutional knowledge** — ignore:
    - Personal conversations, greetings, social chat
    - Ticket-specific discussions that only matter for that ticket
    - Meeting scheduling, time-off notices, HR stuff
    - Messages that are just questions without answers
  - Extract knowledge in these categories:
    - **Data/Schema**: table names, column meanings, join keys, partitions, gotchas, data quality issues, SQL tips
    - **Business Logic**: how things work, why decisions were made, what flags/fields mean, edge cases
    - **Architecture**: system design, pipeline behavior, service interactions, deployment patterns
    - **Product/Strategy**: product changes, feature launches, org changes, strategic direction
    - **Methodology**: analytical approaches, experiment design, statistical methods
  - For each extracted item, output:
    - `category` (maps to which knowledge doc to update)
    - `content` (the actual knowledge, written as a clean documentation entry — not a Slack quote)
    - `confidence` (high/medium/low — how certain is this real institutional knowledge vs. opinion/speculation)
    - `source` (channel + date, for traceability — but NOT the person's name, keep it depersonalized)
    - `existing_section` (if this updates an existing section in the target doc, name it; otherwise null for append)
  - Only extract items with **high or medium confidence**
  - Deduplicate against knowledge that likely already exists in the docs (the prompt should include summaries of current doc contents)

#### 4. Knowledge Doc Updater

- Take the LLM extraction output and apply updates to the actual knowledge files
- Rules:
  - **Never overwrite existing content** — only append or update specific sections
  - New entries should follow the existing format/style of each doc (read the doc headers for format templates)
  - Add a small marker to auto-added entries so I can review them: `<!-- slack-extracted: YYYY-MM-DD -->`
  - Group related items together rather than appending one-by-one
  - If an item contradicts existing documentation, flag it for manual review rather than auto-updating (write to `knowledge/slack_review_queue.md`)
- After updates, auto-commit with message: `knowledge: slack extraction YYYY-MM-DD — N items added`
- Push to remote

#### 5. Review Queue

- Create `knowledge/slack_review_queue.md` for items that need human review:
  - Contradictions with existing docs
  - Low-confidence but potentially valuable items
  - Items that could update multiple docs
  - Anything the LLM is unsure about
- I'll review this file periodically and either approve (move to the right doc) or dismiss

#### 6. Configuration

Create a config file at `knowledge/slack_bot_config.yaml`:

```yaml
# Channels to monitor (add channel IDs after setup)
channels:
  - id: "C_XXXXXXX"
    name: "data-engineering"
    priority: high
  - id: "C_XXXXXXX"
    name: "analytics"
    priority: high
  # ... more channels

# Knowledge extraction settings
extraction:
  model: "claude-sonnet-4-6-20250514"  # Use Sonnet for cost efficiency on daily bulk processing
  min_confidence: "medium"
  max_tokens_per_batch: 100000

# Schedule
schedule:
  time: "06:00"  # Run at 6 AM daily
  timezone: "America/Los_Angeles"

# Paths
paths:
  workspace: "/Users/malachi/Developer/work/mntn/workspace"
  raw_data: "knowledge/slack_raw"
  review_queue: "knowledge/slack_review_queue.md"
```

#### 7. Project Structure

```
workspace/
├── slack_bot/                    # NEW — all bot code lives here
│   ├── README.md                 # Setup instructions, how to configure
│   ├── requirements.txt          # Python dependencies
│   ├── config.yaml               # Channel list, settings (symlink to knowledge/slack_bot_config.yaml)
│   ├── scraper.py                # Slack API message fetcher
│   ├── extractor.py              # LLM knowledge extraction
│   ├── updater.py                # Knowledge doc updater
│   ├── reviewer.py               # Review queue manager
│   ├── run_daily.py              # Orchestrator — runs scrape → extract → update pipeline
│   ├── setup_cron.py             # Helper to install the launchd/cron job
│   └── utils/
│       ├── slack_client.py       # Slack API wrapper with rate limiting
│       ├── doc_parser.py         # Parse existing knowledge docs to avoid duplicates
│       └── git_ops.py            # Auto-commit and push
├── knowledge/
│   ├── slack_raw/                # NEW — daily JSON dumps (gitignored)
│   ├── slack_review_queue.md     # NEW — items needing human review
│   ├── slack_bot_config.yaml     # NEW — bot configuration
│   └── ... (existing docs)
```

### Environment Variables Needed

```bash
SLACK_BOT_TOKEN=xoxb-...          # Slack bot OAuth token
SLACK_APP_TOKEN=xapp-...          # Slack app-level token (if using Socket Mode)
ANTHROPIC_API_KEY=sk-ant-...      # For Claude API extraction
```

### Important Constraints

- **Privacy**: Never store or commit anyone's personal information. Depersonalize all extracted knowledge (no names attached to knowledge entries, only channel + date for traceability).
- **Read-only Slack access**: The bot should NEVER post, react, or interact in Slack. Passive observation only.
- **Cost control**: Use Claude Sonnet (not Opus) for daily extraction to keep API costs reasonable. Batch messages efficiently.
- **Idempotency**: Running the pipeline twice for the same day should not duplicate entries.
- **Git safety**: Auto-commits only touch `knowledge/` files. Never modify code, tickets, or other workspace files.
- **Graceful failure**: If Slack API is down or rate-limited, retry with backoff. If LLM extraction fails, save raw data and retry extraction later. Never lose a day's data.

### Deliverables

1. Step-by-step Slack App creation guide (with screenshots descriptions of each step)
2. Complete Python codebase for the bot
3. Setup script that installs dependencies and configures the cron job
4. Instructions for adding the bot to channels
5. A test mode that processes a single channel for the last hour (for validation before going live)
6. Updates to my workspace config files:
   - Add `slack_raw/` to `.gitignore`
   - Add the new `slack_review_queue.md` and `slack_bot_config.yaml` references to `CLAUDE.md`
   - Add a section to `knowledge/README.md` about the Slack extraction pipeline

### Getting Started

Please start by:
1. Walking me through the Slack App creation (I'll need to do this in the browser)
2. Then build the scraper first so we can validate we're getting messages
3. Then the extraction pipeline
4. Then the doc updater
5. Finally the cron job setup

I have Python 3.11+ on my Mac. I'll provide the Slack bot token and Anthropic API key when we get to that step.
