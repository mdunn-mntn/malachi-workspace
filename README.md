# MNTN Data Workspace

> *"People don't buy what you do; they buy why you do it."*
> — Simon Sinek

---

## Why This Exists

Every data team eventually hits the same wall.

You spend three days tracing a join key through four BigQuery datasets, finally nail it, document it in a Slack message, and move on. Six weeks later, a teammate spends three days doing the exact same thing. Or you do — because you forgot.

You investigate a bug, find the root cause, write up a one-pager in Google Docs, share it in a channel. The channel scrolls. The doc lives in a folder nobody remembers. The next time the same symptom appears, the investigation starts from zero.

You work with an AI assistant — Claude, Copilot, whatever — and it's great in the moment. But every new session is a blank slate. You spend 20 minutes re-establishing context that you already established last week.

You approach your performance review and try to reconstruct what you did over the past year. You know the work was good. You can't prove it.

**This workspace is the fix for all of that.**

It is a structured, version-controlled, AI-augmented working environment designed to ensure that:

- **Knowledge compounds** — every investigation makes the next one faster
- **Context persists** — across sessions, across teammates, across time
- **Work is traceable** — from Jira ticket to query to finding to documentation to impact
- **Performance is evidenced** — not recalled, not reconstructed, recorded as it happened

It is not a documentation burden. It is an operating system for doing data work well — one that pays dividends starting on the second ticket and increasingly with every one after.

---

## The Core Insight

Most data teams separate their work from their documentation. The work happens in notebooks, queries, Slack threads, and meetings. The documentation happens later, reluctantly, incompletely.

This workspace collapses that separation. **The documentation is the work.** Writing the summary isn't overhead — it's the final step of the investigation. Updating the data catalog isn't admin — it's how you make the next query take 20 minutes instead of 2 hours.

And because an AI assistant (Claude) runs inside this workspace with persistent instructions and a pre-loaded knowledge base, the documentation loops back directly into future work. The catalog you update today is the context Claude reads at the start of every session. You are not writing for a folder. You are writing for your future self — and for Claude as your working partner.

---

## How the Feedback Loop Works

```
Jira Ticket
     │
     ▼
tickets/ti_xxx/
├── summary.md        ← you write this throughout
├── queries/          ← SQL lives here, not in notebooks
├── outputs/          ← query results, CSVs
└── artifacts/        ← notebooks, scripts, deliverables
     │
     ▼
Investigation reveals new schema knowledge, join key, gotcha, or business logic
     │
     ▼
knowledge/data_catalog.md        ← updated immediately
knowledge/data_knowledge.md      ← updated immediately
     │
     ▼
Next ticket starts
Claude reads data_catalog.md and data_knowledge.md at session start
Claude already knows the table schema, the gotchas, the join keys
Investigation starts 2 hours ahead of where it would have otherwise
     │
     ▼
Another ticket completes. More knowledge added. Cycle continues.
```

**The compounding effect is real.** Early tickets require more schema discovery work. After 10–15 tickets, the catalog is dense enough that most schema questions are answered before a query is written. After 20+, you are operating at a fundamentally different speed than someone starting cold.

---

## Workspace Structure

```
workspace/
├── README.md                    ← you are here
├── .claude/
│   └── CLAUDE.md                ← persistent instructions for Claude (read every session)
├── knowledge/
│   ├── data_catalog.md          ← table-level reference: schemas, join keys, TTLs, gotchas
│   ├── data_knowledge.md        ← business logic, architecture, tribal knowledge
│   ├── folder_definitions.md    ← authoritative definition of every folder
│   └── README.md
├── tickets/
│   ├── _template/
│   │   └── summary_template.md  ← copy this when starting any new ticket
│   └── ti_xxx_name/             ← one folder per ticket
│       ├── summary.md
│       ├── queries/
│       ├── outputs/
│       └── artifacts/
├── documentation/
│   ├── architecture/            ← system diagrams, pipeline overviews
│   ├── data/                    ← vendor data dictionaries, reference CSVs
│   ├── docs/                    ← written docs about systems, pipelines, processes
│   └── misc/                    ← catch-all; zoom recordings go in misc/zoom_recordings/
└── claude-prompts/              ← planning files and session prompt templates
```

---

## The Knowledge Base: `knowledge/`

This is the most important folder in the workspace. Everything else feeds into it.

### `data_catalog.md`

The table-level reference. One entry per BigQuery (or Greenplum) table that has been touched in any investigation. Each entry covers:

- **Schema** — column names, types, descriptions
- **Partitioning** — how the table is partitioned (critical for cost control and query performance)
- **Clustering** — what the table is clustered on
- **Join keys** — confirmed join paths to other tables
- **TTL / expiration** — how far back the data goes (some tables expire after 10 days)
- **Query tips** — filters that are required, patterns that work, patterns that don't
- **Known gotchas** — data quality issues, behavioral quirks, things that will burn you if you don't know

The catalog is authoritative because it is empirically verified. Every entry was confirmed with a query, not assumed from documentation. If something is in the catalog, it was tested.

### `data_knowledge.md`

Cross-ticket, cross-table knowledge. This is where architecture understanding lives — not "what columns does this table have" but "why does this table exist and how does it fit into the pipeline."

Typical entries include:
- Pipeline architecture diagrams in text form
- Disambiguation of confusingly named tables (e.g., `bid_attempted_log` and `bid_events_log` are the same table)
- Business logic clarifications (e.g., what "NTB" means and why two columns measuring it disagree)
- Gotchas that apply across multiple tables (e.g., epoch units vary by table)
- Deprecation timelines and migration paths

### The update rule

**Neither file is ever "done."** Every ticket that touches a table should update both files if new knowledge was found. This is not optional — it is part of completing a ticket. The summary template has a dedicated section for it (Section 7: Data Documentation Updates).

---

## Tickets: `tickets/`

Every piece of investigation work lives in a ticket folder. Tickets map 1:1 to Jira issues.

### Naming

```
prefix_number_short_description
```

All lowercase. Underscores only. No dashes.

Examples:
- `ti_650_stage_3_vv_audit`
- `dm_3118_rtc_monitor`
- `mm_44_ipdsc_hh_discrepancy`
- `tgt_4016_ecomm_classifier_thresholds`

### Required structure (every ticket, no exceptions)

```
ti_xxx_name/
├── summary.md     ← the written record
├── queries/       ← SQL files only
├── outputs/       ← query results: CSVs, JSONs, intermediate data
└── artifacts/     ← everything else: notebooks, PDFs, scripts, context files
```

All four items must exist before a ticket is considered complete. Missing folders get created immediately — there is no "I'll add it later."

### `summary.md`

The single written record of the ticket. Uses a standard template with eight sections:

1. **Introduction** — what system/feature/data is involved and why this ticket exists
2. **The Problem** — symptoms, who reported it, what the impact is
3. **Plan of Action** — numbered steps of the approach, updated as the plan evolves
4. **Investigation & Findings** — what was discovered, key queries run, data samples, unexpected results
5. **Solution** — what was done to resolve: PRs, recommendations, dashboards
6. **Questions Answered** — Q/A format for specific questions that were resolved
7. **Data Documentation Updates** — what was added to `data_catalog.md` or `data_knowledge.md`
8. **Open Items / Follow-ups** — anything unresolved, handed off, or deferred

The summary is written throughout the investigation, not at the end. It is the ongoing log of what is happening. By the time the work is done, the summary mostly writes itself.

### `queries/`

SQL files only. One query per file, named descriptively.

```
queries/ip_trace_v1.sql              ✓
queries/impression_visit_join.sql    ✓
queries/results.csv                  ✗  (that's an output)
```

Keeping SQL in `.sql` files (not embedded in notebooks or Slack messages) means:
- Queries are searchable and reusable across tickets
- Claude can read and reference them in future sessions
- Git history shows how a query evolved

### `outputs/`

The results of queries and intermediate data files produced during the investigation. CSVs from BigQuery exports, JSON result files, computed datasets that feed into further analysis. Subdirectories are allowed for large result sets (`outputs/by_campaign/`, etc.).

These files are gitignored (no raw data in GitHub) but live on disk and are referenced from `summary.md`.

### `artifacts/`

Everything else. Jupyter notebooks, Python scripts, PDFs, Word docs, architecture images specific to this ticket, context files for LLM sessions, third-party source data delivered for this ticket, presentations, talk tracks.

---

## Reference Material: `documentation/`

For material that is not ticket-specific and not data documentation.

| Subfolder | Contents |
|-----------|----------|
| `architecture/` | Pipeline diagrams, system overviews, flow charts that apply across the platform |
| `data/` | Vendor data dictionaries, reference CSVs from external partners, lookup tables not in BQ |
| `docs/` | Written docs about systems and processes — Airflow docs, pipeline writeups, methodology |
| `misc/` | Catch-all; Zoom recordings go in `misc/zoom_recordings/zoom_yyyy_mm_dd_topic/` |

---

## Working with Claude

The `.claude/CLAUDE.md` file is read by Claude at the start of every session. It contains:

- Always-on behaviors (what to do without being asked)
- Naming and structure conventions
- BigQuery access instructions and safety rules
- The ticket workflow from start to finish
- The commit protocol (small, frequent, after every meaningful change)
- The empirical analysis protocol (never guess schema when a query can confirm it)

Because `CLAUDE.md` and the `knowledge/` files persist across sessions, Claude operates with full institutional context from the moment a session begins. You do not re-brief it on table schemas or explain the pipeline architecture. It already knows.

### What Claude does automatically in this workspace

- Reads `data_catalog.md` and `data_knowledge.md` before any data work
- Commits and pushes after every meaningful change (queries, findings, doc updates)
- Updates the knowledge base immediately when new schema or business logic is discovered
- Creates the correct ticket folder structure when a new ticket is started
- Flags empirically unverified assumptions before writing queries
- Follows all naming conventions without being reminded

### What this means for you

You can say: *"Start TI-XXX. Investigate why the CRM audience match rate dropped for advertiser 37775."*

Claude will:
1. Fetch the Jira ticket
2. Create the folder with the correct structure
3. Write the summary introduction from the Jira metadata
4. Load relevant prior knowledge from the catalog
5. Begin the investigation empirically — checking schema before writing joins, running COUNTs before pulling rows
6. Document findings in real time
7. Update the knowledge base at the end
8. Commit everything

You are the domain expert and decision-maker. Claude is the tireless analyst who never forgets the schema, never skips the commit, and never lets a finding disappear into a Slack thread.

---

## Getting Started: How to Use This Workspace

### Prerequisites

- Git access to this repo
- BigQuery access (authenticated as your `@mountain.com` account)
- Jira access
- Claude Code (the CLI) installed and configured with your API key

### Starting a new ticket

1. Get the Jira ticket ID (e.g., `TI-701`)
2. Open a Claude session in this workspace directory
3. Say: *"Start TI-701"* — Claude will fetch the Jira metadata and create the folder
4. Or create manually:
   ```bash
   mkdir -p tickets/ti_701_short_description/{queries,outputs,artifacts}
   cp tickets/_template/summary_template.md tickets/ti_701_short_description/summary.md
   ```
5. Fill in the Introduction and Problem sections from Jira
6. Commit immediately: `git commit -m "TI-701: init ticket folder"`

### During the investigation

- Run queries. Save them as `.sql` files in `queries/`.
- Export results to `outputs/` and reference them from `summary.md`.
- Update `summary.md` as findings emerge — treat it as a live log, not a retrospective.
- When you learn something new about a table: **update `data_catalog.md` or `data_knowledge.md` immediately.** Do not wait until the ticket is closed.
- Commit frequently. Small commits. After every meaningful addition.

### Completing a ticket

1. Fill in the Solution section
2. Answer the Questions Answered section
3. Fill in Section 7: Data Documentation Updates (what new knowledge was added to `knowledge/`)
4. Final commit and push

### Adding to the knowledge base manually

Open `knowledge/data_catalog.md` or `knowledge/data_knowledge.md` and follow the entry format at the top of each file. Add the ticket ID in the update line so future readers know where the knowledge came from. Commit.

---

## Naming Conventions

These are enforced everywhere, always, with no exceptions:

| Rule | Example |
|------|---------|
| All lowercase | `ti_650_stage_3_vv_audit` not `TI-650-Stage3` |
| Underscores as word separators | `my_file_name.sql` not `my-file-name.sql` |
| Ticket folder pattern: `prefix_number_description` | `dm_3118_rtc_monitor` |
| SQL files: descriptive names | `ip_trace_by_campaign.sql` not `query1.sql` |
| No spaces in any filename | ever |

---

## What Is Gitignored

The following are intentionally excluded from the repository:

- `*.csv`, `*.json`, `*.parquet` — raw data outputs (stay on disk, referenced by path)
- `*.pdf`, `*.docx`, `*.png`, `*.pptx` — binary files and media
- `*.ipynb` checkpoints
- `self_review/` — personal performance notes (local only)
- `.DS_Store`, `.vscode/`, IDE artifacts

**What this means:** the repo contains the thinking (summaries, queries, documentation) but not the raw data. Data files live alongside the repo on your local machine and can be referenced from `summary.md` with their local path or a Google Drive link.

---

## The Structure Philosophy

Every decision in this workspace structure was made to answer one question: *how do we prevent good work from disappearing?*

Work disappears in five ways:
1. It lives only in someone's head and they leave
2. It is written but stored somewhere nobody looks
3. It is stored somewhere people look but in a format nobody can use
4. It is used once and never connected to the next similar problem
5. It exists but the context needed to interpret it is gone

This workspace addresses each one:

1. **Knowledge lives in git, not heads** — `data_catalog.md` and `data_knowledge.md` capture institutional knowledge in a searchable, durable, version-controlled form
2. **Ticket summaries are mandatory, structured, and co-located with the work** — they are not a separate documentation system, they are part of the ticket folder
3. **Queries are `.sql` files, not notebook cells** — they are readable, searchable, and reusable by both humans and AI
4. **Every ticket feeds the knowledge base** — Section 7 of every summary requires documenting what was learned and written to `knowledge/`
5. **Context is explicit and front-loaded** — `CLAUDE.md` and the catalog ensure that every session starts with full context, not a blank slate

---

## Questions

- **Where does X file go?** → Read `knowledge/folder_definitions.md`. It has the authoritative answer for every folder.
- **What tables/schemas are documented?** → Read `knowledge/data_catalog.md`.
- **What are the business logic gotchas?** → Read `knowledge/data_knowledge.md`.
- **How should I name this ticket folder?** → `prefix_number_short_description`, all lowercase, underscores only.
- **Do I have to fill out the whole summary template?** → Yes. All eight sections, by ticket close.
- **What if a query result file is too big to store?** → Store it in `outputs/` (gitignored), reference the path from `summary.md`, link to Google Drive if it needs to be shared.
- **Can I use dashes in filenames?** → No.
