# Folder Definitions — MNTN Workspace

Every folder has an explicit definition. When in doubt about where something goes, check here first.

---

## `knowledge/`

**What goes here:** Durable, cross-ticket reference documentation about the data stack.

| File | Contains |
|------|---------|
| `data_catalog.md` | One entry per BigQuery/Greenplum table: schema, partition keys, clustering, join keys, TTL, known gotchas, and query tips. Updated every time a new table is touched in any ticket. |
| `data_knowledge.md` | Business logic, architecture patterns, disambiguation, tribal knowledge. Things that are true across multiple tickets — not specific to one investigation. |
| `folder_definitions.md` | This file. |

**What does NOT go here:** Ticket-specific findings, one-time query results, raw data exports, meeting notes.

---

## `tickets/`

**What goes here:** One folder per Jira ticket. Every folder follows the exact same structure.

### Folder naming
`prefix_number_short_description` — all lowercase, underscores only.
Examples: `ti_650_stage_3_vv_audit`, `dm_3118_rtc_monitor`, `mm_44_ipdsc_hh_discrepancy`

### Required contents (every ticket folder must have all of these)
```
ti_xxx_name/
├── summary.md     ← the written record of the ticket (use _template/summary_template.md)
├── queries/       ← SQL files only
├── outputs/       ← query result exports (CSVs, JSONs) and intermediate data files
├── meetings/      ← meeting transcripts and notes
└── artifacts/     ← everything else: notebooks, PDFs, Python scripts, Word docs, PNGs
```

### `summary.md`
The single written record of the ticket. Covers: what the problem was, what was done, what was found, what questions were answered, what data docs were updated. Required before the ticket is considered complete.

### `queries/`
**Only `.sql` files.** One query per file, named descriptively.
- `queries/impression_visit_join.sql` ✓
- `queries/results.csv` ✗ — that's an output, not a query

### `outputs/`
**Query results and intermediate data files that Claude or the user produced during the investigation.**
- CSVs from BQ exports
- JSON result files
- Intermediate data that feeds into further analysis
- Subdirectories allowed (e.g., `outputs/by_campaign/`) for large result sets

**Not** raw source data delivered by a third party — that goes in `artifacts/`.

### `meetings/`
**Meeting transcripts and notes related to this ticket.**
- Otter.ai transcripts (`.txt`)
- Manual meeting notes
- Zoom transcript files (`.vtt`)
- Named with ticket prefix: `ti_xxx_meeting_person_n.txt`

### `artifacts/`
**Everything else that isn't a SQL query or a query result.** This is the catch-all for deliverables and supporting files.
- Jupyter notebooks (`.ipynb`)
- Python/Scala scripts
- PDFs, Word docs, HTML files
- Architecture images or diagrams specific to this ticket
- Context markdown files (`complete_context.md`, `llm_context.md`, etc.)
- Third-party source data files delivered for this ticket
- Presentations or talk tracks

**What does NOT go in a ticket folder:**
- Files that apply to multiple tickets → go in `knowledge/` or `documentation/`
- Duplicate copies of files already in `knowledge/`

---

## `documentation/`

**What goes here:** Reference material that is NOT ticket-specific and NOT data documentation.

This folder is for things you might want to keep for context but that don't belong inside a ticket.

### `documentation/architecture/`
System architecture diagrams that apply across multiple tickets or the whole platform.
- Pipeline diagrams, flow charts, system overviews
- Only PNGs/PDFs of architectural diagrams — not ticket-specific findings

### `documentation/data/`
External or third-party data files that serve as reference inputs (not outputs of our own queries).
- Vendor data dictionaries
- Reference CSVs provided by external partners
- Lookup tables not stored in BQ

### `documentation/docs/`
Written reference documents about systems, processes, or pipelines — not specific to one ticket.
- Airflow documentation
- Pipeline writeups
- Marketing attribution methodology docs

### `documentation/misc/`
Catch-all for things that don't fit above and aren't ticket-specific.

#### `documentation/misc/zoom_recordings/`
Zoom meeting recordings and transcripts. One subfolder per meeting.
Naming: `zoom_yyyy_mm_dd_topic/`
- `recording.mp4`
- `audio.m4a`
- `chat.txt`
- `transcript.vtt`
- `otter_ai_transcript.txt`

**What does NOT go in `documentation/`:**
- Ticket-specific investigation files → go in `tickets/ti_xxx/`
- Data documentation (catalog, knowledge) → go in `knowledge/`
- Planning files for Claude → go in `claude-prompts/`

---

## `claude-prompts/`

**What goes here:** Planning files, prompt templates, and session plans used by Claude.

- `naming_convention_plan.md` — the rename spec for this workspace
- Any other structured planning docs for Claude sessions

**What does NOT go here:** Actual ticket work, data files, SQL queries.

---

## Root level

**Only these items belong at root:**
- `.claude/` — Claude project settings and CLAUDE.md
- `.gitignore`
- `knowledge/`
- `tickets/`
- `documentation/`
- `claude-prompts/`

**Nothing else.** No stray folders, no one-off files at root level.
