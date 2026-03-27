# MNTN Workspace — Project Instructions

See global `~/.claude/CLAUDE.md` for the full operating rules (always-on behaviors, naming conventions, commit protocol, empirical analysis protocol, BQ safety rules). This file adds project-specific paths and structure.

## Workspace Structure

```
workspace/
├── knowledge/            ← shared data docs — source of truth, in git, org-accessible
│   ├── data_catalog.md   ← table schemas, partitions, join keys, query tips
│   ├── data_knowledge.md ← business logic, gotchas, tribal knowledge
│   ├── experimentation.md ← experiment methodology, covariate selection, test design lessons
│   └── README.md
├── tickets/
│   ├── _template/        ← copy summary_template.md when starting a new ticket
│   └── ti_xxx_name/      ← one folder per ticket (lowercase, underscores)
│       ├── summary.md    ← required
│       ├── queries/      ← .sql files
│       ├── outputs/      ← csvs, jsons, query results
│       ├── meetings/     ← meeting transcripts, notes
│       └── artifacts/    ← notebooks, pdfs, scripts, deliverables
├── documentation/        ← reference docs, architecture diagrams, code snippets
├── self_review/          ← performance self-assessment (gitignored, never committed)
├── claude-prompts/       ← planning files and prompt templates
└── .claude/scripts/      ← Claude tooling scripts (bq_run.sh, etc.)
```

## Key Paths

| Path | Purpose |
|------|---------|
| `README.md` | Workspace structure, philosophy, and how-to — read at session start, update when workspace conventions change |
| `knowledge/data_catalog.md` | Table schemas and join keys — read at session start, update immediately when new schema learned |
| `knowledge/data_knowledge.md` | Business logic and gotchas — read at session start, update immediately when new knowledge found |
| `knowledge/experimentation.md` | Experiment methodology, covariate selection, test design lessons — update when working on any experiment/analysis ticket |
| `knowledge/folder_definitions.md` | **Exact definition of what goes in every folder** — check here before placing any file |
| `tickets/_template/summary_template.md` | Copy this when starting a new ticket |
| `.claude/scripts/bq_run.sh` | BQ query wrapper — logs performance metrics to `knowledge/bq_perf_log.jsonl` |
| `knowledge/bq_perf_log.jsonl` | Append-only log of BQ query performance (bytes, slots, wall time, cache hits) |
| `self_review/summary.md` | Self-review guide — workflow, rubric, leadership direction (Paulo/Kale/Alyson), how to write rationales |
| `self_review/self_review_2.md` | **Active self-review** — update after every ticket (gitignored) |
| `self_review/self_review_1.html` | Submitted review #1 (archived, do not modify) |

## Self-Review Entry Guide

When adding entries to the active self-review, consider these for every piece of work:
- **Rubric criteria for a 4**: Speed (no oversight, independent blocker resolution, balancing tasks), Craft (quality, standards, technology understanding, credibility), Adaptability (adapting to change, ambiguous problems, supporting peers)
- **Business impact**: tie work to Kale's focus areas — revenue growth, revenue retention, cost reduction
- **Paulo's framing**: frame work as "explaining why the system behaves this way" and "being the go-to person for ecosystem questions." Use verbs like "explained why," "gave the team a clear picture of," "go-to reference"
- **At review time**: argue the rubric not a ticket list, 3-5 high-impact tasks per section, ~9 different tasks across all three sections, format for scannability, one improvement per section

Full guide in `self_review/summary.md`.

## Ticket Work Protocol

**When working on any ticket**, always read `tickets/ti_xxx_name/summary.md` first to orient to the current state, open items, and file structure. This is the ticket card — it tells you what's been done, what's pending, and where everything lives.

## File Naming Convention

**Folder names** carry the descriptive label: `ti_650_stage_3_vv_audit/`

**File names** inside a ticket use the ticket prefix + short descriptor — NOT the full folder description:
- `ti_650_summary.md` (not `stage_3_vv_audit_summary.md`)
- `ti_650_audit_trace_queries.sql` (not `stage_3_vv_audit_trace_queries.sql`)
- `ti_650_column_reference.md` (not `vv_ip_lineage_column_reference.md`)

Pattern: `ti_xxx_short_name.ext` — the ticket number is the anchor, the filename is descriptive of the file's purpose.

Exception: `summary.md` at the ticket root can remain just `summary.md` (it's the standard template file).

## Codex Review
Codex will review your code after you're done. Write with that in mind — keep code clean, well-structured, and ready for automated review.

## Google Drive

Mounted at `~/Library/CloudStorage/GoogleDrive-malachi@mountain.com/My Drive/`.
Ticket-specific Drive files are listed in each `tickets/ti_xxx/summary.md` under "Drive Files".
Drive files cannot be committed to git — reference their paths in summaries only.

## Git
- Remote: `git@github.com:mdunn-mntn/malachi-workspace.git`
- Root: `/Users/malachi/Developer/work/mntn/workspace/`
- Commit and push after every meaningful change — no batching
- No `Co-Authored-By` lines
