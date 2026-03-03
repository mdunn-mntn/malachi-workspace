# MNTN Workspace — Project Instructions

See global `~/.claude/CLAUDE.md` for the full operating rules (always-on behaviors, naming conventions, commit protocol, empirical analysis protocol, BQ safety rules). This file adds project-specific paths and structure.

## Workspace Structure

```
workspace/
├── knowledge/            ← shared data docs — source of truth, in git, org-accessible
│   ├── data_catalog.md   ← table schemas, partitions, join keys, query tips
│   ├── data_knowledge.md ← business logic, gotchas, tribal knowledge
│   └── README.md
├── tickets/
│   ├── _template/        ← copy summary_template.md when starting a new ticket
│   └── ti_xxx_name/      ← one folder per ticket (lowercase, underscores)
│       ├── summary.md    ← required
│       ├── queries/      ← .sql files
│       ├── outputs/      ← csvs, jsons, query results
│       └── artifacts/    ← notebooks, pdfs, scripts, deliverables
├── documentation/        ← reference docs, architecture diagrams, code snippets
└── claude-prompts/       ← planning files and prompt templates
```

## Key Paths

| Path | Purpose |
|------|---------|
| `README.md` | Workspace structure, philosophy, and how-to — read at session start, update when workspace conventions change |
| `knowledge/data_catalog.md` | Table schemas and join keys — read at session start, update immediately when new schema learned |
| `knowledge/data_knowledge.md` | Business logic and gotchas — read at session start, update immediately when new knowledge found |
| `knowledge/folder_definitions.md` | **Exact definition of what goes in every folder** — check here before placing any file |
| `tickets/_template/summary_template.md` | Copy this when starting a new ticket |

## Google Drive

Mounted at `~/Library/CloudStorage/GoogleDrive-malachi@mountain.com/My Drive/`.
Ticket-specific Drive files are listed in each `tickets/ti_xxx/summary.md` under "Drive Files".
Drive files cannot be committed to git — reference their paths in summaries only.

## Git
- Remote: `git@github.com:mdunn-mntn/malachi-workspace.git`
- Root: `/Users/malachi/Developer/work/mntn/workspace/`
- Commit and push after every meaningful change — no batching
- No `Co-Authored-By` lines
