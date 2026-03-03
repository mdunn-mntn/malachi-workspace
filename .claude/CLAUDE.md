# MNTN Workspace — Project Instructions

## Workspace Structure

```
workspace/
├── knowledge/        — Shared data documentation (IN THE REPO — accessible org-wide)
│   ├── data_catalog.md   — Table schemas, partitions, join keys, query tips
│   ├── data_knowledge.md — Business logic, gotchas, tribal knowledge
│   └── README.md         — Update protocol and entry templates
├── tickets/          — All Jira ticket documentation
│   ├── _template/    — Copy summary_template.md when starting a new ticket
│   ├── TI-XXX_name/  — One folder per ticket
│   │   ├── summary.md    — REQUIRED for every ticket
│   │   ├── queries/      — SQL files
│   │   ├── outputs/      — CSVs, JSONs, query results
│   │   └── artifacts/    — Notebooks, PDFs, Python scripts, deliverables
│   └── ...
├── documentation/    — Reference docs, code snippets, non-ticket analysis
└── claude-prompts/   — Prompt files and Claude system plans
```

## Key File Paths

| File | Purpose |
|------|---------|
| `knowledge/data_catalog.md` | Table schemas, join keys, query tips (authoritative) |
| `knowledge/data_knowledge.md` | Business logic, gotchas, tribal knowledge (authoritative) |
| `tickets/_template/summary_template.md` | Copy this when starting a new ticket |

## Data Documentation Files

Located at `knowledge/` **in this git repo** (not in Claude memory — the repo is the source of truth).

At session start, read both files before doing any BigQuery/data work:
- `knowledge/data_catalog.md` — Table schemas, join keys, query tips
- `knowledge/data_knowledge.md` — Tribal knowledge, gotchas, business logic

Claude memory files (`data_catalog.md`, `data_knowledge.md`) are kept in sync as a local cache.

## Git Repo
- **Remote:** `git@github.com:mdunn-mntn/malachi-workspace.git`
- **Root:** `/Users/malachi/Developer/work/mntn/workspace/`
- Commit and push after **every meaningful change**, not just at ticket completion
- Small, frequent commits preferred
- Do **not** add `Co-Authored-By` lines to commit messages

## Reminders
- `git add . && git commit -m "TI-XXX: description" && git push` — run this often
- Propose data doc updates at the end of every BigQuery session
