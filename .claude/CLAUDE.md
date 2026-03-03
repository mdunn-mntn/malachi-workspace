# MNTN Workspace — Project Instructions

## Workspace Structure

```
workspace/
├── tickets/          — All Jira ticket documentation
│   ├── _template/    — Copy summary_template.md when starting a new ticket
│   ├── TI-XXX_name/  — One folder per ticket (queries/, outputs/, artifacts/)
│   └── ...
├── documentation/    — Reference docs, code snippets, non-ticket analysis
└── claude-prompts/   — Prompt files and Claude system plans
```

## Key File Paths

| File | Purpose |
|------|---------|
| `tickets/_template/summary_template.md` | Copy this when starting a new ticket |

## Data Documentation Files

Located in the Claude memory directory for this project:
- `data_catalog.md` — Table schemas, join keys, query tips
- `data_knowledge.md` — Tribal knowledge, gotchas, business logic
- `entry_templates.md` — Exact formats for catalog/knowledge entries

At session start, read these files before doing any BigQuery/data work.

## Git Repo
- **Remote:** `git@github.com:mdunn-mntn/malachi-workspace.git`
- **Root:** `/Users/malachi/Developer/work/mntn/workspace/`
- Commit and push after **every meaningful change**, not just at ticket completion
- Small, frequent commits preferred
- Do **not** add `Co-Authored-By` lines to commit messages

## Reminders
- `git add . && git commit -m "TI-XXX: description" && git push` — run this often
- Propose data doc updates at the end of every BigQuery session
