# Knowledge Base — MNTN Data Documentation

This folder is the **shared, org-accessible** data documentation layer. It lives in the git repo so all team members and Claude sessions have access.

## Files

| File | Purpose |
|------|---------|
| `data_catalog.md` | Table-level reference: schemas, partitions, clustering, join keys, query tips, known gotchas per table |
| `data_knowledge.md` | Tribal knowledge, business logic, architecture patterns, disambiguation, and cross-ticket insights |

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

## Entry Templates

See `data_catalog.md` header for the table entry format.
See `data_knowledge.md` header for the knowledge entry format.
