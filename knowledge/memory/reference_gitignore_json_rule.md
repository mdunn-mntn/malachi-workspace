---
name: reference_gitignore_json_rule
description: "Repo .gitignore has blanket *.json and *.csv rules — any new .json source/config/template or reference .csv you add is silently untracked; force-add it or a doc pointer to it is a lie"
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [gitignore, json ignored, .json not tracked, untracked file, force-add, git add -f, gitignore exception, negation, repo config, silently ignored, csv ignored, .csv not tracked, reference roster tracked, vertical taxonomy csv]
domain: [infra, workflow]
lifecycle: active
last_verified: 2026-08-10
---
The workspace `.gitignore` line 8 is a blanket `*.json`. Any new `.json` file you add anywhere in the repo (a config, a template, a fixture, an MCP snippet) is **silently untracked** — `git add .` skips it and there is no error. This bit the AI Workflow Kit global-layer templates (`templates/global/settings.json`, `mcp_servers.json`) — they existed on disk so the packager worked locally, but a fresh clone would be missing them.

**How to apply:** when adding a `.json` that MUST be tracked, either `git add -f <file>` (once tracked, later edits are picked up by `git add .` regardless of the ignore), or add a scoped negation to `.gitignore` (`!path/to/file.json`) — the negation is cleaner and self-documents intent. After adding, verify with `git ls-files --error-unmatch <file>`. See [[reference_workflow_kit_porting]].

Ticket `outputs/*.json` hit the same rule (2026-08-06): archiving `tickets/audi_1191_airflow_spark_debugger/outputs/code_review_findings_2026_08_06.json` silently no-oped until `git add -f`.

**Same rule, line 2: blanket `*.csv`** (plus `.tsv/.xlsx/.parquet`) — raw data outputs are meant to stay out, but a small **reference roster** that a doc points to must be force-added or the pointer is a lie. Done 2026-08-10 for `tickets/audi_431_blocklist_whitelist/outputs/audi_431_vertical_taxonomy.csv` (152-row DS13 vertical taxonomy, 8 KB, cited from `knowledge/data_catalog.md`). Test before citing a data file from a knowledge doc: `git ls-files --error-unmatch <file>`.
