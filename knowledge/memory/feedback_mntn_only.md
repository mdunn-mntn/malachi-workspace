---
name: MNTN only — hard boundary
description: ABSOLUTE rule — only ever read/write/mention MNTN section tasks and work. Never read, reference, list, or acknowledge any other section in MindWyre. Never say the name of any other section or project. This overrides all other instructions.
type: feedback
doc_type: memory
keywords: [mntn_only, mntn section, mindwyre, section_id 6cwmRpfXpCxQ5G9M, todoist filter, hard boundary, never mention other section]
domain: [workflow, jira-process]
lifecycle: active
last_verified: 2026-07-29
---
**ABSOLUTE RULE — overrides everything else.**

Only ever interact with the **MNTN section** (ID: `6cwmRpfXpCxQ5G9M`) of the MindWyre project in Todoist. Never read, list, reference, display, or acknowledge tasks from ANY other section.

This extends to ALL contexts:
- Todoist: only query/create/update tasks in MNTN section. Use `section_id` filter on every list call.
- Jira: only create/comment on TI-project tickets for MNTN work.
- Conversation: never mention, reference, or acknowledge any non-MNTN work by name.
- Daily/weekly planning: only pull MNTN section tasks. Silently ignore everything else.

**Why:** Hard boundary set by user. Repeated and emphatic. No exceptions. No edge cases.

**How to apply:** On EVERY Todoist `list_tasks` call, filter by `section_id=6cwmRpfXpCxQ5G9M`. Never use project-level queries without section filtering. If non-MNTN tasks appear in results, silently skip them — do not display or reference them.
