---
name: reference_team_name
description: "Squad = \"Audience Intelligence (AUDI)\" (renamed from Targeting Infrastructure). Jira project key ALSO renamed TI→AUDI: NEW tickets are AUDI-XXXX, old ones keep TI-XXXX."
metadata: 
  node_type: memory
  type: reference
  originSessionId: f5c6f1a9-8f60-4386-af98-8983f7eebe17
doc_type: memory
keywords: [team name, audience intelligence, AUDI, targeting infrastructure, jira project key, TI to AUDI, Kale McNaney, ticket naming]
domain: [routing-people, jira-process, leadership]
lifecycle: active
last_verified: 2026-06-30
---
The team's official name is **Audience Intelligence (AUDI)** — renamed from "Targeting Infrastructure" (confirmed by Malachi 2026-06-17). Use **"Audience Intelligence (AUDI)"** as the team name on slide bylines, deck attributions, ticket descriptions, and any artifact where a team name is needed.

**Jira project key — UPDATED (AUDI-1070, 2026-06-30):** the project key was renamed **TI → AUDI**. **New tickets are issued as `AUDI-XXXX`** (e.g. AUDI-1070); existing `TI-XXXX` tickets keep their old keys. Posting a create with `{"project":{"key":"TI"}}` still works (old key is an alias) but returns an `AUDI-` key. New ticket-folder naming: `audi_xxxx_short_description` (older folders stay `ti_xxx`). (Supersedes the earlier "Jira key stays TI" note — true as of 2026-06-17 but the key has since been migrated.)

Org chain (engineering): Audience Intelligence (AUDI) reports to Kale McNaney (Director), then Paulo Black (VP Eng), then Richard Girges (CTO). Pipeline owner: Ryan Kleck (feature store / airflow-ti). (Prior name "Targeting Infrastructure" / "TI squad" appears in older artifacts and Slack — same team.)

**`machine-learning-squad@mountain.com` is also us.** It is an old name for the same squad, and it still appears in airflow-ti `default_args` (e.g. `dags/machine_learning/mntn_match_incrementals_fetch.py`). Seeing it on a DAG's `email` list is NOT a signal to route the work elsewhere. Same for the `dags/machine_learning/` folder: we own it.
