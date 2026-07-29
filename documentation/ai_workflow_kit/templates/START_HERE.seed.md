---
doc_type: routing
title: START HERE — how to find anything
summary: "task → the minimal set of docs to open; the front door to the knowledge base and the per-table catalog"
last_verified: 2025-01-01
keywords: [start, routing, where, how to find, orientation, catalog]
---

# START HERE

The front door for this workspace. **Load indexes, not the whole tree** — open only the docs a map names.

## Find anything in 4 steps
1. **Know the term?** `grep -ri "<term>" knowledge/_ROUTING.md` → open what it names.
2. **Know the table?** (warehouse module) `bq/_CATALOG_INDEX.md` → its `knowledge/bq/<ds>/<table>.md`.
3. **Know the domain?** `bq/_TOPICS.md` (tables by domain) or the doc headers in your knowledge base.
4. **Documenting depth / what to enrich next?** `bq/_COVERAGE.md` (worst-first work queue).

## Task → start-set (open these, nothing more)
| I need to… | open |
|---|---|
| **a fact I captured before** | grep `knowledge/_ROUTING.md` → the `memory/<slug>.md` it names; browse all via `_MEMORY_INDEX.md` |
| **a table's schema / grain / gotchas** (warehouse) | `bq/_CATALOG_INDEX.md` → the table doc |
| **tune a slow/expensive query** (warehouse) | `bq/optimization_playbook.md`, the table's `## Observed cost`, `.claude/scripts/perf_digest.py` |
| **verify how a reported number was produced** (warehouse) | `.claude/scripts/bq_verify.py <ticket \| label \| sql_sha256>` → SQL fingerprint + job id + git commit + cost |
| **an alert / pager / pipeline failure (on-call)** | `on-call/oncall_runbook.md` — §0 classify → §2 catalog → §3 incidents — or run **`/oncall`** |
| **prior work on a topic** | `tickets/INDEX.md` → the ticket's `summary.md` |
| **the design of this system** | `workflows/ARCHITECTURE.md`; operator guide `.claude/README.md` |

<!-- Seed this table with your own domain's start-set as the knowledge base fills. Add a row per recurring
     "I need to…" so a cold session routes to the one doc without ingesting the tree. -->

## The knowledge model (per-doc front-matter)
Every doc carries YAML front-matter that drives the indexes: `doc_type` (gates inclusion), `keywords`
(the ONLY field feeding the keyword→doc index), `domain` (groups the by-domain index), `last_verified`.
`build_index.sh` regenerates all indexes from these. Never hand-edit a generated index.
