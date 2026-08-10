---
name: feedback_constrain_llm_to_real_taxonomy
description: Any LLM step proposing a value that must join to prod data must be enum-constrained to the real roster, never free text.
metadata:
  node_type: memory
  type: feedback
doc_type: memory
keywords: [LLM suggestion, enum constraint, taxonomy, vertical_name, controlled vocabulary, free text, schema enum, join key, AUDI-431]
domain: [workflow, data-catalog]
lifecycle: active
last_verified: 2026-08-10
---

When an LLM/agent step proposes a value that will be JOINED against or written into prod data (a vertical, a category, a segment, a status), pass the real roster into the prompt AND pin it with a JSON-schema `enum`. Never accept free text and never let the model "name it in plain words".

**Why:** AUDI-431 (2026-08-10) asked for a replacement vertical "in plain words" with no roster supplied. 16 of 55 suggestions were invented names that join to nothing ("Media & Entertainment", "Books & Literature"). Malachi caught it with one question: "Are you suggesting the vertical based on your own opinion or out of our list? We have a list." Prod rosters also carry typos the model will silently "fix" (`Learning & Eduction Technology`), which breaks the join in the least visible way.

**How to apply:** (1) pull the roster from the source of truth first (`SELECT DISTINCT` off the target table, not a remembered list); (2) put it in the prompt AND in the schema `enum`, plus an explicit escape value like `NONE - not applicable`; (3) instruct copy-verbatim including typos; (4) hard-validate the returned set against the roster before merging, and keep the old free-text column for audit. [[feedback_facts_not_presentation]] [[feedback_hold_evidenced_verdict]]
