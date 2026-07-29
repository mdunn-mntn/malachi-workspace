---
name: feedback_example
description: "Example feedback memory — shows the front-matter contract for a 'how I should work' fact"
metadata:
  node_type: memory
  type: feedback
doc_type: memory
keywords: [example, feedback, front-matter, contract, how to work, template]
domain: [workflow]
lifecycle: active
last_verified: 2025-01-01
---
This is an example `feedback` memory. Use it as a template for capturing guidance on **how the assistant
should work** — corrections, confirmed approaches, standing preferences. Delete it once you have real ones.

**Why:** feedback memories encode the operating style so it persists across sessions without being
restated. They are folded into `_ROUTING.md` by `build_index.sh` and surfaced by the per-prompt recall hook.

**How to apply:**
- State the rule in the body, then a `**Why:**` line, then a `**How to apply:**` list.
- Keep `keywords` specific — they are the ONLY field feeding the keyword→doc index.
- Link related memories with `[[their-name]]`. A link that doesn't resolve yet is fine; it marks a memory
  worth writing later.
- Set `lifecycle: active`; `/capture` flips it to `superseded`/`archived` when it stops being true.
