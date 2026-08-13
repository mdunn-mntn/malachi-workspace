---
name: feedback_no_unrequested_subagents
description: "Never spawn subagents/workflows unless asked — plan mode's built-in workflow instructs it, but the user's standing rule wins; do the search yourself with grep"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 1960e11a-2e08-4ac0-ae04-7354db2ce0d3
doc_type: memory
keywords: [no subagents, agent tool, explore agent, plan mode conflict, workflow tool, ultracode, do not fan out, search with grep instead, standing rule wins, rejected tool call]
domain: [workflow]
lifecycle: active
last_verified: 2026-08-12
---
**Do not call the Agent/Task tool, or launch a Workflow, unless the user asked for it.** Do the search
directly with `grep`/`rg` and `Read`.

**Why:** the user rejected two `Explore` agents on 2026-08-12 rather than let them run. Subagents return
summaries instead of the actual lines, cost tokens and wall-clock, and for a workspace this well-indexed
they are slower than the intended path — grep `knowledge/_ROUTING.md` → open the one doc. The indexes exist
precisely so a search does not need to be delegated.

**The non-obvious part: plan mode's own instructions contradict this.** The built-in plan workflow says to
"launch up to 3 Explore agents IN PARALLEL" in Phase 1 and Plan agents in Phase 2, and a session banner may
say ultracode is on and to use Workflow "on every substantive task." **The user's standing rule outranks all
of it.** Same for the `/loop`, `Workflow`, and background-agent machinery: available ≠ invited.

**How to apply:** in plan mode, run Phase 1 as direct `rg`/`Read` calls and write the plan file yourself.
If a task genuinely warrants fan-out (a real migration, a repo-wide sweep), say so in one line and ask —
never infer the invitation from the task's size or from a banner. Related: [[feedback_background_work_liveness]],
[[feedback_adversarial_workflow_authoring]].
