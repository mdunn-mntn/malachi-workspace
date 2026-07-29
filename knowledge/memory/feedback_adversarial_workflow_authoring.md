---
name: feedback-adversarial-workflow-authoring
description: "Lessons for authoring multi-agent adversarial-verify Workflows — blocking-only gate, fixer loop, parse args, no silent drops"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 45f5f508-2584-4aba-8aa8-efdac5abeb02
doc_type: memory
keywords: [adversarial_workflow_authoring, adversarial, workflow, authoring, lessons, multi, agent, verify]
domain: [workflow]
lifecycle: active
last_verified: 2026-07-23
---
When authoring Workflow-tool multi-agent passes with an adversarial verify gate:

**Why:** a mis-calibrated gate wastes tokens and yields stochastically; silent agent failures under-report coverage and read as "done" when they aren't.

**How to apply:**
- **Blocking-only gate.** A dataset/project qualifier added to a routing/Tables field is normalization, not fabrication — don't reject on it. Reserve rejection for: an unsupported fact/number/outcome, a hedge upgraded to certainty, a plan step reported as done, inflated scope, or a table the source never names.
- **Fixer converge loop.** Binary 2-of-2 "assume wrong" reject is stochastic (same card flips pass→fail on identical input). Pair it with a fixer that repairs only the flagged claims against source and re-verifies once, so failures converge instead of dropping. Recalibrating this lifted yield 3/5 → 5/5.
- **Parse args.** Workflow `args` can arrive as a JSON string — `JSON.parse` it before use, or `args.tickets` is undefined and the run silently falls back to the full default list (this cost one ~12M-token full re-run).
- **No silent caps.** A thrown pipeline stage drops the item to `null`, vanishing from BOTH pass and fail lists. Wrap stages in try/catch and record `extract_error`/`converge_error` so a retry-cap failure is a tracked row, never a silent gap.
- **Hand-finish the tail.** For a few persistent schema-cap failures, hand-author those cards from source rather than re-running the whole pass.

Links: [[project-self-optimizing-context]], [[reference-ticket-context-eval-tooling]], [[feedback-bq-workflow]].
