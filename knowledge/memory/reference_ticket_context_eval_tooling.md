---
name: reference-ticket-context-eval-tooling
description: "How to run/extend the ticket-context cards + retrieval eval suite — files, eval recording, staleness signal"
metadata: 
  node_type: memory
  type: reference
  originSessionId: 45f5f508-2584-4aba-8aa8-efdac5abeb02
doc_type: memory
keywords: [ticket context eval tooling, retrieval eval, ticket cards, eval_probes, eval_runs.log, build_index.sh, health_scorecard, _ROUTING]
domain: [workflow, infra]
lifecycle: active
last_verified: 2026-07-23
---
Ticket-context + retrieval-eval tooling (all in git):

- **Card/knowledge pass:** `claude-prompts/ticket_context_full.js` — `Workflow scriptPath=...`. Per-ticket extract → verify (2 adversarial, blocking-only) → fixer → land; commit per batch; `args.tickets` = subset resume (the script `JSON.parse`s args when the harness delivers them stringified).
- **Retrieval eval:** `claude-prompts/retrieval_eval.js` runs the probes in `knowledge/eval_probes.md` (the fenced `## PROBES` JSON block). Add a probe for every real cold-start miss — misses become permanent regression tests.
- **Record an eval run:** append a line to `knowledge/eval_runs.log` and commit with `retrieval-eval: run —` in the message. `.claude/scripts/health_scorecard.py` reads that signature → prints `retrieval-eval Nd ago` on SessionStart, STALE >14d (`EVAL_STALE_DAYS`).
- **Routing:** `.claude/scripts/build_index.sh` walks `tickets/` and folds ticket-card `keywords:` into `_ROUTING.md` alongside doc keywords. Rebuild after any front-matter/keyword change.

See [[project-self-optimizing-context]].
