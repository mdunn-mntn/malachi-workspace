---
name: project-self-optimizing-context
description: "Self-optimizing ticket-context system — 90/90 TL;DR cards, ticket keywords in _ROUTING, retrieval eval suite; COMPLETE 2026-07-23"
metadata: 
  node_type: memory
  type: project
  originSessionId: 45f5f508-2584-4aba-8aa8-efdac5abeb02
doc_type: memory
keywords: [self_optimizing_context, ticket cards, tl;dr card, _ROUTING, build_index.sh, retrieval eval, keywords front-matter, health_scorecard, ticket_context_full.js]
domain: [project, workflow]
lifecycle: archived
last_verified: 2026-07-23
---
Built a self-documenting/indexing/learning context layer over all 90 ticket cards (2026-07-23). Every `tickets/**/summary.md` now carries a verified `## TL;DR` card (Question/Answer/How/Tables/Learned/Reuse-when) + `keywords:` front-matter; `build_index.sh` folds those into `knowledge/_ROUTING.md` so prior work is keyword-routable (not just scannable in tickets/INDEX). A 5-probe cold-start retrieval eval is the fitness function (5/5 pass); `health_scorecard.py` flags it STALE >14d on SessionStart.

~45 delta facts surfaced during the pass were merged into the knowledge docs (dedup'd); the rest are staged in `tickets/_extracted_facts_queue.md` (marked PROCESSED — future passes append below the line, re-process only the new tail). Coverage ledger: `tickets/_CONTEXT_COVERAGE.md`.

Pipeline: `claude-prompts/ticket_context_full.js` (extract → 2 adversarial reviewers → fixer → land, commit per batch, resumable via `args.tickets`). Pilot spec: `claude-prompts/ticket_context_pilot.md` + `.js`; scale plan: `ticket_context_scale_plan.md`.

See [[reference_ticket_context_eval_tooling]] for how to run/extend, and [[feedback_adversarial_workflow_authoring]] for the calibration lessons. Related: [[project_structured_bq_catalog]], [[project_super_structure_adoption]].
