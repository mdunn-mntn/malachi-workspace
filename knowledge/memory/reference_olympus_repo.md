---
name: reference_olympus_repo
description: "Media Plan algorithm source lives in steelhouse/olympus GitHub repo — Claude-friendly docs, scoring logic, config params"
metadata: 
  node_type: memory
  type: reference
  originSessionId: fc59db3f-b426-4cbe-9c11-c2bd5011531f
doc_type: memory
keywords: [olympus, media plan algorithm, chris addy, steelhouse/olympus, scoring pipeline, softmax allocation, spendability_score, deliverability guardrail]
domain: [repos, audience-scoring]
lifecycle: active
last_verified: 2026-07-20
---
Media Plan algorithm code is in `github.com/steelhouse/olympus`. Chris Addy (tech lead) maintains it and has populated docs to make it easy to chat with in Claude Code. Contains:
- Scoring pipeline (semantic search → spend filter → softmax allocation)
- Config parameters (alpha, max_networks, min_networks, max_allocation, min_allocation)
- Deliverability guardrail model
- Per-publisher score calculation (score_semantic, score_performance_*, spendability_score, etc.)

**How to apply:** Clone and explore when investigating Media Plan algorithm behavior, concentration tuning, or per-publisher score details. Rescued 2026-07-20 from a stray in-repo `.claude/projects/` copy that was never loading as a memory.
