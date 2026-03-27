---
name: olympus_repo
description: Media Plan algorithm source code lives in steelhouse/olympus GitHub repo — has Claude-friendly docs, scoring logic, config params
type: reference
---

Media Plan algorithm code is in `github.com/steelhouse/olympus`. Chris Addy (tech lead) maintains it and has populated docs to make it easy to chat with in Claude Code. Contains:
- Scoring pipeline (semantic search → spend filter → softmax allocation)
- Config parameters (alpha, max_networks, min_networks, max_allocation, min_allocation)
- Deliverability guardrail model
- Per-publisher score calculation (score_semantic, score_performance_*, spendability_score, etc.)

**How to apply:** Clone and explore when investigating media plan algorithm behavior, concentration tuning, or per-publisher score details.
