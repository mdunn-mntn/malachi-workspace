---
name: reference_dsh_harness
description: DeepSeek Harness (dsh) facts — plugin agent harness on Cordis, replay-eval substrate; research corpus lives in the ti_xxx_dsh_harness_spike ticket.
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [deepseek harness, dsh, cordis, plugin harness, llm-replay, dsh-plugin, agent harness, spatiotemporal composability, dsh-bq, sidecar]
domain: [infra, repos, workflow]
lifecycle: active
last_verified: 2026-08-21
---

DeepSeek Harness (`dsh`, deepseek-ai/deepseek-harness, released 2026-08-14, MIT, v0.1.1-rc.1 developer preview): Node/TS agent harness where models, tools, skills, sessions, sandbox, agent loop, and UI are all hot-swappable Cordis plugins. Key facts verified 2026-08-21:
- Model-agnostic: `llm-pi-ai` adapter runs Anthropic as pure config; `subagent-claude-code` wraps the official Anthropic agent SDK. Not DeepSeek-locked.
- Skills use the same AgentSkills SKILL.md standard as Claude Code; our 6 skills mount verbatim via `skill-filesystem` customDirs.
- Replay substrate: append-only session logs ("model-visible iff logged") + `llm-replay` + `ctx.sessions.fork()` = keyless regression testing of recorded trajectories; dsh's own CI works this way.
- No BigQuery plugin exists in the ~10k-repo `dsh-plugin` ecosystem (surveyed 2026-08-21).
- Security: sandbox restricts writes only — NO outbound network restriction; plugins run in-process with full user permissions. Egress control must be external (pf + proxy).
- Preview risk: breaking changes promised; ecosystem plugins pin exact snapshots; ~3-10x token hunger vs peers.

Full research corpus (7 units) + 3 design docs + master plan: `tickets/ti_xxx_dsh_harness_spike/artifacts/`. [[project_dsh_harness_spike]]
