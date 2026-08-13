---
name: reference_agent_harness_portability
description: "AGENTS.md is a cross-vendor standard most harnesses read natively; .agents/skills/ is the portable skills path; Codex now has hooks + subagents + skills, with a trust-hash gate and a 32 KiB instruction cap"
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [AGENTS.md, agents.md standard, codex, codex cli, cursor, copilot, gemini cli, windsurf, cline, aider, harness portability, cross-vendor, .agents/skills, codex hooks, codex subagents, hooks.json, project_doc_max_bytes, trust hash, AGENTS.override.md, codex exec, portable instruction file, BLUEPRINT.md]
domain: [workflow, infra]
lifecycle: active
last_verified: 2026-08-12
---
Verified 2026-08-12 against vendor primary docs. Full matrix + the Codex adapter live in
`documentation/ai_workflow_kit/BLUEPRINT.md` §6-7; this is the recall pointer.

**`AGENTS.md` is a real cross-vendor standard** (agents.md, stewarded by the Agentic AI Foundation under
the Linux Foundation). Read natively by Codex, Cursor, GitHub Copilot, Windsurf/Devin, and Cline. Gemini
CLI needs a one-line opt-in (`context.fileName` in `.gemini/settings.json`); Aider has no auto-discovery
at all and needs `read: AGENTS.md` in `.aider.conf.yml`. Cursor's CLI and VS Code Copilot also read
`CLAUDE.md`. **The portable move is one file, two names: `ln -s AGENTS.md CLAUDE.md`.**

**`.agents/skills/<name>/SKILL.md` is the portable procedure path** — read by Codex, Cursor, Gemini CLI,
and Copilot CLI. Cursor additionally reads `.claude/skills/` and `.codex/skills/`; Copilot additionally
reads `.github/skills/`. This is the single highest-leverage portability choice: one directory, four
harnesses. `bootstrap.sh` symlinks `.agents/skills -> .claude/skills`.

**Codex is NOT hook-less or subagent-less** (the common assumption, and what the kit assumed before this
check). Current Codex has all three:
- **Hooks** — 11 events (`SessionStart, SessionEnd, SubagentStart, SubagentStop, PreToolUse,
  PermissionRequest, PostToolUse, PreCompact, PostCompact, UserPromptSubmit, Stop`) in `.codex/hooks.json`
  or `[hooks]` in config.toml. Same contract as everywhere else: JSON on stdin, JSON on stdout, exit 2 to
  block — so **hook scripts port unchanged; only the registration file differs.**
- **Subagents** — built-in `default/worker/explorer` plus custom TOML in `.codex/agents/`
  (`name`/`description`/`developer_instructions` required).
- **Skills** — `.agents/skills/`, invoked `$name` or `/skills`, and **implicitly selected on description
  match**. Custom prompts (`~/.codex/prompts/`, `/prompts:<name>`) are formally DEPRECATED; they were the
  only mechanism with `$1`/`$ARGUMENTS` parameter passing, so skills take no arguments.

**Codex gotchas that will bite a port:**
- **Trust-hash gate.** A non-managed hook must be trusted via `/hooks`, and trust is recorded against the
  hook definition's HASH. **Editing a hook silently disables it until re-trusted** — only a startup warning.
- **32 KiB cap.** `project_doc_max_bytes` truncates the combined `AGENTS.md` set (Windsurf: 6,000 chars
  global / 12,000 per workspace rule). The hot-path budget is a hard limit, not a style preference.
- `PreToolUse` can DENY a call but cannot abort the turn; `continue:false` there is parsed and ignored.
- **No matcher** on `UserPromptSubmit` or `Stop` — filter inside the script.
- Hosted tools (web search) bypass the local tool-hook path entirely: hooks are a guardrail, not a boundary.
- Subagent dispatch is **prompt-driven**, not a callable tool, and there is **no per-agent tool allowlist**;
  the parent's runtime overrides are reapplied to children. Adversarial isolation must therefore be a FILE
  boundary (reviewer writes findings, only the fixer edits), not a capability boundary.
- Codex's own docs say its local memories are generated state, not to be hand-edited, and that required
  guidance belongs in `AGENTS.md` or checked-in docs — the same conclusion this kit reached.

**How to apply:** when porting, write one root `AGENTS.md`, move procedures to `.agents/skills/`,
re-register the same hook scripts per harness, and re-trust them on Codex. Everything else in the kit
(commit gate, doctor, index generator, linters, wrappers) is git/shell/python and needs no port at all.
See [[reference_workflow_kit_porting]], [[project_hot_path_budget]].
