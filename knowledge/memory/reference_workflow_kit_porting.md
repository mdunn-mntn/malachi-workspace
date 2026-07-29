---
name: reference_workflow_kit_porting
description: "Port the AI Workflow Kit to a fresh machine/use-case in one command via .claude/scripts/package_kit.sh"
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [port, porting, package_kit, ai workflow kit, portable kit, fresh machine, bootstrap, sanitize, sanitize_map, domain_scrub_map, domain-blind, cross-job, information barrier, global layer, with-global, generic kit, new use case, reverse symlink, PORTING.md, bundle, tarball]
domain: [workflow, infra]
lifecycle: active
last_verified: 2026-07-29
---
The AI Workflow Kit is portable in one command, built for cross-job transfer with a hard information barrier. `bash .claude/scripts/package_kit.sh [OUT_DIR]` emits a self-contained, sanitized, **domain-blind**, generic-seeded `ai-workflow-kit/` bundle (+ `.tar.gz`): it copies the machinery, applies TWO ordered literal maps — `sanitize_map.txt` (strip literal secrets → `<PLACEHOLDER>`) then `domain_scrub_map.txt` (strip job/domain CONTEXT: illustrative table/dataset/pipeline/incident/ticket names + the domain taxonomy → neutral generics) — overlays generic seeds from `documentation/ai_workflow_kit/templates/`, regenerates indexes + `COMPONENTS.md`, and **refuses to emit unless BOTH acceptance gates pass**: a secrets sweep (zero private tokens) AND a domain-blind sweep (zero job-identifying words), plus an in-bundle `verify.sh`. On the target machine: `bash bootstrap.sh` (repo layer) or `bash bootstrap.sh --with-global` (also installs the personal `~/.claude/` framework — CLAUDE.md/settings/MCP snippet — backing up existing files, token never copied), then fill placeholders per `PORTING.md`. Bundle carries the warehouse module + 4 subsystems as placeholdered skeletons; drops the prior job's content, `settings.local.json`, licensed assets, the Databricks one-off, the two example-dense design docs (`INGEST_GUIDE.md`, `bq_velocity_provenance_plan.md`), and `xlsx_demo.py`.

**Non-obvious implementation facts (verified this session):**
- `build_index.sh` crawls ONLY `knowledge/`, `on-call/`, `tickets/`, and root-level files — NOT `documentation/`. That is why the porting templates live under `documentation/ai_workflow_kit/templates/` (front-matter and all) without polluting the real indexes.
- `verify.sh` full-mode index-freshness uses `git diff`, so a bundle must `git init && git add -A` before verify passes; `package_kit.sh` and `bootstrap.sh` both stage a baseline first.
- The native memory dir is `~/.claude/projects/<slug>/memory` where `<slug> = pwd | sed 's#/#-#g'` — a slug of the absolute checkout path, so the reverse-symlink must be recreated per machine (`bootstrap.sh` does it, idempotent + revertible), never copied. See [[reference_commit_gate]].
- Sanitization must be case-insensitive: `Malachi`/`Malachi Dunn` (capitalized) slipped a lowercase-only pass; `package_kit.sh`'s sweep now greps `--ignore-case`.
- `hooks_selftest.sh` test #4 (memory_recall) is coupled to the memory corpus (asserts recall fires on a specific prompt); the packager repoints that one prompt at a generic seed memory's keywords so an empty/sanitized corpus still verifies clean.

- **Two-map design:** `sanitize_map.txt` removes literal secrets; `domain_scrub_map.txt` removes domain context (both ordered, longest-first, applied by one python pass in `package_kit.sh`). To harden further, add rows to either map and re-run — the gate tells you what still leaks.
- **Global layer capture is surgical:** only `~/.claude/CLAUDE.md` (sanitized generic ops rules), `settings.json`, and an MCP snippet (token quarantined) travel in `global/`. `~/.claude/skills/` was empty (dataviz/etc. are Claude Code built-ins). State dirs (`projects/`, `sessions/`, `history.jsonl`, caches) must NOT travel. `bootstrap.sh --with-global` installs with backups; never auto-writes `~/.claude.json`.

**How to apply:** to reuse the workflows for a new use case/job, run `package_kit.sh`, then edit the bundle's `START_HERE.md` + `MEMORY.md` + `.claude/CLAUDE.md` and fill placeholders; the deterministic layer works unchanged. Cross-job safe by construction (two gates). Durable copy of the last-built bundle: `~/Downloads/ai-workflow-kit.tar.gz`.
