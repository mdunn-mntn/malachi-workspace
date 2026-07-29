---
name: reference_workflow_kit_porting
description: "Port the AI Workflow Kit to a fresh machine/use-case in one command via .claude/scripts/package_kit.sh"
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [port, porting, package_kit, ai workflow kit, portable kit, fresh machine, bootstrap, sanitize, sanitize_map, generic kit, new use case, reverse symlink, PORTING.md, bundle, tarball]
domain: [workflow, infra]
lifecycle: active
last_verified: 2026-07-29
---
The AI Workflow Kit is portable in one command. `bash .claude/scripts/package_kit.sh [OUT_DIR]` emits a self-contained, sanitized, generic-seeded `ai-workflow-kit/` bundle (+ `.tar.gz`): it copies the machinery, swaps every private value for a `<PLACEHOLDER>` via `documentation/ai_workflow_kit/sanitize_map.txt`, overlays generic seeds from `documentation/ai_workflow_kit/templates/`, regenerates indexes + `COMPONENTS.md`, and **self-verifies** (a sanitization sweep that must find zero private tokens + an in-bundle `verify.sh`) before it will emit. On the target machine: `bash bootstrap.sh` (preflight → chmod → install commit gate → rebuild the memory symlink for the new path → build indexes → verify), then fill placeholders per `PORTING.md`. Bundle carries the warehouse module + all 4 subsystems (xlsx/transcription/slack bot/audit cron) as placeholdered skeletons; drops MNTN content, `settings.local.json`, licensed assets, and the Databricks one-off.

**Non-obvious implementation facts (verified this session):**
- `build_index.sh` crawls ONLY `knowledge/`, `on-call/`, `tickets/`, and root-level files — NOT `documentation/`. That is why the porting templates live under `documentation/ai_workflow_kit/templates/` (front-matter and all) without polluting the real indexes.
- `verify.sh` full-mode index-freshness uses `git diff`, so a bundle must `git init && git add -A` before verify passes; `package_kit.sh` and `bootstrap.sh` both stage a baseline first.
- The native memory dir is `~/.claude/projects/<slug>/memory` where `<slug> = pwd | sed 's#/#-#g'` — a slug of the absolute checkout path, so the reverse-symlink must be recreated per machine (`bootstrap.sh` does it, idempotent + revertible), never copied. See [[reference_commit_gate]].
- Sanitization must be case-insensitive: `Malachi`/`Malachi Dunn` (capitalized) slipped a lowercase-only pass; `package_kit.sh`'s sweep now greps `--ignore-case`.
- `hooks_selftest.sh` test #4 (memory_recall) is coupled to the memory corpus (asserts recall fires on a specific prompt); the packager repoints that one prompt at a generic seed memory's keywords so an empty/sanitized corpus still verifies clean.

**How to apply:** to reuse the workflows for a new use case, run `package_kit.sh`, then edit the bundle's `START_HERE.md` + `MEMORY.md` + `.claude/CLAUDE.md` and fill placeholders; the deterministic layer (hooks, linters, commit gate, indexing) works unchanged. Durable copy of the last-built bundle: `~/Downloads/ai-workflow-kit.tar.gz`.
