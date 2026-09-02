---
name: reference_workflow_kit_porting
description: "Port the AI Workflow Kit to a fresh machine/use-case in one command via .claude/scripts/package_kit.sh"
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [port, porting, package_kit, BLUEPRINT.md, AGENTS.md, agnostic blueprint, harness agnostic, ai workflow kit, portable kit, fresh machine, bootstrap, sanitize, sanitize_map, domain_scrub_map, domain-blind, cross-job, information barrier, global layer, with-global, generic kit, new use case, reverse symlink, PORTING.md, bundle, tarball]
domain: [workflow, infra]
lifecycle: active
last_verified: 2026-09-02
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

**2026-08-12 refresh — the kit now ships a harness-agnostic layer, and the packager had three real defects:**
- **`documentation/ai_workflow_kit/BLUEPRINT.md`** is the vendor-neutral outline (four loops, a five-layer
  model, 28 primitives each with a no-vendor substitute, a portability ladder, a verified harness matrix,
  a Codex adapter). It is the file to hand someone who asks "how do I build this in <other tool>".
  The rules ship as a root **`AGENTS.md`** (`templates/AGENTS.template.md`) on the cross-vendor standard;
  `bootstrap.sh` symlinks `CLAUDE.md -> AGENTS.md` and `.agents/skills -> .claude/skills`.
  `CLAUDE.template.md` is now a thin Claude-Code addendum that points at `AGENTS.md` instead of restating
  the rules. Harness facts: [[reference_agent_harness_portability]].
- **The bundle shipped `.claude/global_claude_md_snapshot.md`** — a verbatim 16 KB copy of the private
  global `~/.claude/CLAUDE.md`, complete with tracker IDs and the token location — because the rsync
  exclude matched only the basename `CLAUDE.md`. It passed BOTH acceptance gates. Now excluded by name.
  Lesson: an exclude list is a denylist, and a denylist is only as good as the last file someone added.
- **`pyproject.toml` was never copied into the bundle**, so in-bundle `ruff` ran on its own defaults and
  reported ~32 phantom errors. `package_kit.sh` had therefore been unable to emit a bundle at all since
  ruff joined the gate (2026-07-31) — six weeks of a packager nobody ran. Now shipped, with its per-file
  ruff/mypy keys repointed at the bundle's renamed lib, and `ruff format` re-run after the identifier
  renames so the bundle's own doctor passes.
- **The domain gate now scrubs tracker IDs by SHAPE** (regex `[A-Z][A-Z0-9]{1,5}-[0-9]{2,5}` with an
  allowlist for `ISO-8601`/`SHA-256`/`RFC-3339`/`UTF-8`-style technical tokens) rather than by literal map
  rows. Literal maps rot; a shape does not. It immediately caught **21 real ID leaks** across skills,
  scripts, and the xlsx builder that had shipped in every prior bundle. The sweep also gained `ad-?tech`,
  `databricks`, `snowflake`. Validated with a positive AND a negative control before trusting it.
- Also: `slack_bot/` no longer ships (a local app holding a long-lived model API key is the pattern this
  workspace retired, and PORTING.md was telling adopters to stand one up two lines above the rule
  forbidding it); `pi_run_workflow_audit.sh` is renamed `run_workflow_audit_remote.sh` at package time
  (the old name identified a personal always-on host); `bootstrap.sh`'s optional-libs probe used
  `import importlib` then `importlib.util.find_spec`, an `AttributeError` swallowed by `|| true`, so it
  could never report a missing library — a check that could only ever report one outcome.

**How to apply:** to reuse the workflows for a new use case/job, run `package_kit.sh`; to reuse them under a
different AGENT, read `BLUEPRINT.md` §6-7, then edit the bundle's `START_HERE.md` + `MEMORY.md` + `.claude/CLAUDE.md` and fill placeholders; the deterministic layer works unchanged. Cross-job safe by construction (two gates). Durable copy of the last-built bundle: `~/Downloads/ai-workflow-kit.tar.gz`.

**2026-09-02 rebuild — the packager had been broken again, and its own secrets gate was the hole:**
- **The secrets sweep pattern `mountain\.com` could not match an escaped literal.** `pr_gauntlet/SKILL.md`
  and `workflows/pr_gauntlet.js` carry the org domain inside grep/regex text as `mountain\.com` and
  `mountain\\.com`; the sweep's `\.` demands a real dot, so it read clean while the domain shipped.
  Widened to the bare word `mountain`, which then caught the second leak below. **A literal find-replace
  map cannot see escape variants of its own rows — the map needs one row per escaping level.**
- **`lib/xlsx_builder.py` shipped the employer's brand-color names** ("Mountain Green", "Mountain Blue")
  in ~18 comments, in every prior bundle. A color name is a brand name.
- **`.claude/state/` shipped** (`chat_brevity_log.jsonl`, 200 KB of session IDs + reply telemetry) — added
  Aug 2026, never added to the rsync denylist. Same failure class as the `global_claude_md_snapshot.md`
  leak: **the exclude list is a denylist and rots the moment someone adds a directory.**
- **`.claude/skills/pyspark-optimization-databricks-dataproc/`** is now excluded rather than scrubbed. The
  domain map rewrote `Databricks` -> "a vendor platform" mid-sentence and left the lowercase form in every
  URL, producing both a mangled doc and a gate failure. **A stack-specific skill is dropped, not scrubbed.**

**How to apply (updated):** re-run `package_kit.sh` after ANY new file class lands under `.claude/` — the
denylist does not learn. Bundle rebuilt clean 2026-09-02 (7 skills, 13 hooks, 32 scripts, 11 agents; 888 KB).

