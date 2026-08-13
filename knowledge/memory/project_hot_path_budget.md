---
name: project_hot_path_budget
description: "The two CLAUDE.md files are a hot-path budget (~5k tokens): a rule may leave only if a real trigger reloads it, else moving it = deleting it"
metadata:
  node_type: memory
  type: project
doc_type: memory
keywords: [claude.md, hot path, always loaded, token budget, instruction bloat, slimdown, pointer line, trigger test, skill vs claude.md, what goes in claude.md, instruction layer]
domain: [workflow]
lifecycle: active
last_verified: 2026-08-12
---
The two always-loaded `CLAUDE.md` files are a **budget, not a filing cabinet**. Cut from 66,606 chars (~18k tokens/session) to 20,551 chars (~5k) on 2026-08-11. Global 452→137 lines, project 351→67 lines.

**Why:** Anthropic's own guidance is that a bloated CLAUDE.md causes the model to *ignore* the instructions in it, so bloat costs both tokens and compliance. The workspace already had an on-demand layer (skills, `_ROUTING.md`, 180 memory files) that most of the content duplicated.

**How to apply — the trigger test, before adding OR removing anything:**

A rule may live off the hot path ONLY IF something reliably reloads it at the right moment: (a) a skill whose description matches what the user actually says, (b) a deterministic hook, (c) a one-line pointer left hot that names where the detail lives, or (d) `_ROUTING.md` keywords the hot file tells me to grep. **If no trigger fires, moving a rule equals deleting it — I cannot grep for a rule I don't know exists.**

- **Behavioral rules stay hot** (how I write, must-never-dos, safety rails). Cheap in tokens, catastrophic if silently dropped.
- **Procedural and reference content goes on demand** (step-by-step how-tos, curl payloads, path tables, format specs, chart styling).
- **New procedure belongs in a skill or knowledge doc with a pointer, never inlined hot.** Adding to CLAUDE.md is the exception now, not the default.

**Pinned single points of failure — never prune:** MEMORY.md BLUF / Stack / Todoist lines; the global no-`Co-Authored-By` clause (overrides a live harness default); the project `slack_bot` decommission line; the Todoist pointer (`feedback_todoist` is hot-tier-wikilinked so `memory_recall.py` excludes it from `_MEMORY_RECALL.tsv`, making that one line its only trigger); the Pi no-`ANTHROPIC_API_KEY` clause; the `documentation/docs/` pointer (that shelf is NOT in `_ROUTING.md`, so grep will not find it).

**The global file is now version-controlled (2026-08-12).** `~/.claude/CLAUDE.md` is always loaded and lives outside the repo, so until now every edit to it was unreviewable, unrecoverable after a bad overwrite, and lost with the machine — `workflow_audit.sh` §8 already read it, but nothing saved it. `.claude/scripts/sync_global_claude_md.sh` snapshots it to `.claude/global_claude_md_snapshot.md`; `verify.sh` full-mode reports drift as **advisory, never a gate** (it is the user's file across all projects and may legitimately change mid-session); `--restore` recovers it. Deliberately a **backup, not a symlink** — symlinking a file every project depends on into a shared git worktree would take the global rules down with any checkout, stash, or unmounted drive. Snapshot after editing the global file, and stage it with your other paths.

Full audit with per-section verdicts, blocked moves, and the pointer lines: `claude-prompts/workflow_audits/claude_md_slimdown_2026_08_11.md`. Related: [[feedback_terse_chat_replies]], [[project_structured_bq_catalog]].
