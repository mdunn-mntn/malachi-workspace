---
name: project_memory_layer_unification
description: "Auto-memory unified into knowledge/memory/ (in git) under the one _ROUTING index; MEMORY.md shrunk to a hot tier (19.6KB->5.3KB, growth halted); native dir is a reverse-symlink. VALIDATED 2026-07-29: symlink honored (hot tier loads + native writes land in git), grep path works; native proactive recall inconclusive (bonus, non-blocking)."
metadata:
  node_type: memory
  type: project
doc_type: memory
keywords: [memory unification, knowledge/memory, MEMORY.md hot tier, reverse-symlink, native memory tool, lint_memory.py, _MEMORY_INDEX, _MEMORY_LIFECYCLE, doc_type memory, native auto-recall validation]
domain: [workflow, infra]
lifecycle: active
last_verified: 2026-07-29
---

The auto-memory layer was brought under the knowledge layer's index discipline on 2026-07-29 (see git commits `memory: …` on that date; design in `/Users/malachi/.claude/plans/one-of-my-biggest-bright-crown.md`).

**What changed:** 144 memory files moved into `knowledge/memory/` (now in git); each gained `doc_type: memory` + `keywords` + `domain` + `lifecycle` + `last_verified`, so `build_index.sh` folds them into `_ROUTING.md` (one grep surface) and generates `_MEMORY_INDEX.md` + `_MEMORY_LIFECYCLE.md`. `MEMORY.md` shrank from 19.6KB to 5.3KB (a capped hot tier) and no longer grows per fact. The native memory dir (`~/.claude/projects/-Users-malachi-Developer-work-mntn-workspace/memory`) is now a reverse-symlink to `knowledge/memory/`. `health_scorecard.py --memory` + `workflow_audit.sh` §10 add propose-only memory signals; `/capture` writes the new format.

**Why:** MEMORY.md was loaded whole every session (~4,900 tokens) so it couldn't grow — the pruning pressure. The knowledge index never has this problem because it's grepped, not ingested. Unifying memory into that same index removes the growth tax (~3,600 tokens/session saved) and gives memory git history + health tooling.

**Validation result (fresh session, 2026-07-29):** GREEN on the decisive checks. (1) The hot-tier `MEMORY.md` loads through the symlink — the native tool honors the reverse-symlink. (2) The native tool also WRITES through the symlink: `feedback_slack_reply_voice` + `feedback_hold_evidenced_verdict` were auto-written into `knowledge/memory/` and committed to git during normal sessions — unification proven end-to-end. (3) Grep-on-demand (`grep _ROUTING.md` → open one `memory/*.md`) works. Native **proactive** recall (a `<system-reminder>` surfacing a file by `description:` match) did NOT fire for a "frequency capping" probe — inconclusive (recall is probabilistic and the design never depended on it; grep is the primary path). Revert (only if a future session stops loading `MEMORY.md`): `rm "$NATIVE" && mv "$NATIVE".backup-2026-07-29-presymlink "$NATIVE"` (`.claude/README.md` § Auto-memory).

**Steady-state gap found + closed:** the native memory tool writes its OWN raw schema (no `doc_type`/`keywords`), so auto-written files sit OUT of `_ROUTING.md` until normalized. Closed by: `health_scorecard.py --memory` now flags them as `UNINDEXED` (SessionStart line), `workflow_audit.sh` §10 runs `lint_memory --check`, and `/capture` runs `lint_memory --fix` before `build_index.sh`. Run `python3 .claude/scripts/lint_memory.py --fix` any time the `Memory :` line shows `UNINDEXED`. Related: [[feedback_adversarial_workflow_authoring]] (Workflow args arrive as a JSON string — `JSON.parse` first).
