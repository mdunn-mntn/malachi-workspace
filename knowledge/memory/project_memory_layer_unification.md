---
name: project_memory_layer_unification
description: "Auto-memory unified into knowledge/memory/ (in git) under the one _ROUTING index; MEMORY.md shrunk to a hot tier (19.6KB->5.3KB, growth halted); native dir is a reverse-symlink. PENDING: user validates native auto-recall on next fresh session."
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

**How to apply / PENDING user action:** On the **next fresh session**, confirm (1) `MEMORY.md` still auto-loads into context and (2) a `description:` match still surfaces a memory file in a `<system-reminder>`. If either fails, the native tool is rejecting the symlink — revert per `.claude/README.md` (§ Auto-memory): `rm "$NATIVE" && mv "$NATIVE".backup-2026-07-29-presymlink "$NATIVE"`. Files stay safe in git regardless; retrieval falls back to grepping `_ROUTING.md`. Once validated green, this memory can be set `lifecycle: archived`. Related: [[feedback_adversarial_workflow_authoring]] (Workflow args arrive as a JSON string — `JSON.parse` first).
