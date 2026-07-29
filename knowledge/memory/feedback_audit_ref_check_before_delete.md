---
name: feedback_audit_ref_check_before_delete
description: "Auditing/restructuring the workspace — ref-check before every move/delete; \"obvious\" deletes and agent proposals are often backwards"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: fc59db3f-b426-4cbe-9c11-c2bd5011531f
doc_type: memory
keywords: [audit_ref_check_before_delete, audit, ref, check, before, delete, auditing, restructuring]
domain: [workflow]
lifecycle: active
last_verified: 2026-07-20
---
When auditing or restructuring the workspace, **empirically reference-check every file before moving or
deleting it** — grep for who references it, check tracked-vs-untracked, read the ticket's `summary.md` to
find the blessed version. In the 2026-07-20 workspace audit, nearly every "obvious" delete was wrong on
inspection:
- The empty `ti_argocd_secrets_audit/` "ticket missing summary" was a 0-file scaffold — removing it lost nothing (good), but only because it was verified empty first.
- The `ti_200` "clear duplicate" CSVs the semantic agent flagged for deletion were the **blessed latest deliverables** per `summary.md` (the agent had it backwards), and all were gitignored anyway.
- `build_deliverable.py` looked superseded but is **reused by a different ticket** (INCR-75) — archiving it would have broken INCR-75.
- `RolloutTierEvaluations.py` is referenced in 10 files (CLAUDE.md + experimentation.md) → renaming was never "free."

**Why:** a naive structure audit has a high false-positive rate (sanctioned dash-dirs, conventional
`INDEX`/`SKILL`/`MEMORY` names, machine round-trip exports, config json all look like violations). Agent
"delete/archive" proposals are confident but can invert which file is canonical. And most workspace "mess"
is **gitignored local data (3.9 GB) with zero repo impact** — deleting it tidies only the local disk, not
the repo, so the risk rarely buys anything.

**How to apply:** (1) run `.claude/scripts/audit_structure.py` for the mechanical pass, but treat every
finding as a *proposal*; (2) before any move/delete: `git ls-files` (tracked?) + `grep -rIl <basename>`
(referenced?) + read the ticket summary (canonical?); (3) prefer **archive** (git mv → `_archive/`, reversible)
over delete; (4) hold any data deletion for the user; (5) when evidence contradicts the "safe to remove"
label, surface it instead of proceeding. See [[project_super_structure_adoption]], [[feedback_verify_edit_scripts]].
