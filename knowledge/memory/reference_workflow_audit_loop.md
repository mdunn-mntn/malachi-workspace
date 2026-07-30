---
name: reference-workflow-audit-loop
description: "The System-retro loop — /workflow-audit skill + aggregator + weekly Pi cron (deterministic) / Mac (reasoning) split; propose-only, key-free"
metadata: 
  node_type: memory
  type: reference
  originSessionId: 9c582365-7ebc-49fa-9d1e-6d93ac47841b
doc_type: memory
keywords: [workflow audit loop, workflow-audit, system retro, aggregator, propose-only, Pi cron, signals, health_scorecard, request_digest, lint_tickets, kit compliance, verify.sh whole-repo, memory health section, section 11]
domain: [workflow, infra]
lifecycle: active
last_verified: 2026-07-24
---
## /workflow-audit — the workflow reviews itself (System-retro loop)

The cadence trigger that turns the always-on deterministic signals into ONE ranked, **propose-only**
action list. The fifth learning loop from `claude-prompts/self_improvement_engine_plan.md`.

**Two pieces:**
- `.claude/scripts/workflow_audit.sh` — deterministic aggregator. Runs every read-only check (structure
  conformance via `audit_structure.py`, ticket/framing adherence via `lint_tickets.py`, KB health via
  `health_scorecard.py`, coverage debt, perf drift via `perf_digest.py`, request patterns via
  `request_digest.py`) into one markdown signal rollup. Pure Python + git — **no API key, no model**.
  Environment-aware: on a fresh checkout the gitignored request log is absent → §6 request-mining SKIPPED.
- `.claude/skills/workflow-audit/SKILL.md` — the reasoning layer. Reads the rollup, produces a Tier 1
  Safe / Tier 2 Judgment / Tier 3 Standards report at `claude-prompts/workflow_audits/audit_<date>.md`.
  **Propose-only:** no delete/edit authority over knowledge/tickets/CLAUDE.md; it commits only its own
  report. Applies a Tier item ONLY when the user says "do Tier 1 / apply items 1-3".

**Scheduled split (compliant, key-free) — added 2026-07-24:**
- **Pi cron** (Mon 08:00 PT, `~/run_workflow_audit.sh`) runs ONLY the aggregator, commits dated
  `signals_<date>.md`. Guarantees weekly capture even when the Mac sleeps. No key on the Pi (that is the
  pattern MNTN killed with the Slack bot). See [[reference_pi5_server]] / [[reference_pi5_server]].
- **Mac** runs `/workflow-audit` at the next session → reasons over fresh signals (incl. the local-only
  request log) → writes `audit_<date>.md`. Full autonomy on a schedule would need a model always-on =
  a key on the Pi = not allowed; that trade was made deliberately (chose compliance over hands-off).

**Args:** `/workflow-audit` (full) · `adherence` · `perf` · `requests` (Mac-only) · `retro` (also
challenge whether the standards themselves are stale).

**First run (2026-07-24) — both items shipped same day:**
- Request-log extractor FIXED. `log_request.py` was logging harness re-invocations (`<task-notification>`
  → `task/notification/toolu_/output/file/private/tmp`) as if they were prompts. Fix: skip injected
  re-invocations, strip `<...>` markup + file paths, denylist boilerplate/contraction noise. Cleaned 25
  polluted records; top nouns now real (`capture`, `ticket`, `xlsx`). Request-mining un-blinded.
- Signal-file pruning ADDED. Aggregator §9 flags `signals_*.md` beyond keep-8 with a ready `git rm`;
  skill proposes it Tier 1 Safe. Reasoned `audit_*.md` reports always kept.

**§10 Memory health + §11 Kit compliance (added 2026-07-29).** §10 runs `health_scorecard.py --memory` +
`lint_memory --check` (lifecycle rollup, overlap clusters, unresolved wikilinks, UNINDEXED native-writes,
MEMORY.md budget). §11 runs `verify.sh` **whole-repo** (the 3 front-matter linters + index-freshness + the
9-hook self-test) so the same checks the commit gate enforces per-commit are surfaced repo-wide weekly. The
gate ([[reference_commit_gate]]) is per-commit enforcement; §11 is the whole-repo review (propose-only:
Tier 1 `verify.sh --fix` for a stale index, Tier 2 for a real violation).

**The §10 overlap-cluster signal is a REVIEW PROMPT, not a dup-detector (established 2026-07-29).** It
flags active memories sharing a filename-stem token (≥3 files); sharing a token usually means DISTINCT
facets, not duplicates. Reviewed all 18 flagged clusters (fangorn / repo / scoring / bidder / …) in one
pass — **0 genuine near-duplicates**; every file was a distinct fact (the `repo` cluster = 7 files each
about a different repo). Do NOT auto-merge on this signal; read the cluster, merge only true same-fact
redundancy (via `/capture`, the sole merge authority). A cluster review CAN still surface a factual
contradiction to fix (e.g. `reference_hhst_pacing_lever` named the DDM pilot as the prod HHST setter,
corrected to the camperbid→idso chain) — that's a correction, not a merge.
