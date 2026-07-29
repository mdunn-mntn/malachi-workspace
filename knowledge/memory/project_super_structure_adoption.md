---
name: project_super_structure_adoption
description: "Super-structure adoption — thin-kernel roadmap from the 6-plan synthesis; Phase-0 + work-structure SHIPPED, 3 kernels pending"
metadata: 
  node_type: memory
  type: project
  originSessionId: fc59db3f-b426-4cbe-9c11-c2bd5011531f
doc_type: memory
keywords: [super_structure_adoption, thin kernel, work_structure kernel, lint_tickets.py, bq_velocity, audit_structure.py, new_ticket.sh, anti-goals, health_scorecard, self_improvement]
domain: [project, workflow]
lifecycle: active
last_verified: 2026-07-20
---
Synthesis of 6 AI-authored "component plans" into ONE thin system. Thesis: typed front-matter → linter →
generated index; collapse 6 schemas/3 ledgers/6 toolchains into one. Roadmap + verdict table + named
anti-goals: `claude-prompts/super_structure_decision.md` (source of truth for build order).

**Shipped 2026-07-20 (on main):**
- **bq_velocity Phase 0** (`72281533`,`06fe7132`) — provenance-only fields (`phase/sql_sha256/sql_preview/git_commit`) in the existing perf log + read-only `.claude/scripts/bq_verify.py`. Shipped the provenance half ONLY, **not** the dry-run abort gate (the panel wanted a gate; [[feedback_bq_workflow]] forbids cost warnings / preempting long queries — honored). Also fixed a latent apostrophe-in-jq-comment bug that had broken `bq_run.sh` on main since `bafa6bf`.
- **work_structure kernel** (`5ef73a9f`,`9d0c8201`) — ~5 front-matter fields (`doc_type/title/status/date/summary/result`) on every ticket `summary.md`; `tickets/INDEX.md` now populated with a blessed one-line `result` column; `.claude/scripts/lint_tickets.py` (mirrors `lint_coverage.py`; rule = `status:done ⇒ real result`; 86 cards, 0 violations); `data/final/` documented as a convention in `folder_definitions.md`. Backfill done via the 8-shard Workflow factory.

**ALL kernels shipped 2026-07-20 (commits after 9d0c8201):**
- **analysis_methodology** (`61d9338d`) — `experimentation.md` new section "⭐ Before you report a number": write-the-null-first, the Shocking-Number Gate (triangulate + uncertainty-on-both + sign-the-bias + 1 adversarial pass), consolidated sanity checklist (cross-links AUDI-1089 patterns, doesn't restate). Adversarially reviewed (2 lenses); fixed a self-contradictory 184x reuse.
- **self_improvement** (`2319977a`) — `health_scorecard.py` (read-only: days-since-`/capture` via `: capture` ritual grep, orphans >120d, dup-H1 titles) in the SessionStart print; `log_request.py` UserPromptSubmit hook → gitignored keyword-only `knowledge/.request_log.jsonl` → `request_digest.py` proposes a `/skill`. No delete authority; hook fuzzed 7 failure modes (silent, exit 0).
- **deck_structure** (`29fddfea`) — `presentation_playbook.md` Part 10 "The De-Slop Pass" (slop list + advisory grep, scope-guarded: no mandated style, doesn't override facts-not-presentation) + warn-only step 11 in `presentation_critique.md`.
- **execution_engine** (blessed) — 6 live-crawl lessons appended to `workflows/agent_pass_runbook.md`; roster+linters+manifest blessed as THE factory.

The whole 6-plan synthesis is DONE — thin extension live on `main`, zero named anti-goals built.

**Then hardened + audited to completion (2026-07-20, commits `02c3681f`→`35be1c74`):** built `.claude/scripts/audit_structure.py` (read-only structure auditor vs folder_definitions; refined to whitelist sanctioned dash-dirs / conventional INDEX·SKILL·MEMORY names / config json / Claude auto-dirs) and `.claude/scripts/new_ticket.sh` (one-command conforming ticket scaffold: validates name, derives Jira ID, prefills lint-passing front-matter, refreshes INDEX; `--parent` for epic children). Reconciled the stale `folder_definitions.md` root spec + documented naming carve-outs. Audit finding: the repo was **~95% structurally clean** — the "mess" was 3.9 GB of *gitignored* data + ~20 tracked-cruft files, not disordered structure. Cleaned junk (Spark markers, run-captures, __pycache__/.idea), archived 8 superseded versions, untracked `todoist-mcp-transfer/` (kept on disk for the MCP — `.mcp.json` doesn't reference it) + the stray in-repo `.claude/projects/` memory tree (rescued [[reference_olympus_repo]] to global first), slimmed a vendored PR to a link. **The plumbing has no meaningful gaps left; the bottleneck is now USE, not tooling** — highest leverage = run the finished setup on Tier-1 work (BER-2250). See [[feedback_audit_ref_check_before_delete]].

**Named anti-goals (do NOT build):** autonomous Gardener/net-negative-bytes auto-delete, any 2nd ledger, 9-rung hard gate, parallel deck tooling, `result_sha256` re-run-assert (pin-and-show instead — BQ tables mutate), `summary→README` rename, hard `_v2/_final` ban, `claims.yaml`, new agents, headless "runs-itself" timer loop. NB: the Pi cron is only the bounded Slack-extraction bot → review queue (still runs nightly per CLAUDE.md), NOT a self-improvement engine — don't repeat the earlier "decommissioned" misclaim. See [[project_structured_bq_catalog]] for the shipped catalog this builds on.
