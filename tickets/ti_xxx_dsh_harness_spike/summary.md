---
doc_type: ticket
title: "Local spike: dsh plugin harness"
status: in_progress
date: 2026-08-21
summary: "Evaluate DeepSeek Harness as sidecar automation harness and self-improvement engine substrate"
result: "not started"
question: "Can a pinned dsh sidecar run our skills, bq gate, and a harvesting engine v0 with every unit passing its adversarial test gate?"
framing_state: locked
---

# Local spike: dsh plugin harness

**Jira:** none (local-only spike)
**Status:** backlog
**Date Started:** 2026-08-21
**Assignee:** Malachi

---
## 0. Framing (locked 2026-08-21)
- **Question (the unknown):** Under the exact `0.1.1-rc.1` pin, can a dsh sidecar run the kit's components (6 skills mounted verbatim, bq cost gate as a `tools/pre-execute` deny plugin, memory-recall inject) plus an engine v0 harvesting real signals, with every unit passing its REJECT-by-default gate (vitest + behavioral + 2 adversarial reviews + budget)?
- **Goal (why / the decision):** Decides adopt-vs-abandon for dsh as the automation substrate, and whether the self-improvement loop graduates from propose-only to machine-gated. Waiting: Malachi only (local-only spike). North-star tie: Tier 3 infrastructure (velocity multiplier); no direct OKR tie — deliberate personal-stack investment, flagged per the leverage check.
- **Objective (done-when):** v0 exists = Phase 1-4 gates all green: dsh boots pinned with Keychain-routed Anthropic; skill catalog lists all 6; `bq_query` logs to the perf log and refuses an over-budget query with no force path; guard denies planted raw `bq query`; recall injects on a known TSV keyword; `harvest.py` emits ≥3 evidenced candidates; CC-0 auto-apply works. Binary per gate. Phases 5-6 are follow-on work, not this spike.
- **Approach (how):** Master plan `artifacts/ti_xxx_master_plan.md` Phases 1-4: sibling repo `dsh-lab` pinned exact, thin TS adapters over existing workspace scripts (bq_run.sh stays the implementation), per-unit test protocol from design C, Anthropic-only via llm-pi-ai (note: dsh API calls are API-billed, not subscription — engine spend caps stay enforced, $5/day fail-stop), engine v0 keyless Python. Each build step is one agent, independently testable.
- **What would change the answer:** Skills or bq plugin can't mount under the rc pin after 3 fix cycles (Phase 2 red) → abandon early. Any Sev-1 (secret exposure, egress finding) → kill switch, pause. Churn to keep the rc working >10h before v0 → abandon. Follow-on phases governed by kill criteria K1-K5 in design C §5.

## 1. Introduction
DeepSeek released deepseek-harness (`dsh`, 2026-08-14, MIT, 177k stars, v0.1.1-rc.1 developer preview): a Node/TS agent harness where every component (models, tools, skills, sessions, sandbox, agent loop, UI) is a hot-swappable Cordis plugin. Its primitives (append-only session logs, `llm-replay`, session fork, `tools/pre-execute` deny hooks, AgentSkills-standard skills shared with Claude Code) are a candidate substrate for two things the current kit lacks: (a) a headless automation harness, (b) replay-based evaluation that could close the self-improvement loop the kit deliberately keeps propose-only.

This spike: deep research (done 2026-08-21, 7-unit workflow), a reconciled master plan (3 design agents: integration, engine, verification/security), and a phased build with per-unit adversarial testing.

## 2. The Problem
- The kit's self-improvement loop is open: signals accrue and `/workflow-audit` proposes, but a human executes every improvement. The user wants improvement application automated behind a machine gate at least as rigorous as the human one.
- No way today to regression-test the harness itself (prompts, hooks, skills, knowledge edits) against recorded real work. dsh's replay substrate offers exactly this.
- Risk side: dsh is preview software (breaking changes promised, ~3-10x token hunger, no network egress restriction in its sandbox, in-process plugins) — adoption must be severable and hard-gated. MNTN constraints: Anthropic-only model routing (user decision 2026-08-21), no LLM keys on servers, knowledge stays git-based markdown.

## 3. Plan of Action
Master plan: `artifacts/ti_xxx_master_plan.md` (phases 0-6). Designs: `artifacts/ti_xxx_design_{a_integration,b_engine,c_verification}.md`. Research corpus: `artifacts/ti_xxx_research_*.md` (6 units) + `meetings/ti_xxx_01_fireship_dsh_video_2026_08_20.txt` (verbatim transcript).
1. Phase 0: local ticket + /frame + /capture research findings (in progress)
2. Phase 1: `dsh-lab` sibling repo foundations (pin, gate stub, profiles, Keychain launcher, behavioral harness)
3. Phase 2: core plugins (skills mount, `@mntn/dsh-bq` tool+guard, `@mntn/dsh-kit`, replay runner, parity suite)
4. Phase 3: security for headless (dshagent + pf egress, adversarial reviewer, killswitch + provenance)
5. Phase 4: engine v0 (keyless: harvest, transcript miner, corpus seed, entropy snapshot; auto-apply CC-0 only)
6. Phase 5: engine on dsh (replay verify, hypothesize/build/verify/adopt/observe, autonomy ladder)
7. Phase 6: integration scenarios, chaos drills, 10-day soak, go/no-go vs kill criteria

## 4. Investigation & Findings
Research corpus in `artifacts/` (2026-08-21). Headlines:
- dsh is model-agnostic: `llm-pi-ai` adapter runs Anthropic as pure config; `subagent-claude-code` pins the official Anthropic agent SDK. Not DeepSeek-locked.
- Skills: same AgentSkills SKILL.md standard as Claude Code; our 6 skills mount verbatim via `skill-filesystem` customDirs. Zero copies.
- No BigQuery plugin exists in the ~10k-repo `dsh-plugin` ecosystem (24 data repos surveyed, all tiny, none fit) — `@mntn/dsh-bq` is an ecosystem first.
- Replay substrate: "model-visible ⟺ logged" invariant + `llm-replay` + session fork = keyless regression testing of full trajectories; dsh's own CI works this way.
- Security (Hedemark audit + Wavect): sandbox restricts writes only; NO outbound network restriction; plugins run in-process with user permissions; npm provenance gaps. Egress control must be external (pf + proxy).
- Press verdict: better architecture, worse daily coder than Claude Code today; ~10x Pi token usage; 3-6 months to production maturity. Sidecar stance confirmed correct.
- Cordis paper: revertible effects + reactive coeffects give exact-withdrawal unload (Thm 61/62) and as-if-static confluence (Thm 73); inverse correctness is an author obligation the runtime does NOT verify — we test unload in every plugin's vitest suite rather than trusting it.

### Phase 1 build findings (2026-08-21, all 5 gates PASSED)
- `dsh-lab` sibling repo live: pin `@deepseek-ai/dsh@0.1.1-rc.1`, node@24 keg-only (system node 22.16 fails the `^22.19` engine floor), pnpm 11.22. Web UI boots in 4s; headless "Reply exactly OK" round-trips through Keychain key + `llm-pi-ai` + `claude-sonnet-5`, exit 0.
- **Telemetry surprise:** the default composition mounts `session-telemetry-otel` ACTIVE with OTLP endpoint defaulting to `https://harness-telemetry.deepseeksvc.com/v1/logs` (mode env-gated to DISABLED, but the row is live). Our gate caught it; disabled by patch row in every profile. Validates the assert-absence policy over trust-the-default.
- **Boot gotcha:** composed sandbox+approval defaults must match a named permission preset or the plugin tree fails to load. Added `ci-headless` preset (workspace-write + approval never) for headless/test profiles.
- **Version pinning gotcha:** cross-package deps only resolve at the same rc pin (`dsh-llm-replay@0.0.1-rc.1` depends on a never-published package; `@0.1.1-rc.1` installs clean). Confirms the exact-pin-everything policy.
- Session logs: `$DSH_HOME/sessions/<cwd-slug>/session-<uuid>/session.jsonl.zstd`; usage on `assistant/message` `data.usage.{inputTokens,outputTokens,cacheWriteTokens}`. Full runtime facts: `dsh-lab/tests/README.md`.
- macOS: no `timeout(1)`; behavioral harness uses a perl alarm wrapper (same footgun class as `find -newermt`).
- Gate `dshkit_verify.sh` REJECT-by-default proven: dummy unit without evidence bundle rejected naming the missing pieces; pnpm build-script allowlist held to 3 native deps (subprocess-local, koffi, node-pty).

### Phase 2.1-2.5 build findings (2026-08-21, gates PASSED after adversarial review)
- **Skills mount verbatim**: `skill-filesystem` `customSkillDirs` → `.claude/skills`; `/frame` loaded headless by name, exact heading returned. Note: skill-filesystem + tool-skill ship `disabled: true` at this rc; enable via patch rows.
- **`@mntn/dsh-bq`** (tool + guard) and **`@mntn/dsh-kit`** (recall/orient/commands): 55 unit tests total, behavioral cases green, per-unit gate green.
- **Adversarial review process paid for itself immediately.** 4 fresh-context reviewers (2/unit) found 3 BLOCKERs + 12 MAJORs across the two units, all with empirical repros. Highlights: (1) JS regex port of the bash bq guard missed `bq query` on line 2+ of multiline commands (grep is line-anchored, JS `^` is not) — a working governance bypass, now fixed with per-line evaluation + live multiline deny proven in-harness; (2) the dry-run cost cap was DEAD CODE: `--format=prettyjson` suppresses bq's "N bytes" sentence (verified in bq CLI source) — the sole cost control in no-approval profiles silently never fired; now format-flag-free dry-run + fail-closed on unparseable estimates; (3) unhandled stdin EPIPE in the spawn helper CRASHED the whole dsh host process (reproduced with 1MB input); (4) orient's `git pull` fired from a dsh test session and mutated the real workspace (evidenced in the session log) — session_start_routing.sh gained `ORIENT_NO_PULL=1`, dsh orients read-only; (5) recall block was unwrapped user-role text = prompt-injection laundering in no-approval profiles — now wrapped in an explicit not-instructions envelope; (6) tool errors returned as SUCCESS strings would corrupt isError consumers/Code Mode — all failure paths now throw; (7) provenance footer could attribute another query's perf-log entry — now sha256-bound to the exact SQL. Full dispositions: `dsh-lab/packages/*/reviews/`.
- **Behavioral asserts hardened**: model self-report ("reply YES if...") replaced by deterministic session-log content assertions (`events_content_matches` on the injected plugin message).
- Recall parity details: fires only on fresh user text (no per-step double-fire), skips delegated subagents (`delegationDepth`), 66ms measured overhead.
- select1 live BQ behavioral case still BLOCKED on expired gcloud auth (user to run `gcloud auth login`).

## 5. Solution
What was done to resolve the issue:
- Code changes (PRs, commits)
- Configuration changes
- Recommendations made
- Dashboards/reports created

## 6. Questions Answered
Specific questions that were resolved during this ticket:
- **Q:** {question}
  **A:** {answer}

## 7. Data Documentation Updates
What new knowledge was added to `data_catalog.md` or `data_knowledge.md` as a result of this ticket.

## 8. Open Items / Follow-ups
Anything not resolved, handed off, or deferred.
