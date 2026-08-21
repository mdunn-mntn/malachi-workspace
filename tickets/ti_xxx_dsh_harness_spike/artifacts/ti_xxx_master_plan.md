# DeepSeek Harness (dsh) — Research Complete + Master Plan

## Context

DeepSeek released `dsh` (deepseek-ai/deepseek-harness, 2026-08-14, MIT, 177k★, v0.1.1-rc.1 developer preview): a Node/TS agent harness where everything — models, tools, skills, sessions, sandbox, agent loop, UI — is a hot-swappable Cordis plugin ("A Programming Paradigm for Spatiotemporal Composability": revertible effects + reactive coeffects, exact-withdrawal unload, HMR). Deep research ran as a 7-unit workflow (docs site, repo internals, Cordis paper, plugin ecosystem, press/security analysis, video transcript, workspace inventory) + 3 design agents (integration, self-improvement engine, verification/security).

**Goal:** use dsh as the substrate to close the loop the current kit deliberately leaves open — from propose-only self-audit to a machine-gated, self-improving workflow — while porting/wrapping existing components so they run host-agnostic, each built and adversarially tested as an independent unit, then integrated and soak-tested as one system.

**Why dsh fits:** append-only session logs ("model-visible ⟺ logged") + `ctx.sessions.fork()` + `llm-replay` = replay-eval of recorded real work against changed compositions (harness A/B testing — how dsh's own CI works). `tools/pre-execute` deny = our hard gates as typed plugins. Skills use the same AgentSkills SKILL.md standard as Claude Code (our 6 skills mount verbatim). Model-agnostic (`llm-pi-ai` → Anthropic as pure config). No BigQuery plugin exists in the ~10k-repo ecosystem — `dsh-bq` is an ecosystem first.

**Key risks (from research):** preview churn (breaking changes promised; ecosystem pins exact snapshots); token hunger (~3-10x peers); sandbox has NO outbound-network restriction (read + exfiltrate possible); plugins run in-process with full user permissions; npm provenance gaps. All addressed in Phase 3 + kill criteria.

**Also delivered by research:** full verbatim transcript of the Fireship video (2026-08-20, "DeepSeek is back... and Silicon Valley is terrified", 5m32s) — obtained via caption API, no yt-dlp needed.

## User decisions (confirmed via AskUserQuestion)
1. **Models:** Anthropic-only now; DeepSeek toggleable later (a `disabled: true` llm-deepseek row in the lab profile only; no `DEEPSEEK_API_KEY` anywhere until explicitly enabled — compliance flag stands).
2. **Role:** dsh = sidecar lab + automation harness. Claude Code stays the daily interactive driver. Components built host-agnostic.
3. **Autonomy:** machine-gated ladder. Auto-adopt only what passes replay evals + adversarial refutation + rollback watch; change classes earn autonomy gradually; permanent floors (no deletes, no prod, no spend over budget, no external posts, no secrets, verifier never self-modifies).
4. **Ticket:** AUDI spike — scaffold locally + draft Jira for confirm.

## Placement (reconciled across the 3 designs)
- **New sibling repo `/Users/malachi/Developer/work/mntn/dsh-lab/`** — all dsh TypeScript, profiles, tests, security infra. Reasons: `$WS/.gitignore` ignores `*.json` globally (Node repo would fight it); the Pi cron `--ff-only` pulls `$WS` unattended (preview churn doesn't belong there); `dsh-bq` must be publishable from a repo with zero MNTN internals.
  Layout: `packages/{dsh-bq,dsh-kit,engine-guard,replay-tools}` · `profiles/{mntn-analyst,mntn-automation,mntn-lab,mntn-engine,test-headless}` (installed to `~/.dsh/profiles/` by `scripts/install_profiles.sh`; `$DSH_HOME/cordis.patch.yml` kept empty by policy) · `bin/dsh-mntn` (Keychain→env launcher) · `tests/{behavioral,replay,chaos,integration,parity}` · `scripts/{dshkit_verify.sh,dsh_behave.sh,dsh_replay.sh,egress_selftest.sh,killswitch.sh,...}` · `launchd/` · `compatibility.json`, `ALLOWLIST.txt`, `GATE_MANIFEST.sha256`, `VERSIONS.md`.
- **`$WS/engine/`** — the self-improvement engine's knowledge-facing surfaces (python/md/jsonl, fits the workspace repo): `FLOORS.yml`, `engine.config.yml`, `ENGINE.md`, `ENGINE_LOG.md`, `candidates/`, `corpus/` (manifest committed; raw cases gitignored, Mac-local), `metrics/entropy.jsonl`, `scripts/{harvest,transcript_miner,entropy_snapshot,adopt,observe,rollback,ladder}.py|sh`, `workflows/{hypothesize,verify}.js`.
- **`$WS` touched by dsh only via config**: skills mounted by `skill-filesystem` customDirs → `$WS/.claude/skills` (no copies, no symlinks); dsh sessions run with cwd `$WS` so knowledge/perf-log writes land in git as today.
- dsh pinned EXACT `0.1.1-rc.1`; upgrades are deliberate tested events (dump-config diff → parity suite → replay suite → smoke), time logged to `churn_log.jsonl`.

## Architecture: what maps where (Part A summary)
- **Hard gate port (rewrite, small):** `enforce_bq_wrapper.sh` regex → `tools/pre-execute` deny in `@mntn/dsh-bq` guard sub-plugin; vitest cases transliterated from `hooks_selftest.sh` so both hosts test against the same corpus.
- **Thin wraps (script stays the implementation):** `memory_recall.py` (prompt-interception spawn → `agent.inject()`), `session_start_routing.sh` (orient at agent creation), `flag_net_new_tables.sh` (post-execute in dsh-bq), `log_request.py` (analyst profile only).
- **Reuse verbatim:** all 6 skills; all 31 scripts; knowledge base + indexes + git hooks (repo-level, host-agnostic).
- **Don't port:** Pi cron trio (keyless, stays), brevity pair + comms nudges (Claude Code chat ergonomics), memory reverse symlink, OpenViking/MemOS (parallel stores violate git-as-system-of-record), dsh-data-agent/dsh-sql (bypass bq_run.sh invariants), marketplace plugins (in-process, unreviewed), `/frame` automation (interactive by design), subagent-claude-code in daily profiles (257MB, pointless when CC is the driver — lab A/B only).
- **`@mntn/dsh-bq`:** `bq_query` tool (`sql, ticket, label, phase, dry_run`) shelling to `bq_run.sh` via `ctx.subprocess`; no `--force` path exists when `allowForce=false` (cost-gate refusals un-overridable by the model); returns rows + provenance footer (job_id, est→billed GB, warnings); after success feeds `flag_net_new_tables.sh` for doc-debt parity.
- **New capability:** `ctx.commands` `/verify` `/orient` `/bq-perf` `/audit-signals` — deterministic ops with NO model turn.
- **Model routing:** `llm-pi-ai` row, `apiKeyEnv: ANTHROPIC_API_KEY`, key exported from login Keychain by `bin/dsh-mntn` at launch. No plaintext keys anywhere; `git grep` secret-audit in dsh-lab pre-commit. dsh never installed on the Pi.

## The self-improvement engine (Part B summary)
Pipeline: **HARVEST** (deterministic python, daily launchd: mines perf log, request log, brevity log, doc-debt queue, incident log, signals_*.md, dsh session logs, Claude Code transcripts → `candidates/queue.jsonl` with evidence counts) → **HYPOTHESIZE** (dsh_workflow script; spec with ONE pre-registered metric + min_delta; spec without a metric is refused — anti-reward-hacking anchor) → **BUILD** (isolated `git worktree` + candidate profile; plugins smoke-tested in creator-mode memory first) → **VERIFY** (the machine gate, 4 mandatory parts: Tier-1 keyless deterministic replay of corpus cases under the candidate composition (context-assembly + tool-schema + deterministic-tool-equality asserts); Tier-2 live re-runs + fork-continuations at the recorded friction point, graded by per-case deterministic checks; 2 adversarial refuters (read-only, refute-first, gaming hunt); statics (verify.sh, lints, vitest). PASS = quantitative: Tier-1 100%, pre-registered metric ≥ min_delta, no guard metric regressed beyond tolerance, no reproducible refutation) → **ADOPT** (deterministic `adopt.sh`: git merge with `Engine-Candidate`/`Engine-Verdict-SHA` trailers through normal pre-commit gates; plugins via HMR-watched `engine.patch.yml` row — Cordis hot-swap, row id = rollback handle; floor-violating diffs demote to the human PROPOSE queue) → **OBSERVE** (7-day/20-session window; nightly metric + full-corpus replay watch; auto-rollback = git revert / row `disabled: true`; every rollback demotes the class 14 days and becomes a corpus case).
- **Eval corpus:** seed = 5 retrieval probes + 3 recent ticket sessions; grows ≤1 case per closed ticket (the `/frame` binary Objective compiles into the case's checks — the framing gate is the labeling mechanism), every incident, every rollback. 20% holdout invisible to builder agents. Tier-2 capped 40 cases / $3 per run; Tier-1 unbounded (keyless; can also run nightly on the Pi over committed logs).
- **Metrics (no composite scalar — nothing to Goodhart):** retrieval_hit_rate, duplicate/overlap counts, orphan/stale docs, doc_debt, tokens_to_answer, usd_per_query, repeat_incident_rate, brevity_breach_rate, requests_served_by_existing_skill, hot_tier_budget → `metrics/entropy.jsonl`. Paired-metric rule: cost/brevity metrics count only on runs whose task checks PASS.
- **Self-documentation:** every adoption writes `knowledge/decisions/NNNN_engine_<slug>.md` (evidence, scores, reviewer verdicts, window outcome) + doc CHANGELOG regions + git trailers; `ENGINE_LOG.md` is itself harvested.
- **Floors (permanent, enforced by engine-guard `tools/pre-execute` denials + protected-path pre-commit + `FLOORS.yml`):** no knowledge deletion (append/supersede; sole exception: reverting the engine's own commit inside its window) · no prod mutations · no spend over budget (default $5/day engine-wide, fail-stop) · no secrets access · nothing leaves the machine except `git push` to the private repo and the model API. Kill switches: `touch $WS/engine/STOP` + `dsh-lab/scripts/killswitch.sh`.

## Verification & governance (Part C summary)
- **Prime directive:** every unit REJECTED by default. `dshkit_verify.sh` (plugs into `verify.sh` full) fails any unit missing `test/`, `behavioral/<case>/`, 2 fresh `reviews/`, or `BUDGET.yaml`. No override flag. Coverage floors 90%/file, 100% on guard/deny paths. Unit tests must prove clean unload (Cordis inverse correctness is author-obligated — tested, not trusted).
- **Behavioral harness:** hermetic `$DSH_HOME` mktemp + `test-headless` profile (approval `never`, workspace-write) + `expect.yaml` asserts over session jsonl (events_present/absent, files, exit code, `system_prompt_count: 1` standing regression for the known duplicate-prompt bug, budget rollup). B-replay tier keyless; B-live tier Mac-only, budgeted.
- **Integration ("one major unit"):** S1 analytics E2E (bq gate denies a planted raw `bq query`; perf log gains exact rows), S2 on-call 3-surface write-back with zero prod-verb events, S3 full engine cycle with forced-failure auto-revert, S4 host-kit regression (`verify.sh full` + `hooks_selftest.sh` still green with dsh present). Golden-transcript replay on every config change/version bump/weekly. Chaos drills: kill -9 mid-run, credential revocation, corrupt config (verify Cordis rollback empirically), egress cut, kill-switch drill. Soak: 10 working days, zero Sev-1, ≤2 Sev-2, cost cap 8/10 days — precedes any headless autonomy.
- **Security hardening (before any unattended run on real data):** dedicated macOS user `dshagent` + pf default-deny + loopback allowlisting proxy (api.anthropic.com, googleapis; `egress_selftest.sh` verifies) · OS-level filesystem scoping (dshagent cannot read `/Users/malachi/**`) + dsh-guard path denials (`~/.ssh`, credentials, Keychain) · Keychain-only keys with `apiKeyEnv` indirection, secret-grep audits · plugin allowlist = first-party + our own only, `ignore-scripts=true`, empty `allowBuilds`, supervised lockfile diffs; dsh_workflow adoption only by pinned SHA after its own gate pass (default NO, phase-2 decision) · telemetry asserted absent per profile dump-config · Code mode + `cordis_*` self-modification tools disabled in all v1 profiles (creator-mode lab excepted, supervised) · flagged: `dshagent` gcloud identity may need IT sign-off; pf/user-creation sudo steps are human-run.
- **Autonomy ladder (reconciled B+C):** L0 supervised interactive (start) → L1 headless read-only/report (needs egress green + 10-day soak + kill-switch drill + corpus ≥20) → L2 auto-adopt CC-0 (index rebuilds, OBSERVED appends, routing keywords, corpus additions — ships at engine v0) then CC-1 knowledge appends (30 clean human-approved proposals + 0 escaped defects 30 days) → L3+ (skills, plugins, harness config, engine-self per Part B rungs 2-5 with meta-eval) — gated on dsh leaving rc and a ≥90-day L2 record. Auto-demotion: escaped defect = class drops to human-gated 14 days; Sev-1 = kill switch + L0; 2 escaped defects/30 days = engine paused.
- **NEVER auto-apply (hard-coded outside the engine):** knowledge deletion, prod, spend breach, external posts, security config, the verifier itself (`GATE_MANIFEST.sha256` self-check), CLAUDE.md, git history.
- **Program kill criteria (monthly review, logged data):** K1 churn >6h/mo × 2mo (freeze) or >10h/mo (abandon review) · K2 one exfil/credential incident = pause, two = abandon · K3 corpus pass rate <90% 3 weeks running, or version bump drifts >20% goldens twice · K4 dsh >3x baseline cost 4 weeks or >$200/mo without documented win · K5 upstream stalls ≥60 days or standing bug mitigation >2h/week. Any K → killswitch + written go/no-go; the existing kit runs regardless — dsh is always severable.

## Execution phases (each step ≈ one agent, ≤ half-day, independently testable)

**Phase 0 — ticket + capture (no dsh):**
0.1 Scaffold spike folder via `new_ticket.sh` (e.g. `ti_xxx_dsh_harness_spike`), copy the 4 research/design docs from scratch into `artifacts/` (research corpus: `/private/tmp/claude-501/-Users-malachi-Developer-work-mntn-workspace/a7dff816-cf0a-4406-a0f4-a2b6c1594db0/tasks/wjqpkdiln.output`; designs: `.../tasks/{a3bdb6a3efae06596,a33e53f5911379509,adee5bc232d2a4a18}.output` — copy NOW, scratch is session-scoped), file the Fireship transcript into `meetings/`, draft linted Jira spike for user confirm, `/frame` it.
0.2 `/capture` the research findings into `knowledge/` (dsh facts, Cordis model, ecosystem gaps, security posture) + commit.

**Phase 1 — foundations (A1-3 + C1-2):**
1.1 Bootstrap `dsh-lab` repo, pnpm workspace, exact pin, `VERSIONS.md`; test: web UI boots, `--dump-default-config` prints.
1.2 Gate stub: `dshkit_verify.sh` v0 (REJECT-by-default structure check + secret/`!!js` lints), `GATE_MANIFEST.sha256`, `ALLOWLIST.txt`, `$DSH_HOME` as git repo; test: rejects a test-less dummy unit.
1.3 Profiles skeleton + `install_profiles.sh`; test: `--dump-config` shows composed rows, home patch empty.
1.4 Keychain launcher + Anthropic routing via llm-pi-ai; test: headless "Reply exactly OK" exits 0; secret-grep clean.
1.5 Behavioral harness `dsh_behave.sh` + hermetic test-headless profile + 3 seed cases + dup-system-prompt regression fixture; acceptance includes documenting session-jsonl path, usage-event schema, replay format.

**Phase 2 — core plugins (A4-9,12; every unit passes the Phase-1 gate):**
2.1 Mount 6 skills via skill-filesystem customDirs; test: catalog lists all 6, `/frame` interactive in Web UI.
2.2 `@mntn/dsh-bq` tool; test: headless SELECT 1 → new perf-log line; oversized query → gate refusal, no force path.
2.3 dsh-bq guard; test: planted raw `bq query` denied; allowlist (`--dry_run`, INFORMATION_SCHEMA, `bq show/ls`) passes.
2.4 `@mntn/dsh-kit` recall inject; test: TSV-keyword prompt → injected pointer block in session log; unknown prompt → none.
2.5 dsh-kit orient + `ctx.commands`; test: `/verify` runs with zero model turns.
2.6 Replay runner `dsh_replay.sh` + first 3 goldens; test: unmodified replay green, broken schema red.
2.7 Parity suite (`hooks_selftest.sh` cases vs dsh guards) + upgrade checklist; test: breaking the guard regex fails suite.
2.8 (timeboxed spike) `hooks-claude-code` bridge for advisory hooks only; deliverable may be a "not worth it" note.

**Phase 3 — security for headless (C4-6; human runs sudo steps):**
3.1 `dshagent` account + pf anchor + loopback proxy + `egress_selftest.sh`; BLOCKS all unattended work until green.
3.2 Adversarial-review agent (`dsh-reviewer-adversarial.md`, refute-first, no Write/Edit) + review-record staleness check in gate.
3.3 Provenance trailers in commit-msg hook + `killswitch.sh` + `DISABLED` flag + first kill-switch drill.

**Phase 4 — engine v0 (B1-5, keyless, no dsh dependency):**
4.1 `engine/` scaffold + `FLOORS.yml` + protected-path pre-commit; test: FLOORS.yml commit without human trailer rejected.
4.2 `harvest.py` over the 7 signal files; test: ≥3 candidates from today's logs, counts match manual grep.
4.3 `transcript_miner.py` (Claude Code jsonl → Tier-2 cases + signals); 4.4 corpus schema + seed (5 probes + 3 ticket sessions); 4.5 `entropy_snapshot.py`; tests per design.
→ v0 ships: harvest + evidence-backed proposals into the existing propose channel + auto-apply of CC-0 only.

**Phase 5 — engine on dsh (B6-14):**
5.1 Tier-1 replay runner (`mntn-replay` profile + replay-tools plugin); 5.2 `hypothesize.js` with pre-registration (metric-less spec refused); 5.3 builder (worktree + candidate profile); 5.4 `verify.js` core (seeded regression fails it); 5.5 adversarial refuters (planted brevity-gaming candidate caught); 5.6 `adopt.sh` + provenance (knowledge-deleting candidate demotes to PROPOSE); 5.7 `observe.py` + `rollback.sh` + circuit breaker; 5.8 scheduling (launchd + dsh-routines + STOP switch); 5.9 `ladder.py` + Pi nightly Tier-1 (keyless env audit).

**Phase 6 — integration as one unit (C§2):**
6.1 S1-S4 end-to-end scenarios green within budgets; 6.2 chaos drills all pass; 6.3 10-day soak; 6.4 go/no-go review vs kill criteria → L1 grant, CC-1 clock starts; `/capture` closes the spike with the decision.

## Verification of the plan itself
Every phase ends with its named test criteria; nothing advances to the next phase with a red gate. The whole program is severable at every point: the existing kit is never modified except additively (`engine/` dir, one agent file, commit-msg trailer extension), and dsh-lab can be deleted wholesale.

## Critical files
- `$WS/.claude/scripts/bq_run.sh`, `$WS/.claude/hooks/enforce_bq_wrapper.sh`, `$WS/.claude/hooks/memory_recall.py`, `$WS/.claude/scripts/hooks_selftest.sh`, `$WS/.claude/agents/reviewer-adversarial.md` — contracts the dsh adapters wrap/port.
- `$WS/claude-prompts/self_improvement_engine_plan.md`, `$WS/knowledge/eval_probes.md`, `$WS/.claude/scripts/{workflow_audit.sh,health_scorecard.py}` — predecessors the engine extends.
- New: `dsh-lab/` (sibling repo), `$WS/engine/`, `$WS/.claude/agents/dsh-reviewer-adversarial.md`.
