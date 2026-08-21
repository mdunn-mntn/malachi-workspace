# PART B — The Self-Improvement Engine on dsh

> Design agent output, 2026-08-21. Companion docs: `ti_xxx_design_a_integration.md`, `ti_xxx_design_c_verification.md`, `ti_xxx_master_plan.md` (reconciled plan; per the plan, the engine's Cordis plugins `engine-guard`/`replay-tools` live in `dsh-lab/packages/`, not `$WS/engine/plugins/`).

`$WS` = `/Users/malachi/Developer/work/mntn/workspace`. `$DSH_HOME` = `~/.dsh` (dsh is not yet installed — Part A stands it up; every dsh-dependent step below declares that dependency).

**Stance.** The human gate is removed for *improvement application* and replaced by a machine gate that is strictly harder to pass than the human ever was: a replay/eval corpus with quantitative PASS, adversarial refutation, pre-registered metrics, and an auto-rollback observation window. "No human input" ≠ "no verification." Five floors remain gated permanently and are named as such in §6.0 — they are policy, not rungs on the ladder.

The engine reuses the kit's proven loop shape (`instrument → digest → curate → fold back`, per `$WS/claude-prompts/self_improvement_engine_plan.md`) but closes it: the *curate → fold back* half, today PROPOSE-ONLY via `/workflow-audit`, becomes BUILD → VERIFY → ADOPT → OBSERVE on dsh primitives.

## New surfaces (all under `$WS/engine/`, committed except where noted)

```
engine/
  ENGINE.md                 # operator doc; how to stop, inspect, audit the engine
  FLOORS.yml                # the five permanent gates + budgets — protected path (§6.0)
  engine.config.yml         # ladder state, thresholds, cadences, per-run budgets
  ENGINE_LOG.md             # append-only, one line per pipeline run (itself harvested)
  candidates/queue.jsonl    # HARVEST output
  candidates/<id>/          # spec.json, spec.md, build.json, verdict.json, adopted.json
  corpus/manifest.jsonl     # committed; case metadata + checks
  corpus/cases/<case-id>/   # GITIGNORED (raw session logs = sensitive); Mac-local
  metrics/entropy.jsonl     # one snapshot per run, append-only
  workflows/                # hypothesize.js, verify.js (dsh_workflow scripts)
  scripts/                  # harvest.py, transcript_miner.py, entropy_snapshot.py,
                            # adopt.sh, observe.py, rollback.sh, ladder.py
  profile/engine.patch.yml  # rows the engine has adopted into the work profile (HMR-watched)
```
(Engine Cordis plugins `engine-guard` — floors enforcement via tools/pre-execute deny — and `replay-tools` — serves recorded tool/result events in Tier-1 — live in `dsh-lab/packages/` per the reconciled plan.)

dsh profiles: `mntn` (daily driver, Part A) · `mntn-engine` (engine's own sessions: dsh_workflow + engine-guard, sandbox `workspace-write`, approval `never` — deterministic reject is the fail-closed machine gate for anything unanticipated) · `mntn-candidate` (= mntn bundles + `--patch $WS/engine/candidates/<id>/candidate.patch.yml`) · `mntn-replay` (= candidate + `llm-replay` on the `ctx.llm` seam + `replay-tools`; keyless).

---

## 1. The pipeline

```
HARVEST ──▶ HYPOTHESIZE ──▶ BUILD ──▶ VERIFY ──▶ ADOPT ──▶ OBSERVE ──┐
 (det.)      (LLM, Mac)     (LLM)     (gate)     (det.)    (det.)     │
    ▲                                    │ FAIL                 rollback / demote
    └──── failures & rollbacks become corpus cases & new candidates ──┘
```

### (a) HARVEST — deterministic candidate mining
**Surface:** `engine/scripts/harvest.py`, pure Python, keyless. Mac launchd daily 08:15 (`com.mntn.engine.harvest.plist`, missed-run catch-up on wake); the Pi's existing Monday cron (`.claude/scripts/pi_run_workflow_audit.sh`) keeps producing `claude-prompts/workflow_audits/signals_<date>.md`, which becomes one harvest input rather than the terminus.

**Inputs (real files):**
- `$WS/knowledge/bq_perf_log.jsonl` — slow/costly/repeated queries (via `perf_digest.py` functions)
- `$WS/knowledge/.request_log.jsonl` — recurring verb+noun shapes (via `request_digest.py`; Mac-only, gitignored)
- `$WS/.claude/state/chat_brevity_log.jsonl` — breach rate
- `$WS/knowledge/bq/_UNDOCUMENTED.queue` — doc debt
- `$WS/knowledge/eval_runs.log` — retrieval-suite staleness/failures
- `$WS/on-call/incident_log.jsonl` — repeat incidents not in the catalog
- `$WS/improvements_backlog.md` + latest `signals_*.md` — already-diagnosed friction
- dsh session logs under the configured `session_root` (`session-persistence-jsonl`) — tool-error signatures, repeated manual tool-call n-grams (≥3 recurrences of the same 3+-step sequence = automation candidate), `approval/asked` events, `compaction/*` events (context-overflow friction)
- Claude Code transcripts `~/.claude/projects/-Users-malachi-Developer-work-mntn-workspace/*.jsonl` — same n-gram mining + user-correction detection ("no, actually…" turns following an assistant turn)

**Output contract** — one line per candidate in `engine/candidates/queue.jsonl`:
```json
{"id":"c-2026-08-22-001","class":"doc_append|index_rebuild|routing_keyword|knowledge_edit|skill_edit|skill_new|tool_plugin|workflow_script|harness_config|engine_change",
 "signal":"recurring_friction|repeated_sequence|stale_doc|costly_query|retrieval_miss|repeat_incident",
 "evidence":[{"source":"knowledge/bq_perf_log.jsonl","count":7,"examples":["job_id…"]}],
 "status":"queued","created":"2026-08-22"}
```

### (b) HYPOTHESIZE — candidate spec with pre-registered metric
**Surface:** `engine/workflows/hypothesize.js` on `dsh_workflow` (`approvalMode: generated-and-local`, QuickJS sandbox, per-run token budget from `engine.config.yml`), invoked headless: `dsh --profile mntn-engine "run hypothesize"` by launchd after harvest; midday re-runs via `dsh-routines` (overlap/missed-run safety) while dsh is up.

**Output contract** — `engine/candidates/<id>/spec.json`: `{change_class, target_paths[], design (prose in spec.md), preregistered: {metric: "<name from §3>", direction, min_delta}, guards_acknowledged: true, risk_notes, est_build_cost}`. **A spec without a pre-registered metric is invalid** — verify.js refuses it. This is the anti-reward-hacking anchor: the metric is named before any code exists.

### (c) BUILD — isolated worktree + candidate profile
**Surface:** `dsh_workflow` script reusing the kit's ingestion pattern (`implementer` agent); `git worktree add $WS/../workspace-cand-<id> -b engine/<id>`. Knowledge/skill/doc changes: edits in the worktree only. Tool plugins: a bundle under `engine/candidates/<id>/plugin/` (`package.json` with `dsh.bundle.patch`), smoke-tested first in-memory via creator mode (`cordis_define`/`cordis_run`/`cordis_stop` — Cordis failure containment means a failed activation rolls back and cannot damage the builder's runtime), then as a file bundle in `mntn-candidate`. Exit criteria: `verify.sh full` green in the worktree, `ruff` green, plugin `vitest` green. Output: `build.json {branch, head_sha, files_changed[], tests_added}`.

### (d) VERIFY — the machine gate
**Surface:** `engine/workflows/verify.js` (dsh_workflow; its run dir `.dsh/workflow-runs/<run-id>/` with `events.jsonl` + cost accounting *is* the audit evidence). Four components, all mandatory:

1. **Tier-1 deterministic replay (keyless — can also run on the Pi).** For each corpus case with a dsh trajectory: boot `mntn-replay` (candidate composition + `llm-replay` serving recorded assistant turns + `replay-tools` serving recorded `tool/result` events for nondeterministic tools — `bq_run.sh`, network, dates) in a disposable copy of the workspace at the recorded git SHA. Asserts: (i) context assembly — `deriveMessages()` of the replayed log vs recorded (the "model-visible iff logged" invariant makes this exact); allowed diffs only where the candidate declares them; (ii) every recorded tool call still resolves to a registered tool with a compatible schema; (iii) deterministic tools re-execute to equal results; (iv) zero plugin activation failures. This is precisely how the dsh repo CI tests itself (keyless snapshot-replay of full transcripts).
2. **Tier-2 live eval (keyed, Mac).** Re-run corpus cases fresh under `mntn-candidate`; grade with each case's deterministic checks; record tokens/latency/$ per case from `ctx.tokenMeter` / workflow cost accounting. Includes **fork-continuations**: `ctx.sessions.fork(source, boundary)` at the recorded friction point, candidate continues live, checks grade the continuation — this tests the improvement exactly where the friction occurred.
3. **Adversarial refutation.** dsh_workflow's adversarial-verification pattern: two fresh reviewer agents, read-only toolset (capability-enforced, mirroring `$WS/.claude/agents/reviewer-adversarial.md`), prompted to (i) find a reproducible case where the change is worse, (ii) show the pre-registered metric is being gamed (e.g. brevity by truncation, cost by refusing work). Structured verdict JSON; any reproducible refutation = FAIL.
4. **Statics:** `verify.sh full`, `hooks_selftest.sh`, `lint_comms/lint_tickets/lint_memory/lint_coverage`, vitest.

**PASS (quantitative, all required):** Tier-1 = 100% (any invariant/schema break is a hard fail) · pre-registered metric moves ≥ `min_delta` · no guard metric regresses beyond tolerance (retrieval suite: no probe newly failing; tokens-to-answer ≤ +10%; $ per case ≤ +10%; wall latency ≤ +20%; all thresholds in `engine.config.yml`) · both reviewers return `no-refutation` · statics green. Output: `verdict.json {pass, baseline:{…}, candidate:{…}, replays, reviewers[], cost_usd}`.

### (e) ADOPT — deterministic, machine-gated merge
**Surface:** `engine/scripts/adopt.sh` — no LLM needed; runs only if `verdict.pass && class ≤ current_rung(class)` per `engine.config.yml`.
- **Knowledge / skills / docs / scripts:** `git merge --no-ff engine/<id>` to main with trailers `Engine-Candidate: <id>` and `Engine-Verdict-SHA: <sha256 of verdict.json>`; normal pre-commit gate runs (no `--no-verify`); push.
- **Plugins / harness config:** append the row to `$WS/engine/profile/engine.patch.yml`, which the `mntn` profile includes via an HMR-watched patch layer — Cordis config reconciliation hot-swaps the live instance with no restart; the row's stable `id` is the rollback handle.
- **Floor check:** any diff that net-deletes lines from `knowledge/**.md` (beyond `superseded:` markers), touches `FLOORS.yml`/credentials/prod paths, or posts off-machine is demoted to the human PROPOSE queue (`improvements_backlog.md`, AWAITING APPROVAL format) — enforced twice: by `engine-guard` (`tools/pre-execute` deny) at runtime and by a pre-commit protected-path check.

### (f) OBSERVE — post-adoption window + auto-rollback
**Surface:** `engine/scripts/observe.py`, deterministic, nightly launchd. Window = 7 days or 20 work sessions, whichever is later. Monitors the candidate's pre-registered metric + all guards from live signals (§3 sources) scoped to the change, plus: nightly Tier-1 replay of the full corpus on main, plugin failure signatures in dsh session logs (Cordis records activation failures on the fiber), hook failures in `hooks_selftest.sh`.
**Rollback triggers:** guard metric beyond tolerance for 3 consecutive sessions · any attributable hard failure · any corpus case newly red. **Mechanics** (`rollback.sh`): plugins → set `disabled: true` on the row (Cordis unload = exact withdrawal, Thm 61/62), then remove the row; git changes → `git revert <merge-commit>` (allowed: reverting an engine commit inside its window is change-withdrawal, not knowledge deletion; after the window closes, corrections are forward-only per the append/supersede convention). Every rollback: demotes that change class one rung for 14 days (circuit breaker), appends the failure as a new corpus case, and writes the postmortem into the provenance record.

---

## 2. The eval corpus

`engine/corpus/manifest.jsonl` (committed) + `engine/corpus/cases/<case-id>/` (gitignored — raw trajectories contain prompts/data; Mac-local, Time-Machine backed).

**Case contract** (`case.json`): `{id, source: "dsh"|"claude-code", task_prompt, workspace_sha, entry_constraints, checks: [{type: file_exists|output_contains|json_path_eq|probe_reached|cost_max_usd|tokens_max|tool_called}], baseline: {pass, tokens, usd, latency_s}, tags[], tier: [1,2], holdout: bool, added, last_green}`. Checks are deterministic assertions; an LLM judge is permitted only as a secondary check with a fixed rubric and its transcript logged into the case dir.

**What becomes a case:**
1. The 5 retrieval probes in `$WS/knowledge/eval_probes.md` (+ `claude-prompts/retrieval_eval.js`) — seed set, already labeled with `must_reach` targets.
2. **Every closed ticket's golden session.** The framing gate is the labeling mechanism: `/frame` forces a *binary* Objective into `summary.md` §0 — that Objective compiles directly into the case's checks. `/capture` gains one step at ticket close: nominate the session, emit the case skeleton.
3. **Every incident** in `on-call/incident_log.jsonl`: input = the raw alert log, expected = the verdict class + catalog row matched.
4. **Every VERIFY true-positive failure and every OBSERVE rollback** — misses become permanent regression tests (the exact policy `eval_probes.md` already states for retrieval).

**Both transcript sources feed it, differently:** dsh session logs feed Tier-1 (replayable — `SessionEventMap` carries every model-visible byte and every tool result) *and* Tier-2. Claude Code `~/.claude/projects/...jsonl` transcripts cannot drive dsh's `llm-replay` (different event vocabulary), so `transcript_miner.py` converts them to **Tier-2-only** cases (first user prompt + derived checks) and to harvest signals. Stated honestly: Claude-sourced cases are fresh-run evals, not replays.

**Growth/rotation policy:** ≤1 new case per closed ticket; active Tier-2 set capped at 40 cases with a full-run budget ceiling (`engine.config.yml: verify.tier2_max_usd`, default $3/run); Tier-1 set unbounded (keyless ≈ free). 20% of cases flagged `holdout: true` — never readable by HYPOTHESIZE/BUILD agents (excluded from the worktree; `engine-guard` denies reads of holdout dirs in builder sessions), checked only inside VERIFY. Cases untouched by any candidate diff for 6 months rotate to a monthly archive tier. After every adoption, Tier-2 re-runs on new main to refresh `baseline` (prevents delta drift).

---

## 3. Knowledge-entropy: the numbers the loop optimizes

Snapshot per run → `engine/metrics/entropy.jsonl` by `entropy_snapshot.py`. Every candidate pre-registers exactly ONE target metric; all others act as guards. No composite scalar exists — nothing to Goodhart.

| Metric | Direction | Source of truth |
|---|---|---|
| `retrieval_hit_rate` (probe pass fraction) | ↑ | `knowledge/eval_runs.log` + `retrieval_eval.js` runs |
| `retrieval_probe_count` (suite coverage) | ↑ | `knowledge/eval_probes.md` `## PROBES` JSON |
| `duplicate_h1_count`, `overlap_merge_candidates` | ↓ | `.claude/scripts/health_scorecard.py --memory` |
| `orphan_docs` (>120d untouched) | ↓ | `health_scorecard.py` |
| `doc_debt` | ↓ | `wc -l knowledge/bq/_UNDOCUMENTED.queue` |
| `stale_docs` (`schema_synced > last_verified`) | ↓ | front-matter scan (logic in `lint_coverage.py`/`build_index.sh`) |
| `tokens_to_answer` (mean over Tier-2 benchmark Qs) | ↓ | `ctx.tokenMeter` per case; dsh_workflow cost accounting in `.dsh/workflow-runs/` |
| `usd_per_query` (BQ) | ↓ | `knowledge/bq_perf_log.jsonl` `gb_billed` via `perf_digest.py` |
| `repeat_incident_rate` (novel repeats vs caught-by-catalog) | ↓ | `on-call/incident_log.jsonl` vs runbook §2 catalog |
| `brevity_breach_rate` | ↓ | `.claude/state/chat_brevity_log.jsonl` (`over: true` fraction) |
| `requests_served_by_existing_skill` | ↑ | `request_digest.py` clusters × `.claude/skills/` inventory |
| `hot_tier_budget` (MEMORY.md line count vs budget) | flat | `health_scorecard.py --memory` |

Paired-metric rule (anti-gaming): cost/token/brevity metrics only count on runs whose task checks PASS — "cheaper by failing" scores zero.

---

## 4. Self-documentation (provenance is automatic, not optional)

- **Every adoption writes** `$WS/knowledge/decisions/NNNN_engine_<slug>.md` from `decisions/` conventions: What changed / Why (harvest evidence, sources + counts) / Eval scores (baseline vs candidate table from `verdict.json`) / Reviewer verdicts / Rollback plan / Observation-window outcome (appended by `observe.py` at window close). `build_index.sh` regenerates `decisions/INDEX.md` in the same commit — so provenance is retrievable through the normal `_ROUTING.md` ladder.
- **Doc edits** append one dated line inside each touched doc's existing `<!-- CHANGELOG -->` region (survives regeneration by design).
- **Git trailers** `Engine-Candidate:` / `Engine-Verdict-SHA:` on every engine commit make `git log --grep Engine-Candidate` the complete adoption audit.
- **`engine/ENGINE_LOG.md`** appends one line per pipeline run (stage outcomes, cost, verdicts) — the engine's own trajectory, which HARVEST mines: the engine observes itself with the same instruments it points at everything else.

---

## 5. Bootstrap and the graduated autonomy ladder

**v0 (ships first, no dsh dependency):** HARVEST + HYPOTHESIZE emitting evidence-backed specs into the existing propose channel (`claude-prompts/workflow_audits/` + `improvements_backlog.md`), plus auto-apply of **rung 0 only** — classes whose verifier is already deterministic and whose failure is impossible-by-construction: `build_index.sh` rebuilds (byte-stable, idempotent), `_MEMORY_RECALL.tsv` keyword additions, appends to `<!-- OBSERVED:* -->` regions, corpus case additions, `eval_runs.log` entries.

**Ladder (state lives in `engine.config.yml`; promotion executed by `ladder.py` from ENGINE_LOG track record):**

| Rung | Change class | Unlock criteria (all required) |
|---|---|---|
| 0 | index rebuilds, doc/OBSERVED appends, routing keywords, corpus additions | `verify.sh` green; ships at v0 |
| 1 | knowledge doc edits (supersede lines, front-matter, new docs), memory files | ≥20 corpus cases; 4 weeks rung-0 with 0 rollbacks; retrieval suite ≥ baseline 4 consecutive weeks |
| 2 | skills (edit, then new), CLAUDE.md/prompt lines | ≥30 cases incl. ≥5 skill-exercising; ≥3 clean rung-1 windows; one-time human spot-audit of 10 adoptions finds 0 bad passes (a calibration event, not a recurring gate) |
| 3 | tool plugins + hooks | Tier-1 replay covers plugin-exercising sessions; plugin vitest harness in CI; 2 clean rung-2 months |
| 4 | harness config (profile patches, presets — floors excluded) | 1 clean quarter; config diffs exercised in Tier-1 |
| 5 | the engine itself (workflows, scripts, thresholds) | meta-eval: candidate engine re-judges a frozen corpus of past candidates with known-correct verdicts (its confusion matrix must be ≥ current engine); previous engine retained as fallback; thresholds may tighten freely, loosening requires the meta-eval; rate-limited 1/week |

Any rollback demotes its class one rung for 14 days. Demotion is automatic; promotion never skips rungs. (Note: the reconciled master plan overlays Part C's L0-L3 operational ladder on these rungs — rungs 2+ additionally gated on dsh leaving rc and a ≥90-day clean L2 record.)

---

## 6. Safety analysis

**6.0 The floors (permanent, never on the ladder — enforced by `engine-guard` `tools/pre-execute` denials + monotonic `ctx.tools.guard()` + protected-path pre-commit + `FLOORS.yml`):**
1. **Knowledge deletion** — append/supersede only, forever (rollback-window `git revert` of the engine's *own* commits is the sole exception).
2. **Prod mutations** — `/oncall`'s "never hot-patch prod" is absolute; engine sessions have no prod credentials.
3. **Spend above budget** — `FLOORS.yml: max_usd_per_day` (default $5 engine-wide, `ctx.tokenMeter` aggregation checked at every stage start; `BQ_GB_ABORT` stays for queries). Exceeding = fail-stop with a report, never fail-open.
4. **Secrets/keys** — no reads of `~/.dsh/.credentials.yaml`/Keychain paths in engine sessions; no key ever on Pi (MNTN policy, 2026-06-10 precedent).
5. **Nothing leaves the machine** — no Jira/Slack/email/GitHub-comment posts from engine sessions (`git push` to the private workspace repo is the one allowed egress). dsh's sandbox does not confine network (per the Hedemark audit), so this floor is enforced at the tool layer by `engine-guard`, not assumed from the sandbox.

**Failure modes → countermeasures:**
- **Eval overfitting** — holdout set invisible to builder agents; corpus grows from real work continuously; alarm metric: `green_on_corpus_but_rolled_back` rate in ENGINE_LOG (>0 twice in a month freezes the ladder).
- **Reward hacking (brevity/cost)** — pre-registration before build; paired-metric rule (§3); reviewers explicitly prompted to find gaming; signal logs are append-only and write-denied to candidate sessions (`engine-guard` blocks writes to `*_log.jsonl` / `.claude/state/` from builder profiles).
- **Compounding bad edits** — one in-flight candidate per class; a class's observation window must close before the next adoption touching the same files; weekly full-corpus regression on main; append/supersede means bad edits are visible diffs, never silent overwrites.
- **Self-modification runaway** — engine cannot raise its own rung (ladder state changes are rung-5 + meta-eval + rate-limited); `FLOORS.yml` and `engine-guard` source are protected paths whose modification the guard itself denies and pre-commit rejects; kill switch: `touch $WS/engine/STOP` checked at every stage start + `launchctl unload` one-liner documented in `ENGINE.md`.
- **Replay blind spots (nondeterminism)** — Tier-1 asserts structure, not text; nondeterministic tool results replayed from the log via `replay-tools`; time-sensitive cases pin dates in the prompt; flaky Tier-2 checks retried once then flagged; residual risk explicitly absorbed by OBSERVE — replay is the gate, observation is the backstop.
- **In-process plugin risk** (dsh plugins run with your permissions) — engine-authored plugins load first in creator-mode memory (contained failure), then under the candidate profile in the disposable worktree, never directly into `mntn`; adopted rows carry pinned ids for one-step `disabled: true` withdrawal.

**Placement & cadence (no LLM keys on servers):**
- **Mac (keys in login Keychain):** launchd LaunchAgents — 08:15 `harvest.py`; 08:30 `dsh --profile mntn-engine "hypothesize"`; 09:00 build+verify for the top candidate (wrapped in `caffeinate -i`); nightly 21:00 `observe.py`. While present: `dsh-routines` rows inside the running dsh instance for midday verify batches (overlap/missed-run/timeout safety built in).
- **Pi (keyless forever):** unchanged Monday `workflow_audit.sh` cron; **plus** nightly Tier-1 replay batch and `entropy_snapshot.py` over *committed* logs (both deterministic/keyless) — commits `signals_latest` additions exactly as today. Mac-local sources (request log, session logs, Claude transcripts) never leave the Mac.

---

## 7. Ordered build steps (one agent each, ≤ half-day, independently testable)

| # | Step | Test criterion | Needs dsh? |
|---|---|---|---|
| 1 | Scaffold `engine/` + `FLOORS.yml` + `engine.config.yml` + protected-path pre-commit check | commit touching `FLOORS.yml` without human trailer is rejected by `.githooks/pre-commit` | no |
| 2 | `harvest.py` v0 over the 7 existing signal files → `queue.jsonl` | ≥3 candidates on today's logs; evidence counts match manual `grep -c` | no |
| 3 | `transcript_miner.py` (Claude Code jsonl → signals + case skeletons) | 3 known transcripts convert; `case.json` validates against schema | no |
| 4 | Corpus schema + seed (5 probes + 3 recent ticket sessions with checks) | all seed checks pass against recorded outcomes (baseline green) | no |
| 5 | `entropy_snapshot.py` → `metrics/entropy.jsonl` | 2 metrics hand-verified; rerun is byte-stable | no |
| 6 | Tier-1 replay runner: `mntn-replay` profile + `replay-tools` plugin | unmodified session replays green; deliberately broken tool schema replays red | **yes** |
| 7 | `hypothesize.js` on dsh_workflow with pre-registration | valid spec from seeded queue; spec without metric refused | yes |
| 8 | Builder: worktree + branch + candidate profile + statics | a doc-append candidate builds; `verify.sh full` green in worktree | yes |
| 9 | `verify.js` core: Tier-1 + statics + cost deltas + PASS computation | known-good passes; seeded probe-breaking regression fails | yes |
| 10 | Adversarial reviewers in `verify.js` | a planted brevity-gaming candidate (truncation) is refuted | yes |
| 11 | `adopt.sh` (rung 0) + provenance writer + `decisions/INDEX.md` rebuild | adoption commit carries trailers; a knowledge-deleting candidate demotes to PROPOSE queue | no |
| 12 | `observe.py` + `rollback.sh` + circuit breaker | synthetic metric regression reverts a dummy adoption; plugin row flips `disabled: true` | partial |
| 13 | Scheduling: launchd plists + dsh-routines rows + `engine/STOP` kill switch | STOP file halts next run; `launchctl unload` verified | yes |
| 14 | `ladder.py` state machine + Pi Tier-1 nightly deployment | synthetic ENGINE_LOG history promotes/demotes correctly; Pi run stays keyless (`env` audit) | yes |

Steps 1–5 ship v0 (harvest+propose+rung-0) with zero dsh dependency; 6 is the first Part-A-dependent step.

## Critical files
- `$WS/claude-prompts/self_improvement_engine_plan.md` — the predecessor design this engine supersedes; its loop taxonomy and ledger contract carry forward
- `$WS/.claude/scripts/workflow_audit.sh` — the existing deterministic aggregator HARVEST extends
- `$WS/knowledge/eval_probes.md` — seed of the eval corpus (with `claude-prompts/retrieval_eval.js` as the runner pattern)
- `$WS/.claude/scripts/health_scorecard.py` — primary entropy-metric source `entropy_snapshot.py` wraps
- `$WS/.claude/settings.json` — hook wiring that all signal capture (and any new engine hooks) flows through
