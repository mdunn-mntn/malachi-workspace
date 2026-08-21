# dsh harness spike — what we built, before vs now (2026-08-21)

## What we accomplished (one day)
Took a 7-day-old open-source agent harness (DeepSeek Harness, `dsh`), deep-researched it, and turned it
into a **working, adversarially-tested sidecar** for the analytics workflow kit — plus the substrate for a
**self-improvement engine** that can propose and safely apply its own improvements. Built as Phases 1-6, every
unit passing a REJECT-by-default gate and two fresh-context adversarial reviews before it counted as done.

Concretely shipped:
- **`dsh-lab/`** (new local-only sibling repo, pinned `dsh@0.1.1-rc.1`): two first-party plugins — `@mntn/dsh-bq`
  (governed BigQuery tool + a guard that blocks raw `bq query`) and `@mntn/dsh-kit` (memory-recall injection,
  orientation, deterministic slash-commands) — plus 5 composed profiles, a per-unit test gate, a hermetic
  behavioral harness, a **keyless replay-eval** harness, an integration+chaos suite, a kill switch, and an
  installed **network egress cage** (dedicated `dshagent` user + pf default-deny + allowlisting proxy). 55 unit
  tests green; 9/9 runnable integration/chaos scenarios pass; egress_selftest 6/6.
- **`engine/`** (in the workspace): the self-improvement loop — HARVEST → HYPOTHESIZE → VERIFY → ADOPT →
  OBSERVE → rollback — with five permanent **floors** enforced in code by a commit guard, an autonomy **ladder**,
  and full provenance. Proven end-to-end on real signals.
- The whole program is **severable**: delete `dsh-lab/` + `engine/` and the existing kit, knowledge base, and
  Pi loops are untouched. Nothing existing was modified destructively.

## How it worked BEFORE
| Concern | Before |
|---|---|
| **Harness** | One harness (Claude Code), interactive only. No headless/automation surface of our own. |
| **Regression-testing the workflow itself** | None. No way to prove a change to a prompt, hook, skill, or knowledge doc didn't quietly break real work — you found out in the next session. |
| **Self-improvement loop** | **Open.** Signals accrued (perf log, request log, brevity meter, doc-debt queue) and `/workflow-audit` *proposed* fixes, but a **human executed every single improvement**. The loop never closed. |
| **BigQuery governance** | A bash PreToolUse hook (`enforce_bq_wrapper.sh`) blocked raw `bq` — Claude-Code-only, untyped, untested against a shared corpus. |
| **Improvement safety** | Enforced by human presence + convention (append-not-overwrite, no-delete). No machine gate. |
| **Portability of the kit** | Skills/scripts were host-specific in practice; no proof they'd run anywhere else. |

## How it works NOW
| Concern | Now |
|---|---|
| **Harness** | Claude Code stays the daily driver; **dsh is a sidecar** — Web UI for inspection, headless one-shots, and the engine runtime. Same skills, same knowledge base, same governed BigQuery. |
| **Regression-testing** | **Record a real session, replay it against a changed composition, diff the result.** Keyless, deterministic. A corrupted change drifts; a safe one is byte-identical. This is the machine gate the loop needed. |
| **Self-improvement loop** | **Closed behind a machine gate.** HARVEST mines the same signals into evidenced candidates; HYPOTHESIZE (LLM) writes a fix with a *pre-registered metric* (a spec without one is refused); VERIFY runs replay + adversarial reviewers + statics and computes a quantitative PASS; ADOPT auto-applies **only** what passes AND only at an earned autonomy rung; OBSERVE watches the metric and auto-rolls-back a regression. |
| **BigQuery governance** | The same guard is now a **typed, tested plugin** (`tools/pre-execute` deny) with 35 unit tests transliterated from the bash corpus — and it caught a real hole the bash version had (multiline bypass). The `bq_query` tool binds provenance to the exact SQL. |
| **Improvement safety** | **Enforced in code.** Five floors (no knowledge deletion, no prod, no spend over $5/day, no secrets, no external egress) blocked by a commit guard; the verifier can't modify itself; an installed OS-level egress cage; a one-command kill switch. |
| **Portability** | Skills mount into dsh **verbatim** (same AgentSkills standard); the wrapper scripts stay the single implementation both hosts call. Proven, not asserted. |

## Why it's an improvement (the actual value)
1. **The loop closes without losing rigor.** The human gate that stood between "the system noticed a problem"
   and "the system fixed it" is replaced by a gate that is *harder* to pass than a human: recorded-trajectory
   replay + adversarial refutation + a pre-registered metric + an auto-rollback watch. "No human input" stops
   meaning "no verification."
2. **You can now prove a workflow change is safe before it ships.** Replay-eval didn't exist before. A prompt or
   knowledge edit can be regression-tested against real past work.
3. **Governance is structural, not disciplinary.** Before, "don't delete knowledge / don't touch prod" were
   conventions a tired human upheld. Now they're commit-guard denials and an OS egress cage — the system
   *can't* cross them, and the verifier can't quietly weaken its own rules.
4. **The BigQuery guard got stronger by being ported** — the adversarial review of the port found a multiline
   bypass and a dead cost-cap that the original bash hook shipped with; both fixed, in both hosts.
5. **It's a safe bet.** Preview software, isolated in a pinned sibling repo, severable in one `rm`, with a kill
   switch and a graduated autonomy ladder that earns each rung on a logged track record. Worst case costs the
   thin adapters — the working kit is untouched.

## What "ready" means
Everything buildable is done, gated, and committed. The only remaining item is **time**: use dsh as your
supervised sidecar for ~10 days (the soak), and if nothing Sev-1 happens, flip on L1 unattended autonomy.
That's not a build step — it's just using it. Quickstart: `dsh-lab/QUICKSTART.md`.
