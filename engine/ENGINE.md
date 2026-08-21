# The Self-Improvement Engine

The workflow reviews and improves itself. HARVEST (mine friction signals) → HYPOTHESIZE (a candidate change with a pre-registered metric) → BUILD (isolated worktree) → VERIFY (replay corpus + adversarial refutation + statics; quantitative PASS) → ADOPT (machine-gated merge) → OBSERVE (post-adoption window + auto-rollback). Design: `tickets/ti_xxx_dsh_harness_spike/artifacts/ti_xxx_design_b_engine.md`.

**v0 (this build, keyless):** HARVEST + a candidate queue + the eval corpus + entropy metrics. Auto-apply is rung 0 ONLY (index rebuilds, OBSERVED-region appends, routing keywords, corpus additions) — everything else is a PROPOSE-ONLY row in `improvements_backlog.md`. The dsh-driven half (HYPOTHESIZE→OBSERVE) is Phase 5.

## Stop / inspect / audit
- **Stop everything:** `touch engine/STOP` (halts every stage at entry) or `dsh-lab/scripts/killswitch.sh`.
- **What it did:** `engine/ENGINE_LOG.md` (one line per run) + `git log --grep Engine-Candidate`.
- **Candidates:** `engine/candidates/queue.jsonl`.
- **Metrics over time:** `engine/metrics/entropy.jsonl`.

## Floors (never crossed — see FLOORS.yml)
No knowledge deletion, no prod mutation, no spend over $5/day, no secret access, no external egress, the verifier never modifies itself.

## Files
- `FLOORS.yml` — permanent gates (protected path). `engine.config.yml` — thresholds + ladder state.
- `scripts/harvest.py` — deterministic candidate miner (keyless, launchd daily). `scripts/transcript_miner.py` — Claude Code jsonl → cases + signals. `scripts/entropy_snapshot.py` — metrics snapshot.
- `corpus/manifest.jsonl` (committed) + `corpus/cases/<id>/` (gitignored, Mac-local).
