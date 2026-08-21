---
name: reference_dsh_lab_runtime
description: How to run and operate the dsh-lab sidecar + engine — toolchain, profiles, gates, commands, kill switch (built 2026-08-21).
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [dsh-lab, dsh-mntn, mntn-analyst, mntn-automation, test-headless, dshkit_verify, dsh_behave, dsh_replay, engine run_engine, killswitch, node@24, pnpm, engine STOP, ORIENT_NO_PULL]
domain: [infra, repos, workflow]
lifecycle: active
last_verified: 2026-08-21
---

Operating the dsh sidecar (built for `project_dsh_harness_spike`, local-only, pinned `@deepseek-ai/dsh@0.1.1-rc.1`).

**Toolchain gotcha:** needs node@24 (system node 22.16 fails the `^22.19` engine floor); `dsh-lab/bin/dsh-mntn` prepends `/opt/homebrew/opt/node@24/bin` and exports the Anthropic key from the login Keychain (`security find-generic-password -a $USER -s anthropic_api_key -w`). pnpm build allowlist held to 3 native deps (subprocess-local, koffi, node-pty).

**Enter the harness:** `dsh-lab/bin/dsh-mntn --profile mntn-analyst` (Web UI :3080) · `... --profile mntn-automation "task"` (headless) · `--dump-config` inspects composition.
**Profiles:** mntn-analyst (interactive, skills+bq+kit), mntn-automation (headless, approval never, bq cost-cap 5GB), test-headless (hermetic CI), + a replay overlay. Source of truth = `dsh-lab/profiles/`; installed to `~/.dsh/profiles/` by `scripts/install_profiles.sh` (the only writer); home `cordis.patch.yml` kept empty.

**Gates/tests:** `dsh-lab/scripts/dshkit_verify.sh` (REJECT-by-default per-unit gate) · `pnpm test` (55 unit) · `scripts/dsh_behave.sh <case>` (hermetic behavioral) · `scripts/dsh_replay.sh record|replay <name>` (keyless regression) · `tests/integration/dsh_integration.sh` (scenarios+chaos).

**Engine (in workspace `engine/`):** `python3 engine/scripts/harvest.py` (keyless, daily via launchd) · `run_engine.py --dry` (classify) · `--candidate <id> --llm` (hypothesize) · `ladder.py` (rung state) · `observe.py <date>` · `adopt.sh <class> <id>` (rung-0 auto). Floors in `engine/FLOORS.yml` enforced by `.claude/scripts/engine_protected_paths.sh` (commit-msg guard): editing FLOORS/engine-scripts/.githooks needs an `Engine-Floor-Change: approved-by-human` commit trailer.

**Kill switch:** `dsh-lab/scripts/killswitch.sh` sets `dsh-lab/DISABLED` + `engine/STOP` + unloads launchd; every entrypoint refuses (exit 3). Re-enable manual. dsh sessions orient read-only (`ORIENT_NO_PULL=1`) — never pull the shared worktree from a session-start hook. **BQ billing project:** the query must bill to `dw-main-silver` (the us-central1 reservation), NOT the personal default `mntn-coredw-prod` (no jobs.create). `gcloud auth login` can reset core/project — reset with `gcloud config set project dw-main-silver`. Python: engine scripts kept 3.9-compatible (launchd/CLT python is 3.9; `datetime.UTC` is 3.11-only, use `timezone.utc`). [[project_dsh_harness_spike]] [[reference_dsh_harness]]
