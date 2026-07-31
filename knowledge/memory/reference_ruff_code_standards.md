---
name: reference_ruff_code_standards
description: "Ruff is the single Python lint+format tool (replaces flake8/isort/black), pinned 0.16.x in pyproject.toml, two-tier: durable code (lib/, .claude/scripts) linted+formatted, tickets/** excluded. mypy stays OUT of the gate. Config = pyproject.toml."
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [ruff, ruff check, ruff format, pyproject.toml, flake8, black, isort, mypy, clean code, self-documenting code, two-tier lint, durable code, throwaway artifacts, ANN type hints, pydocstyle, E501, required-version, per-file-ignores, force-exclude, format-on-save, vscode settings, charliermarsh.ruff, language server, clean_code_enforcement_plan]
domain: [workflow, repos, infra]
lifecycle: active
last_verified: 2026-07-31
---

Python code-quality standard for this repo (adopted 2026-07-31, Phase 0+1 shipped). Full plan: `claude-prompts/clean_code_enforcement_plan.md`.

**Tool:** Ruff is the ONE Python linter + formatter — replaces flake8 + isort + black (all retired). Config is `pyproject.toml` `[tool.ruff]`. Enforced by the commit gate ([[reference_commit_gate]]).

**Two tiers (load-bearing — 78% of .py is throwaway):**
- **Durable** = `lib/*.py` + `.claude/scripts/*.py` (~15 files): linted + formatted. Select = `E,W,F,I,B,UP,SIM,C4,E741,N,C901` (+ `D100-D104` presence + `ANN` scoped to `lib/mntn_xlsx.py` ONLY via per-file-ignores). `line-length=100`, `E501` ignored (formatter owns wrapping; residual literals get `# noqa: E501`); `C901 max-complexity=25` = regression backstop, not a refactor mandate. Currently 0 ruff errors, no noqa.
- **Throwaway** = `tickets/**/artifacts/*.py` (~60k LOC of run-once analysis): `extend-exclude=["tickets/**"]` — not linted, not formatted. `slack_bot/**` excluded too (decommissioned).

**Two hard gotchas (proven):**
- **PIN the version + set explicit `select`.** Ruff 0.16 (Jul 2026) changed the DEFAULT rule set 59→413 and dropped E401/E402/E702/E741 from defaults. `required-version=">=0.16,<0.17"` + explicit `select` so the gate never silently drifts. Installed: ruff 0.16.1 at `/opt/homebrew/bin/ruff`.
- **mypy stays OUT of the commit gate — never, not even "soft."** On pandas/openpyxl it spews unactionable errors and trains the `--no-verify` habit. When added (Phase 3), it's `mypy lib/` run manually/advisory only.

**How the gate uses it:** `verify.sh --staged` runs `ruff format --check` + `ruff check` on staged durable `.py` (`--force-exclude` makes the tickets exclude apply even for by-path files); `verify.sh --fix` runs `ruff format` + `ruff check --fix` and re-stages. Blocked commit → `verify.sh --fix`, re-stage. Skips cleanly if ruff absent (portable).

**Status: Phases 0-3 all shipped 2026-07-31.** Phase 2 (`N` + `C901@25`, IMP-019 done) and Phase 3 (`D`-presence + `ANN` on `lib/mntn_xlsx.py` + advisory `mypy lib/mntn_xlsx.py`, IMP-020 done) are live. `lib/mntn_xlsx.py` is fully type-annotated (was 3/121 typed) and mypy-clean. **mypy is advisory only (`mypy lib/mntn_xlsx.py`), NEVER in the gate.** Next widening: lower `C901 max-complexity` toward ~12 as `audit_structure.audit`/`mntn_xlsx.table`/`health_scorecard.main` get refactored; extend `D`/`ANN` + `mypy` to new `lib/*.py` as they land (per-file-ignores already gate them in).

**Why type hints are the real gap** (not style): a typed signature is the biggest self-documentation win — a contract legible to humans, mypy, and the Codex reviewer at once, and it lets an AI agent know a signature without reading the body. Consistent with the house rule [[feedback_sparse_code_comments]] (ruff governs docstrings/format, not inline comments).

**Format-on-save (editor, local):** `.vscode/settings.json` (gitignored, per-machine) sets `[python]` defaultFormatter=`charliermarsh.ruff` + formatOnSave + `codeActionsOnSave` (`fixAll.ruff`, `organizeImports.ruff`). Requires the **charliermarsh.ruff** VSCode extension installed. So durable `.py` is formatted+import-fixed before it reaches the gate (gate becomes a backstop). mypy 1.9 (installed) runs clean on the fully-typed `lib/mntn_xlsx.py`.

**GOTCHA — CLI vs editor exclude (verified 2026-07-31):** `ruff format`/`ruff check` on an EXPLICITLY-passed path IGNORES `exclude`/`extend-exclude` (e.g. `ruff format tickets/foo.py` WILL reformat it) — that is exactly why the gate passes `--force-exclude`. But the **VSCode language server DOES honor `exclude` for the open file**, so format-on-save on a `tickets/**` analysis script does nothing. If format-on-save ever churns a ticket file, that is the one signal the exclude isn't being honored.
