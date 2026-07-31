---
name: reference_ruff_code_standards
description: "Ruff is the single Python lint+format tool (replaces flake8/isort/black), pinned 0.16.x in pyproject.toml, two-tier: durable code (lib/, .claude/scripts) linted+formatted, tickets/** excluded. mypy stays OUT of the gate. Config = pyproject.toml."
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [ruff, ruff check, ruff format, pyproject.toml, flake8, black, isort, mypy, clean code, self-documenting code, two-tier lint, durable code, throwaway artifacts, ANN type hints, pydocstyle, E501, required-version, per-file-ignores, force-exclude, clean_code_enforcement_plan]
domain: [workflow, repos, infra]
lifecycle: active
last_verified: 2026-07-31
---

Python code-quality standard for this repo (adopted 2026-07-31, Phase 0+1 shipped). Full plan: `claude-prompts/clean_code_enforcement_plan.md`.

**Tool:** Ruff is the ONE Python linter + formatter — replaces flake8 + isort + black (all retired). Config is `pyproject.toml` `[tool.ruff]`. Enforced by the commit gate ([[reference_commit_gate]]).

**Two tiers (load-bearing — 78% of .py is throwaway):**
- **Durable** = `lib/*.py` + `.claude/scripts/*.py` (~15 files): linted + formatted. Select = `E,W,F,I,B,UP,SIM,C4,E741`, `line-length=100`, `E501` ignored (the formatter owns wrapping — unwrappable URL/SQL literals get `# noqa: E501`). Currently 0 ruff errors, no noqa.
- **Throwaway** = `tickets/**/artifacts/*.py` (~60k LOC of run-once analysis): `extend-exclude=["tickets/**"]` — not linted, not formatted. `slack_bot/**` excluded too (decommissioned).

**Two hard gotchas (proven):**
- **PIN the version + set explicit `select`.** Ruff 0.16 (Jul 2026) changed the DEFAULT rule set 59→413 and dropped E401/E402/E702/E741 from defaults. `required-version=">=0.16,<0.17"` + explicit `select` so the gate never silently drifts. Installed: ruff 0.16.1 at `/opt/homebrew/bin/ruff`.
- **mypy stays OUT of the commit gate — never, not even "soft."** On pandas/openpyxl it spews unactionable errors and trains the `--no-verify` habit. When added (Phase 3), it's `mypy lib/` run manually/advisory only.

**How the gate uses it:** `verify.sh --staged` runs `ruff format --check` + `ruff check` on staged durable `.py` (`--force-exclude` makes the tickets exclude apply even for by-path files); `verify.sh --fix` runs `ruff format` + `ruff check --fix` and re-stages. Blocked commit → `verify.sh --fix`, re-stage. Skips cleanly if ruff absent (portable).

**Roadmap (not yet done):** Phase 2 = add `N` + `C901` (IMP-019). Phase 3 = `D` (google) + `ANN201`→`ANN001` scoped to `lib/` ONLY + manual `mypy lib/` (IMP-020) — closes the 3/121-typed gap on the reused library API. `lib/mntn_xlsx.py` already has `from __future__ import annotations`.

**Why type hints are the real gap** (not style): a typed signature is the biggest self-documentation win — a contract legible to humans, mypy, and the Codex reviewer at once, and it lets an AI agent know a signature without reading the body. Consistent with the house rule [[feedback_sparse_code_comments]] (ruff governs docstrings/format, not inline comments).
