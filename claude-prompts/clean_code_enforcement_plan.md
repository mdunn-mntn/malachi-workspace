# Clean / AI-Legible Code Enforcement — Adoption Plan

> Research-backed, adversarially verified (Ruff/mypy claims fact-checked against 2026 sources).
> Tailored to this repo's two-tier reality: durable code (`lib/`, `.claude/scripts/`) vs throwaway
> analysis (`tickets/**/artifacts/`, ~60k LOC). Status: SHIPPED. Phases 0-3 all live as of 2026-07-31 (ruff adopted + gate-wired, durable tier reformatted + fully type-annotated, mypy advisory). Config: `pyproject.toml`.

## 1. Verdict (BLUF)

**Adopt Ruff (pinned) as the single Python linter + formatter, retire flake8/isort/black, and wire it
into the existing `verify.sh --staged` gate — staged-scoped, Python-only, two-tier by path.** Your
flake8 7.0 is base-only with zero config; Ruff reimplements 100% of what you run today plus import-sort
and a formatter in one binary + one `pyproject.toml`, and slots into the gate without a second toolchain.
No reason a solo analyst repo keeps flake8 alongside it.

Two non-negotiables the adversarial pass proved out:
- **Pin the Ruff version AND set an explicit `select`.** Ruff 0.16 (Jul 2026) blew the default rule set
  from 59 → 413 and dropped `E401/E402/E702/E741` from defaults. Unpinned/default-select = your gate
  silently changes what it flags.
- **Keep mypy out of the commit gate entirely** (not even "soft"). On pandas/openpyxl it produces walls
  of unactionable errors and trains the `--no-verify` habit that erodes the one gate also enforcing
  front-matter + commit-msg caps.

Measured backlog: **707 flake8 hits on `lib/` + `.claude/scripts/`, 671 (94.9%) are E501 long-line.**
E501 stops being noise because **you stop selecting it** (formatter owns length), not because the
formatter rewraps them — it can't wrap the comments/docstrings/URLs/embedded-SQL strings that dominate.

## 2. What each tier gets

| Surface | Files | Enforcement | Why |
|---|---|---|---|
| **DURABLE — `lib/`** | 2 (`mntn_xlsx.py` 966 LOC + demo) | `ruff format` + full lint select + scoped `D`/`ANN` + manual `mypy` | Reusable public API; the one place typed signatures pay off for humans + Codex |
| **DURABLE — `.claude/scripts/`** | ~13 | `ruff format` + full lint select, **no `D`/`ANN`** | Long-lived tooling but internal; docstring/type gates here are marginal nag |
| **THROWAWAY — `tickets/**/artifacts/`** | ~202 .py, ~60k LOC | **Excluded** (not linted, not formatted) | Run-once analysis; strict linting = pure noise → `--no-verify` |
| **SQL — `*.sql`** | 431 | **Off** | One-off analytical SQL, high BigQuery-dialect friction, zero durable payoff |
| **Markdown KB** | large | **Off for generic linters** — already gated by front-matter linters + `build_index` | markdownlint would fight hand-authored prose |

The two-tier split is load-bearing: 78% of `.py` files are throwaway, so one global ruleset is either too
weak for `lib/` or noise on chart scripts.

## 3. Starter ruleset (durable code)

**On:** `E, W, F, I, B, UP, SIM, C4, E741`. **Line length 100** (formatter owns it; residual URL/SQL
literals get `# noqa: E501`). **`target-version = "py311"`** so `UP` modernizes `Optional[X]` → `X | None`.

**Deliberately off at first:** `E501` (formatter owns wrapping), `D`/`ANN` (Phase 3, `lib/` only),
`C901`/`N` (Phase 2), and forever: `ANN401`/`ANN002`/`ANN003`, `D2xx`/`D4xx` docstring mood/period,
`PLR` magic-numbers (pure nag, fights the house "sparse comments / simplest deliverable" style),
`ANN101/ANN102` (removed in Ruff 0.8). Not adopted: pylint, the pre-commit framework.

**Phase-1 `pyproject.toml`** — the whole two-tier policy:

```toml
[tool.ruff]
required-version = ">=0.16,<0.17"     # PIN: 0.16 changed defaults 59->413. Bump deliberately.
target-version = "py311"
line-length = 100
extend-exclude = ["tickets/**"]       # throwaway analysis: not linted, not formatted

[tool.ruff.lint]
select = ["E", "W", "F", "I", "B", "UP", "SIM", "C4", "E741"]   # explicit; never rely on defaults
ignore = ["E501"]                     # formatter owns length; noqa the residual literals

[tool.ruff.format]
# ruff format is the SINGLE formatter owner (black retired). Deterministic output.
```

**Phase-3 additions** (scopes `D`/`ANN` to `lib/` only):

```toml
[tool.ruff.lint]
select = ["E","W","F","I","B","UP","SIM","C4","E741","N","C901","D","ANN"]

[tool.ruff.lint.per-file-ignores]
".claude/scripts/*.py" = ["D", "ANN"]   # tooling: lint yes, no docstring/type contract
# lib/ keeps D + ANN; tickets/** already excluded above.

[tool.ruff.lint.pydocstyle]
convention = "google"                    # least vertical space; auto-disables conflicting D2xx/D4xx

[tool.ruff.lint.mccabe]
max-complexity = 12                      # C901 warn-band for the branchy index builders
```

Start `ANN` at `ANN201` (returns) then `ANN001` (args), ignore `ANN401`. `lib/mntn_xlsx.py` already
imports `from __future__ import annotations`, so this is low-friction.

## 4. Type checking

**Add mypy — manual/advisory only, NEVER in the commit gate.** Scope to `lib/`, non-strict, latest
version (do not pin the stale 1.9). `no_implicit_optional` is already the modern default, so the only
real starting lever is silencing stub gaps.

```toml
[tool.mypy]
files = ["lib"]
ignore_missing_imports = true          # pandas/numpy/openpyxl stub gaps
# Phase 3+: check_untyped_defs, disallow_incomplete_defs, warn_redundant_casts, warn_unused_ignores
```

Run `mypy lib/` on demand, or in `verify.sh` full mode (not `--staged`), advisory, after `ANN` coverage
rises. Skip pyright/ty/pylint — not worth the tooling for a solo repo.

## 5. How it composes with the existing gate

**Do NOT add the pre-commit framework.** Its value (install mgmt, version pinning, staged-scoping) is
already covered by `core.hooksPath=.githooks` + `verify.sh --staged` + commit-msg caps + `--fix` +
`--no-verify`. Version pinning moves to `pyproject.toml` (`required-version`).

**In `verify.sh --staged`** (block only on files THIS commit stages):
```bash
staged_py=$(git diff --cached --name-only --diff-filter=ACM \
  | grep -E '^(lib/|\.claude/scripts/).*\.py$' || true)
if [ -n "$staged_py" ]; then
  ruff format --check --force-exclude $staged_py || fail=1
  ruff check --force-exclude $staged_py || fail=1
fi
```

**In `verify.sh --fix`** (auto-repair + re-stage):
```bash
if [ -n "$staged_py" ]; then
  ruff format --force-exclude $staged_py
  ruff check --fix --force-exclude $staged_py
  git add $staged_py
fi
```

`--force-exclude` is required so `extend-exclude = tickets/**` applies even when the hook passes files by
path. **Never run repo-wide `ruff check` in the gate** — that resurrects the 707-hit backlog as a wall.

## 6. Legacy onboarding (turn it on today, zero hand-fixes)

Only 13 durable files, so clear them mechanically in one pass:
1. `ruff format lib/ .claude/scripts/` → commit **in isolation** as `ruff: one-time reformat, no logic
   change`. Safe/mechanical; makes every future agent Edit a minimal, stable-line-anchor diff (the core
   AI-legibility win). Will churn `mntn_xlsx.py` even though black-clean today — one-time cost paid once.
2. `ruff check --fix lib/ .claude/scripts/` → autofixes `I/UP/SIM/C4` + safe `B`; commit separately.
   Hand-resolve the handful of residual hits (13 files, trivial).
3. Turn on the gate (§5). From here it's fail-only-on-staged, so **zero of the 60k throwaway LOC and zero
   untouched durable code ever needs hand-fixing.**
4. Fallback if step 2 leaves residue: `ruff check --add-noqa lib/ .claude/scripts/` freezes existing
   violations behind `# noqa`. Prefer clearing over freezing given the tiny surface.

## 7. AI-legibility extras

**Already best-practice — leave alone:** front-matter indexes (`build_index.sh` → `_ROUTING.md`, memory
index), `verify.sh` as the single agent-runnable entrypoint, CLAUDE.md as the concise conventions file.
Matches Anthropic/AGENTS.md guidance.

**Two real gaps:**
1. **Config-as-docs.** No Python config exists today, so house style lives only as CLAUDE.md prose an
   agent must infer. A committed `pyproject.toml` IS machine-readable style docs Codex + future sessions
   read directly.
2. **Deterministic `ruff format`** = one-line semantic change → one-line diff (not a reflow), cutting
   agent edit token cost every time.

**Optional, defer or skip:** a thin (10–20 line) root `AGENTS.md` that *points to* CLAUDE.md + names the
two tiers + `verify.sh` (the Codex reviewer reads AGENTS.md natively; Claude uses CLAUDE.md). Only add if
it stays pure pointers — a second conventions file that copies CLAUDE.md will drift. Also: per-developer
editor **format-on-save** with Ruff.

## 8. Phased rollout (each phase independently shippable + reversible)

- **Phase 0 — consolidate tooling (1 commit, no gating).** `pip install "ruff>=0.16,<0.17"`. Add the
  Phase-1 `pyproject.toml` with `select = ["E","W","F","I"]` + `[tool.ruff.format]`. Retire flake8/isort/
  black from dev deps. **Human decision:** confirm retiring black (`ruff format` becomes sole owner).
- **Phase 1 — reformat + wire the gate (~95% of the value).** Run §6 onboarding (format + `--fix`,
  isolated commits). Widen `select` to the full starter set. Add the §5 blocks to `verify.sh`.
- **Phase 2 — readability ratchet (durable only).** Add `N` + `C901` (`max-complexity=12`, warn-band).
  Staged-scoped, so it only bites files you touch.
- **Phase 3 — type/docstring contracts (`lib/` only).** Add `D` (`convention=google`) + `ANN201`→`ANN001`
  scoped to `lib/`. Annotate `lib/mntn_xlsx.py` public API first. Stand up manual `mypy lib/`, advisory.

## 9. What NOT to do

- **Don't put mypy in the commit gate** — not even soft. Manual `mypy lib/` only.
- **Don't run two formatters** (black + `ruff format` = format war). Pick `ruff format`, retire black.
- **Don't run repo-wide `ruff check` in the gate** — resurrects the 707-hit backlog. Staged-scoped only.
- **Don't enforce `D`/`ANN` across the whole durable bucket** — `lib/` only, never `.claude/scripts` or
  the 60k throwaway LOC.
- **Don't enable `E501` alongside the formatter** — let the formatter own length; `# noqa: E501` residual.
- **Don't add sqlfluff or markdownlint** — SQL is one-off/high-dialect-friction; the markdown KB is
  already gated by the front-matter linters.
- **Don't add pre-commit, pylint, or an AGENTS.md that duplicates CLAUDE.md.**
- **Don't rely on Ruff's default `select` or leave the version unpinned.** Don't pin a stale mypy (1.9).

## Key sources
Ruff FAQ/config/formatter (docs.astral.sh/ruff), Ruff 0.8 + 0.16 release notes, mypy existing-code guide
(mypy.readthedocs.io), Anthropic best-practices (code.claude.com), AGENTS.md standard (agents.md),
"how coding agents read your code" (modem.dev).
