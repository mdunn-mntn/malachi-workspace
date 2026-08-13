#!/usr/bin/env bash
# verify.sh — the AI Workflow Kit "doctor". One entry point for every DETERMINISTIC check.
# Reused by the git commit gate (.githooks/) and the weekly audit (workflow_audit.sh §11).
#
# Modes:
#   (default) full   — whole-repo: the 3 front-matter linters + index-freshness + hook self-test.
#                       Advisory structure summary. Exit 1 on any HARD failure.
#   --staged          — pre-commit subset: same linters but FAIL ONLY on violations in files THIS
#                       commit stages (+ staged-aware index-freshness). Skips the hook self-test.
#   --fix             — auto-repair: lint_memory --fix, rebuild indexes, git-add the regenerated
#                       index files. (The gate's failure message points here.)
#
# Philosophy: enforce MECHANICAL correctness (front-matter schema, index sync, formatting). JUDGMENT
# checks (coverage depth, structure carve-outs) stay propose-only in workflow_audit.sh — not here.
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
S="$ROOT/.claude/scripts"
MODE="${1:-full}"
cd "$ROOT"

# Every file build_index.sh regenerates (the freshness surface).
GEN_INDEXES=(knowledge/INDEX.md knowledge/_ROUTING.md knowledge/_MEMORY_INDEX.md \
  knowledge/_MEMORY_LIFECYCLE.md knowledge/_MEMORY_RECALL.tsv knowledge/bq/_CATALOG_INDEX.md \
  knowledge/bq/_TOPICS.md knowledge/bq/_COVERAGE.md knowledge/decisions/INDEX.md \
  knowledge/runbooks/INDEX.md tickets/INDEX.md documentation/ai_workflow_kit/COMPONENTS.md)

if [ "$MODE" = "--fix" ]; then
  python3 "$S/lint_memory.py" --fix >/dev/null 2>&1 || true
  # ruff auto-repair on STAGED durable Python (format + safe fixes), then re-stage — mirrors the
  # index re-stage below. Throwaway tickets/** excluded by pyproject; --force-exclude enforces it.
  if command -v ruff >/dev/null 2>&1; then
    fix_py=$(git diff --cached --name-only --diff-filter=ACM | grep -E '^(lib/|\.claude/scripts/).*\.py$' || true)
    if [ -n "$fix_py" ]; then
      ruff format --force-exclude $fix_py >/dev/null 2>&1 || true
      ruff check --fix --force-exclude $fix_py >/dev/null 2>&1 || true
      git add $fix_py 2>/dev/null || true
    fi
  fi
  bash "$S/build_index.sh" >/dev/null 2>&1 || true
  bash "$S/build_kit_manifest.sh" >/dev/null 2>&1 || true
  git add "${GEN_INDEXES[@]}" 2>/dev/null || true
  echo "verify --fix: ran lint_memory --fix, ruff format+fix on staged durable py, rebuilt indexes, staged changes."
  exit 0
fi

FAIL=0
fail() { FAIL=1; echo "  ✗ $1"; }
pass() { echo "  ✓ $1"; }

STAGED=""
[ "$MODE" = "--staged" ] && STAGED=$(git diff --cached --name-only)

# run a linter; in --staged, fail only on violations whose (prefixed) path is staged.
# args: <label> <path_prefix> <cmd...>   (path_prefix normalizes a linter's VIOLATION path to repo-relative)
run_linter() {
  local label="$1" prefix="$2"; shift 2
  local out; out=$("$@" 2>&1)
  local viols; viols=$(grep '^VIOLATION ' <<<"$out" || true)
  if [ "$MODE" = "--staged" ] && [ -n "$viols" ]; then
    local keep=""
    while IFS= read -r line; do
      [ -z "$line" ] && continue
      local p; p=$(sed -E 's/^VIOLATION ([^:]+):.*/\1/' <<<"$line")
      if grep -qxF "${prefix}${p}" <<<"$STAGED"; then keep+="$line"$'\n'; fi
    done <<<"$viols"
    viols="$keep"
  fi
  if [ -n "${viols//[$'\n ']/}" ]; then
    fail "$label"; sed 's/^/      /' <<<"$viols"
  else
    pass "$label"
  fi
}

echo "verify ($MODE):"
run_linter "bq_table front-matter (lint_coverage)" "knowledge/" python3 "$S/lint_coverage.py" --check
run_linter "ticket/framing front-matter (lint_tickets)" "" python3 "$S/lint_tickets.py" --check
run_linter "memory front-matter (lint_memory)" "" python3 "$S/lint_memory.py" --check

# --- ruff: durable-tier Python (lib/ + .claude/scripts). Staged-scoped in --staged (block only on
#     files THIS commit stages), whole-tier in full mode. Throwaway tickets/** excluded by pyproject;
#     --force-exclude enforces it even for files passed by path. Degrades to a skip if ruff is absent
#     (keeps the gate portable). Line length is owned by `ruff format` (E501 ignored in pyproject). ---
if ! command -v ruff >/dev/null 2>&1; then
  echo "  · ruff not installed — skipping Python lint (pip install 'ruff>=0.16,<0.17')"
elif [ "$MODE" = "--staged" ]; then
  ruff_py=$(grep -E '^(lib/|\.claude/scripts/).*\.py$' <<<"$STAGED" || true)
  if [ -z "$ruff_py" ]; then
    pass "ruff (no durable Python staged)"
  else
    rc=0
    fmt_out=$(ruff format --check --force-exclude $ruff_py 2>&1) || rc=1
    chk_out=$(ruff check --force-exclude $ruff_py 2>&1) || rc=1
    if [ "$rc" -eq 0 ]; then pass "ruff (durable Python)"
    else
      fail "ruff (durable Python) — run: .claude/scripts/verify.sh --fix, then re-stage"
      printf '%s\n%s\n' "$fmt_out" "$chk_out" | grep -vE '^(All checks passed|[0-9]+ files? |$)' | sed 's/^/      /'
    fi
  fi
else
  ruff_py=$(git ls-files -- lib .claude/scripts | grep -E '\.py$' || true)
  rc=0
  fmt_out=$(ruff format --check --force-exclude $ruff_py 2>&1) || rc=1
  chk_out=$(ruff check --force-exclude $ruff_py 2>&1) || rc=1
  if [ "$rc" -eq 0 ]; then pass "ruff (durable Python, whole tier)"
  else
    fail "ruff (durable Python) — run: .claude/scripts/verify.sh --fix"
    printf '%s\n%s\n' "$fmt_out" "$chk_out" | grep -vE '^(All checks passed|[0-9]+ files? |$)' | sed 's/^/      /'
  fi
fi

# --- index freshness: regenerate, then any generated index that differs from what's staged/committed
#     is out of sync. In --staged, skip entirely unless a front-matter-bearing doc is staged.
#     (Assumes the `git add .` workflow: working tree ≈ index at commit time.) ---
if [ "$MODE" = "--staged" ] && ! grep -qE '^(knowledge/|on-call/.*\.md$|tickets/.*/summary\.md$|[^/]+\.md$|\.claude/(hooks|scripts|skills|agents)/)' <<<"$STAGED"; then
  pass "index freshness (no front-matter docs staged)"
else
  bash "$S/build_index.sh" >/dev/null 2>&1 || true
  bash "$S/build_kit_manifest.sh" >/dev/null 2>&1 || true
  if git diff --quiet -- "${GEN_INDEXES[@]}"; then
    pass "index freshness"
  else
    fail "index freshness — regenerated indexes differ from staged: $(git diff --name-only -- "${GEN_INDEXES[@]}" | tr '\n' ' ')(run: .claude/scripts/verify.sh --fix, then re-stage)"
  fi
fi

# --- full-only: hook self-test (hard) + advisory structure summary (never blocks) ---
if [ "$MODE" != "--staged" ]; then
  if bash "$S/hooks_selftest.sh" >/tmp/verify_hst.out 2>&1; then
    pass "hooks self-test ($(grep -oE '[0-9]+ passed' /tmp/verify_hst.out | head -1))"
  else
    fail "hooks self-test"; sed 's/^/      /' /tmp/verify_hst.out
  fi
  python3 "$S/audit_structure.py" --json /tmp/verify_struct.json >/dev/null 2>&1 || true
  hi=$(python3 -c "import json;d=json.load(open('/tmp/verify_struct.json'));f=d if isinstance(d,list) else d.get('findings',[]);print(sum(1 for x in f if x.get('severity')=='high'))" 2>/dev/null || echo "?")
  echo "  · structure: ${hi} high-severity finding(s) — advisory (propose-only; see workflow_audit §1)"
  # ~/.claude/CLAUDE.md is the only instruction file with no git history. Advisory, never a gate:
  # it is the user's file across all projects and may legitimately change mid-session.
  gdrift=$(bash "$S/sync_global_claude_md.sh" --check 2>&1) || echo "  · $gdrift"
fi

if [ "$FAIL" -ne 0 ]; then
  echo "verify: FAILED — fix the ✗ above, or run '.claude/scripts/verify.sh --fix'. Emergency bypass: git commit --no-verify."
  exit 1
fi
echo "verify: all hard checks passed."
exit 0
