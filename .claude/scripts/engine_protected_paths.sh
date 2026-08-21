#!/usr/bin/env bash
# engine_protected_paths.sh <commit-msg-file> — commit-msg guard for the engine's floors.
# A commit that stages FLOORS.yml or another protected verifier path must carry the trailer
#   Engine-Floor-Change: approved-by-human
# in its message; otherwise it is blocked. This keeps the self-improvement engine from silently
# editing its own guardrails (Ken Thompson rule). Staged-scoped, defensive: no protected path => exit 0.
set -uo pipefail
ROOT="$(git rev-parse --show-toplevel 2>/dev/null)" || exit 0
MSG_FILE="${1:-}"

PROTECTED_RE='^(engine/FLOORS\.yml|engine/scripts/|engine/workflows/|\.githooks/|\.claude/scripts/engine_protected_paths\.sh)'
staged=$(git diff --cached --name-only 2>/dev/null | grep -E "$PROTECTED_RE" || true)
[[ -z "$staged" ]] && exit 0

trailer='Engine-Floor-Change: approved-by-human'
if [[ -n "$MSG_FILE" && -f "$MSG_FILE" ]] && grep -qF "$trailer" "$MSG_FILE"; then
  exit 0
fi

echo "[engine-floor] BLOCKED: this commit stages a protected engine/verifier path:" >&2
echo "$staged" | sed 's/^/    /' >&2
echo "  These are the engine's own guardrails. A human approves the change by adding this trailer" >&2
echo "  to the commit message, then re-committing:" >&2
echo "    $trailer" >&2
exit 1
