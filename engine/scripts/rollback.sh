#!/usr/bin/env bash
# rollback.sh <engine-commit-sha> [reason] — withdraw an adopted engine change inside its
# observation window. Reverting the engine's OWN commit is the sole allowed exception to the
# no-knowledge-deletion floor (it is change-withdrawal, not deletion). After the window closes,
# corrections are forward-only (append/supersede).
set -uo pipefail
WS="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$WS"
SHA="${1:?engine commit sha to revert}"
REASON="${2:-observed regression}"

# Only revert commits the engine itself authored (must carry an Engine-Candidate trailer).
if ! git show -s --format=%B "$SHA" 2>/dev/null | grep -q '^Engine-Candidate:'; then
  echo "rollback: $SHA is not an engine commit (no Engine-Candidate trailer) — refusing"
  exit 1
fi

cand=$(git show -s --format=%B "$SHA" | sed -n 's/^Engine-Candidate: //p' | head -1)
git revert --no-edit "$SHA" 2>/dev/null || { echo "rollback: revert failed (conflict?) — manual"; exit 1; }
# Append the rollback fact to ENGINE_LOG (append-only).
stamp=$(date -u +%Y-%m-%d)
printf '%s | stage=ROLLBACK | candidates=0 | adopted=0 | rolled_back=1 | cost_usd=0 | reverted %s (%s): %s\n' \
  "$stamp" "$SHA" "$cand" "$REASON" >> engine/ENGINE_LOG.md
git add engine/ENGINE_LOG.md
git commit -q -F - <<EOF
engine: rollback $cand

Engine-Reverts: $SHA
Engine-Candidate: $cand
EOF
echo "rollback: reverted $SHA (candidate $cand); class demoted for the cooldown window"
