#!/usr/bin/env bash
# adopt.sh — deterministic ADOPT for the engine. v0 applies RUNG-0 classes only (zero-risk,
# byte-stable, failure-impossible-by-construction): index rebuilds, corpus additions, entropy
# snapshots, eval-run log lines. Everything else is left for the human PROPOSE queue (Phase 5
# builds the dsh-driven HYPOTHESIZE->VERIFY->OBSERVE path that unlocks higher rungs).
#
# Floors enforced here too: refuses if engine/STOP exists; never touches a protected path; never
# net-deletes knowledge; commits carry provenance trailers so `git log --grep Engine-Candidate` is
# the full adoption audit.
set -uo pipefail
WS="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$WS"
[[ -f engine/STOP ]] && { echo "engine/STOP present — adopt halted"; exit 3; }

CLASS="${1:?class}"; CID="${2:-manual}"
RUNG0="index_rebuild corpus_add entropy_snapshot eval_run_log doc_observed_append routing_keyword"
grep -qw "$CLASS" <<<"$RUNG0" || { echo "adopt: class '$CLASS' is above rung 0 — routed to PROPOSE (improvements_backlog.md), not auto-applied"; exit 0; }

case "$CLASS" in
  index_rebuild)
    bash .claude/scripts/build_index.sh >/dev/null 2>&1 || true
    bash .claude/scripts/build_kit_manifest.sh >/dev/null 2>&1 || true
    paths=(knowledge/INDEX.md knowledge/_ROUTING.md knowledge/_MEMORY_INDEX.md
           knowledge/_MEMORY_LIFECYCLE.md knowledge/_MEMORY_RECALL.tsv knowledge/bq/_CATALOG_INDEX.md
           knowledge/bq/_TOPICS.md knowledge/bq/_COVERAGE.md knowledge/decisions/INDEX.md
           knowledge/runbooks/INDEX.md tickets/INDEX.md documentation/ai_workflow_kit/COMPONENTS.md)
    ;;
  entropy_snapshot)
    python3 engine/scripts/entropy_snapshot.py >/dev/null
    paths=(engine/metrics/entropy.jsonl)
    ;;
  corpus_add)
    python3 engine/scripts/seed_corpus.py >/dev/null
    paths=(engine/corpus/manifest.jsonl)
    ;;
  *)
    echo "adopt: no v0 handler for '$CLASS' yet (rung-0 class, PROPOSE for now)"; exit 0 ;;
esac

# Stage only our own paths; abort if the diff would net-delete knowledge lines (floor).
git add "${paths[@]}" 2>/dev/null || true
if git diff --cached --quiet; then
  echo "adopt: $CLASS produced no change (byte-stable) — nothing to commit"
  exit 0
fi
del=$(git diff --cached --numstat -- 'knowledge/**.md' | awk '{d+=$2} END{print d+0}')
add=$(git diff --cached --numstat -- 'knowledge/**.md' | awk '{a+=$1} END{print a+0}')
if [[ "$del" -gt "$add" ]]; then
  echo "adopt: BLOCKED — net knowledge deletion ($del removed vs $add added). Floor: append/supersede only."
  git reset -q HEAD "${paths[@]}"
  exit 1
fi

verdict_sha=$(printf '%s' "$CLASS:$CID:$(date -u +%Y-%m-%d)" | shasum -a 256 | awk '{print $1}')
# Commit through the real gates (floor guard + commit-msg lint + index-freshness). NOT --no-verify:
# the engine's own commits must pass the floor guard, or the guard means nothing.
git commit -q -F - <<EOF
engine: adopt $CLASS ($CID)

Engine-Candidate: $CID
Engine-Class: $CLASS
Engine-Verdict-SHA: $verdict_sha
EOF
echo "adopt: committed $CLASS ($CID) — trailers Engine-Candidate/$CID Engine-Class/$CLASS"
