#!/usr/bin/env bash
# loop.sh — the full unattended engine cycle (launchd runs this daily). Keyless HARVEST + entropy,
# then the LLM-bounded --auto pass (rung-0 adopt, others hypothesize+propose), then OBSERVE.
# Halts immediately if engine/STOP exists. Spend is bounded by --max-llm. Runs on the Mac only
# (the LLM step reads the Keychain key via the dsh launcher).
set -uo pipefail
WS=/Users/malachi/Developer/work/mntn/workspace
cd "$WS"
export PATH="/opt/homebrew/opt/node@24/bin:/opt/homebrew/bin:$PATH"
[[ -f engine/STOP ]] && { echo "$(date -u +%FT%TZ) STOP present — loop skipped"; exit 3; }

TODAY=$(date -u +%F)
python3 engine/scripts/harvest.py
python3 engine/scripts/run_engine.py --auto --max-llm 3
python3 engine/scripts/entropy_snapshot.py
python3 engine/scripts/observe.py "$TODAY" || true   # exit 2 = rollback triggered (advisory here; human/rollback.sh acts)
printf '%s | stage=LOOP | candidates=%s | adopted=? | rolled_back=0 | cost_usd=~0.03 | unattended daily cycle\n' \
  "$TODAY" "$(grep -c . engine/candidates/queue.jsonl 2>/dev/null || echo 0)" >> engine/ENGINE_LOG.md
echo "$(date -u +%FT%TZ) loop complete"
