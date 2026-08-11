#!/usr/bin/env bash
# stall_monitor.sh — the ONE correct stall-detector for background/async work on this Mac.
#
# Hand-writing this check has produced false STALL alarms four times (PS-8572, AUDI-1191 x2,
# AUDI-431) because `find -newermt '-15 minutes'` is a GNU idiom that BSD find rejects and the
# `bfs` alias errors on outright — the poll then returns empty and EVERY tick reads as idle.
# Use this script instead of writing the check inline. See memory feedback_background_work_liveness.
#
# Usage (inside a Monitor command):
#   bash .claude/scripts/stall_monitor.sh <watch_dir> [idle_minutes] [poll_seconds]
#
# Emits ONE line and exits when either:
#   - the watch dir disappears           -> "DONE: <dir> gone"
#   - no file has been modified in N min -> "STALL: <dir> idle >Nmin (newest <age>s ago)"
# Silence means healthy. Exit 0 = done, 1 = stalled.
set -uo pipefail

DIR="${1:?usage: stall_monitor.sh <watch_dir> [idle_minutes] [poll_seconds]}"
IDLE_MIN="${2:-15}"
POLL_S="${3:-300}"
IDLE_S=$((IDLE_MIN * 60))

newest_mtime() {
  # stat -f %m epoch arithmetic only — never find date strings on macOS.
  find "$DIR" -type f -exec stat -f '%m' {} + 2>/dev/null | sort -rn | head -1
}

while true; do
  sleep "$POLL_S"
  [[ -d "$DIR" ]] || { echo "DONE: $DIR gone"; exit 0; }
  newest="$(newest_mtime)"
  if [[ -z "$newest" ]]; then
    echo "STALL: $DIR has no files after ${IDLE_MIN}m"; exit 1
  fi
  age=$(( $(date +%s) - newest ))
  if (( age > IDLE_S )); then
    echo "STALL: $DIR idle >${IDLE_MIN}m (newest ${age}s ago)"; exit 1
  fi
done
