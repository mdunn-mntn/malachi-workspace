#!/usr/bin/env bash
# PreToolUse:Bash — block a raw `bq ... query` so every real query goes through .claude/scripts/bq_run.sh
# (dry-run gate + cost logging + net-new-table detection). Reads the tool-call JSON on stdin.
# exit 2 + stderr = block the call and feed the message back to the model; exit 0 = allow.
# Defensive: any parse failure or non-match exits 0 (never wedges the session).
set -uo pipefail
CMD="$(jq -r '.tool_input.command // ""' 2>/dev/null)" || exit 0
[[ -z "$CMD" ]] && exit 0

# Fire only when `bq query` is invoked at COMMAND position (line start, or after a real command
# separator | & ;) — NOT when "bq"/"query" merely appear inside the SQL of a bq_run.sh call, a
# comment, or an echo. A bq_run.sh invocation never has `bq query` at command position, so it passes.
if printf '%s' "$CMD" | grep -qE '(^|[|&;])[[:space:]]*bq[[:space:]]+([^|;&]*[[:space:]])?query([[:space:]]|$)'; then
  # Allow the cheap/legit direct forms: an explicit --dry_run, or an INFORMATION_SCHEMA metadata read.
  if printf '%s' "$CMD" | grep -qE '(^|[[:space:]])--dry_run([[:space:]]|=|$)' \
     || printf '%s' "$CMD" | grep -qE 'INFORMATION_SCHEMA\.'; then
    exit 0
  fi
  echo "BLOCKED: run BigQuery through .claude/scripts/bq_run.sh — not raw 'bq query'." >&2
  echo "  It dry-runs (sample-first gate), logs real cost to knowledge/bq/bq_perf_log.jsonl," >&2
  echo "  and flags net-new tables for documentation. Example:" >&2
  echo "    .claude/scripts/bq_run.sh --ticket TI-X --label \"desc\" --phase sample \\" >&2
  echo "      'SELECT ... FROM \`ds.table\` WHERE <partition> BETWEEN ...'" >&2
  echo "  (Disable this guard: remove the PreToolUse hook in .claude/settings.json — see .claude/README.md.)" >&2
  exit 2
fi
exit 0
