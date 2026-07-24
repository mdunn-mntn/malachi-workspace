#!/usr/bin/env bash
# new_ticket.sh — scaffold a ticket folder that conforms to folder_definitions.md, one command.
#
# Creates tickets/<folder>/ (or tickets/<parent>/<folder>/ for an epic child) with the required
# subdirs (queries/ outputs/ meetings/ artifacts/) and a summary.md whose front-matter is PREFILLED
# so it passes lint_tickets immediately (status defaults to backlog, result "not started").
#
# Usage:
#   new_ticket.sh <folder_name> [--title "TI-XXX: Title"] [--summary "one line"] \
#                 [--status backlog|in_progress|blocked|done] [--parent <epic_folder>] \
#                 [--epic] [--jira <url>] [--no-index]
#
#   folder_name : lowercase + underscores only, e.g. ti_650_stage_3_vv_audit (validated)
#   --parent    : nest inside an existing epic folder (tickets/<parent>/<folder>/)
#   --epic      : this folder is itself an epic (doc_type: epic)
#   --no-index  : skip the build_index.sh refresh at the end
#
# Examples:
#   new_ticket.sh ti_777_new_investigation --summary "Investigate the X regression"
#   new_ticket.sh ti_842_present_results --parent ber_2250_incrementality_overhaul --status backlog
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TDIR="$ROOT/tickets"

die() { echo "new_ticket: $*" >&2; exit 1; }

[[ $# -ge 1 ]] || die "usage: new_ticket.sh <folder_name> [--title ..] [--summary ..] [--status ..] [--parent <epic>] [--epic] [--jira <url>] [--no-index]"
FOLDER="$1"; shift

TITLE=""; SUMMARY=""; STATUS="backlog"; PARENT=""; DOCTYPE="ticket"; JIRA=""; RUN_INDEX=1
while [[ $# -gt 0 ]]; do
  case "$1" in
    --title)   TITLE="$2"; shift 2;;
    --summary) SUMMARY="$2"; shift 2;;
    --status)  STATUS="$2"; shift 2;;
    --parent)  PARENT="$2"; shift 2;;
    --epic)    DOCTYPE="epic"; shift;;
    --jira)    JIRA="$2"; shift 2;;
    --no-index) RUN_INDEX=0; shift;;
    *) die "unknown arg: $1";;
  esac
done

# --- validate ---
[[ "$FOLDER" =~ ^[a-z][a-z0-9_]*$ ]] || die "folder '$FOLDER' must be lowercase + underscores only (no dashes/spaces/uppercase) — e.g. ti_650_stage_3_vv_audit"
case "$STATUS" in backlog|in_progress|blocked|done) ;; *) die "status must be backlog|in_progress|blocked|done";; esac

DEST="$TDIR/$FOLDER"
if [[ -n "$PARENT" ]]; then
  [[ -d "$TDIR/$PARENT" ]] || die "parent epic '$PARENT' does not exist under tickets/"
  DEST="$TDIR/$PARENT/$FOLDER"
fi
[[ -e "$DEST" ]] && die "$DEST already exists — refusing to overwrite"

# --- derive Jira ID + defaults from the folder name (prefix_number_description) ---
PREFIX="$(printf '%s' "$FOLDER" | cut -d_ -f1)"
SECOND="$(printf '%s' "$FOLDER" | cut -d_ -f2)"
if [[ "$SECOND" =~ ^[0-9]+$ ]]; then
  TICKET_ID="$(printf '%s' "$PREFIX" | tr '[:lower:]' '[:upper:]')-$SECOND"
  DESC="$(printf '%s' "$FOLDER" | cut -d_ -f3-)"
else
  TICKET_ID="$(printf '%s' "$PREFIX" | tr '[:lower:]' '[:upper:]')"
  DESC="$(printf '%s' "$FOLDER" | cut -d_ -f2-)"
fi
DESC_SPACED="$(printf '%s' "$DESC" | tr '_' ' ')"
[[ -n "$TITLE" ]]   || TITLE="$TICKET_ID: $DESC_SPACED"
[[ -n "$SUMMARY" ]] || SUMMARY="$DESC_SPACED"
[[ -n "$JIRA" ]]    || JIRA="https://mntn.atlassian.net/browse/$TICKET_ID"
TODAY="$(date +%F)"
# result must be non-empty to pass lint_tickets even for a backlog ticket
RESULT="not started"; [[ "$STATUS" == "done" ]] && RESULT="{fill: the blessed one-line finding}"

# --- scaffold ---
mkdir -p "$DEST"/{queries,outputs,meetings,artifacts}

# front-matter (prefilled) + the template body (minus its own front-matter), with light substitutions
{
  cat <<EOF
---
doc_type: $DOCTYPE
title: "$TITLE"
status: $STATUS
date: $TODAY
summary: "$SUMMARY"
result: "$RESULT"
question: ""
framing_state: draft
---

# $TITLE

**Jira:** $JIRA
**Status:** $STATUS
**Date Started:** $TODAY
**Assignee:** Malachi

---
EOF
  # append the template body from after its own front-matter (2nd '---'), skipping its H1/header block
  awk 'p; /^---$/{c++; if(c==2){p=1}}' "$TDIR/_template/summary_template.md" \
    | awk 'started||/^## /{started=1; print}'
} > "$DEST/summary.md"

echo "✅ created $DEST/"
echo "   summary.md (doc_type:$DOCTYPE status:$STATUS) + queries/ outputs/ meetings/ artifacts/"

# --- verify it passes the linter ---
if python3 "$ROOT/.claude/scripts/lint_tickets.py" --check >/tmp/nt_lint 2>&1; then
  echo "   lint_tickets: ✓ ($(grep -o '[0-9]* cards' /tmp/nt_lint | head -1))"
else
  echo "   ⚠ lint_tickets flagged something — check: python3 .claude/scripts/lint_tickets.py --check"
fi

# --- refresh tickets/INDEX.md so the new ticket appears ---
if [[ "$RUN_INDEX" == "1" ]]; then
  bash "$ROOT/.claude/scripts/build_index.sh" >/dev/null 2>&1 && echo "   tickets/INDEX.md refreshed"
fi

echo
echo "Next: run  /frame $TICKET_ID  to lock §0 Framing (Question/Goal/Objective/Approach) BEFORE work starts."
echo "      status:in_progress is gated until framing_state is locked (or 'skip: <reason>' for a trivial ticket)."
echo "      Then fill §1–3 of summary.md and commit:"
echo "  git add tickets/${PARENT:+$PARENT/}$FOLDER tickets/INDEX.md && git commit -m \"$TICKET_ID: scaffold ticket\""
