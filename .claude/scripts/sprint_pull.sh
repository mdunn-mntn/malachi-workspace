#!/usr/bin/env bash
# sprint_pull.sh — list MY not-Done issues in a sprint, matched to their local ticket folder.
#
# Usage:
#   sprint_pull.sh                 # active sprint on the AUDI board, my open issues, TSV
#   sprint_pull.sh --next          # the next future sprint instead
#   sprint_pull.sh --sprint 8649   # a specific sprint id
#   sprint_pull.sh --all           # include Done issues too
#   sprint_pull.sh --json          # one JSON object per line
#
# Columns (TSV): key  type  status  points  folder  title      folder = "-" when none exists yet.
set -euo pipefail

BOARD=1814
JIRA=https://mntn.atlassian.net
EMAIL=malachi@mountain.com
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

SPRINT=""; WHICH=active; FMT=tsv; DONE_FILTER='AND statusCategory != Done'
while [[ $# -gt 0 ]]; do
  case "$1" in
    --sprint) SPRINT="$2"; shift 2;;
    --next)   WHICH=future; shift;;
    --all)    DONE_FILTER=""; shift;;
    --json)   FMT=json; shift;;
    -h|--help) sed -n '2,12p' "$0"; exit 0;;
    *) echo "sprint_pull: unknown arg $1" >&2; exit 1;;
  esac
done

[[ -n "${JIRA_API_TOKEN:-}" ]] || { echo "sprint_pull: JIRA_API_TOKEN not set (source ~/.zshrc)" >&2; exit 1; }
AUTH="$EMAIL:$JIRA_API_TOKEN"

if [[ -z "$SPRINT" ]]; then
  SPRINT=$(curl -s -u "$AUTH" "$JIRA/rest/agile/1.0/board/$BOARD/sprint?state=$WHICH" \
    | jq -r --arg w "$WHICH" '[.values[] | select(.state==$w)] | sort_by(.startDate) | .[0].id')
fi
[[ "$SPRINT" =~ ^[0-9]+$ ]] || { echo "sprint_pull: could not resolve a $WHICH sprint on board $BOARD" >&2; exit 1; }

NAME=$(curl -s -u "$AUTH" "$JIRA/rest/agile/1.0/sprint/$SPRINT" | jq -r '.name + " (" + .state + ", ends " + (.endDate // "?")[0:10] + ")"')
echo "# sprint $SPRINT — $NAME" >&2

ISSUES=$(curl -s -u "$AUTH" -X POST -H "Content-Type: application/json" \
  "$JIRA/rest/api/3/search/jql" \
  -d "{\"jql\":\"sprint = $SPRINT AND assignee = currentUser() $DONE_FILTER ORDER BY status ASC, key ASC\",\"maxResults\":100,\"fields\":[\"summary\",\"status\",\"issuetype\",\"customfield_10012\",\"parent\",\"labels\"]}")

echo "$ISSUES" | jq -c '.issues[] | {key,type:.fields.issuetype.name,status:.fields.status.name,points:.fields.customfield_10012,parent:.fields.parent.key,title:.fields.summary}' \
| while read -r row; do
    key=$(jq -r .key <<<"$row"); num=${key##*-}
    folder=$(find "$ROOT/tickets" -maxdepth 2 -type d -name "*_${num}_*" -print -quit 2>/dev/null || true)
    folder=${folder:+tickets/${folder#"$ROOT/tickets/"}}
    row=$(jq -c --arg f "${folder:--}" '. + {folder:$f}' <<<"$row")
    if [[ "$FMT" == json ]]; then echo "$row"
    else jq -r '[.key,.type,.status,(.points|tostring),.folder,.title] | @tsv' <<<"$row"; fi
  done
