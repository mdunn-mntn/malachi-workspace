#!/usr/bin/env bash
# preflight.sh — probe the session's external dependencies, correctly.
#
# Exists because hand-rolled probes keep producing FALSE failures. On 2026-08-12 a
# `timeout 60 gcloud auth print-access-token && echo OK || echo FAILED` reported dead
# credentials and sent the user off to re-authenticate: macOS ships no `timeout`, so the
# shell exited 127 on the missing binary and the `||` branch fired. A wrapper failing is
# indistinguishable from the probe failing when you test with && / ||.
#
# Rules this script follows, and you should too:
#   * probe BARE — never wrap in a GNU-only binary (timeout, gtimeout, stdbuf, coreutils)
#   * when a real deadline is needed use with_deadline() below (perl alarm, always present)
#   * report the probe's own exit code, never the wrapper's
#
# Usage: bash .claude/scripts/preflight.sh [--quiet]
# Exit 0 = everything a session normally needs is reachable; 1 = at least one is not.
set -uo pipefail

QUIET=0
[[ "${1:-}" == "--quiet" ]] && QUIET=1
FAIL=0

say()  { [[ $QUIET -eq 1 ]] || printf '%s\n' "$*"; }
ok()   { say "  ok    $1"; }
bad()  { printf '  FAIL  %s\n' "$1"; [[ -n "${2:-}" ]] && printf '        fix: %s\n' "$2"; FAIL=1; }
warn() { printf '  warn  %s\n' "$1"; }

# Portable deadline: perl is on every macOS box, `timeout` is not.
with_deadline() { local s="$1"; shift; perl -e 'alarm shift; exec @ARGV' "$s" "$@"; }

say "preflight:"

if gcloud auth print-access-token >/dev/null 2>&1; then
  ok "gcloud auth ($(gcloud config get-value account 2>/dev/null))"
else
  bad "gcloud auth — token will not refresh" "gcloud auth login"
fi

if [[ -n "${JIRA_API_TOKEN:-}" ]]; then
  code=$(curl -s -o /dev/null -w '%{http_code}' -u "malachi@mountain.com:${JIRA_API_TOKEN}" \
         "https://mntn.atlassian.net/rest/api/2/myself" 2>/dev/null)
  [[ "$code" == "200" ]] && ok "jira api (http $code)" \
                         || bad "jira api returned http $code" "check JIRA_API_TOKEN in ~/.zshrc"
else
  bad "JIRA_API_TOKEN not set in this shell" "source ~/.zshrc"
fi

DRIVE="$HOME/Library/CloudStorage/GoogleDrive-malachi@mountain.com/My Drive"
[[ -d "$DRIVE/Tickets" ]] && ok "drive mount" \
                          || bad "drive not mounted at $DRIVE" "open Google Drive.app"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DIRTY=$(git -C "$ROOT" status --porcelain 2>/dev/null | wc -l | tr -d ' ')
if [[ "$DIRTY" == "0" ]]; then
  ok "git tree clean"
else
  warn "git tree has $DIRTY uncommitted path(s) — another session may be mid-edit."
  printf '        stage specific paths, never `git add .` (feedback_shared_worktree_commits)\n'
  git -C "$ROOT" status --porcelain | head -5 | sed 's/^/        /'
fi

[[ $FAIL -eq 0 ]] && say "preflight: all clear" || printf 'preflight: %s\n' "one or more checks failed"
exit $FAIL
