#!/usr/bin/env bash
# PreToolUse(Bash + mcp__github__create_pull_request) — advisory backstop for the PR gauntlet:
# when a PR is about to be created and HEAD has no gauntlet pass marker, remind to run
# /pr_gauntlet first. Prints to stderr; exit 0 never blocks. Hooks are shell and cannot invoke
# a skill — firing /pr_gauntlet is on the session; this is only the backstop (capture_reminder
# pattern). Marker: .git/pr_gauntlet_pass holding the sha the gauntlet passed on.
set -uo pipefail

HOOK_INPUT="$(cat 2>/dev/null || true)"
export HOOK_INPUT
parsed="$(python3 -c '
import json, os, re, sys
try:
    d = json.loads(os.environ.get("HOOK_INPUT", ""))
except Exception:
    sys.exit(0)
tool = d.get("tool_name", "")
cmd = (d.get("tool_input") or {}).get("command", "")
if tool != "mcp__github__create_pull_request" and not re.search(r"\bgh\s+pr\s+create\b", cmd):
    sys.exit(0)
m = re.search(r"(?:^|&&|;)\s*cd\s+([^\s;&|]+)", cmd) or re.search(r"git\s+-C\s+([^\s;&|]+)", cmd)
print("FIRE:" + (m.group(1) if m else ""))
' 2>/dev/null || true)"

[[ "$parsed" != FIRE:* ]] && exit 0
repo="${parsed#FIRE:}"
repo="${repo:-${CLAUDE_PROJECT_DIR:-$PWD}}"
repo="${repo/#\~/$HOME}"

gitdir="$(git -C "$repo" rev-parse --git-dir 2>/dev/null || true)"
[[ -z "$gitdir" ]] && exit 0
[[ "$gitdir" != /* ]] && gitdir="$repo/$gitdir"

head="$(git -C "$repo" rev-parse HEAD 2>/dev/null || true)"
marker="$gitdir/pr_gauntlet_pass"
if [[ -n "$head" && -f "$marker" ]] && grep -q "$head" "$marker" 2>/dev/null; then
  exit 0
fi
echo "[pr-gauntlet] this HEAD has not passed the gauntlet — run /pr_gauntlet before creating the PR." >&2
exit 0
