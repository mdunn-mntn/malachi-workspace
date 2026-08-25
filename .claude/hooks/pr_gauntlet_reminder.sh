#!/usr/bin/env bash
# PreToolUse(Bash + mcp__github__create_pull_request) — the PR gauntlet GATE: BLOCKS (exit 2)
# any PR creation whose HEAD has no gauntlet pass marker, so the session must run /pr_gauntlet
# and retry. Hooks are shell and cannot invoke a skill — the block forces the auto-fire instead.
# Marker: .git/pr_gauntlet_pass holding the sha the gauntlet passed on (the skill writes it).
# Emergency bypass: put PR_GAUNTLET_SKIP=1 in the command. Any parse failure exits 0.
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
if tool != "mcp__github__create_pull_request" and not re.search(
    r"(?:^|[;&|]|\$\()\s*gh\s+pr\s+create\b", cmd, re.M
):
    sys.exit(0)
if "PR_GAUNTLET_SKIP=1" in cmd:
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
echo "[pr-gauntlet] BLOCKED: this HEAD has not passed the gauntlet. Run /pr_gauntlet on this branch now (it writes .git/pr_gauntlet_pass on PASS), then retry the PR creation. Human-authorized emergency bypass: PR_GAUNTLET_SKIP=1 in the command." >&2
exit 2
