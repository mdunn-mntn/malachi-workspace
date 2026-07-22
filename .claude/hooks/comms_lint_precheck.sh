#!/usr/bin/env bash
# PreToolUse(Bash) — when a Bash command is a Jira REST v2 write (comment or issue-create curl),
# lint the payload against the Terse Comms Standard BEFORE it posts. Advisory: prints to stderr,
# exit 0 (never blocks). To make it a hard gate, change the final `return 0` in lint_comms.py's
# run_hook() to `return 2`. This is the real enforcement point — the draft only exists here.
set -uo pipefail
ROOT="${CLAUDE_PROJECT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
exec python3 "$ROOT/.claude/scripts/lint_comms.py" --hook
