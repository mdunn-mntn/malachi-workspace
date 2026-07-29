#!/usr/bin/env bash
# install_git_hooks.sh — one-time: point git at the committed .githooks/ gate.
# Run once per clone. Idempotent. Undo with: git config --unset core.hooksPath
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
chmod +x "$ROOT/.githooks/"* 2>/dev/null || true
git -C "$ROOT" config core.hooksPath .githooks
echo "Installed: core.hooksPath -> .githooks"
echo "  pre-commit : .claude/scripts/verify.sh --staged  (file format + index freshness, staged-scoped)"
echo "  commit-msg : lint_comms.py --kind commit         (subject <=72, body <=500/6 bullets, no em-dash)"
echo "Fix failures with: .claude/scripts/verify.sh --fix   ·   emergency bypass: git commit --no-verify"
