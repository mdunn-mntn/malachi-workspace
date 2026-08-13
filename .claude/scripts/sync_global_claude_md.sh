#!/usr/bin/env bash
# sync_global_claude_md.sh — version-control the ONE instruction file that lives outside the repo.
#
# ~/.claude/CLAUDE.md is the always-loaded global operating layer, and it is the only part of the
# instruction stack with no git history: an edit to it is invisible to review, unrecoverable after a
# bad overwrite, and lost with the machine. workflow_audit.sh already READS it (§8 standards drift)
# but nothing snapshots it. This keeps a committed copy in the repo.
#
# The snapshot is a BACKUP, not the live file. ~/.claude/CLAUDE.md stays the source of truth (other
# projects load it too, and a symlink into a shared git worktree would take the user's global rules
# down with any checkout, stash, or unmounted drive).
#
# Modes:
#   --pull   (default) copy ~/.claude/CLAUDE.md → the snapshot. Prints a diffstat if it changed.
#            Does NOT commit — stage it with the rest of your work.
#   --check  exit 1 if the snapshot is stale (drifted or missing). Quiet when in sync.
#   --restore  copy the snapshot BACK over ~/.claude/CLAUDE.md. Disaster recovery; prompts first.
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LIVE="$HOME/.claude/CLAUDE.md"
SNAP="$ROOT/.claude/global_claude_md_snapshot.md"
MODE="${1:---pull}"

if [ ! -f "$LIVE" ]; then
  echo "sync_global_claude_md: no $LIVE — nothing to sync." >&2
  exit 0
fi

case "$MODE" in
  --check)
    if [ ! -f "$SNAP" ]; then
      echo "global CLAUDE.md has never been snapshotted — run .claude/scripts/sync_global_claude_md.sh"
      exit 1
    fi
    if ! cmp -s "$LIVE" "$SNAP"; then
      echo "global CLAUDE.md drifted from its snapshot ($(diff <(cat "$SNAP") <(cat "$LIVE") | grep -c '^[<>]') line(s)) — run .claude/scripts/sync_global_claude_md.sh"
      exit 1
    fi
    exit 0
    ;;
  --restore)
    if [ ! -f "$SNAP" ]; then
      echo "sync_global_claude_md: no snapshot at $SNAP to restore from." >&2
      exit 1
    fi
    if cmp -s "$LIVE" "$SNAP"; then
      echo "sync_global_claude_md: already identical, nothing to restore."
      exit 0
    fi
    echo "This OVERWRITES your live $LIVE with the committed snapshot."
    diff "$LIVE" "$SNAP" | head -40
    printf 'Restore? [y/N] '
    read -r reply
    case "$reply" in
      [yY]*) cp "$SNAP" "$LIVE"; echo "restored $LIVE from snapshot." ;;
      *) echo "aborted." ;;
    esac
    ;;
  --pull|"")
    if [ -f "$SNAP" ] && cmp -s "$LIVE" "$SNAP"; then
      echo "sync_global_claude_md: in sync ($(wc -c <"$LIVE" | tr -d ' ') bytes)."
      exit 0
    fi
    if [ -f "$SNAP" ]; then
      echo "sync_global_claude_md: global CLAUDE.md changed since the last snapshot:"
      diff "$SNAP" "$LIVE" | sed 's/^/  /' | head -40
    else
      echo "sync_global_claude_md: first snapshot of $LIVE."
    fi
    cp "$LIVE" "$SNAP"
    echo "wrote $SNAP — stage and commit it with your other paths."
    ;;
  *)
    echo "usage: sync_global_claude_md.sh [--pull|--check|--restore]" >&2
    exit 2
    ;;
esac
