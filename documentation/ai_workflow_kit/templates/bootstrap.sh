#!/usr/bin/env bash
# bootstrap.sh — stand up the AI Workflow Kit on a fresh machine. Run once from the bundle root.
#   preflight deps → chmod → git init + install commit gate → rebuild the memory reverse-symlink for
#   THIS checkout path → build indexes → verify. Idempotent; safe to re-run.
set -uo pipefail
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(pwd)"
WITH_GLOBAL=0; for a in "$@"; do [ "$a" = "--with-global" ] && WITH_GLOBAL=1; done
say() { printf '%s\n' "$*"; }
hr()  { printf -- '----------------------------------------------------------------\n'; }

hr; say "AI Workflow Kit — bootstrap"; say "repo: $ROOT"; hr

# 1. Toolchain preflight -------------------------------------------------------
miss_hard=0
need() { command -v "$1" >/dev/null 2>&1 && say "  ok   $1" || { say "  MISS $1  ($2)"; return 1; }; }
say "[core — required]"
need git    "version control + commit gate"      || miss_hard=1
need python3 "every linter, hook, index builder" || miss_hard=1
say "[core — recommended]"
need jq   "bq wrapper / introspection"     || true
need gh   "github MCP + share_deck.sh"     || true
need node "MCP servers"                    || true
need npx  "MCP servers (jira/bigquery)"    || true
say "[warehouse module — optional]"
need bq     "BigQuery wrapper"               || true
need gcloud "BigQuery auth (gcloud auth login + application-default login)" || true
say "[transcription — optional]"
need ffmpeg "audio chunking" || true
say "[python libs — optional, only if you use those subsystems]"
python3 - <<'PY' || true
import importlib.util
for mod, why in [("openpyxl",".xlsx builder"),("pandas",".xlsx builder"),("numpy",".xlsx builder"),
                 ("PyYAML","config files")]:
    ok = importlib.util.find_spec(mod) is not None
    print(f"  {'ok  ' if ok else 'MISS'} {mod}  ({why})")
PY
if [ "$miss_hard" = 1 ]; then hr; say "FATAL: install the required core tools above, then re-run."; exit 1; fi

# 2. Permissions ---------------------------------------------------------------
hr; say "[chmod] making hooks + scripts executable"
chmod +x .claude/hooks/* .claude/scripts/*.sh .githooks/* bootstrap.sh 2>/dev/null || true

# 3. Git + commit gate ---------------------------------------------------------
hr
if [ ! -d .git ]; then say "[git] initializing repo"; git init -q; fi
say "[git] installing commit gate"
bash .claude/scripts/install_git_hooks.sh || say "  (install_git_hooks returned non-zero — continuing)"

# 4. Memory reverse-symlink for THIS path -------------------------------------
# Claude Code's native memory dir for a project is ~/.claude/projects/<slug>/memory, where <slug> is the
# absolute checkout path with every '/' turned into '-'. Point it INTO the repo so native memory writes
# land in git. The slug is path-derived, so it must be recreated per machine — it cannot be copied.
hr
SLUG="$(printf '%s' "$ROOT" | sed 's#/#-#g')"
NATIVE="$HOME/.claude/projects/$SLUG/memory"
TARGET="$ROOT/knowledge/memory"
say "[memory] native path: $NATIVE"
mkdir -p "$(dirname "$NATIVE")"
if [ -L "$NATIVE" ] && [ "$(readlink "$NATIVE")" = "$TARGET" ]; then
  say "  already linked -> $TARGET"
elif [ -e "$NATIVE" ] || [ -L "$NATIVE" ]; then
  mv "$NATIVE" "$NATIVE.backup-presymlink" && say "  moved existing aside -> $NATIVE.backup-presymlink"
  ln -s "$TARGET" "$NATIVE" && say "  linked -> $TARGET"
else
  ln -s "$TARGET" "$NATIVE" && say "  linked -> $TARGET"
fi
say "  revert: rm \"$NATIVE\"  (then restore .backup-presymlink if present)"

# 4b. Cross-harness wiring: one rules file, one skills dir ---------------------
# AGENTS.md is the cross-vendor standard (Codex, Cursor, Copilot, Windsurf, Cline read it natively).
# Symlink rather than copy so the rules can never drift between tools. See BLUEPRINT.md §6.
hr; say "[harness] wiring the portable instruction + skill paths"
if [ -e CLAUDE.md ] || [ -L CLAUDE.md ]; then
  say "  CLAUDE.md exists — left alone (make it a symlink to AGENTS.md to keep one copy)"
else
  ln -s AGENTS.md CLAUDE.md && say "  CLAUDE.md -> AGENTS.md"
fi
mkdir -p .agents
if [ -e .agents/skills ] || [ -L .agents/skills ]; then
  say "  .agents/skills exists — left alone"
else
  ln -s ../.claude/skills .agents/skills && say "  .agents/skills -> ../.claude/skills  (Codex + Cursor read this path)"
fi

# 5. Generate indexes ----------------------------------------------------------
hr; say "[index] building indexes + component manifest"
bash .claude/scripts/build_index.sh || say "  (build_index returned non-zero)"
bash .claude/scripts/build_kit_manifest.sh || say "  (build_kit_manifest returned non-zero)"
# explicit paths, never `git add -A` — the rule this kit ships in AGENTS.md 3 applies to itself.
git add .claude .githooks .agents knowledge tickets workflows lib on-call documentation global \
        AGENTS.md CLAUDE.md README.md PORTING.md bootstrap.sh .gitignore .mcp.json pyproject.toml \
        2>/dev/null || true   # baseline for verify's index-freshness (git diff) comparison

# 6. Verify --------------------------------------------------------------------
hr; say "[verify] running the deterministic doctor"
if bash .claude/scripts/verify.sh; then say "  verify: PASS"; else say "  verify: FAIL — see output above"; fi

# 6b. Global personal ~/.claude framework (opt-in) -----------------------------
hr
if [ "$WITH_GLOBAL" = 1 ] && [ -d global ]; then
  say "[global] installing personal ~/.claude framework"
  mkdir -p "$HOME/.claude"
  if [ -f "$HOME/.claude/CLAUDE.md" ]; then
    cp "$HOME/.claude/CLAUDE.md" "$HOME/.claude/CLAUDE.md.backup-preport"
    say "  backed up existing ~/.claude/CLAUDE.md -> CLAUDE.md.backup-preport"
  fi
  cp global/CLAUDE.md "$HOME/.claude/CLAUDE.md"; say "  installed ~/.claude/CLAUDE.md"
  if [ -f "$HOME/.claude/settings.json" ]; then
    cp global/settings.json "$HOME/.claude/settings.json.from-kit"
    say "  kept your ~/.claude/settings.json; wrote settings.json.from-kit to merge by hand"
  else
    cp global/settings.json "$HOME/.claude/settings.json"; say "  installed ~/.claude/settings.json"
  fi
  say "  MCP: merge global/mcp_servers.json into ~/.claude.json by hand + fill the token (never auto-written)"
  say "  revert: restore ~/.claude/CLAUDE.md.backup-preport"
else
  say "[global] skipped — run 'bash bootstrap.sh --with-global' to also install your ~/.claude/ framework"
fi

# 7. Next steps ----------------------------------------------------------------
hr; say "Placeholders still to fill (edit these, then re-run verify):"
grep -rIl -- '<[A-Z_]*>' .claude .mcp.json 2>/dev/null | sed 's/^/  /' | sort -u | head -40
say ""
say "Then: gcloud auth login (+ application-default login) · gh auth login · set JIRA_API_TOKEN ·"
say "      authorize claude.ai connectors · fill .claude/scripts/config.env · see PORTING.md."
hr; say "bootstrap complete."
