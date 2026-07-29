#!/usr/bin/env bash
# package_kit.sh — extract, sanitize, and assemble a portable copy of the AI Workflow Kit.
# Copies the machinery, overlays generic seeds, swaps every private value for a <PLACEHOLDER>, regenerates
# indexes + COMPONENTS.md, self-verifies (sanitization sweep + verify.sh), and emits ai-workflow-kit/ + .tar.gz.
# Idempotent: same repo in -> same bundle out. Read-only against the source repo.
#   usage: bash .claude/scripts/package_kit.sh [OUTPUT_PARENT_DIR]   (default: $TMPDIR or /tmp)
set -euo pipefail
SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TPL="$SRC/documentation/ai_workflow_kit/templates"
MAP="$SRC/documentation/ai_workflow_kit/sanitize_map.txt"
DMAP="$SRC/documentation/ai_workflow_kit/domain_scrub_map.txt"
DEST_PARENT="${1:-${TMPDIR:-/tmp}}"; DEST_PARENT="${DEST_PARENT%/}"
B="$DEST_PARENT/ai-workflow-kit"
say() { printf '%s\n' "$*"; }

say "== package_kit =="; say "source: $SRC"; say "bundle: $B"
rm -rf "$B"; mkdir -p "$B"

# 1. Copy machinery (allowlist; exclude local/secret/content) ------------------
RS=(rsync -a --exclude='__pycache__' --exclude='*.pyc')
"${RS[@]}" --exclude='settings.local.json' --exclude='CLAUDE.md' --exclude='databricks_setup.md' \
           --exclude='scripts/databricks_smoke.py' --exclude='scripts/package_kit.sh' "$SRC/.claude/" "$B/.claude/"
"${RS[@]}" "$SRC/.githooks/" "$B/.githooks/"
# workflows: drop the two dense MNTN-example docs (plan + ingest war-stories); keep design + agent runbook
"${RS[@]}" --exclude='bq_velocity_provenance_plan.md' --exclude='INGEST_GUIDE.md' "$SRC/workflows/" "$B/workflows/"
"${RS[@]}" "$SRC/tickets/_template/" "$B/tickets/_template/"
mkdir -p "$B/lib"; cp "$SRC/lib/mntn_xlsx.py" "$B/lib/mntn_xlsx.py"   # xlsx_demo dropped (adtech sample data)
"${RS[@]}" --exclude='logs' "$SRC/slack_bot/" "$B/slack_bot/"
mkdir -p "$B/documentation/ai_workflow_kit"
cp "$SRC/documentation/ai_workflow_kit/README.md" "$SRC/documentation/ai_workflow_kit/INSTRUCTION_INVENTORY.md" "$B/documentation/ai_workflow_kit/"
cp "$SRC/.mcp.json" "$B/.mcp.json"
# knowledge/ structure + templates only (NO content docs)
mkdir -p "$B/knowledge/bq" "$B/knowledge/runbooks" "$B/knowledge/decisions" "$B/knowledge/memory" "$B/on-call"
cp "$SRC/knowledge/bq/_TABLE_TEMPLATE.md" "$B/knowledge/bq/_TABLE_TEMPLATE.md"
cp "$SRC/knowledge/runbooks/_TEMPLATE.md" "$B/knowledge/runbooks/_TEMPLATE.md"
cp "$SRC/knowledge/decisions/_TEMPLATE.md" "$B/knowledge/decisions/_TEMPLATE.md"

# 2. Overlay generic seeds (replace content-heavy files) -----------------------
cp "$TPL/CLAUDE.template.md"     "$B/.claude/CLAUDE.md"
cp "$TPL/MEMORY.seed.md"         "$B/knowledge/memory/MEMORY.md"
cp "$TPL/START_HERE.seed.md"     "$B/knowledge/START_HERE.md"
cp "$TPL/oncall_runbook.seed.md" "$B/on-call/oncall_runbook.md"
cp "$TPL/README.seed.md"         "$B/README.md"
cp "$TPL/PORTING.md"             "$B/PORTING.md"
cp "$TPL/bootstrap.sh"           "$B/bootstrap.sh"
cp "$TPL/memory_examples/"*.md   "$B/knowledge/memory/"
cp "$TPL/slack_recovery.seed.md" "$B/slack_bot/RECOVERY.md"   # replace the MNTN decommission note (names Compass + channels)
# global/ layer — the sanitized personal ~/.claude/ framework (bootstrap installs with --with-global)
mkdir -p "$B/global"
cp "$TPL/global/CLAUDE.md" "$TPL/global/settings.json" "$TPL/global/mcp_servers.json" "$TPL/global/README.md" "$B/global/"

# 3. Seed empties + a fresh .gitignore ----------------------------------------
: > "$B/on-call/incident_log.jsonl"
: > "$B/knowledge/bq/bq_perf_log.jsonl"
: > "$B/knowledge/bq/_UNDOCUMENTED.queue"
cat > "$B/.gitignore" <<'GI'
# local-only / generated — never committed
.claude/settings.local.json
.claude/.session_state
.claude/projects/
**/__pycache__/
*.pyc
self_review/
# data + media artifacts
*.csv
*.parquet
*.xlsx
# keyword-only prompt telemetry (local)
knowledge/.request_log.jsonl
# generated indexes are committed; keep the recall map tracked
!knowledge/_MEMORY_RECALL.tsv
GI

# 4. Rename xlsx code identifiers (before sanitize, so the org-word rule can't corrupt them) ---
mv "$B/lib/mntn_xlsx.py" "$B/lib/xlsx_builder.py"

# 5. All text transforms in one deterministic python pass ----------------------
python3 - "$B" "$MAP" "$DMAP" <<'PY'
import os, sys
B, MAP, DMAP = sys.argv[1], sys.argv[2], sys.argv[3]

# ordered literal find->replace from a map file
def load(path):
    out = []
    for ln in open(path, encoding="utf-8"):
        if ln.startswith("#") or "\t" not in ln:
            continue
        f, r = ln.rstrip("\n").split("\t", 1)
        if f:
            out.append((f, r))
    return out
pairs = load(MAP)     # sanitize: strip literal secrets
dpairs = load(DMAP)   # domain scrub: strip job/domain context

# targeted code-identifier renames (only the two xlsx files); longest-first so prefixes don't collide
IDENT = [("mntn_xlsx_demo", "xlsx_demo"), ("mntn_xlsx", "xlsx_builder"),
         ("MntnWorkbook", "BrandWorkbook"), ("mntn_logo", "logo")]
XLSX = {os.path.join(B, "lib", "xlsx_builder.py")}

# repoint the ONE content-coupled self-test prompt at a generic seed memory's keywords
SELFTEST = os.path.join(B, ".claude", "scripts", "hooks_selftest.sh")
SELFTEST_OLD = "who owns MNTN frequency capping and what is the counter key"
SELFTEST_NEW = "show the example feedback front-matter contract template"

# the two WORKSPACE= assignments become self-resolving (not a placeholder)
WS_FILES = {os.path.join(B, ".claude", "scripts", "bq_run.sh"),
            os.path.join(B, ".claude", "scripts", "transcribe.sh")}
WS_OLD = 'WORKSPACE="<WORKSPACE_PATH>"'
WS_NEW = 'WORKSPACE="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"'

changed = 0
for dp, _, fns in os.walk(B):
    for fn in fns:
        p = os.path.join(dp, fn)
        try:
            s0 = open(p, encoding="utf-8").read()
        except (UnicodeDecodeError, IsADirectoryError):
            continue  # skip binaries
        s = s0
        if p in XLSX:
            for f, r in IDENT:
                s = s.replace(f, r)
        if p == SELFTEST:
            s = s.replace(SELFTEST_OLD, SELFTEST_NEW)
        for f, r in pairs:                 # sanitize map (strip secrets)
            s = s.replace(f, r)
        for f, r in dpairs:                # domain scrub (strip job context)
            s = s.replace(f, r)
        if p in WS_FILES:                  # self-resolving workspace root
            s = s.replace(WS_OLD, WS_NEW)
        if s != s0:
            open(p, "w", encoding="utf-8").write(s)
            changed += 1
print(f"  transformed {changed} file(s)")
PY

# 6. Permissions + regenerate indexes/manifest in-bundle -----------------------
chmod +x "$B/.claude/hooks/"* "$B/.claude/scripts/"*.sh "$B/.githooks/"* "$B/bootstrap.sh" 2>/dev/null || true
bash "$B/.claude/scripts/build_index.sh"
bash "$B/.claude/scripts/build_kit_manifest.sh"

# 7. Self-verify: sanitization sweep (ZERO private tokens) ----------------------
say "== sanitization sweep =="
LEAK=0
if grep -rInE -e 'malachi' -e 'mdunn' -e 'dunn' -e '192\.168\.' -e 'mountain\.com' -e 'dw-main' -e 'pi5@' -e 'audience intelligence' --ignore-case "$B" ; then LEAK=1; fi
if grep -rIni 'mntn' "$B" ; then LEAK=1; fi
if [ "$LEAK" -ne 0 ]; then say "FAIL: private tokens leaked (above). Fix sanitize_map.txt and re-run."; exit 1; fi
say "  clean: no private tokens found"

# 7b. Self-verify: domain-blind sweep (ZERO job/domain context) -----------------
say "== domain-blind sweep =="
if grep -rInEi -e 'spend_log' -e 'logdata' -e 'summarydata' -e 'integrationprod' -e 'advertiser' \
   -e 'fangorn' -e 'ipdsc' -e 'bombora' -e 'shopper_graph' -e '[^a-z]compass[^a-z]' -e 'incrementality' \
   -e 'conquest' -e '[^a-z]WGU[^a-z]' -e 'win_logs' -e 'bid_logs' -e '[^a-z]bidder' -e 'INC-00[1-9]' \
   -e '[^a-z]BUK[^a-z]' -e '[^a-z]ROAS[^a-z]' -e '[^a-z]CPV[^a-z]' -e 'audi_[0-9]' -e 'ber_[0-9]' \
   -e 'ti_[0-9][0-9][0-9]' "$B" ; then
  say "FAIL: job/domain context leaked (above). Extend domain_scrub_map.txt and re-run."; exit 1; fi
say "  clean: no job/domain context found"

# 8. Self-verify: the deterministic doctor inside the bundle -------------------
# verify.sh's index-freshness uses `git diff`, so give the bundle a throwaway git baseline.
say "== verify.sh (in-bundle) =="
git -C "$B" init -q && git -C "$B" add -A
bash "$B/.claude/scripts/verify.sh" || { say "FAIL: bundle did not verify clean."; exit 1; }
rm -rf "$B/.git"   # ship pristine; bootstrap.sh re-inits on the target machine

# 9. Emit tarball --------------------------------------------------------------
tar czf "$DEST_PARENT/ai-workflow-kit.tar.gz" -C "$DEST_PARENT" ai-workflow-kit
say ""
say "== done =="
say "bundle : $B"
say "tarball: $DEST_PARENT/ai-workflow-kit.tar.gz"
say "next   : cd into a copy and run  bash bootstrap.sh"
